# Databricks notebook source
# Live validation for brief 02 (reliable Spark Connect capture).
#
# Runs bounded serverless workloads only: small `spark.range` frames and a `noop`
# sink write. No table is created, read, or modified; nothing is persisted.
# The final cell prints a sanitized PlanMetrics fixture for offline replay.

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from databricks import dbutils, display, displayHTML, spark  # noqa: F401

# COMMAND ----------

import json
import re

from pyspark.sql import functions as F

from sparkparse.capture import SparkparseCapture
from sparkparse.connect import SparkConnectCapture, probe_connect_support

FAILURES: list[str] = []


def check(name: str, condition: bool, detail: str = "") -> None:
    status = "PASS" if condition else "FAIL"
    print(f"[{status}] {name}{f' — {detail}' if detail else ''}")
    if not condition:
        FAILURES.append(name)


def details(dfs, node_id: int, query_id: int) -> dict:
    row = dfs.dag.filter(
        (dfs.dag["node_id"] == node_id) & (dfs.dag["query_id"] == query_id)
    ).to_dicts()[0]
    return json.loads(row["details"])["detail"]


# COMMAND ----------

# Capability probe: records the client surface this validation actually ran against.
support = probe_connect_support(spark)
print(json.dumps(support.as_dict(), indent=2))
check("client exposes _build_metrics", support.metrics_hook)
check(
    "no action method is missing",
    support.missing_actions == (),
    str(support.missing_actions),
)
check("runtime version", spark.version is not None, spark.version)

# COMMAND ----------

# Distinct action kinds, each expected to become its own query.
with SparkparseCapture(
    "get", spark=spark, backend="connect", log_name="brief02"
) as cap:
    left = spark.range(2_000).withColumnRenamed("id", "key")
    right = spark.range(500).withColumnRenamed("id", "key")

    left.join(right, on="key").count()  # to_table
    left.join(right, on="key").limit(10).toPandas()  # to_pandas
    list(left.limit(50).toLocalIterator())  # to_table_as_iterator
    spark.sql("SELECT 1 AS one").collect()  # to_table via SQL
    left.write.format("noop").mode("overwrite").save()  # execute_command

inner = cap._connect_cap
result = cap.result
dfs = cap.dfs
assert result is not None and dfs is not None

print(json.dumps(inner.executions, indent=2, default=str))
print(json.dumps([d.model_dump(mode="json") for d in result.diagnostics], indent=2))

actions = {execution["action"] for execution in inner.executions}
check("to_table intercepted", "to_table" in actions, str(sorted(actions)))
check("to_pandas intercepted", "to_pandas" in actions)
check("iterator action intercepted", "to_table_as_iterator" in actions)
check("write routed through execute_command", "execute_command" in actions)
check(
    "each action is its own query",
    len({e["query_id"] for e in inner.executions}) == len(inner.executions),
)
check(
    "operation ids recorded",
    all(e["operation_id"] for e in inner.executions),
    str([e["operation_id"] for e in inner.executions][:3]),
)
check("no unattributed metrics", all(e["attributed"] for e in inner.executions))

# COMMAND ----------

# Timing semantics: client elapsed vs cumulative operator time.
durations = dfs.dag["query_duration_seconds"].drop_nulls().unique().to_list()
print(f"client elapsed per query: {durations}")
check("client elapsed recorded", bool(durations) and all(d > 0 for d in durations))
check(
    "elapsed coverage is partial, not claimed as server time",
    result.capabilities.query_elapsed_time.status.value == "partial",
    result.capabilities.query_elapsed_time.reason or "",
)
check(
    "task metrics stay unavailable",
    result.capabilities.task_metrics.status.value == "unavailable",
)

cumulative = [
    detail["cumulative_operator_time"]
    for detail in (json.loads(row["details"])["detail"] for row in dfs.dag.to_dicts())
    if "cumulative_operator_time" in detail
]
print(f"nodes carrying cumulTime: {len(cumulative)}; sample: {cumulative[:1]}")
check("cumulative operator time retained with source unit", bool(cumulative))

# COMMAND ----------

# Operator mapping: unknown Photon operators must degrade, not abort.
node_types = dfs.dag.group_by("node_type").len().sort("len", descending=True)
display(node_types)
unknown = dfs.dag.filter(dfs.dag["node_type"] == "Unknown")
print(f"unknown operators: {unknown.height}")
for row in unknown.to_dicts():
    print(f"  {json.loads(row['details'])['detail']['raw_name']!r}")
check("capture completed despite unknown operators", dfs.dag.height > 0)

# COMMAND ----------

# Adversarial join case: two unrelated joins with different keys in one capture.
with SparkparseCapture(
    "get", spark=spark, backend="connect", log_name="joins"
) as joins:
    customers = spark.range(500).withColumnRenamed("id", "customer_id")
    orders = spark.range(2_000).withColumn("customer_id", F.col("id") % 500)
    products = spark.range(300).withColumnRenamed("id", "product_id")
    line_items = spark.range(2_000).withColumn("product_id", F.col("id") % 300)

    orders.join(customers, on="customer_id").count()
    line_items.join(products, on="product_id").count()

jdfs = joins.dfs
assert jdfs is not None
join_rows = [
    (row["query_id"], row["node_id"], json.loads(row["details"])["detail"])
    for row in jdfs.dag.to_dicts()
    if "Join" in row["node_type"]
]
for query_id, node_id, detail in join_rows:
    print(
        f"  query={query_id} node={node_id} source={detail.get('join_details_source')} "
        f"type={detail.get('join_type')} left={detail.get('left_keys')}"
    )

by_query: dict[int, set[str]] = {}
for query_id, _, detail in join_rows:
    by_query.setdefault(query_id, set()).update(detail.get("left_keys") or [])
print(f"keys per query: {by_query}")
check(
    "no cross-query key leakage",
    all(
        keys <= {"customer_id"} or keys <= {"product_id"} for keys in by_query.values()
    ),
    str(by_query),
)
check(
    "join nodes never assert input order",
    all(detail.get("input_roles") == "unordered" for _, _, detail in join_rows),
)
print(
    "join resolution sources: "
    + str({detail.get("join_details_source") for _, _, detail in join_rows})
)

# COMMAND ----------

# Hook lifecycle against the live client.
client = spark._client
original_metrics = client._build_metrics
original_to_table = client.to_table

with SparkparseCapture("get", spark=spark, backend="connect"):
    try:
        with SparkConnectCapture(spark=spark):
            check("second capture on one client rejected", False, "no error raised")
    except RuntimeError as exc:
        check(
            "second capture on one client rejected",
            "already being captured" in str(exc),
            str(exc),
        )
    spark.range(10).count()

check("_build_metrics restored", client._build_metrics == original_metrics)
check("to_table restored", client.to_table == original_to_table)
check("no hook attributes left on client", "_build_metrics" not in client.__dict__)
check("session still usable", spark.range(5).count() == 5)

# COMMAND ----------

# Sanitized PlanMetrics fixture for offline replay (identifiers replaced, metrics kept).
_IDENT = re.compile(r"\b[\w]+\.[\w]+\.[\w]+\b")


def sanitize(name: str) -> str:
    name = _IDENT.sub("main.sample.table", name)
    return re.sub(r"\[[^\]]*\]", "[col_a, col_b]", name)


recorded = joins._connect_cap.to_plan_metrics()
for execution in recorded:
    for plan_node in execution["plan_metrics"]:
        plan_node["name"] = sanitize(plan_node["name"])

fixture = {
    "source": "databricks_serverless",
    "provenance": "Recorded by notebooks/validate_connect_capture.py; identifiers sanitized.",
    "client_version": support.client_version,
    "runtime_version": spark.version,
    "executions": recorded,
}

print("SANITIZED_FIXTURE_BEGIN")
print(json.dumps(fixture, indent=2))
print("SANITIZED_FIXTURE_END")

# COMMAND ----------

if FAILURES:
    raise AssertionError(f"BRIEF02_VALIDATION_FAILED: {FAILURES}")
print("BRIEF02_VALIDATION_PASSED")
