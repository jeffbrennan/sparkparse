# Databricks notebook source

# stdlib imports + Databricks runtime globals for type checking.
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from databricks import dbutils, display, displayHTML, spark  # noqa: F401

# COMMAND ----------

from sparkparse.analyze import find_row_count_explosions
from sparkparse.capture import SparkparseCapture
from sparkparse.viz import plot_dag

# COMMAND ----------

# # Create two small tables with a non-unique key so the join produces M:N row explosion.
# spark.sql("CREATE CATALOG IF NOT EXISTS sparkparse_demo")
# spark.sql("CREATE SCHEMA IF NOT EXISTS sparkparse_demo.test")

# _top_zip = spark.sql("""
#   SELECT pickup_zip
#   FROM samples.nyctaxi.trips
#   WHERE pickup_zip IS NOT NULL
#   GROUP BY pickup_zip
#   ORDER BY COUNT(*) DESC
#   LIMIT 1
# """).collect()[0]["pickup_zip"]

# _trips = spark.read.table("samples.nyctaxi.trips").filter(f"pickup_zip = {_top_zip}")

# left = _trips.select("pickup_zip", "trip_distance", "fare_amount").limit(1000)
# if not spark.catalog.tableExists("sparkparse_demo.test.left_trips"):
#     left.write.mode("overwrite").saveAsTable("sparkparse_demo.test.left_trips")

# right = (
#     _trips.select("pickup_zip", "trip_distance", "fare_amount")
#     .withColumnRenamed("trip_distance", "trip_distance_b")
#     .withColumnRenamed("fare_amount", "fare_amount_b")
#     .limit(1000)
# )
# if not spark.catalog.tableExists("sparkparse_demo.test.right_trips"):
#     right.write.mode("overwrite").saveAsTable("sparkparse_demo.test.right_trips")

# print(f"pickup_zip: {_top_zip}")

# COMMAND ----------

# Capture the M:N join so sparkparse can see the physical plan metrics.
# Unmapped Photon operators degrade to NodeType.Unknown with their raw names kept;
# pass strict=True to raise instead, or read the unknown_operators diagnostic.
with SparkparseCapture(
    action="analyze",
    spark=spark,
    temp_dir="/tmp/sparkparse_node_type_debug",
    log_name="node_type_debug",
) as cap:
    left_df = cap.spark.read.table("sparkparse_demo.test.left_trips")
    right_df = cap.spark.read.table("sparkparse_demo.test.right_trips")
    exploded = left_df.join(right_df, on="pickup_zip", how="inner")
    display(exploded.limit(20))
    print(f"exploded row count: {exploded.count():,}")

dfs = cap.dfs
assert dfs is not None
assert cap.result is not None
assert cap.result.metadata.backend == "spark_connect"
assert cap.result.metadata.capture_end is not None
assert cap.result.capabilities.task_metrics.status.value == "unavailable"
assert cap.analysis is not None
print(f"dag:      {dfs.dag.shape}")
print(f"combined: {dfs.combined.shape}")
print(
    f"capture:  {cap.result.metadata.capture_id} ({cap.result.metadata.status.value})"
)

# COMMAND ----------

print("node types seen:")
display(
    dfs.dag.select("node_id", "node_type", "node_name", "node_duration_minutes").sort(
        "node_id"
    )
)

# COMMAND ----------

import json

print("all dag nodes (node_type repr + raw_name):")
for row in dfs.dag.sort("node_id").to_dicts():
    detail = (
        (json.loads(row["details"]).get("detail") or {}) if row.get("details") else {}
    )
    print(
        f"  [{row['node_id']:3d}] type={row['node_type']!r:30s}  raw_name={detail.get('raw_name', '')!r}"
    )
    if detail.get("left_keys") or detail.get("location"):
        print(
            f"         left_keys={detail.get('left_keys')}  right_keys={detail.get('right_keys')}  location={detail.get('location')}"
        )
    if row.get("child_nodes"):
        print(f"         child_nodes={row['child_nodes']!r}")

# COMMAND ----------

# Debug: per-execution provenance from the Connect adapter.
# SparkparseCapture delegates to SparkConnectCapture via ._connect_cap on serverless.
_inner = cap._connect_cap  # SparkConnectCapture instance
print(f"client support:  {_inner.support}")
print(f"executions:      {json.dumps(_inner.executions, indent=2, default=str)}")
print(f"diagnostics:     {[d.code for d in _inner.diagnostics]}")
for row in dfs.dag.to_dicts():
    detail = json.loads(row["details"])["detail"]
    if "join_details_source" in detail:
        print(
            f"  query={row['query_id']} node={row['node_id']} "
            f"source={detail['join_details_source']} type={detail.get('join_type')} "
            f"left={detail.get('left_keys')} right={detail.get('right_keys')}"
        )

# COMMAND ----------

display(find_row_count_explosions(dfs, ratio_threshold=1.1))

# COMMAND ----------

displayHTML(plot_dag(dfs))

# COMMAND ----------

# Brief 01 contract checks against real serverless execution. Only task-local
# temporary artifacts are written; existing source tables are read-only.
import tempfile
from pathlib import Path

from sparkparse import history
from sparkparse.capture import capture
from sparkparse.models import CaptureResult


def assert_round_trip(result):
    restored = CaptureResult.model_validate_json(result.model_dump_json())
    assert restored.dag.schema == result.dag.schema
    assert restored.combined.schema == result.combined.schema
    assert restored.dag.equals(result.dag)
    assert restored.combined.equals(result.combined)


assert cap.result.dag.height > 0, "Must test populated results, not an empty success"
assert_round_trip(cap.result)
assert cap.analysis["metadata"] == cap.result.metadata.model_dump(mode="json")
assert {"capture_id", "plan_version", "source_execution_id"} <= set(
    cap.result.dag.columns
)

client = spark._client
original_metrics = client._build_metrics
original_table = client.to_table


def assert_hooks_restored():
    assert client._build_metrics == original_metrics
    assert client.to_table == original_table


def workload():
    # Reuse the validated Photon join shape, with a small returned result.
    return (
        spark.read.table("sparkparse_demo.test.left_trips")
        .join(spark.read.table("sparkparse_demo.test.right_trips"), on="pickup_zip")
        .limit(5)
        .collect()
    )


with tempfile.TemporaryDirectory(prefix="sparkparse-brief01-") as artifacts:
    rules = Path(artifacts) / "alerts.toml"
    rules.write_text("""[[alerts]]
name = "unavailable-task-metric"
log_name = "brief01"
metric = "bytes_read"
condition = "threshold"
threshold = -1
on_trigger = "raise"
""")
    history_path = str(Path(artifacts) / "history")
    with SparkparseCapture(
        "analyze",
        spark=spark,
        backend="connect",
        log_name="brief01",
        history_path=history_path,
        alert_config=str(rules),
    ) as recorded:
        assert workload()
    assert_hooks_restored()
    assert recorded.result is not None
    assert recorded.last_record is not None
    assert recorded.result.dag.height > 0
    assert recorded.last_record.run_id == recorded.result.metadata.capture_id
    assert recorded.last_record.bytes_read is None
    assert recorded.last_record.n_tasks is None
    assert recorded.last_record.duration_s is None
    assert recorded.triggered_alerts == []
    stored = history.read(history_path)
    assert stored.height == 1
    assert stored["bytes_read"][0] is None
    assert_round_trip(recorded.result)

    with SparkparseCapture("viz", spark=spark, backend="connect") as visual:
        assert workload()
    assert_hooks_restored()
    assert visual.result is not None
    assert visual.result.dag.height > 0
    assert visual.report and "task_metrics" in visual.report
    assert "No plan captured" not in visual.report

    with SparkparseCapture("get", spark=spark, backend="connect") as empty:
        pass
    assert_hooks_restored()
    assert empty.result is not None
    assert empty.result.dag.is_empty()
    assert_round_trip(empty.result)

    class WorkloadFailure(Exception):
        pass

    failed = SparkparseCapture("get", spark=spark, backend="connect")
    try:
        with failed:
            assert workload()
            raise WorkloadFailure("expected verification failure")
    except WorkloadFailure:
        pass
    else:
        raise AssertionError("Workload exception was suppressed")
    assert_hooks_restored()
    assert failed.result is not None
    assert failed.result.metadata.status.value == "failed"
    assert failed.result.dag.height > 0

    @capture(action="get", spark=spark, backend="connect")
    def decorated():
        return workload()

    _, first = decorated()
    _, second = decorated()
    assert first.result is not None
    assert second.result is not None
    assert first.result.metadata.capture_id != second.result.metadata.capture_id
    assert_hooks_restored()
    assert workload(), "Borrowed session must remain usable after capture"

print(
    "BRIEF01_CONTRACT_VERIFIED: populated/empty serialization, analysis metadata, "
    "history nulls, alert suppression, HTML report, exception recovery, "
    "fresh decorator state, session preservation, and hook restoration"
)
