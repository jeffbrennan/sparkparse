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

# Create two small tables with a non-unique key so the join produces M:N row explosion.
spark.sql("CREATE CATALOG IF NOT EXISTS sparkparse_demo")
spark.sql("CREATE SCHEMA IF NOT EXISTS sparkparse_demo.test")

_top_zip = spark.sql("""
  SELECT pickup_zip
  FROM samples.nyctaxi.trips
  WHERE pickup_zip IS NOT NULL
  GROUP BY pickup_zip
  ORDER BY COUNT(*) DESC
  LIMIT 1
""").collect()[0]["pickup_zip"]

_trips = spark.read.table("samples.nyctaxi.trips").filter(f"pickup_zip = {_top_zip}")

left = _trips.select("pickup_zip", "trip_distance", "fare_amount").limit(1000)
if not spark.catalog.tableExists("sparkparse_demo.test.left_trips"):
    left.write.mode("overwrite").saveAsTable("sparkparse_demo.test.left_trips")

right = (
    _trips.select("pickup_zip", "trip_distance", "fare_amount")
    .withColumnRenamed("trip_distance", "trip_distance_b")
    .withColumnRenamed("fare_amount", "fare_amount_b")
    .limit(1000)
)
if not spark.catalog.tableExists("sparkparse_demo.test.right_trips"):
    right.write.mode("overwrite").saveAsTable("sparkparse_demo.test.right_trips")

print(f"pickup_zip: {_top_zip}")

# COMMAND ----------

# Capture the M:N join so sparkparse can see the physical plan metrics.
# _map_node_type() now raises ValueError for any unmapped Photon node type —
# the traceback will show the exact name to add to _PHOTON_NODE_TYPE_MAP.
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

dfs = cap._parsed_logs
assert dfs is not None
print(f"dag:      {dfs.dag.shape}")
print(f"combined: {dfs.combined.shape}")

# COMMAND ----------

print("node types seen:")
display(
    dfs.dag.select("node_id", "node_type", "node_name", "node_duration_minutes").sort("node_id")
)

# COMMAND ----------

import json

print("all dag nodes (node_type repr + raw_name):")
for row in dfs.dag.sort("node_id").to_dicts():
    detail = (json.loads(row["details"]).get("detail") or {}) if row.get("details") else {}
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

# COMMAND ----------

# Probe the Databricks SQL history API and call the GraphQL plan endpoint
# the SQL UI uses (POST /graphql/HistoryStatementPlanById).
#
# Call sequence:
#   1. HistoryStatementPlanMetadata(query_id) → planMetadata.id
#   2. HistoryStatementPlanById(planMetadata.id, queryStartTimeMs) → graph nodes

import json as _json
import urllib.request as _req

from databricks.sdk import WorkspaceClient

_w = WorkspaceClient()
_host = _w.config.host.rstrip("/")  # already includes https://
_token = _w.config.token
print(f"host: {_host}")


def _graphql(operation: str, variables: dict, query: str = "") -> dict:
    body = _json.dumps(
        {"operationName": operation, "variables": variables, "query": query}
    ).encode()
    r = _req.Request(
        f"{_host}/graphql/{operation}",  # _host already has https://
        data=body,
        headers={"Authorization": f"Bearer {_token}", "Content-Type": "application/json"},
        method="POST",
    )
    return _json.loads(_req.urlopen(r).read())


# Full inline query captured from browser network tab for the plan graph endpoint.
_PLAN_BY_ID_QUERY = (
    'query HistoryStatementPlanById($input: SqlgatewayHistoryGetQueryPlanInput!) '
    '@component(name: "DBSQLX.QueryHistory") {\n'
    '  sqlgatewayHistoryGetQueryPlan(input: $input) {\n'
    '    apiError { code message helpUrl traceId __typename }\n'
    '    plans {\n'
    '      nodes {\n'
    '        id name tag hidden collapsed stageLinks subgraphParent insightIds\n'
    '        metadata { key label value values insightIds metaValues { value insightIds __typename } __typename }\n'
    '        metrics { label key metricType value hidden insightTypes __typename }\n'
    '        keyMetrics { durationMs peakMemoryBytes rowsNum __typename }\n'
    '        __typename\n'
    '      }\n'
    '      edges { fromId toId __typename }\n'
    '      executionId errorMessage missingReason source sparkUiUrl timeCompletedMs timeSubmittedMs\n'
    '      __typename\n'
    '    }\n'
    '    __typename\n'
    '  }\n'
    '}'
)

# Get recent finished queries — handle both iterator and ListQueriesResponse return types.
_raw = _w.query_history.list(max_results=20)
_queries = getattr(_raw, "res", None)
if _queries is None:
    try:
        _queries = list(_raw)
    except TypeError:
        _queries = []
_queries = _queries or []

print(f"queries found: {len(_queries)}")
_with_plans = [q for q in _queries if q.as_dict().get("plans_state") == "EXISTS"]
print(f"queries with plans_state=EXISTS: {len(_with_plans)}")
if _queries:
    print("first query fields:")
    print(_json.dumps(_queries[0].as_dict(), indent=2)[:1500])

print()
for q in _with_plans[:1]:
    d = q.as_dict()
    query_id = d.get("query_id") or d.get("id")
    start_ms = d.get("query_start_time_ms") or 0
    print(f"query_id={query_id}  start_ms={start_ms}  plans_state={d.get('plans_state')}")

    # Step 1: fetch plan metadata to get the planMetadata.id required by HistoryStatementPlanById.
    print("\n--- HistoryStatementPlanMetadata ---")
    try:
        meta = _graphql("HistoryStatementPlanMetadata", {"id": query_id})
        print(_json.dumps(meta, indent=2)[:2000])
    except Exception as e:
        print(f"error: {e}")

    # Step 2: call HistoryStatementById to see the full query object shape.
    print("\n--- HistoryStatementById ---")
    try:
        stmt = _graphql("HistoryStatementById", {"id": query_id})
        print(_json.dumps(stmt, indent=2)[:2000])
    except Exception as e:
        print(f"error: {e}")

    # Step 3: call HistoryStatementPlanById.
    # input.id is planMetadata.id (not query_id) — update once Step 1 reveals the correct field.
    print("\n--- HistoryStatementPlanById (using query_id as id — may need planMetadata.id) ---")
    try:
        graph = _graphql(
            "HistoryStatementPlanById",
            {"input": {"id": query_id, "queryStartTimeMs": str(start_ms), "includeRawPlans": False}},
            _PLAN_BY_ID_QUERY,
        )
        print(_json.dumps(graph, indent=2)[:3000])
    except Exception as e:
        print(f"error: {e}")

# COMMAND ----------

display(find_row_count_explosions(dfs, ratio_threshold=1.1))

# COMMAND ----------

displayHTML(plot_dag(dfs))
