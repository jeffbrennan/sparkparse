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

import base64 as _b64
import json as _json
import urllib.request as _req

from databricks.sdk import WorkspaceClient

_w = WorkspaceClient()
_host = _w.config.host
_token = _w.config.token
print(f"host: {_host}")


def _graphql(operation: str, variables: dict) -> dict:
    body = _json.dumps(
        {
            "operationName": operation,
            "variables": variables,
            "query": "",
        }
    ).encode()
    r = _req.Request(
        f"https://{_host}/graphql/{operation}",
        data=body,
        headers={"Authorization": f"Bearer {_token}", "Content-Type": "application/json"},
        method="POST",
    )
    return _json.loads(_req.urlopen(r).read())


def _build_lookup_key(query_id: str, start_ms: int, endpoint_id: str, user_id: int) -> str:
    def _vi(n: int) -> bytes:
        out = b""
        while n > 0x7F:
            out += bytes([(n & 0x7F) | 0x80])
            n >>= 7
        return out + bytes([n])

    def _ld(f: int, b: bytes) -> bytes:
        return _vi((f << 3) | 2) + _vi(len(b)) + b

    proto = (
        _ld(1, query_id.encode())
        + _vi(2 << 3)
        + _vi(start_ms)
        + _ld(3, endpoint_id.encode())
        + _vi(4 << 3)
        + _vi(user_id)
    )
    return _b64.b64encode(proto).decode().rstrip("=")


# Get recent finished queries — handle both iterator and ListQueriesResponse return types
_raw = _w.query_history.list(max_results=10)
_queries = getattr(_raw, "res", None)
if _queries is None:
    try:
        _queries = list(_raw)
    except TypeError:
        _queries = []
_queries = _queries or []

print(f"queries found: {len(_queries)}")
if _queries:
    print("first query fields:")
    print(_json.dumps(_queries[0].as_dict(), indent=2)[:1500])

# Try the GraphQL plan endpoint for the first query once we know field names
print()
for q in _queries[:1]:
    d = q.as_dict()
    query_id = d.get("query_id") or d.get("statement_id") or d.get("id")
    start_ms = d.get("query_start_time_ms") or d.get("start_time_ms") or 0
    endpoint_id = d.get("warehouse_id") or d.get("endpoint_id") or "0000000000000000"
    user_id = (
        (d.get("user") or {}).get("id") or d.get("user_id") or d.get("executed_as_user_id") or 0
    )
    if isinstance(user_id, str):
        user_id = int(user_id)
    print(f"query_id={query_id}  start_ms={start_ms}  endpoint_id={endpoint_id}  user_id={user_id}")
    lk = _build_lookup_key(
        query_id=query_id, start_ms=start_ms, endpoint_id=endpoint_id, user_id=user_id
    )
    print(f"lookupKey: {lk}")
    try:
        result = _graphql("HistoryStatementPlanById", {"lookupKey": lk})
        print(_json.dumps(result, indent=2)[:1000])
    except Exception as e:
        print(f"error: {e}")

# COMMAND ----------

display(find_row_count_explosions(dfs, ratio_threshold=1.1))

# COMMAND ----------

displayHTML(plot_dag(dfs))
