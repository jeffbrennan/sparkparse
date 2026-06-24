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

# Debug: show logical plans captured from to_table() intercept and extracted join info.
# SparkparseCapture delegates to SparkConnectCapture via ._connect_cap on serverless.
from sparkparse.connect import _extract_join_info

_inner = cap._connect_cap  # SparkConnectCapture instance
print(f"_connect_cap type: {type(_inner)}")
print(f"captured plans:   {len(_inner._captured_plans)}")
print(f"captured queries: {len(_inner._captured_queries)}")
for i, proto_rel in enumerate(_inner._captured_plans):
    if proto_rel is None:
        print(f"  plan[{i}]: None (capture failed)")
        continue
    try:
        rel_type = proto_rel.WhichOneof("rel_type")
        joins = _extract_join_info(proto_rel)
        print(f"  plan[{i}]: type={type(proto_rel).__name__!r}  rel_type={rel_type!r}  joins={joins}")
    except Exception as e:
        print(f"  plan[{i}]: type={type(proto_rel).__name__!r}  error reading proto — {e}")

# Also probe what the plan object looks like so we know the right attribute to call.
# Temporarily re-patch to_table just to inspect the plan argument type.
_probe_info: list = []
_orig = _inner.spark._client.to_table if hasattr(_inner.spark._client, "to_table") else None
if _orig:
    def _probe_to_table(plan, *args, **kwargs):
        _probe_info.append({
            "plan_type": type(plan).__name__,
            "plan_module": type(plan).__module__,
            "plan_attrs": [a for a in dir(plan) if not a.startswith("__")],
        })
        return _orig(plan, *args, **kwargs)
    _inner.spark._client.to_table = _probe_to_table
    try:
        # Run a trivial query to trigger to_table
        _inner.spark.range(1).count()
    finally:
        _inner.spark._client.to_table = _orig
    if _probe_info:
        p = _probe_info[0]
        print(f"\nplan object: type={p['plan_type']}  module={p['plan_module']}")
        print(f"  attrs: {p['plan_attrs']}")

# COMMAND ----------

display(find_row_count_explosions(dfs, ratio_threshold=1.1))

# COMMAND ----------

displayHTML(plot_dag(dfs))
