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

display(find_row_count_explosions(dfs, ratio_threshold=1.1))

# COMMAND ----------

displayHTML(plot_dag(dfs))
