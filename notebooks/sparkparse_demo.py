# Databricks notebook source

# COMMAND ----------

# Install the sparkparse wheel bundled alongside this notebook.
# Walks up ancestor directories to find dist/*.whl — version-agnostic.
# For scheduled job runs the wheel is already injected via databricks.yml environments.
import glob
import pathlib
import subprocess

_p = pathlib.PurePosixPath(
    dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
)
_whl = next(
    (whl for ancestor in _p.parents for whl in glob.glob(f"/Workspace{ancestor}/dist/*.whl")),
    None,
)
if _whl:
    subprocess.check_call(["pip", "install", "--quiet", _whl])
    print(f"sparkparse installed from: {_whl}")
else:
    print(
        "WARNING: sparkparse whl not found — run `uv build --wheel && databricks bundle deploy` first"
    )

# COMMAND ----------

# Locate sample log — works regardless of whether DAB syncs under files/ or bundle root
import pathlib

p = pathlib.PurePosixPath(dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get())

sample_log_dir = next(
    f"/Workspace{ancestor}/tests/data/full_logs"
    for ancestor in p.parents
    if pathlib.Path(f"/Workspace{ancestor}/tests/data/full_logs").exists()
)
sample_log_file = "complex_transformation_medium"
print(f"log: {sample_log_dir}/{sample_log_file}")

# COMMAND ----------

# Set OUT_DIR to save parsed outputs alongside the log; None to skip.
# Unity Catalog volume example: "/Volumes/main/default/sparkparse_demo"
# DBFS example: "/dbfs/tmp/sparkparse_demo"
OUT_DIR = None

# COMMAND ----------

from sparkparse.parse import get_parsed_metrics

dfs = get_parsed_metrics(log_dir=sample_log_dir, log_file=sample_log_file, out_dir=OUT_DIR)
print(f"dag:      {dfs.dag.shape}")
print(f"combined: {dfs.combined.shape}")

# COMMAND ----------

# Physical plan nodes: one row per (query, node)
import polars as pl

display(
    dfs.dag.select(
        "query_id",
        "node_type",
        "node_name",
        "node_duration_minutes",
        "accumulator_totals",
    )
)

# COMMAND ----------

# Task-level metrics: one row per task
display(
    dfs.combined.select(
        "query_id",
        "stage_id",
        "task_id",
        "task_duration_seconds",
        "bytes_read",
        "records_read",
        "shuffle_bytes_read",
        "memory_bytes_spilled",
        "disk_bytes_spilled",
    )
)

# COMMAND ----------

from sparkparse.analyze import get_issues

issues = get_issues(dfs)
print(f"{len(issues)} issue(s) found")
display(pl.DataFrame(issues))

# COMMAND ----------

from sparkparse.analyze import (
    find_largest_scans,
    find_shuffle_heavy_stages,
    find_skewed_tasks,
    find_spill,
)

display(find_spill(dfs))

# COMMAND ----------

display(find_skewed_tasks(dfs))

# COMMAND ----------

display(find_largest_scans(dfs, n=10))

# COMMAND ----------

display(find_shuffle_heavy_stages(dfs))

# COMMAND ----------

# LLM-friendly plan summary: raw facts, no pre-assigned severity
import json

from sparkparse.analyze import to_plan_summary

summary = to_plan_summary(dfs, log_name="demo")
print("totals:")
print(json.dumps(summary["totals"], indent=2, default=str))
print(f"\n{len(summary['queries'])} queries — first query nodes:")
for node in summary["queries"][0]["nodes"]:
    duration = f"{node['duration_minutes']:.2f} min" if node["duration_minutes"] else "—"
    print(f"  [{node['node_id']:3d}] {node['node_type']:30s}  {duration:>12}")

# COMMAND ----------

# Interactive DAG with hotspot coloring — runs inline on serverless, no Dash server needed.
# metric can be "node_duration_minutes" or any accumulator name (e.g. "number of output rows").
# `sparkparse viz --log-dir <path>` launches the full interactive dashboard locally.
from sparkparse.viz import plot_dag

displayHTML(plot_dag(dfs))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Live capture: NYC taxi data
# MAGIC
# MAGIC `SparkparseCapture` auto-detects the runtime:
# MAGIC - **Classic cluster**: restarts the SparkSession to enable event logging (resets in-memory DataFrames). Use `cap.spark` inside the block.
# MAGIC - **Serverless**: captures plan-level metrics via Spark Connect without restarting the session. `cap.spark` is the same session.

# COMMAND ----------

from sparkparse.capture import SparkparseCapture

# Change to a Volumes path to persist the event log after the cluster terminates (classic only).
CAPTURE_LOG_DIR = "/tmp/sparkparse_nyctaxi"

with SparkparseCapture(
    action="analyze",
    spark=spark,
    temp_dir=CAPTURE_LOG_DIR,
    log_name="nyctaxi_demo",
) as cap:
    trips = cap.spark.read.table("samples.nyctaxi.trips")
    result = (
        trips.groupBy("pickup_zip")
        .agg({"fare_amount": "avg", "trip_distance": "sum"})
        .orderBy("avg(fare_amount)", ascending=False)
    )
    display(result.limit(20))

dfs_taxi = cap._parsed_logs
print(f"dag:      {dfs_taxi.dag.shape}")
print(f"combined: {dfs_taxi.combined.shape}")

# COMMAND ----------

issues_taxi = get_issues(dfs_taxi)
print(f"{len(issues_taxi)} issue(s) found")
display(pl.DataFrame(issues_taxi))

# COMMAND ----------

displayHTML(plot_dag(dfs_taxi))
