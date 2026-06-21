# Databricks notebook source

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

from sparkparse.parse import get_parsed_metrics

dfs = get_parsed_metrics(log_dir=sample_log_dir, log_file=sample_log_file, out_dir=None)
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

# MAGIC %md
# MAGIC `sparkparse viz --log-dir <path>` launches an interactive Dash dashboard locally; not available on Databricks serverless.
