"""Canonical output frame schemas shared by every capture backend.

An empty result still has to be typed: downstream analysis distinguishes "no
rows" from "column missing", and an untyped empty frame collapses that
difference.
"""

from __future__ import annotations

import polars as pl

from sparkparse.models import ParsedLogDataFrames

ACCUM_TOTALS_STRUCT = pl.Struct(
    {
        "metric_name": pl.Utf8,
        "metric_type": pl.Utf8,
        "value": pl.Float64,
        # Exact integer counterpart of ``value``; null when the metric is
        # fractional or was rescaled. ``value`` is float64 and cannot hold a
        # count above 2**53 without rounding it.
        "value_exact": pl.Int64,
        "readable_value": pl.Float64,
        "readable_unit": pl.Utf8,
        "readable_str": pl.Utf8,
    }
)

DAG_SCHEMA: dict[str, pl.PolarsDataType] = {
    "log_name": pl.Utf8,
    "parsed_log_name": pl.Utf8,
    "query_id": pl.Int64,
    "query_function": pl.Utf8,
    "query_header": pl.Utf8,
    "query_start_timestamp": pl.Utf8,
    "query_end_timestamp": pl.Utf8,
    "query_duration_seconds": pl.Float64,
    "source_execution_id": pl.Utf8,
    "node_id": pl.Int64,
    "node_type": pl.Utf8,
    "node_name": pl.Utf8,
    "child_nodes": pl.Utf8,
    "whole_stage_codegen_id": pl.Int64,
    "details": pl.Utf8,
    "accumulator_totals": pl.List(ACCUM_TOTALS_STRUCT),
    "n_accumulator_totals": pl.Int64,
    "node_duration_minutes": pl.Float64,
    "n_accumulators": pl.Int64,
    "node_id_adj": pl.Int64,
}

COMBINED_SCHEMA: dict[str, pl.PolarsDataType] = {
    "log_name": pl.Utf8,
    "parsed_log_name": pl.Utf8,
    "query_id": pl.Int64,
    "stage_id": pl.Int64,
    "task_id": pl.Int64,
    "task_duration_seconds": pl.Float64,
    "bytes_read": pl.Int64,
    "records_read": pl.Int64,
    "bytes_written": pl.Int64,
    "records_written": pl.Int64,
    "memory_bytes_spilled": pl.Int64,
    "disk_bytes_spilled": pl.Int64,
    "shuffle_bytes_read": pl.Int64,
    "shuffle_bytes_written": pl.Int64,
    "shuffle_remote_bytes_read": pl.Int64,
    "shuffle_local_bytes_read": pl.Int64,
    "executor_run_time_seconds": pl.Float64,
    "jvm_gc_time_seconds": pl.Float64,
    "executor_id": pl.Utf8,
    "nodes": pl.List(pl.Utf8),
}


def empty_capture_dataframes() -> ParsedLogDataFrames:
    """Return the canonical typed empty frames used by capture backends."""
    return ParsedLogDataFrames(
        dag=pl.DataFrame(schema=DAG_SCHEMA),
        combined=pl.DataFrame(schema=COMBINED_SCHEMA),
    )
