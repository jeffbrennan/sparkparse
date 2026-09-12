"""Builders for hand-made plan/task frames used by analysis tests.

Real event logs cannot be produced for every adversarial case (a cartesian
product with no condition, a Photon-only metric name, a capture with no task
rows), so these helpers build the same frame shapes directly.
"""

from __future__ import annotations

import json
from typing import Any

import polars as pl

from sparkparse.connect import empty_capture_dataframes
from sparkparse.models import ParsedLogDataFrames

_METRIC_TYPES = {
    "size": "size",
    "timing": "timing",
    "sum": "sum",
    "average": "average",
}


def accum(name: str, value: float | int | None, metric_type: str = "sum") -> dict:
    """Return one accumulator-total struct as clean.py produces them."""
    return {
        "stage_id": None,
        "task_id": None,
        "accumulator_id": abs(hash(name)) % 10_000,
        "metric_name": name,
        "metric_type": _METRIC_TYPES.get(metric_type, metric_type),
        "value": None if value is None else float(value),
        "unit": "",
        "readable_value": None if value is None else float(value),
        "readable_unit": "",
        "readable_str": None if value is None else str(value),
    }


def node(
    node_id: int,
    node_type: str,
    *,
    children: tuple[int, ...] | list[int] = (),
    metrics: list[dict] | None = None,
    detail: dict[str, Any] | None = None,
    duration_minutes: float | None = 0.0,
    node_name: str | None = None,
) -> dict[str, Any]:
    return {
        "node_id": node_id,
        "node_type": node_type,
        "node_name": node_name or f"[{node_id}] {node_type}",
        "child_nodes": ", ".join(str(child) for child in children),
        "details": None
        if detail is None
        else json.dumps({"node_id": node_id, "node_type": node_type, "detail": detail}),
        "accumulator_totals": metrics or [],
        "node_duration_minutes": duration_minutes,
    }


def make_dag(
    nodes_by_query: dict[int, list[dict[str, Any]]],
    query_function: str = "count",
    query_duration_seconds: float = 1.0,
) -> pl.DataFrame:
    rows: list[dict[str, Any]] = []
    for query_id, nodes in nodes_by_query.items():
        for entry in nodes:
            rows.append(
                {
                    "query_id": query_id,
                    "query_function": query_function,
                    "query_header": f"query_{query_id}",
                    "query_start_timestamp": "2026-01-01T00:00:00",
                    "query_end_timestamp": "2026-01-01T00:00:01",
                    "query_duration_seconds": query_duration_seconds,
                    **entry,
                }
            )
    return pl.DataFrame(rows)


_TASK_DEFAULTS: dict[str, Any] = {
    "log_name": "synthetic",
    "parsed_log_name": "synthetic",
    "query_id": 0,
    "stage_id": 0,
    "task_id": 0,
    "task_duration_seconds": 1.0,
    "bytes_read": 0,
    "records_read": 0,
    "bytes_written": 0,
    "records_written": 0,
    "memory_bytes_spilled": 0,
    "disk_bytes_spilled": 0,
    "shuffle_bytes_read": 0,
    "shuffle_bytes_written": 0,
    "executor_run_time_seconds": 1.0,
    "jvm_gc_time_seconds": 0.0,
    "executor_id": "1",
    "attempt": 0,
    "failed": False,
    "speculative": False,
    "nodes": [],
}


def make_combined(rows: list[dict[str, Any]]) -> pl.DataFrame:
    return pl.DataFrame([{**_TASK_DEFAULTS, **row} for row in rows])


def empty_combined() -> pl.DataFrame:
    """The canonical empty task frame a plan-only capture produces."""
    return empty_capture_dataframes().combined


def make_dfs(
    dag: pl.DataFrame, combined: pl.DataFrame | None = None
) -> ParsedLogDataFrames:
    return ParsedLogDataFrames(
        dag=dag, combined=combined if combined is not None else empty_combined()
    )
