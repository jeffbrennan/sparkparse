"""LLM-friendly analysis and programmatic helpers for Spark execution logs."""

import json
from typing import Any

import polars as pl

from sparkparse.models import NodeType, ParsedLogDataFrames
from sparkparse.storage import write_text

_JOIN_NODE_TYPES = frozenset(
    [
        NodeType.BroadcastHashJoin,
        NodeType.SortMergeJoin,
        NodeType.BroadcastNestedLoopJoin,
    ]
)


def _detail_dict(details_str: str | None) -> dict[str, Any] | None:
    """Return the 'detail' sub-dict from a node's details JSON, or None."""
    if not details_str:
        return None
    try:
        return json.loads(details_str).get("detail")
    except (json.JSONDecodeError, AttributeError):
        return None


def to_plan_summary(
    dfs: ParsedLogDataFrames,
    log_name: str,
    out_path: str | None = None,
) -> dict[str, Any]:
    """
    Return a token-efficient dict of execution plan data for LLM analysis.

    Presents raw facts (nodes, durations, bytes, join types, paths) without
    pre-assigned severity. The value over df.explain() is runtime metrics
    (per-node durations correlated from accumulator updates).

    When ``out_path`` is provided, the JSON-serialized summary is also written
    there (local path or cloud URI via storage.write_text).
    """
    dag = dfs.dag
    combined = dfs.combined

    queries: list[dict[str, Any]] = []
    for query_id in dag["query_id"].unique().sort().to_list():
        qdf = dag.filter(pl.col("query_id") == query_id)
        first = qdf.row(0, named=True)

        nodes = []
        for row in qdf.sort("node_id").to_dicts():
            acc_totals = row.get("accumulator_totals") or []
            metrics = [
                {"name": m["metric_name"], "value": m["readable_str"]}
                for m in acc_totals
                if isinstance(m, dict)
            ]

            detail = _detail_dict(row.get("details"))
            node_detail: dict[str, Any] | None = None
            if detail is not None:
                nt = row["node_type"]
                if nt == NodeType.Scan:
                    loc = detail.get("location", {})
                    node_detail = {
                        "paths": loc.get("location", []),
                        "read_schema": detail.get("read_schema"),
                    }
                elif nt in (NodeType.BroadcastHashJoin, NodeType.SortMergeJoin):
                    node_detail = {
                        "join_type": detail.get("join_type"),
                        "left_keys": detail.get("left_keys"),
                        "right_keys": detail.get("right_keys"),
                        "join_condition": detail.get("join_condition"),
                    }
                elif nt == NodeType.BroadcastNestedLoopJoin:
                    node_detail = {
                        "join_type": detail.get("join_type"),
                        "join_condition": detail.get("join_condition"),
                    }

            entry: dict[str, Any] = {
                "node_id": row["node_id"],
                "node_type": row["node_type"],
                "node_name": row["node_name"],
                "child_nodes": row.get("child_nodes"),
                "duration_minutes": row["node_duration_minutes"],
                "metrics": metrics,
            }
            if node_detail is not None:
                entry["details"] = node_detail
            nodes.append(entry)

        queries.append(
            {
                "query_id": query_id,
                "query_function": first["query_function"],
                "start": first["query_start_timestamp"],
                "end": first["query_end_timestamp"],
                "duration_seconds": first["query_duration_seconds"],
                "nodes": nodes,
            }
        )

    agg = combined.select(
        pl.sum("bytes_read").alias("bytes_read"),
        pl.sum("records_read").alias("records_read"),
        pl.sum("bytes_written").alias("bytes_written"),
        pl.sum("records_written").alias("records_written"),
        pl.sum("memory_bytes_spilled").alias("memory_bytes_spilled"),
        pl.sum("disk_bytes_spilled").alias("disk_bytes_spilled"),
        pl.sum("shuffle_bytes_read").alias("shuffle_bytes_read"),
        pl.sum("shuffle_bytes_written").alias("shuffle_bytes_written"),
        pl.sum("executor_run_time_seconds").alias("executor_run_time_seconds"),
        pl.sum("jvm_gc_time_seconds").alias("jvm_gc_time_seconds"),
    ).row(0, named=True)

    summary = {
        "log_name": log_name,
        "queries": queries,
        "totals": agg,
    }

    if out_path is not None:
        write_text(out_path, json.dumps(summary, indent=2, default=str))

    return summary


def find_cartesian_joins(dfs: ParsedLogDataFrames) -> pl.DataFrame:
    """
    Return DAG nodes that are cartesian or cross joins.

    Includes BroadcastNestedLoopJoin and any hash/merge join with join_type=Cross.
    """
    dag = dfs.dag

    bnlj_mask = pl.col("node_type") == NodeType.BroadcastNestedLoopJoin

    cross_ids: list[int] = []
    for row in dag.filter(
        pl.col("node_type").is_in([NodeType.BroadcastHashJoin, NodeType.SortMergeJoin])
    ).to_dicts():
        detail = _detail_dict(row.get("details"))
        if detail and detail.get("join_type") == "Cross":
            cross_ids.append(row["node_id"])

    cross_mask = pl.col("node_id").is_in(cross_ids) if cross_ids else pl.lit(False)

    return (
        dag.filter(bnlj_mask | cross_mask)
        .select(
            "query_id",
            "node_id",
            "node_name",
            "node_type",
            "details",
            "node_duration_minutes",
        )
        .sort("query_id", "node_id")
    )


def find_largest_scans(dfs: ParsedLogDataFrames, n: int = 10) -> pl.DataFrame:
    """
    Return the top N scan nodes by total bytes read.

    Correlates scan nodes to task-level bytes_read via the nodes list in combined.
    """
    dag = dfs.dag
    combined = dfs.combined

    scan_nodes = dag.filter(pl.col("node_type") == NodeType.Scan)
    if scan_nodes.is_empty():
        return pl.DataFrame(
            schema={
                "query_id": pl.Int64,
                "node_id": pl.Int64,
                "node_name": pl.String,
                "paths": pl.List(pl.String),
                "bytes_read": pl.Int64,
                "records_read": pl.Int64,
                "node_duration_minutes": pl.Float64,
            }
        )

    node_bytes = (
        combined.select("query_id", "nodes", "bytes_read", "records_read")
        .explode("nodes")
        .rename({"nodes": "node_name"})
        .group_by("query_id", "node_name")
        .agg(
            pl.sum("bytes_read").alias("bytes_read"),
            pl.sum("records_read").alias("records_read"),
        )
    )

    return (
        scan_nodes.select("query_id", "node_id", "node_name", "details", "node_duration_minutes")
        .join(node_bytes, on=["query_id", "node_name"], how="left")
        .with_columns(
            pl.col("details")
            .map_elements(
                lambda s: json.loads(s)["detail"]["location"]["location"] if s is not None else [],
                return_dtype=pl.List(pl.String),
            )
            .alias("paths")
        )
        .drop("details")
        .sort("bytes_read", descending=True)
        .head(n)
    )


def find_repeated_scans(dfs: ParsedLogDataFrames) -> pl.DataFrame:
    """
    Return scan paths read more than once across queries.

    Repeated scans are candidates for caching with DataFrame.cache() / persist().
    """
    dag = dfs.dag
    scan_nodes = dag.filter(pl.col("node_type") == NodeType.Scan)

    empty = pl.DataFrame(
        schema={
            "path": pl.String,
            "scan_count": pl.UInt32,
            "query_ids": pl.List(pl.Int64),
        }
    )

    if scan_nodes.is_empty():
        return empty

    records: list[dict[str, Any]] = []
    for row in scan_nodes.to_dicts():
        detail = _detail_dict(row.get("details"))
        if not detail:
            continue
        for path in detail.get("location", {}).get("location", []):
            records.append({"path": path, "query_id": row["query_id"]})

    if not records:
        return empty

    return (
        pl.DataFrame(records)
        .unique()
        .group_by("path")
        .agg(
            pl.len().alias("scan_count"),
            pl.col("query_id").alias("query_ids"),
        )
        .filter(pl.col("scan_count") > 1)
        .sort("scan_count", descending=True)
    )


def find_spill(dfs: ParsedLogDataFrames) -> pl.DataFrame:
    """
    Return query/stage combinations with non-zero memory or disk spill.
    """
    return (
        dfs.combined.filter(
            (pl.col("memory_bytes_spilled") > 0) | (pl.col("disk_bytes_spilled") > 0)
        )
        .group_by("query_id", "stage_id")
        .agg(
            pl.sum("memory_bytes_spilled").alias("memory_bytes_spilled"),
            pl.sum("disk_bytes_spilled").alias("disk_bytes_spilled"),
            pl.len().alias("task_count"),
        )
        .sort("memory_bytes_spilled", descending=True)
    )
