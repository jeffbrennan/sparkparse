"""SparkConnectCapture — metric capture for Databricks serverless (Spark Connect)."""

from __future__ import annotations

import json
import logging
import re
from collections import defaultdict
from typing import TYPE_CHECKING, Any

import polars as pl

from sparkparse.models import NodeType, ParsedLogDataFrames

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

_log = logging.getLogger(__name__)

_PHOTON_NODE_TYPE_MAP: dict[str, NodeType] = {
    # Photon execution nodes
    "PhotonScan": NodeType.Scan,
    "PhotonGroupingAgg": NodeType.HashAggregate,
    "PhotonAgg": NodeType.HashAggregate,
    "PhotonSort": NodeType.Sort,
    "PhotonShuffleExchangeSink": NodeType.Exchange,
    "PhotonShuffleExchangeSource": NodeType.Exchange,
    "PhotonBroadcastHashJoin": NodeType.BroadcastHashJoin,
    "PhotonSortMergeJoin": NodeType.SortMergeJoin,
    "PhotonBroadcastNestedLoopJoin": NodeType.BroadcastNestedLoopJoin,
    "PhotonProject": NodeType.Project,
    "PhotonFilter": NodeType.Filter,
    "PhotonBroadcastExchange": NodeType.BroadcastExchange,
    "PhotonUnion": NodeType.Union,
    "PhotonExpand": NodeType.Expand,
    "PhotonTopK": NodeType.TakeOrderedAndProject,
    "PhotonWindow": NodeType.Window,
    "PhotonShuffleMapStage": NodeType.Exchange,
    "PhotonResultStage": NodeType.ResultQueryStage,
    "PhotonColumnarToRow": NodeType.ColumnarToRow,
    # Standard Spark / AQE nodes
    "AdaptiveSparkPlan": NodeType.AdaptiveSparkPlan,
    "ResultQueryStage": NodeType.ResultQueryStage,
    "ShuffleQueryStage": NodeType.ShuffleQueryStage,
    "BroadcastQueryStage": NodeType.BroadcastQueryStage,
    "TableCacheQueryStage": NodeType.TableCacheQueryStage,
    "AQEShuffleRead": NodeType.AQEShuffleRead,
    "CollectLimit": NodeType.CollectLimit,
    "GlobalLimit": NodeType.GlobalLimit,
    "LocalLimit": NodeType.LocalLimit,
    "Project": NodeType.Project,
    "Filter": NodeType.Filter,
    "Sort": NodeType.Sort,
    "Exchange": NodeType.Exchange,
    "BroadcastExchange": NodeType.BroadcastExchange,
    "BroadcastHashJoin": NodeType.BroadcastHashJoin,
    "SortMergeJoin": NodeType.SortMergeJoin,
    "HashAggregate": NodeType.HashAggregate,
    "Union": NodeType.Union,
    "Coalesce": NodeType.Coalesce,
    "ReusedExchange": NodeType.ReusedExchange,
}

_ACCUM_TOTALS_STRUCT = pl.Struct(
    {
        "metric_name": pl.Utf8,
        "metric_type": pl.Utf8,
        "value": pl.Float64,
        "readable_value": pl.Float64,
        "readable_unit": pl.Utf8,
        "readable_str": pl.Utf8,
    }
)

_DAG_SCHEMA: dict[str, pl.PolarsDataType] = {
    "log_name": pl.Utf8,
    "parsed_log_name": pl.Utf8,
    "query_id": pl.Int64,
    "query_function": pl.Utf8,
    "query_header": pl.Utf8,
    "query_start_timestamp": pl.Utf8,
    "query_end_timestamp": pl.Utf8,
    "query_duration_seconds": pl.Float64,
    "node_id": pl.Int64,
    "node_type": pl.Utf8,
    "node_name": pl.Utf8,
    "child_nodes": pl.Utf8,
    "whole_stage_codegen_id": pl.Int64,
    "details": pl.Utf8,
    "accumulator_totals": pl.List(_ACCUM_TOTALS_STRUCT),
    "n_accumulator_totals": pl.Int64,
    "node_duration_minutes": pl.Float64,
    "n_accumulators": pl.Int64,
    "node_id_adj": pl.Int64,
}

_COMBINED_SCHEMA: dict[str, pl.PolarsDataType] = {
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


_JOIN_NODE_TYPES: frozenset[NodeType] = frozenset({
    NodeType.BroadcastHashJoin,
    NodeType.SortMergeJoin,
    NodeType.BroadcastNestedLoopJoin,
    NodeType.CartesianProduct,
})

_JOIN_TYPES: frozenset[str] = frozenset({
    "Inner", "LeftOuter", "RightOuter", "FullOuter",
    "LeftSemi", "LeftAnti", "Cross",
})

_BRACKET_RE = re.compile(r"\[([^\]]*)\]")
_EXPR_ID_RE = re.compile(r"#\w+")


def _map_node_type(name: str) -> NodeType | None:
    prefix = name.split()[0] if name else ""
    return _PHOTON_NODE_TYPE_MAP.get(prefix)


def _parse_join_details(name: str) -> dict[str, Any]:
    """Extract left/right keys and join type from a join node name.

    Spark Connect join names look like:
        PhotonBroadcastHashJoin [left_col#id], [right_col#id], Inner, BuildRight
    """
    groups = _BRACKET_RE.findall(name)

    def clean(group: str) -> list[str]:
        return [_EXPR_ID_RE.sub("", k).strip() for k in group.split(",") if k.strip()]

    result: dict[str, Any] = {}
    if len(groups) >= 2:
        result["left_keys"] = clean(groups[0])
        result["right_keys"] = clean(groups[1])

    remainder = _BRACKET_RE.sub("", name)
    for token in (t.strip() for t in remainder.split(",")):
        if token in _JOIN_TYPES:
            result["join_type"] = token
            break

    return result


def _readable_size(bytes_val: float) -> tuple[float, str]:
    for thresh, unit in [
        (1 << 40, "TiB"),
        (1 << 30, "GiB"),
        (1 << 20, "MiB"),
        (1 << 10, "KiB"),
    ]:
        if bytes_val >= thresh:
            return round(bytes_val / thresh, 1), unit
    return bytes_val, "B"


def _readable_timing(ms_val: float) -> tuple[float, str]:
    if ms_val >= 3_600_000:
        return round(ms_val / 3_600_000, 1), "hr"
    if ms_val >= 60_000:
        return round(ms_val / 60_000, 1), "min"
    if ms_val >= 1_000:
        return round(ms_val / 1_000, 1), "s"
    return round(ms_val, 1), "ms"


def _convert_metric(
    raw_value: float, metric_type: str
) -> tuple[str, float, float, str, str]:
    """Return (norm_metric_type, value, readable_value, readable_unit, readable_str).

    value is in display units: ms for timing, bytes for size, raw for others.
    """
    if metric_type == "nsTiming":
        value = raw_value / 1_000_000
        norm_type = "timing"
        readable_value, readable_unit = _readable_timing(value)
    elif metric_type == "timing":
        value = float(raw_value)
        norm_type = "timing"
        readable_value, readable_unit = _readable_timing(value)
    elif metric_type == "size":
        value = float(raw_value)
        norm_type = "size"
        readable_value, readable_unit = _readable_size(value)
    else:
        value = float(raw_value)
        norm_type = metric_type
        readable_value = round(value, 2)
        readable_unit = ""

    readable_str = (
        f"{readable_value} {readable_unit}".strip()
        if readable_unit
        else str(readable_value)
    )
    return norm_type, value, float(readable_value), readable_unit, readable_str


def _build_accum_struct(m: Any) -> dict[str, Any]:
    norm_type, value, readable_value, readable_unit, readable_str = _convert_metric(
        m.value, m.metric_type
    )
    return {
        "metric_name": m.name,
        "metric_type": norm_type,
        "value": value,
        "readable_value": readable_value,
        "readable_unit": readable_unit,
        "readable_str": readable_str,
    }


class SparkConnectCapture:
    """Context manager that captures Spark Connect (serverless) query metrics.

    Monkey-patches spark._client._build_metrics to intercept PlanMetrics from
    each gRPC response. On exit, builds a ParsedLogDataFrames with the same
    dag schema as event-log-based parsing so all downstream functions work.

    Usage::

        with SparkConnectCapture(spark=spark, log_name="my_job") as cap:
            df.collect()
        dfs = cap.dfs
        plot_dag(dfs)
    """

    def __init__(self, spark: SparkSession, log_name: str | None = None) -> None:
        self.spark = spark
        self._log_name = log_name or "sparkconnect"
        self._captured_queries: list[list[Any]] = []
        self._dfs: ParsedLogDataFrames | None = None
        self._orig_build_metrics: Any = None

    def __enter__(self) -> SparkConnectCapture:
        client: Any = getattr(self.spark, "_client")
        orig = client._build_metrics
        self._orig_build_metrics = orig
        captured_queries = self._captured_queries

        def _patched(metrics_proto: Any) -> Any:
            result = orig(metrics_proto)
            if result:
                captured_queries.append(list(result))
            return result

        client._build_metrics = _patched
        return self

    def __exit__(self, exc_type: Any, *_: Any) -> None:
        client: Any = getattr(self.spark, "_client")
        client._build_metrics = self._orig_build_metrics
        if exc_type:
            return
        self._dfs = self._build_dataframes()
        _log.info(
            "SparkConnectCapture: captured %d quer(y/ies)", len(self._captured_queries)
        )

    @property
    def dfs(self) -> ParsedLogDataFrames | None:
        return self._dfs

    @property
    def _parsed_logs(self) -> ParsedLogDataFrames | None:
        return self._dfs

    def _build_dataframes(self) -> ParsedLogDataFrames:
        dag = self._build_dag_df()
        combined = self._build_combined_df()
        return ParsedLogDataFrames(dag=dag, combined=combined)

    def _build_dag_df(self) -> pl.DataFrame:
        if not self._captured_queries:
            return pl.DataFrame(schema=_DAG_SCHEMA)

        rows: list[dict[str, Any]] = []
        unmapped: dict[str, str] = {}  # raw name -> prefix, for error reporting
        for query_id, plan_metrics in enumerate(self._captured_queries):
            children: dict[int, list[int]] = defaultdict(list)
            for node in plan_metrics:
                if node.parent_plan_id != node.plan_id:
                    children[node.parent_plan_id].append(node.plan_id)

            child_nodes_map = {
                nid: ", ".join(str(c) for c in sorted(cs))
                for nid, cs in children.items()
            }

            root = next(
                (n for n in plan_metrics if n.parent_plan_id == n.plan_id), None
            )
            query_header = root.name if root else f"query_{query_id}"

            # Derive query duration from root node's cumulTime when available
            query_duration_seconds: float | None = None
            if root:
                for m in root.metrics:
                    if m.name == "cumulTime" and m.metric_type == "nsTiming":
                        query_duration_seconds = round(m.value / 1_000_000_000, 2)
                        break

            for node in plan_metrics:
                node_id = node.plan_id
                node_type = _map_node_type(node.name)
                if node_type is None:
                    prefix = node.name.split()[0] if node.name else ""
                    unmapped[node.name] = prefix
                    continue
                child_nodes = child_nodes_map.get(node_id)

                accum_totals: list[dict[str, Any]] = []
                node_duration_minutes: float | None = None

                for m in node.metrics:
                    accum_totals.append(_build_accum_struct(m))
                    if m.name == "cumulTime" and m.metric_type == "nsTiming":
                        node_duration_minutes = m.value / 60_000_000_000

                node_type_str = str(node_type)
                node_name = f"[{node_id}] {node_type_str}"

                detail_data: dict[str, Any] = {"raw_name": node.name}
                if node_type in _JOIN_NODE_TYPES:
                    detail_data.update(_parse_join_details(node.name))
                if node_type == NodeType.Scan:
                    # node.name is like "PhotonScan parquet catalog.schema.table [col, ...]"
                    parts = node.name.split() if node.name else []
                    if len(parts) >= 3:
                        table = parts[2].split("[")[0].rstrip()
                        detail_data["location"] = {"location": [table]}
                details = json.dumps({"detail": detail_data})

                rows.append(
                    {
                        "log_name": self._log_name,
                        "parsed_log_name": self._log_name,
                        "query_id": query_id,
                        "query_function": None,
                        "query_header": query_header,
                        "query_start_timestamp": None,
                        "query_end_timestamp": None,
                        "query_duration_seconds": query_duration_seconds,
                        "node_id": node_id,
                        "node_type": node_type_str,
                        "node_name": node_name,
                        "child_nodes": child_nodes,
                        "whole_stage_codegen_id": None,
                        "details": details,
                        "accumulator_totals": accum_totals,
                        "n_accumulator_totals": len(accum_totals),
                        "node_duration_minutes": node_duration_minutes,
                        "n_accumulators": 0,
                        "node_id_adj": node_id,
                    }
                )

        if unmapped:
            lines = "\n".join(
                f"  {name!r} (prefix={prefix!r})"
                for name, prefix in sorted(unmapped.items())
            )
            raise ValueError(
                f"Unmapped Spark Connect node type(s) — add to _PHOTON_NODE_TYPE_MAP "
                f"in sparkparse/connect.py:\n{lines}"
            )

        df = pl.DataFrame(rows, schema_overrides=_DAG_SCHEMA)
        return df

    def _build_combined_df(self) -> pl.DataFrame:
        return pl.DataFrame(schema=_COMBINED_SCHEMA)
