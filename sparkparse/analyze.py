"""Analysis of Spark execution data: raw plan export and evidence-based findings.

Two separate concerns live here and stay separate:

``to_plan_summary``
    A raw, machine-readable export of plan structure and normalized runtime
    metrics. It states facts and coverage; it assigns no severity.

``analyze_dfs`` / ``get_issues``
    Diagnostic rules. Every rule reports its own status (``evaluated``,
    ``unsupported``, ``insufficient_data``) so a silent rule is never mistaken
    for a clean result, and every finding carries the evidence, units and
    thresholds that produced it.

The ``find_*`` helpers remain the DataFrame-shaped interface for notebooks.
"""

from __future__ import annotations

import hashlib
import json
import statistics
from collections.abc import Callable, Iterable
from typing import Any

import polars as pl

from sparkparse.metrics import (
    SOURCE_CONNECT_PLAN,
    SOURCE_EVENT_LOG,
    SOURCE_TASK_METRICS,
    canonical_metrics,
    normalize_metrics,
)
from sparkparse.models import (
    AnalysisReport,
    CapabilityStatus,
    CaptureCapabilities,
    CaptureCapability,
    CaptureResult,
    EvidenceValue,
    Finding,
    FindingConfidence,
    FindingSeverity,
    MetricUnit,
    NodeType,
    ParsedLogDataFrames,
    RuleAssessment,
    RuleStatus,
)
from sparkparse.storage import write_text

SUMMARY_SCHEMA_VERSION = "2"
ANALYSIS_SCHEMA_VERSION = "1"

Dfs = ParsedLogDataFrames | CaptureResult

_JOIN_NODE_TYPES = frozenset(
    [
        NodeType.BroadcastHashJoin,
        NodeType.SortMergeJoin,
        NodeType.BroadcastNestedLoopJoin,
        NodeType.CartesianProduct,
    ]
)

_SOURCE_SCAN_TYPES = frozenset(
    [NodeType.Scan, NodeType.BatchScan, NodeType.LocalTableScan]
)

# Operators that pass every input row through unchanged. Walking down through
# them to find an input row count is safe; anything else (limits, filters,
# aggregates) changes cardinality and terminates the walk.
_ROW_PRESERVING_TYPES = frozenset(
    [
        NodeType.AQEShuffleRead,
        NodeType.BroadcastExchange,
        NodeType.BroadcastQueryStage,
        NodeType.ColumnarToRow,
        NodeType.Exchange,
        NodeType.InMemoryTableScan,
        NodeType.ResultQueryStage,
        NodeType.ShuffleQueryStage,
        NodeType.Sort,
        NodeType.TableCacheQueryStage,
        NodeType.WholeStageCodegen,
    ]
)


# ---------------------------------------------------------------------------
# coverage
# ---------------------------------------------------------------------------


def _infer_capabilities(dfs: ParsedLogDataFrames) -> CaptureCapabilities:
    """Infer coverage for frames that did not come from a capture result.

    Only presence is inferred here; a capture result always carries the richer
    contract produced at capture time.
    """
    dag = dfs.dag
    combined = dfs.combined
    connect = "source_execution_id" in dag.columns
    source = "spark_connect_plan_metrics" if connect else "event_log"

    def capability(available: bool, reason: str) -> CaptureCapability:
        return CaptureCapability(
            status=(
                CapabilityStatus.available
                if available
                else CapabilityStatus.unavailable
            ),
            source=source,
            reason=None if available else reason,
        )

    has_tasks = (
        combined.height > 0
        and "task_id" in combined.columns
        and combined["task_id"].drop_nulls().len() > 0
    )
    has_stage_timing = has_tasks and "stage_start_timestamp" in combined.columns
    has_operator_metrics = dag.height > 0 and any(
        bool(row) for row in dag.get_column("accumulator_totals").to_list()
    )
    return CaptureCapabilities(
        plan_structure=capability(dag.height > 0, "No plan nodes were parsed."),
        operator_metrics=capability(
            has_operator_metrics, "No operator metrics were observed."
        ),
        query_elapsed_time=capability(
            dag.height > 0 and "query_duration_seconds" in dag.columns,
            "No query timing was observed.",
        ),
        task_metrics=capability(has_tasks, "No task rows are present in this capture."),
        stage_timing=capability(
            has_stage_timing, "No stage timing is present in this capture."
        ),
        scan_details=capability(dag.height > 0, "No plan details were parsed."),
        join_details=capability(dag.height > 0, "No plan details were parsed."),
    )


def resolve_capabilities(dfs: Dfs) -> CaptureCapabilities:
    """Return the coverage contract for a capture result or plain frames."""
    if isinstance(dfs, CaptureResult):
        return dfs.capabilities
    return _infer_capabilities(dfs)


def _metric_source(dfs: Dfs) -> str:
    if isinstance(dfs, CaptureResult):
        if (dfs.metadata.transport or "").lower() in {"spark_connect", "connect"}:
            return SOURCE_CONNECT_PLAN
        if dfs.metadata.backend == "spark_connect":
            return SOURCE_CONNECT_PLAN
        return SOURCE_EVENT_LOG
    return (
        SOURCE_CONNECT_PLAN
        if "source_execution_id" in dfs.dag.columns
        else SOURCE_EVENT_LOG
    )


def measured_stages(dfs: Dfs, columns: tuple[str, ...]) -> int:
    """Return how many stages reported every one of ``columns`` on some task.

    Rules count the stages they could assess, not the ones that tripped their
    threshold: "no findings out of 40 assessed stages" and "no findings because
    nothing was measurable" are different results.
    """
    combined = dfs.combined
    if not _has_tasks(dfs) or any(column not in combined.columns for column in columns):
        return 0
    predicate = pl.col(columns[0]).is_not_null()
    for column in columns[1:]:
        predicate = predicate & pl.col(column).is_not_null()
    return combined.filter(predicate).select("query_id", "stage_id").unique().height


def _has_tasks(dfs: Dfs) -> bool:
    capabilities = resolve_capabilities(dfs)
    if capabilities.task_metrics.status in (
        CapabilityStatus.unavailable,
        CapabilityStatus.not_applicable,
    ):
        return False
    return dfs.combined.height > 0


# ---------------------------------------------------------------------------
# node access
# ---------------------------------------------------------------------------


def _detail_dict(details_str: str | None) -> dict[str, Any] | None:
    """Return the 'detail' sub-dict from a node's details JSON, or None."""
    if not details_str:
        return None
    try:
        return json.loads(details_str).get("detail")
    except (json.JSONDecodeError, AttributeError):
        return None


def _child_ids(row: dict[str, Any]) -> list[int]:
    children_str = row.get("child_nodes")
    if not children_str:
        return []
    ids: list[int] = []
    for child in str(children_str).split(","):
        child = child.strip()
        if not child:
            continue
        try:
            ids.append(int(child))
        except ValueError:
            continue
    return ids


class NodeIndex:
    """Query-scoped node lookup. Node IDs are only unique within a query."""

    def __init__(self, dag: pl.DataFrame, source: str) -> None:
        self.source = source
        self.by_query: dict[Any, dict[int, dict[str, Any]]] = {}
        self.parents: dict[Any, dict[int, list[int]]] = {}
        for row in dag.to_dicts():
            query_id = row["query_id"]
            nodes = self.by_query.setdefault(query_id, {})
            nodes[row["node_id"]] = row
        for query_id, nodes in self.by_query.items():
            parents: dict[int, list[int]] = {}
            for node_id, row in nodes.items():
                for child in _child_ids(row):
                    parents.setdefault(child, []).append(node_id)
            self.parents[query_id] = parents

    def node(self, query_id: Any, node_id: int) -> dict[str, Any] | None:
        return self.by_query.get(query_id, {}).get(node_id)

    def metrics(self, row: dict[str, Any]) -> dict[str, Any]:
        return canonical_metrics(row.get("accumulator_totals"), self.source)

    def metric(self, row: dict[str, Any], canonical: str) -> float | int | None:
        metric = self.metrics(row).get(canonical)
        return None if metric is None else metric.value

    def output_rows(self, row: dict[str, Any]) -> float | int | None:
        """Return measured output rows, preserving a legitimate zero."""
        return self.metric(row, "output_rows")

    def input_rows(
        self, query_id: Any, node_id: int, _depth: int = 0
    ) -> tuple[float | int | None, int | None]:
        """Return (rows, node_id) for the nearest measured input row count.

        Descends only through operators that cannot change cardinality, so the
        value is the join's actual immediate input, not a source scan count.
        """
        if _depth > 12:
            return None, None
        row = self.node(query_id, node_id)
        if row is None:
            return None, None
        rows = self.output_rows(row)
        if rows is not None:
            return rows, node_id
        try:
            node_type = NodeType(row["node_type"])
        except ValueError:
            return None, None
        if node_type not in _ROW_PRESERVING_TYPES:
            return None, None
        for child in _child_ids(row):
            child_rows, child_id = self.input_rows(query_id, child, _depth + 1)
            if child_rows is not None:
                return child_rows, child_id
        return None, None

    def parent_ids(self, query_id: Any, node_id: int) -> list[int]:
        return self.parents.get(query_id, {}).get(node_id, [])


def _get_output_rows(acc_totals: list[dict] | None) -> int | None:
    """Return output rows from accumulator totals, or None when not measured.

    A measured zero is returned as ``0``.
    """
    value = canonical_metrics(acc_totals).get("output_rows")
    if value is None or value.value is None:
        return None
    return int(value.value)


def _get_scan_paths(details_str: str | None) -> list[str]:
    """Return file paths from a Scan/BatchScan/LocalTableScan node's details."""
    detail = _detail_dict(details_str)
    if detail is None:
        return []
    location = detail.get("location", {})
    if isinstance(location, dict):
        paths = location.get("location", [])
        return [str(path) for path in paths] if paths else []
    return []


def _find_source_scans(
    dag: pl.DataFrame, query_id: Any, start_node_ids: list[int]
) -> list[dict[str, Any]]:
    """Trace from start nodes down to source scans within one query.

    Scans identify lineage only. They sit below filters and aggregations, so
    their row counts are not join-input cardinality.
    """
    query_nodes = {
        r["node_id"]: r for r in dag.filter(pl.col("query_id") == query_id).to_dicts()
    }
    found: list[dict[str, Any]] = []
    visited: set[int] = set()

    def walk(node_id: int) -> None:
        if node_id in visited:
            return
        visited.add(node_id)
        node = query_nodes.get(node_id)
        if node is None:
            return
        if node["node_type"] in _SOURCE_SCAN_TYPES:
            found.append(
                {
                    "node_id": node_id,
                    "node_type": node["node_type"],
                    "paths": _get_scan_paths(node.get("details")),
                    "output_rows": _get_output_rows(node.get("accumulator_totals")),
                }
            )
            return
        for child in _child_ids(node):
            walk(child)

    for start_id in start_node_ids:
        walk(start_id)
    return found


# ---------------------------------------------------------------------------
# formatting
# ---------------------------------------------------------------------------


def _fmt_rows(n: float | int | None) -> str:
    """Format a row count compactly for issue messages."""
    if n is None:
        return "unknown"
    n = int(n)
    for threshold, suffix in [
        (10**12, "T"),
        (10**9, "B"),
        (10**6, "M"),
        (10**3, "K"),
    ]:
        if n >= threshold:
            return f"{n / threshold:.1f}{suffix}"
    return str(n)


def _fmt_bytes(n: float | int | None) -> str:
    if n is None:
        return "unknown"
    for unit, threshold in [
        ("TiB", 2**40),
        ("GiB", 2**30),
        ("MiB", 2**20),
        ("KiB", 2**10),
    ]:
        if n >= threshold:
            return f"{n / threshold:.1f} {unit}"
    return f"{int(n)} B"


def _path_name(path: str) -> str:
    """Return a compact file/table name from a full scan path."""
    return path.rstrip("/").split("/")[-1] or path


class Redactor:
    """Replaces paths and expressions with stable, non-reversible tokens."""

    def __init__(self, enabled: bool) -> None:
        self.enabled = enabled

    def _token(self, value: str, prefix: str) -> str:
        digest = hashlib.sha256(value.encode("utf-8")).hexdigest()[:12]
        return f"{prefix}:{digest}"

    def path(self, value: str | None) -> str | None:
        if value is None or not self.enabled:
            return value
        return self._token(value, "path")

    def paths(self, values: Iterable[str] | None) -> list[str] | None:
        if values is None:
            return None
        return [self.path(value) or "" for value in values]

    def expr(self, value: Any) -> Any:
        if not self.enabled or value is None:
            return value
        if isinstance(value, list):
            return [self.expr(item) for item in value]
        return self._token(str(value), "expr")


# ---------------------------------------------------------------------------
# plan summary
# ---------------------------------------------------------------------------

_TOTAL_COLUMNS = (
    "bytes_read",
    "records_read",
    "bytes_written",
    "records_written",
    "memory_bytes_spilled",
    "disk_bytes_spilled",
    "shuffle_bytes_read",
    "shuffle_bytes_written",
    "executor_run_time_seconds",
    "jvm_gc_time_seconds",
)

_TOTAL_UNITS = {
    "bytes_read": MetricUnit.bytes,
    "records_read": MetricUnit.rows,
    "bytes_written": MetricUnit.bytes,
    "records_written": MetricUnit.rows,
    "memory_bytes_spilled": MetricUnit.bytes,
    "disk_bytes_spilled": MetricUnit.bytes,
    "shuffle_bytes_read": MetricUnit.bytes,
    "shuffle_bytes_written": MetricUnit.bytes,
    "executor_run_time_seconds": MetricUnit.seconds,
    "jvm_gc_time_seconds": MetricUnit.seconds,
}

_COMPACT_DEFAULT_NODES = 25


def _safe_node_name(row: dict[str, Any], redactor: Redactor) -> str | None:
    """Return the node's display name, redacted when it carries workload text.

    Connect keeps the server's operator name inside the display name for
    operators it could not map (``[7] Unknown (PhotonScan … customers.ssn)``),
    so the name itself can hold literals. The identifying part — node id and
    resolved type — is rebuilt; anything beyond it becomes a token.
    """
    name = row.get("node_name")
    if not redactor.enabled or name is None:
        return name
    node_id = row.get("node_id_adj", row.get("node_id"))
    canonical = f"[{node_id}] {row['node_type']}"
    remainder = str(name).removeprefix(canonical).strip()
    if not remainder:
        return canonical
    return f"{canonical} ({redactor.expr(remainder)})"


def _node_detail(
    node_type: str, detail: dict[str, Any], redactor: Redactor
) -> dict[str, Any] | None:
    if node_type == NodeType.Scan:
        location = detail.get("location", {})
        paths = location.get("location", []) if isinstance(location, dict) else []
        return {
            "paths": redactor.paths(paths),
            "read_schema": redactor.expr(detail.get("read_schema")),
        }
    if node_type in (NodeType.BroadcastHashJoin, NodeType.SortMergeJoin):
        return {
            "join_type": detail.get("join_type"),
            "left_keys": redactor.expr(detail.get("left_keys")),
            "right_keys": redactor.expr(detail.get("right_keys")),
            "join_condition": redactor.expr(detail.get("join_condition")),
        }
    if node_type in (
        NodeType.BroadcastNestedLoopJoin,
        NodeType.CartesianProduct,
    ):
        return {
            "join_type": detail.get("join_type"),
            "join_condition": redactor.expr(detail.get("join_condition")),
        }
    return None


def to_plan_summary(
    dfs: Dfs,
    log_name: str,
    out_path: str | None = None,
    *,
    compact: bool = False,
    top_n: int | None = None,
    redact: bool = False,
) -> dict[str, Any]:
    """
    Return a token-efficient dict of execution plan data for LLM analysis.

    Presents raw facts (nodes, numeric metrics with units, join types, paths)
    without pre-assigned severity. The value over ``df.explain()`` is runtime
    metrics correlated from accumulator updates.

    Metric values are numeric and carry ``unit``, ``scope`` and ``aggregation``
    so they round-trip through JSON without loss. Unmapped metrics keep
    ``canonical: null`` rather than being guessed onto a canonical name.

    Parameters
    ----------
    compact:
        Drop display-only fields and keep the ``top_n`` longest-running nodes
        per query, reporting how many were omitted.
    top_n:
        Nodes to keep per query. Defaults to 25 in compact mode, all otherwise.
    redact:
        Replace file paths and expressions with stable hashed tokens so real
        workloads can be shared.
    """
    dag = dfs.dag
    combined = dfs.combined
    capabilities = resolve_capabilities(dfs)
    source = _metric_source(dfs)
    redactor = Redactor(redact)
    node_limit = (
        top_n if top_n is not None else (_COMPACT_DEFAULT_NODES if compact else None)
    )

    queries: list[dict[str, Any]] = []
    for query_id in dag["query_id"].unique().sort().to_list():
        qdf = dag.filter(pl.col("query_id") == query_id)
        first = qdf.row(0, named=True)

        node_rows = qdf.sort("node_id").to_dicts()
        omitted = 0
        if node_limit is not None and len(node_rows) > node_limit:
            ranked = sorted(
                node_rows,
                key=lambda r: (
                    r.get("node_duration_minutes") is None,
                    -(r.get("node_duration_minutes") or 0.0),
                ),
            )
            kept = sorted(ranked[:node_limit], key=lambda r: r["node_id"])
            omitted = len(node_rows) - len(kept)
            node_rows = kept

        nodes = []
        for row in node_rows:
            metrics = []
            for metric in normalize_metrics(row.get("accumulator_totals"), source):
                entry_metric: dict[str, Any] = {
                    "name": metric.raw_name,
                    "canonical": metric.canonical,
                    "value": metric.value,
                    "unit": metric.unit.value,
                    "aggregation": metric.aggregation.value,
                    "scope": metric.scope.value,
                }
                if not compact:
                    entry_metric["readable"] = metric.readable
                metrics.append(entry_metric)

            detail = _detail_dict(row.get("details"))
            node_detail = (
                _node_detail(row["node_type"], detail, redactor)
                if detail is not None
                else None
            )

            entry: dict[str, Any] = {
                "node_id": row["node_id"],
                "node_type": row["node_type"],
                "child_nodes": row.get("child_nodes"),
                "duration_minutes": row["node_duration_minutes"],
                "metrics": metrics,
            }
            if not compact:
                entry["node_name"] = _safe_node_name(row, redactor)
            if node_detail is not None:
                entry["details"] = node_detail
            nodes.append(entry)

        query_entry: dict[str, Any] = {
            "query_id": query_id,
            "query_function": first["query_function"],
            "start": first["query_start_timestamp"],
            "end": first["query_end_timestamp"],
            "duration_seconds": first["query_duration_seconds"],
            "node_count": qdf.height,
            "nodes": nodes,
        }
        if omitted:
            query_entry["omitted_node_count"] = omitted
        queries.append(query_entry)

    if combined.height == 0 or not _has_tasks(dfs):
        # An empty task table is a valid Connect result, not evidence of zero work.
        agg: dict[str, Any] = dict.fromkeys(_TOTAL_COLUMNS)
    else:
        agg = combined.select(
            *(pl.sum(column).alias(column) for column in _TOTAL_COLUMNS)
        ).row(0, named=True)

    summary: dict[str, Any] = {
        "schema_version": SUMMARY_SCHEMA_VERSION,
        "log_name": log_name,
        "queries": queries,
        "totals": agg,
        "total_units": {
            column: _TOTAL_UNITS[column].value for column in _TOTAL_COLUMNS
        },
        "coverage": capabilities.model_dump(mode="json"),
    }

    if isinstance(dfs, CaptureResult):
        summary["metadata"] = dfs.metadata.model_dump(mode="json")
        summary["capabilities"] = dfs.capabilities.model_dump(mode="json")
        summary["diagnostics"] = [d.model_dump(mode="json") for d in dfs.diagnostics]
        for query in queries:
            coverage = dfs.capabilities.query_elapsed_time.query_coverage.get(
                str(query["query_id"])
            )
            if coverage != CapabilityStatus.available:
                query["duration_seconds"] = None

    if out_path is not None:
        write_text(out_path, json.dumps(summary, indent=2, default=str))

    return summary


# ---------------------------------------------------------------------------
# join classification
# ---------------------------------------------------------------------------


def _join_classification(row: dict[str, Any]) -> tuple[str, str | None]:
    """Return (kind, reason) for a join node.

    ``kind`` is one of ``cartesian``, ``conditional_nested_loop`` or ``keyed``.
    A nested loop join that evaluates a condition is not a cartesian product,
    even though both are nested loops.
    """
    node_type = row["node_type"]
    detail = _detail_dict(row.get("details")) or {}
    join_type = detail.get("join_type")
    condition = detail.get("join_condition")
    has_condition = bool(condition) and str(condition).lower() != "none"

    if node_type == NodeType.CartesianProduct:
        return "cartesian", "CartesianProduct operator"
    if join_type == "Cross":
        return "cartesian", "join_type=Cross"
    if node_type == NodeType.BroadcastNestedLoopJoin:
        if has_condition:
            return "conditional_nested_loop", "nested loop join with a join condition"
        return "cartesian", "nested loop join with no join condition"
    return "keyed", None


def _join_rows(dag: pl.DataFrame) -> list[dict[str, Any]]:
    if dag.height == 0 or "node_type" not in dag.columns:
        return []
    return dag.filter(pl.col("node_type").is_in(_JOIN_NODE_TYPES)).to_dicts()


# ---------------------------------------------------------------------------
# find_* helpers (DataFrame interface)
# ---------------------------------------------------------------------------


def find_cartesian_joins(dfs: Dfs) -> pl.DataFrame:
    """
    Return join nodes that produce a cartesian product.

    Includes ``CartesianProduct``, nested loop joins with no join condition, and
    any join whose ``join_type`` is ``Cross``. Nested loop joins that evaluate a
    condition are excluded: they are not cartesian products. Matching is scoped
    per query, so a node ID reused by another query is not misattributed.
    """
    dag = dfs.dag
    empty = pl.DataFrame(
        schema={
            "query_id": pl.Int64,
            "node_id": pl.Int64,
            "node_name": pl.String,
            "node_type": pl.String,
            "details": pl.String,
            "node_duration_minutes": pl.Float64,
            "reason": pl.String,
        }
    )
    if dag.height == 0:
        return empty

    keys: list[tuple[Any, int, str]] = []
    for row in _join_rows(dag):
        kind, reason = _join_classification(row)
        if kind == "cartesian":
            keys.append((row["query_id"], row["node_id"], reason or "cartesian"))
    if not keys:
        return empty.cast({"query_id": dag.schema["query_id"]})  # type: ignore[arg-type]

    reasons = pl.DataFrame(
        {
            "query_id": [key[0] for key in keys],
            "node_id": [key[1] for key in keys],
            "reason": [key[2] for key in keys],
        },
        schema_overrides={
            "query_id": dag.schema["query_id"],
            "node_id": dag.schema["node_id"],
        },
    )
    return (
        dag.join(reasons, on=["query_id", "node_id"], how="inner")
        .select(
            "query_id",
            "node_id",
            "node_name",
            "node_type",
            "details",
            "node_duration_minutes",
            "reason",
        )
        .sort("query_id", "node_id")
    )


def find_nested_loop_joins(dfs: Dfs) -> pl.DataFrame:
    """Return nested loop joins that evaluate a join condition.

    These are quadratic in comparisons but are not cartesian products.
    """
    dag = dfs.dag
    records = [
        {
            "query_id": row["query_id"],
            "node_id": row["node_id"],
            "node_name": row["node_name"],
            "node_type": row["node_type"],
            "join_type": (_detail_dict(row.get("details")) or {}).get("join_type"),
            "join_condition": (_detail_dict(row.get("details")) or {}).get(
                "join_condition"
            ),
            "node_duration_minutes": row["node_duration_minutes"],
        }
        for row in _join_rows(dag)
        if _join_classification(row)[0] == "conditional_nested_loop"
    ]
    if not records:
        return pl.DataFrame(
            schema={
                "query_id": pl.Int64,
                "node_id": pl.Int64,
                "node_name": pl.String,
                "node_type": pl.String,
                "join_type": pl.String,
                "join_condition": pl.String,
                "node_duration_minutes": pl.Float64,
            }
        )
    return pl.DataFrame(records).sort("query_id", "node_id")


_EXPLOSION_SCHEMA = {
    "query_id": pl.Int64,
    "node_id": pl.Int64,
    "node_type": pl.String,
    "join_kind": pl.String,
    "join_type": pl.String,
    "left_keys": pl.List(pl.String),
    "right_keys": pl.List(pl.String),
    "output_rows": pl.Int64,
    "left_input_rows": pl.Int64,
    "right_input_rows": pl.Int64,
    "left_input_node_id": pl.Int64,
    "right_input_node_id": pl.Int64,
    "ratio": pl.Float64,
    "inputs_resolved": pl.Boolean,
    "left_scan_paths": pl.List(pl.String),
    "right_scan_paths": pl.List(pl.String),
}


def find_row_count_explosions(dfs: Dfs, ratio_threshold: float = 1.1) -> pl.DataFrame:
    """
    Return joins whose output rows exceed their largest measured input side.

    Input row counts come from the join's immediate inputs, descending only
    through operators that cannot change cardinality. Source scans are traced
    separately for each branch to name the tables involved, but scan counts are
    lineage only: they sit below filters and aggregations and are not join-input
    cardinality.

    Expansion is an observation. Duplicate keys are a hypothesis: outer joins
    and intentional many-to-many joins expand legitimately.
    """
    dag = dfs.dag
    index = NodeIndex(dag, _metric_source(dfs))
    records: list[dict[str, Any]] = []

    for row in _join_rows(dag):
        output_rows = index.output_rows(row)
        if output_rows is None or output_rows <= 0:
            continue

        detail = _detail_dict(row.get("details")) or {}
        child_ids = _child_ids(row)
        if len(child_ids) < 2:
            continue

        query_id = row["query_id"]
        left_rows, left_node = index.input_rows(query_id, child_ids[0])
        right_rows, right_node = index.input_rows(query_id, child_ids[1])
        known = [rows for rows in (left_rows, right_rows) if rows is not None]
        if not known:
            continue
        max_input = max(known)
        if max_input <= 0:
            continue

        ratio = output_rows / max_input
        if ratio < ratio_threshold:
            continue

        left_scans = _find_source_scans(dag, query_id, child_ids[:1])
        right_scans = _find_source_scans(dag, query_id, child_ids[1:2])
        kind, _ = _join_classification(row)

        records.append(
            {
                "query_id": query_id,
                "node_id": row["node_id"],
                "node_type": row["node_type"],
                "join_kind": kind,
                "join_type": detail.get("join_type"),
                "left_keys": detail.get("left_keys"),
                "right_keys": detail.get("right_keys"),
                "output_rows": int(output_rows),
                "left_input_rows": None if left_rows is None else int(left_rows),
                "right_input_rows": None if right_rows is None else int(right_rows),
                "left_input_node_id": left_node,
                "right_input_node_id": right_node,
                "ratio": ratio,
                "inputs_resolved": left_rows is not None and right_rows is not None,
                "left_scan_paths": [
                    path for scan in left_scans for path in scan["paths"]
                ],
                "right_scan_paths": [
                    path for scan in right_scans for path in scan["paths"]
                ],
            }
        )

    if not records:
        return pl.DataFrame(schema=_EXPLOSION_SCHEMA)

    return pl.DataFrame(records, schema_overrides=_EXPLOSION_SCHEMA).sort(
        "ratio", descending=True
    )


_SCAN_SCHEMA = {
    "query_id": pl.Int64,
    "node_id": pl.Int64,
    "node_name": pl.String,
    "paths": pl.List(pl.String),
    "bytes_read": pl.Int64,
    "records_read": pl.Int64,
    "scan_bytes": pl.Int64,
    "output_rows": pl.Int64,
    "bytes_source": pl.String,
    "node_duration_minutes": pl.Float64,
}


def find_largest_scans(dfs: Dfs, n: int = 10) -> pl.DataFrame:
    """
    Return the top N scan nodes ranked by observed size.

    Bytes are taken from task attribution when task metrics exist and from the
    scan's own ``scan_bytes`` operator metric otherwise; ``bytes_source`` names
    which. Task-attributed bytes are shared across every plan node a task
    touched, so they are an upper bound for one node, not an exact figure.

    When no size metric is available for any scan, rows are reported instead and
    ranking falls back to ``output_rows``. Scans with no observed size at all are
    never presented as the "largest".
    """
    dag = dfs.dag
    index = NodeIndex(dag, _metric_source(dfs))
    scan_nodes = dag.filter(pl.col("node_type") == NodeType.Scan)
    if scan_nodes.is_empty():
        return pl.DataFrame(schema=_SCAN_SCHEMA)

    task_bytes: dict[tuple[Any, str], dict[str, Any]] = {}
    if _has_tasks(dfs) and "nodes" in dfs.combined.columns:
        attributed = (
            dfs.combined.select("query_id", "nodes", "bytes_read", "records_read")
            .explode("nodes")
            .rename({"nodes": "node_name"})
            .drop_nulls("node_name")
            .group_by("query_id", "node_name")
            .agg(
                pl.sum("bytes_read").alias("bytes_read"),
                pl.sum("records_read").alias("records_read"),
            )
        )
        task_bytes = {
            (row["query_id"], row["node_name"]): row for row in attributed.to_dicts()
        }

    records: list[dict[str, Any]] = []
    for row in scan_nodes.to_dicts():
        metrics = index.metrics(row)
        scan_bytes = metrics.get("scan_bytes")
        scan_bytes_value = None if scan_bytes is None else scan_bytes.value
        attributed_row = task_bytes.get((row["query_id"], row["node_name"]))

        if attributed_row is not None and attributed_row.get("bytes_read") is not None:
            bytes_read = attributed_row["bytes_read"]
            records_read = attributed_row.get("records_read")
            bytes_source = "task_attribution_shared"
        elif scan_bytes_value is not None:
            bytes_read = int(scan_bytes_value)
            records_read = None
            bytes_source = "operator_metric"
        else:
            bytes_read = None
            records_read = None
            bytes_source = None

        output_rows = index.output_rows(row)
        records.append(
            {
                "query_id": row["query_id"],
                "node_id": row["node_id"],
                "node_name": row["node_name"],
                "paths": _get_scan_paths(row.get("details")),
                "bytes_read": None if bytes_read is None else int(bytes_read),
                "records_read": None if records_read is None else int(records_read),
                "scan_bytes": None
                if scan_bytes_value is None
                else int(scan_bytes_value),
                "output_rows": None if output_rows is None else int(output_rows),
                "bytes_source": bytes_source,
                "node_duration_minutes": row["node_duration_minutes"],
            }
        )

    frame = pl.DataFrame(records, schema_overrides=_SCAN_SCHEMA)
    sized = frame.filter(pl.col("bytes_read").is_not_null())
    if sized.height:
        return sized.sort("bytes_read", descending=True).head(n)
    rowed = frame.filter(pl.col("output_rows").is_not_null())
    if rowed.height:
        return rowed.sort("output_rows", descending=True).head(n)
    # Nothing observed: return the empty frame rather than an arbitrary ranking.
    return frame.clear()


_REPEATED_SCAN_SCHEMA = {
    "path": pl.String,
    "scan_count": pl.UInt32,
    "query_ids": pl.List(pl.Int64),
    "node_ids": pl.List(pl.Int64),
    "distinct_queries": pl.UInt32,
    "distinct_read_schemas": pl.UInt32,
    "repeats_within_query": pl.Boolean,
}


def find_repeated_scans(dfs: Dfs) -> pl.DataFrame:
    """
    Return scan paths read more than once, including repeats inside one query.

    ``scan_count`` counts scan node occurrences, not distinct queries, so a path
    scanned twice in the same query is reported. ``distinct_read_schemas``
    distinguishes identical re-reads from scans that project different columns.
    """
    dag = dfs.dag
    scan_nodes = dag.filter(pl.col("node_type").is_in(_SOURCE_SCAN_TYPES))
    empty = pl.DataFrame(schema=_REPEATED_SCAN_SCHEMA)
    if scan_nodes.is_empty():
        return empty

    records: list[dict[str, Any]] = []
    for row in scan_nodes.to_dicts():
        detail = _detail_dict(row.get("details"))
        if not detail:
            continue
        read_schema = str(detail.get("read_schema"))
        for path in _get_scan_paths(row.get("details")):
            records.append(
                {
                    "path": path,
                    "query_id": row["query_id"],
                    "node_id": row["node_id"],
                    "read_schema": read_schema,
                }
            )

    if not records:
        return empty

    return (
        pl.DataFrame(records)
        .unique(subset=["path", "query_id", "node_id"])
        .group_by("path")
        .agg(
            pl.len().cast(pl.UInt32).alias("scan_count"),
            pl.col("query_id").alias("query_ids"),
            pl.col("node_id").alias("node_ids"),
            pl.col("query_id").n_unique().cast(pl.UInt32).alias("distinct_queries"),
            pl.col("read_schema")
            .n_unique()
            .cast(pl.UInt32)
            .alias("distinct_read_schemas"),
        )
        .with_columns(
            (pl.col("scan_count") > pl.col("distinct_queries")).alias(
                "repeats_within_query"
            )
        )
        .filter(pl.col("scan_count") > 1)
        .sort("scan_count", descending=True)
    )


def find_spill(dfs: Dfs) -> pl.DataFrame:
    """
    Return query/stage combinations with non-zero memory or disk spill.

    Task metrics only. Use :func:`find_operator_spill` for the plan-metric view
    that Spark Connect captures can supply.
    """
    empty = pl.DataFrame(
        schema={
            "query_id": pl.Int64,
            "stage_id": pl.Int64,
            "memory_bytes_spilled": pl.Int64,
            "disk_bytes_spilled": pl.Int64,
            "task_count": pl.UInt32,
        }
    )
    if not _has_tasks(dfs) or "memory_bytes_spilled" not in dfs.combined.columns:
        return empty
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


def find_operator_spill(dfs: Dfs) -> pl.DataFrame:
    """
    Return plan nodes reporting operator-level spill.

    The storage medium of operator spill is unspecified, so it is reported on
    its own and never added to the task-level memory/disk counters. Operator
    spill cannot be distributed across stages or tasks.
    """
    dag = dfs.dag
    index = NodeIndex(dag, _metric_source(dfs))
    records: list[dict[str, Any]] = []
    for row in dag.to_dicts():
        value = index.metric(row, "spill_bytes")
        if value is None or value <= 0:
            continue
        records.append(
            {
                "query_id": row["query_id"],
                "node_id": row["node_id"],
                "node_type": row["node_type"],
                "spill_bytes": int(value),
            }
        )
    schema = {
        "query_id": pl.Int64,
        "node_id": pl.Int64,
        "node_type": pl.String,
        "spill_bytes": pl.Int64,
    }
    if not records:
        return pl.DataFrame(schema=schema)
    return pl.DataFrame(records, schema_overrides=schema).sort(
        "spill_bytes", descending=True
    )


def find_skewed_tasks(dfs: Dfs, skew_ratio: float = 5.0) -> pl.DataFrame:
    """
    Return stages whose slowest task exceeds ``skew_ratio`` × the median task.

    ``max_bytes_ratio`` compares the slowest task's input bytes with the stage
    median. A high duration ratio alone identifies a straggler; data skew needs
    the size evidence too.
    """
    empty = pl.DataFrame(
        schema={
            "query_id": pl.Int64,
            "stage_id": pl.Int64,
            "task_count": pl.UInt32,
            "median_task_s": pl.Float64,
            "p95_task_s": pl.Float64,
            "max_task_s": pl.Float64,
            "skew_ratio": pl.Float64,
            "median_task_bytes": pl.Float64,
            "max_task_bytes": pl.Int64,
            "bytes_ratio": pl.Float64,
        }
    )
    if not _has_tasks(dfs):
        return empty

    combined = dfs.combined
    task_bytes = pl.col("bytes_read").fill_null(0) + pl.col(
        "shuffle_bytes_read"
    ).fill_null(0)
    frame = (
        combined.with_columns(task_bytes.alias("task_input_bytes"))
        .group_by("query_id", "stage_id")
        .agg(
            pl.len().alias("task_count"),
            pl.median("task_duration_seconds").alias("median_task_s"),
            pl.col("task_duration_seconds").quantile(0.95).alias("p95_task_s"),
            pl.max("task_duration_seconds").alias("max_task_s"),
            pl.median("task_input_bytes").alias("median_task_bytes"),
            pl.col("task_input_bytes")
            .sort_by("task_duration_seconds", descending=True)
            .first()
            .alias("max_task_bytes"),
        )
        .filter(pl.col("median_task_s") > 0)
        .with_columns(
            (pl.col("max_task_s") / pl.col("median_task_s")).alias("skew_ratio"),
            pl.when(pl.col("median_task_bytes") > 0)
            .then(pl.col("max_task_bytes") / pl.col("median_task_bytes"))
            .otherwise(None)
            .alias("bytes_ratio"),
        )
        .filter(pl.col("skew_ratio") >= skew_ratio)
        .sort("skew_ratio", descending=True)
    )
    return frame


def find_shuffle_heavy_stages(dfs: Dfs, threshold_bytes: int = 0) -> pl.DataFrame:
    """
    Return stages whose shuffle read or write exceeds ``threshold_bytes``.

    ``total_shuffle_bytes`` is retained for compatibility but read and write
    describe the two sides of the same movement; the filter uses the larger of
    the two rather than their sum.
    """
    empty = pl.DataFrame(
        schema={
            "query_id": pl.Int64,
            "stage_id": pl.Int64,
            "shuffle_write_bytes": pl.Int64,
            "shuffle_read_bytes": pl.Int64,
            "total_shuffle_bytes": pl.Int64,
            "max_side_bytes": pl.Int64,
        }
    )
    if not _has_tasks(dfs):
        return empty
    return (
        dfs.combined.group_by("query_id", "stage_id")
        .agg(
            pl.sum("shuffle_bytes_written").alias("shuffle_write_bytes"),
            pl.sum("shuffle_bytes_read").alias("shuffle_read_bytes"),
        )
        .with_columns(
            (pl.col("shuffle_write_bytes") + pl.col("shuffle_read_bytes")).alias(
                "total_shuffle_bytes"
            ),
            pl.max_horizontal("shuffle_write_bytes", "shuffle_read_bytes").alias(
                "max_side_bytes"
            ),
        )
        .filter(pl.col("max_side_bytes") > threshold_bytes)
        .sort("max_side_bytes", descending=True)
    )


def find_long_running_nodes(dfs: Dfs, threshold_min: float = 1.0) -> pl.DataFrame:
    """
    Return DAG nodes whose measured duration exceeds ``threshold_min`` minutes.
    """
    return (
        dfs.dag.filter(
            pl.col("node_duration_minutes").is_not_null()
            & (pl.col("node_duration_minutes") >= threshold_min)
        )
        .select(
            "query_id", "node_id", "node_type", "node_name", "node_duration_minutes"
        )
        .sort("node_duration_minutes", descending=True)
    )


_GC_SCHEMA = {
    "query_id": pl.Int64,
    "stage_id": pl.Int64,
    "executor_run_time_seconds": pl.Float64,
    "jvm_gc_time_seconds": pl.Float64,
    "gc_ratio": pl.Float64,
}


def gc_measured_stages(dfs: Dfs) -> pl.DataFrame:
    """Return per-stage GC and executor run time for stages that measured both.

    A task row exists whether or not it carries GC telemetry, and summing a
    column of nulls yields zero — which reads as "no GC" rather than "not
    measured". Only tasks reporting both values are aggregated, so an empty
    result means the telemetry is absent, not that GC was free.
    """
    columns = ("executor_run_time_seconds", "jvm_gc_time_seconds")
    if not _has_tasks(dfs) or any(
        column not in dfs.combined.columns for column in columns
    ):
        return pl.DataFrame(schema=_GC_SCHEMA).drop("gc_ratio")
    return (
        dfs.combined.filter(
            pl.col("executor_run_time_seconds").is_not_null()
            & pl.col("jvm_gc_time_seconds").is_not_null()
        )
        .group_by("query_id", "stage_id")
        .agg(
            pl.sum("executor_run_time_seconds").alias("executor_run_time_seconds"),
            pl.sum("jvm_gc_time_seconds").alias("jvm_gc_time_seconds"),
        )
    )


def find_gc_overhead(
    dfs: Dfs, ratio_threshold: float = 0.1, min_run_time_seconds: float = 1.0
) -> pl.DataFrame:
    """
    Return stages whose JVM GC time is a large fraction of executor run time.

    Stages with less than ``min_run_time_seconds`` of executor time are excluded:
    a high ratio over a few milliseconds of work says nothing. Stages that never
    reported GC telemetry are excluded by :func:`gc_measured_stages`.
    """
    measured = gc_measured_stages(dfs)
    if measured.height == 0:
        return pl.DataFrame(schema=_GC_SCHEMA)
    return (
        measured.filter(pl.col("executor_run_time_seconds") >= min_run_time_seconds)
        .with_columns(
            (pl.col("jvm_gc_time_seconds") / pl.col("executor_run_time_seconds")).alias(
                "gc_ratio"
            )
        )
        .filter(pl.col("gc_ratio") >= ratio_threshold)
        .sort("gc_ratio", descending=True)
    )


def find_inefficient_scans(
    dfs: Dfs, retained_threshold: float = 0.1, min_rows: int = 1000
) -> pl.DataFrame:
    """
    Return scans that read far more rows than the pipeline retained.

    Two independent denominators are used, whichever is present:

    - ``scanned_rows`` from the scan itself (Photon), compared with its output.
    - the output of the filter directly above the scan (classic Spark), which
      shows work a pushed-down or partition filter could have avoided.

    Without a denominator no verdict is produced; the scan is simply absent.
    """
    dag = dfs.dag
    index = NodeIndex(dag, _metric_source(dfs))
    records: list[dict[str, Any]] = []

    for row in dag.filter(pl.col("node_type").is_in(_SOURCE_SCAN_TYPES)).to_dicts():
        query_id = row["query_id"]
        scanned = index.metric(row, "scanned_rows")
        output = index.output_rows(row)

        denominator: float | int | None = None
        retained: float | int | None = None
        basis: str | None = None
        filter_node: int | None = None

        if scanned is not None and output is not None and scanned > 0:
            denominator, retained, basis = scanned, output, "scanned_vs_output_rows"
        elif output is not None and output > 0:
            for parent_id in index.parent_ids(query_id, row["node_id"]):
                parent = index.node(query_id, parent_id)
                if parent is None:
                    continue
                if parent["node_type"] != NodeType.Filter:
                    continue
                parent_rows = index.output_rows(parent)
                if parent_rows is None:
                    continue
                denominator, retained = output, parent_rows
                basis, filter_node = "filter_above_scan", parent_id
                break

        if denominator is None or retained is None or denominator < min_rows:
            continue
        fraction = retained / denominator
        if fraction > retained_threshold:
            continue
        records.append(
            {
                "query_id": query_id,
                "node_id": row["node_id"],
                "node_name": row["node_name"],
                "paths": _get_scan_paths(row.get("details")),
                "basis": basis,
                "rows_in": int(denominator),
                "rows_out": int(retained),
                "retained_fraction": fraction,
                "filter_node_id": filter_node,
            }
        )

    schema = {
        "query_id": pl.Int64,
        "node_id": pl.Int64,
        "node_name": pl.String,
        "paths": pl.List(pl.String),
        "basis": pl.String,
        "rows_in": pl.Int64,
        "rows_out": pl.Int64,
        "retained_fraction": pl.Float64,
        "filter_node_id": pl.Int64,
    }
    if not records:
        return pl.DataFrame(schema=schema)
    return pl.DataFrame(records, schema_overrides=schema).sort("retained_fraction")


def find_aqe_adjustments(dfs: Dfs) -> pl.DataFrame:
    """
    Return AQE shuffle reads that changed partitioning at runtime.

    Only the final plan is retained by the parser, so the pre-AQE plan cannot be
    compared. What is observable is the adjustment the optimizer recorded on the
    surviving plan: skewed partitions it split, and partition counts it
    coalesced.
    """
    dag = dfs.dag
    index = NodeIndex(dag, _metric_source(dfs))
    records: list[dict[str, Any]] = []
    for row in dag.filter(pl.col("node_type") == NodeType.AQEShuffleRead).to_dicts():
        detail = _detail_dict(row.get("details")) or {}
        records.append(
            {
                "query_id": row["query_id"],
                "node_id": row["node_id"],
                "arguments": detail.get("arguments"),
                "partitions": index.metric(row, "partitions"),
                "skewed_partitions": index.metric(row, "skewed_partitions"),
            }
        )
    schema = {
        "query_id": pl.Int64,
        "node_id": pl.Int64,
        "arguments": pl.String,
        "partitions": pl.Float64,
        "skewed_partitions": pl.Float64,
    }
    if not records:
        return pl.DataFrame(schema=schema)
    return pl.DataFrame(records, schema_overrides=schema).sort("query_id", "node_id")


# ---------------------------------------------------------------------------
# rules
# ---------------------------------------------------------------------------


def _evidence(
    name: str, value: Any, unit: MetricUnit = MetricUnit.none, source: str | None = None
) -> EvidenceValue:
    return EvidenceValue(name=name, value=value, unit=unit, source=source)


def _scan_lineage(left: list[str], right: list[str]) -> str:
    left_names = [_path_name(path) for path in left]
    right_names = [_path_name(path) for path in right]
    if left_names and right_names:
        return f"left: {', '.join(left_names)}; right: {', '.join(right_names)}"
    if left_names or right_names:
        return f"scans: {', '.join(left_names + right_names)}"
    return "source scans unknown"


RuleFn = Callable[[Dfs], tuple[RuleAssessment, list[Finding]]]


def _assessment(
    rule_id: str,
    status: RuleStatus,
    findings: list[Finding],
    *,
    reason: str | None = None,
    entities: int = 0,
) -> RuleAssessment:
    return RuleAssessment(
        rule_id=rule_id,
        status=status,
        reason=reason,
        entities_evaluated=entities,
        findings=len(findings),
    )


def _rule_cartesian_join(dfs: Dfs) -> tuple[RuleAssessment, list[Finding]]:
    rule_id = "cartesian_join"
    joins = _join_rows(dfs.dag)
    if not joins:
        return (
            _assessment(
                rule_id,
                RuleStatus.evaluated,
                [],
                reason="No join operators in this capture.",
            ),
            [],
        )

    index = NodeIndex(dfs.dag, _metric_source(dfs))
    findings: list[Finding] = []
    for row in joins:
        kind, reason = _join_classification(row)
        if kind != "cartesian":
            continue
        query_id = row["query_id"]
        children = _child_ids(row)
        left_rows, _ = (
            index.input_rows(query_id, children[0]) if children else (None, None)
        )
        right_rows, _ = (
            index.input_rows(query_id, children[1])
            if len(children) > 1
            else (None, None)
        )
        output_rows = index.output_rows(row)
        detail = _detail_dict(row.get("details")) or {}

        evidence = [
            _evidence("join_type", detail.get("join_type")),
            _evidence("output_rows", output_rows, MetricUnit.rows),
            _evidence("left_input_rows", left_rows, MetricUnit.rows),
            _evidence("right_input_rows", right_rows, MetricUnit.rows),
        ]
        if left_rows is not None and right_rows is not None:
            evidence.append(
                _evidence(
                    "input_product",
                    int(left_rows) * int(right_rows),
                    MetricUnit.rows,
                    source="derived",
                )
            )
        scans = (
            _find_source_scans(dfs.dag, query_id, children[:1]),
            _find_source_scans(dfs.dag, query_id, children[1:2]),
        )
        lineage = _scan_lineage(
            [path for scan in scans[0] for path in scan["paths"]],
            [path for scan in scans[1] for path in scan["paths"]],
        )
        ratio = None
        known = [rows for rows in (left_rows, right_rows) if rows is not None]
        if output_rows is not None and known and max(known) > 0:
            ratio = output_rows / max(known)

        observation = (
            f"{row['node_type']} is a cartesian product ({reason})"
            f" and produced {_fmt_rows(output_rows)} rows"
        )
        if ratio is not None:
            observation += f" ({ratio:.1f}× its largest input)"
        observation += f"; {lineage}"

        findings.append(
            Finding(
                rule_id=rule_id,
                severity=FindingSeverity.critical,
                category="Cartesian Join",
                observation=observation,
                confidence=FindingConfidence.high
                if output_rows is not None
                else FindingConfidence.medium,
                query_id=query_id,
                node_ids=[row["node_id"]],
                evidence=evidence,
                caveat="A cartesian product is sometimes intended (small dimension "
                "cross products). Row counts are measured; intent is not.",
                next_investigation="Check the join predicate: an unqualified join "
                "key or a condition Spark could not turn into an equi-join forces "
                "this plan.",
            )
        )
    return (
        _assessment(rule_id, RuleStatus.evaluated, findings, entities=len(joins)),
        findings,
    )


def _rule_nested_loop_join(dfs: Dfs) -> tuple[RuleAssessment, list[Finding]]:
    rule_id = "nested_loop_join"
    joins = _join_rows(dfs.dag)
    index = NodeIndex(dfs.dag, _metric_source(dfs))
    findings: list[Finding] = []
    for row in joins:
        if _join_classification(row)[0] != "conditional_nested_loop":
            continue
        detail = _detail_dict(row.get("details")) or {}
        query_id = row["query_id"]
        children = _child_ids(row)
        left_rows, _ = (
            index.input_rows(query_id, children[0]) if children else (None, None)
        )
        right_rows, _ = (
            index.input_rows(query_id, children[1])
            if len(children) > 1
            else (None, None)
        )
        comparisons = (
            int(left_rows) * int(right_rows)
            if left_rows is not None and right_rows is not None
            else None
        )
        observation = (
            f"{row['node_type']} ({detail.get('join_type')}) evaluates a non-equi "
            f"condition, comparing every row pair"
        )
        if comparisons is not None:
            observation += f" (~{_fmt_rows(comparisons)} comparisons)"
        findings.append(
            Finding(
                rule_id=rule_id,
                severity=FindingSeverity.warning,
                category="Nested Loop Join",
                observation=observation,
                confidence=FindingConfidence.high,
                query_id=query_id,
                node_ids=[row["node_id"]],
                evidence=[
                    _evidence("join_type", detail.get("join_type")),
                    _evidence("join_condition", detail.get("join_condition")),
                    _evidence("left_input_rows", left_rows, MetricUnit.rows),
                    _evidence("right_input_rows", right_rows, MetricUnit.rows),
                    _evidence("comparisons", comparisons, MetricUnit.items, "derived"),
                ],
                caveat="This is not a cartesian product: the join condition does "
                "filter rows. It is quadratic in comparisons, not necessarily in "
                "output.",
                next_investigation="Check whether part of the condition can be "
                "expressed as an equality so Spark can use a hash or merge join.",
            )
        )
    return (
        _assessment(rule_id, RuleStatus.evaluated, findings, entities=len(joins)),
        findings,
    )


def _rule_join_row_expansion(dfs: Dfs) -> tuple[RuleAssessment, list[Finding]]:
    rule_id = "join_row_expansion"
    joins = _join_rows(dfs.dag)
    if not joins:
        return (
            _assessment(
                rule_id,
                RuleStatus.evaluated,
                [],
                reason="No join operators in this capture.",
            ),
            [],
        )

    explosions = find_row_count_explosions(dfs, ratio_threshold=1.1)
    if explosions.height == 0:
        index = NodeIndex(dfs.dag, _metric_source(dfs))
        measurable = any(
            index.output_rows(row) is not None
            and any(
                index.input_rows(row["query_id"], child)[0] is not None
                for child in _child_ids(row)
            )
            for row in joins
        )
        if not measurable:
            return (
                _assessment(
                    rule_id,
                    RuleStatus.insufficient_data,
                    [],
                    reason="No join reported both output rows and an immediate "
                    "input row count, so expansion cannot be computed.",
                    entities=len(joins),
                ),
                [],
            )

    findings: list[Finding] = []
    for row in explosions.to_dicts():
        if row["join_kind"] == "cartesian":
            # Reported by the cartesian rule; expansion there is definitional.
            continue
        keys_known = bool(row["left_keys"]) and bool(row["right_keys"])
        key_str = (
            f"keys {row['left_keys']} = {row['right_keys']}"
            if keys_known
            else "no equi-join keys"
        )
        lineage = _scan_lineage(row["left_scan_paths"], row["right_scan_paths"])
        resolved = row["inputs_resolved"]
        confidence = FindingConfidence.high if resolved else FindingConfidence.medium
        ratio_text = (
            f"{row['ratio']:.1f}× input"
            if resolved
            else f"at least {row['ratio']:.1f}× the one input side that reported rows"
        )
        findings.append(
            Finding(
                rule_id=rule_id,
                severity=FindingSeverity.critical
                if resolved
                else FindingSeverity.warning,
                category="Row Count Explosion",
                observation=(
                    f"{row['node_type']} produced {_fmt_rows(row['output_rows'])} rows"
                    f" ({ratio_text}) with {key_str}; {lineage}"
                ),
                confidence=confidence,
                query_id=row["query_id"],
                node_ids=[row["node_id"]],
                evidence=[
                    _evidence("join_type", row["join_type"]),
                    _evidence("output_rows", row["output_rows"], MetricUnit.rows),
                    _evidence(
                        "left_input_rows", row["left_input_rows"], MetricUnit.rows
                    ),
                    _evidence(
                        "right_input_rows", row["right_input_rows"], MetricUnit.rows
                    ),
                    _evidence("ratio", row["ratio"], MetricUnit.ratio, "derived"),
                ],
                threshold=_evidence("ratio", 1.1, MetricUnit.ratio),
                caveat="Expansion is observed from the join's immediate inputs. "
                "Duplicate keys are a hypothesis: outer joins and intentional "
                "many-to-many joins expand legitimately."
                + (
                    ""
                    if row["inputs_resolved"]
                    else " One input side had no measured row count, so the ratio "
                    "uses the side that did."
                ),
                next_investigation="Count distinct join keys on each side and "
                "compare with row counts to confirm duplicate keys.",
            )
        )
    return (
        _assessment(
            rule_id, RuleStatus.evaluated, findings, entities=explosions.height
        ),
        findings,
    )


def _rule_repeated_scan(dfs: Dfs) -> tuple[RuleAssessment, list[Finding]]:
    rule_id = "repeated_scan"
    repeated = find_repeated_scans(dfs)
    capabilities = resolve_capabilities(dfs)
    if capabilities.scan_details.status in (
        CapabilityStatus.unavailable,
        CapabilityStatus.unknown,
    ):
        return (
            _assessment(
                rule_id,
                RuleStatus.insufficient_data,
                [],
                reason="Scan details carry no source paths in this capture.",
            ),
            [],
        )

    findings: list[Finding] = []
    for row in repeated.to_dicts():
        same_projection = row["distinct_read_schemas"] == 1
        where = (
            "within a single query"
            if row["repeats_within_query"] and row["distinct_queries"] == 1
            else f"across {row['distinct_queries']} queries"
        )
        findings.append(
            Finding(
                rule_id=rule_id,
                severity=FindingSeverity.warning,
                category="Repeated Scan",
                observation=(
                    f"'{_path_name(row['path'])}' scanned {row['scan_count']}× {where}"
                ),
                confidence=FindingConfidence.high
                if same_projection
                else FindingConfidence.medium,
                query_id=row["query_ids"][0] if row["distinct_queries"] == 1 else None,
                node_ids=list(row["node_ids"]),
                evidence=[
                    _evidence("path", row["path"]),
                    _evidence("scan_count", row["scan_count"], MetricUnit.items),
                    _evidence(
                        "distinct_queries", row["distinct_queries"], MetricUnit.items
                    ),
                    _evidence(
                        "distinct_read_schemas",
                        row["distinct_read_schemas"],
                        MetricUnit.items,
                    ),
                ],
                caveat="Caching is a candidate, not an automatic fix: it trades "
                "memory for re-read cost, and"
                + (
                    " these scans project the same columns."
                    if same_projection
                    else " these scans project different columns, so a cache would "
                    "hold more than any one of them needs."
                ),
                next_investigation="Compare the re-read cost with the memory a "
                "cached copy would hold before persisting.",
            )
        )
    return (
        _assessment(rule_id, RuleStatus.evaluated, findings, entities=repeated.height),
        findings,
    )


def _rule_scan_efficiency(dfs: Dfs) -> tuple[RuleAssessment, list[Finding]]:
    rule_id = "scan_efficiency"
    scans = dfs.dag.filter(pl.col("node_type").is_in(_SOURCE_SCAN_TYPES))
    if scans.height == 0:
        return (
            _assessment(rule_id, RuleStatus.evaluated, [], reason="No scan operators."),
            [],
        )

    inefficient = find_inefficient_scans(dfs)
    index = NodeIndex(dfs.dag, _metric_source(dfs))
    measurable = any(
        index.metric(row, "scanned_rows") is not None
        or index.output_rows(row) is not None
        for row in scans.to_dicts()
    )
    if not measurable:
        return (
            _assessment(
                rule_id,
                RuleStatus.insufficient_data,
                [],
                reason="No scan reported row counts, so no pruning denominator "
                "exists. A pruning verdict without one would be a guess.",
                entities=scans.height,
            ),
            [],
        )

    findings: list[Finding] = []
    for row in inefficient.to_dicts():
        basis_text = (
            "the scan read far more rows than it emitted"
            if row["basis"] == "scanned_vs_output_rows"
            else "the filter directly above the scan removed most rows"
        )
        findings.append(
            Finding(
                rule_id=rule_id,
                severity=FindingSeverity.warning,
                category="Scan Efficiency",
                observation=(
                    f"Scan of '{', '.join(_path_name(p) for p in row['paths']) or '?'}'"
                    f" kept {row['retained_fraction'] * 100:.1f}% of"
                    f" {_fmt_rows(row['rows_in'])} rows — {basis_text}"
                ),
                confidence=FindingConfidence.medium,
                query_id=row["query_id"],
                node_ids=[row["node_id"]]
                + ([row["filter_node_id"]] if row["filter_node_id"] else []),
                evidence=[
                    _evidence("rows_in", row["rows_in"], MetricUnit.rows),
                    _evidence("rows_out", row["rows_out"], MetricUnit.rows),
                    _evidence(
                        "retained_fraction",
                        row["retained_fraction"],
                        MetricUnit.ratio,
                        "derived",
                    ),
                    _evidence("basis", row["basis"]),
                ],
                threshold=_evidence("retained_fraction", 0.1, MetricUnit.ratio),
                caveat="Pushed-down and partition filters are not recorded in the "
                "parsed scan detail, so this does not prove the filter was not "
                "pushed down — only that most rows read were discarded.",
                next_investigation="Check whether the filter columns are partition "
                "columns or support predicate pushdown for this file format.",
            )
        )
    return (
        _assessment(rule_id, RuleStatus.evaluated, findings, entities=scans.height),
        findings,
    )


def _rule_spill(dfs: Dfs) -> tuple[RuleAssessment, list[Finding]]:
    rule_id = "spill"
    findings: list[Finding] = []
    task_spill = find_spill(dfs)
    entities = measured_stages(dfs, ("memory_bytes_spilled", "disk_bytes_spilled"))
    if _has_tasks(dfs) and entities == 0:
        return (
            _assessment(
                rule_id,
                RuleStatus.insufficient_data,
                [],
                reason="Task rows are present but none reported spill counters, "
                "so spill cannot be ruled in or out.",
            ),
            [],
        )

    for row in task_spill.to_dicts():
        memory, disk = row["memory_bytes_spilled"], row["disk_bytes_spilled"]
        parts = []
        if memory:
            parts.append(f"{_fmt_bytes(memory)} in-memory")
        if disk:
            parts.append(f"{_fmt_bytes(disk)} on disk")
        findings.append(
            Finding(
                rule_id=rule_id,
                severity=FindingSeverity.critical,
                category="Spill",
                observation=(
                    f"{' and '.join(parts)} spilled by {row['task_count']} tasks in"
                    f" query {row['query_id']} stage {row['stage_id']}"
                ),
                confidence=FindingConfidence.high,
                query_id=row["query_id"],
                stage_id=row["stage_id"],
                evidence=[
                    _evidence(
                        "memory_bytes_spilled",
                        memory,
                        MetricUnit.bytes,
                        SOURCE_TASK_METRICS,
                    ),
                    _evidence(
                        "disk_bytes_spilled",
                        disk,
                        MetricUnit.bytes,
                        SOURCE_TASK_METRICS,
                    ),
                    _evidence("task_count", row["task_count"], MetricUnit.items),
                ],
                caveat="Memory and disk spill are reported separately: they "
                "describe the same records at different stages and are not "
                "disjoint byte totals to be added.",
                next_investigation="Compare partition sizes with executor memory "
                "for this stage.",
            )
        )

    if not _has_tasks(dfs):
        operator_spill = find_operator_spill(dfs)
        entities = operator_spill.height
        for row in operator_spill.to_dicts():
            findings.append(
                Finding(
                    rule_id=rule_id,
                    severity=FindingSeverity.critical,
                    category="Spill",
                    observation=(
                        f"{row['node_type']} spilled {_fmt_bytes(row['spill_bytes'])}"
                        f" in query {row['query_id']}"
                    ),
                    confidence=FindingConfidence.medium,
                    query_id=row["query_id"],
                    node_ids=[row["node_id"]],
                    evidence=[
                        _evidence(
                            "spill_bytes",
                            row["spill_bytes"],
                            MetricUnit.bytes,
                            "operator_metric",
                        )
                    ],
                    caveat="Operator-level spill with an unspecified storage "
                    "medium. Task metrics are unavailable in this capture, so no "
                    "stage or task distribution can be given.",
                    next_investigation="Re-run on compute that exposes task "
                    "metrics if per-partition attribution is needed.",
                )
            )
        if operator_spill.height == 0:
            operator_metrics = resolve_capabilities(dfs).operator_metrics.status
            if operator_metrics == CapabilityStatus.available:
                return (
                    _assessment(
                        rule_id,
                        RuleStatus.evaluated,
                        findings,
                        reason="Task metrics are unavailable; operator spill "
                        "metrics were used and reported no spill.",
                    ),
                    findings,
                )
            return (
                _assessment(
                    rule_id,
                    RuleStatus.insufficient_data,
                    findings,
                    reason="Task metrics are unavailable and no operator spill "
                    "metric was reported, so spill cannot be ruled in or out.",
                ),
                findings,
            )

    return (
        _assessment(rule_id, RuleStatus.evaluated, findings, entities=entities),
        findings,
    )


def _rule_shuffle_volume(dfs: Dfs) -> tuple[RuleAssessment, list[Finding]]:
    rule_id = "shuffle_volume"
    threshold = 2**30
    findings: list[Finding] = []

    if _has_tasks(dfs):
        assessed = measured_stages(dfs, ("shuffle_bytes_written", "shuffle_bytes_read"))
        if assessed == 0:
            return (
                _assessment(
                    rule_id,
                    RuleStatus.insufficient_data,
                    [],
                    reason="Task rows are present but none reported shuffle byte "
                    "counters, so shuffle volume cannot be assessed.",
                ),
                [],
            )
        stages = find_shuffle_heavy_stages(dfs, threshold_bytes=threshold)
        for row in stages.to_dicts():
            findings.append(
                Finding(
                    rule_id=rule_id,
                    severity=FindingSeverity.warning,
                    category="Heavy Shuffle",
                    observation=(
                        f"{_fmt_bytes(row['shuffle_write_bytes'])} written /"
                        f" {_fmt_bytes(row['shuffle_read_bytes'])} read in shuffle"
                        f" for query {row['query_id']} stage {row['stage_id']}"
                    ),
                    confidence=FindingConfidence.high,
                    query_id=row["query_id"],
                    stage_id=row["stage_id"],
                    evidence=[
                        _evidence(
                            "shuffle_write_bytes",
                            row["shuffle_write_bytes"],
                            MetricUnit.bytes,
                            SOURCE_TASK_METRICS,
                        ),
                        _evidence(
                            "shuffle_read_bytes",
                            row["shuffle_read_bytes"],
                            MetricUnit.bytes,
                            SOURCE_TASK_METRICS,
                        ),
                    ],
                    threshold=_evidence("max_side_bytes", threshold, MetricUnit.bytes),
                    caveat="Read and write describe the two sides of the same data "
                    "movement and are not added together.",
                    next_investigation="Check whether a broadcast join or "
                    "pre-partitioned source removes this exchange.",
                )
            )
        return (
            _assessment(rule_id, RuleStatus.evaluated, findings, entities=assessed),
            findings,
        )

    index = NodeIndex(dfs.dag, _metric_source(dfs))
    entities = 0
    for row in dfs.dag.to_dicts():
        written = index.metric(row, "shuffle_write_bytes")
        if written is None:
            continue
        entities += 1
        if written < threshold:
            continue
        findings.append(
            Finding(
                rule_id=rule_id,
                severity=FindingSeverity.warning,
                category="Heavy Shuffle",
                observation=(
                    f"{row['node_type']} wrote {_fmt_bytes(written)} to shuffle in"
                    f" query {row['query_id']}"
                ),
                confidence=FindingConfidence.medium,
                query_id=row["query_id"],
                node_ids=[row["node_id"]],
                evidence=[
                    _evidence(
                        "shuffle_write_bytes",
                        written,
                        MetricUnit.bytes,
                        "operator_metric",
                    )
                ],
                caveat="Operator-scoped shuffle bytes. Task metrics are "
                "unavailable, so there is no stage attribution.",
                next_investigation="Inspect the exchange's partitioning to see "
                "whether the redistribution is required.",
            )
        )
    if entities == 0:
        return (
            _assessment(
                rule_id,
                RuleStatus.unsupported,
                findings,
                reason="Task metrics are unavailable and no operator reported "
                "shuffle bytes in this capture.",
            ),
            findings,
        )
    return (
        _assessment(rule_id, RuleStatus.evaluated, findings, entities=entities),
        findings,
    )


def _rule_task_straggler(dfs: Dfs) -> tuple[RuleAssessment, list[Finding]]:
    rule_id = "task_straggler"
    if not _has_tasks(dfs):
        return (
            _assessment(
                rule_id,
                RuleStatus.unsupported,
                [],
                reason="Task-level timing is not available from this capture "
                "source, so per-task duration cannot be compared.",
            ),
            [],
        )

    assessed = measured_stages(dfs, ("task_duration_seconds",))
    if assessed == 0:
        return (
            _assessment(
                rule_id,
                RuleStatus.insufficient_data,
                [],
                reason="Task rows are present but none reported a duration, so "
                "task times cannot be compared.",
            ),
            [],
        )
    skewed = find_skewed_tasks(dfs, skew_ratio=5.0)
    findings: list[Finding] = []
    for row in skewed.to_dicts():
        bytes_ratio = row["bytes_ratio"]
        size_evidence = bytes_ratio is not None and bytes_ratio >= 2.0
        category = "Data Skew" if size_evidence else "Task Straggler"
        observation = (
            f"Slowest task ran {row['skew_ratio']:.1f}× the stage median"
            f" (p50 {row['median_task_s']:.1f}s, p95 {row['p95_task_s']:.1f}s,"
            f" max {row['max_task_s']:.1f}s) in query {row['query_id']}"
            f" stage {row['stage_id']}"
        )
        if size_evidence:
            observation += (
                f"; it also read {bytes_ratio:.1f}× the median task's input bytes"
            )
        findings.append(
            Finding(
                rule_id=rule_id,
                severity=FindingSeverity.warning,
                category=category,
                observation=observation,
                confidence=FindingConfidence.high
                if size_evidence
                else FindingConfidence.medium,
                query_id=row["query_id"],
                stage_id=row["stage_id"],
                evidence=[
                    _evidence(
                        "median_task_s", row["median_task_s"], MetricUnit.seconds
                    ),
                    _evidence("p95_task_s", row["p95_task_s"], MetricUnit.seconds),
                    _evidence("max_task_s", row["max_task_s"], MetricUnit.seconds),
                    _evidence("skew_ratio", row["skew_ratio"], MetricUnit.ratio),
                    _evidence(
                        "max_task_bytes", row["max_task_bytes"], MetricUnit.bytes
                    ),
                    _evidence(
                        "median_task_bytes",
                        row["median_task_bytes"],
                        MetricUnit.bytes,
                    ),
                    _evidence("task_count", row["task_count"], MetricUnit.items),
                ],
                threshold=_evidence("skew_ratio", 5.0, MetricUnit.ratio),
                caveat="A long task is a straggler. Calling it data skew needs the "
                "size evidence"
                + (
                    "; it is present here."
                    if size_evidence
                    else ", which this stage does not show — a slow executor or "
                    "retry can produce the same duration profile."
                ),
                next_investigation="Inspect the partition key distribution feeding "
                "this stage, and whether the slow task was a retry or speculative "
                "attempt.",
            )
        )
    return (
        _assessment(rule_id, RuleStatus.evaluated, findings, entities=assessed),
        findings,
    )


def _rule_gc_overhead(dfs: Dfs) -> tuple[RuleAssessment, list[Finding]]:
    rule_id = "gc_overhead"
    if not _has_tasks(dfs):
        return (
            _assessment(
                rule_id,
                RuleStatus.unsupported,
                [],
                reason="JVM GC and executor run time are task metrics; this "
                "capture source does not supply them.",
            ),
            [],
        )
    measured = gc_measured_stages(dfs)
    if measured.height == 0:
        return (
            _assessment(
                rule_id,
                RuleStatus.insufficient_data,
                [],
                reason="Task rows are present but no task reported both JVM GC "
                "time and executor run time, so GC share cannot be computed.",
            ),
            [],
        )
    stages = find_gc_overhead(dfs, ratio_threshold=0.1)
    findings: list[Finding] = []
    for row in stages.to_dicts():
        findings.append(
            Finding(
                rule_id=rule_id,
                severity=FindingSeverity.warning,
                category="GC Overhead",
                observation=(
                    f"JVM GC took {row['gc_ratio'] * 100:.0f}% of executor run time"
                    f" ({row['jvm_gc_time_seconds']:.1f}s of"
                    f" {row['executor_run_time_seconds']:.1f}s) in query"
                    f" {row['query_id']} stage {row['stage_id']}"
                ),
                confidence=FindingConfidence.high,
                query_id=row["query_id"],
                stage_id=row["stage_id"],
                evidence=[
                    _evidence(
                        "jvm_gc_time_seconds",
                        row["jvm_gc_time_seconds"],
                        MetricUnit.seconds,
                        SOURCE_TASK_METRICS,
                    ),
                    _evidence(
                        "executor_run_time_seconds",
                        row["executor_run_time_seconds"],
                        MetricUnit.seconds,
                        SOURCE_TASK_METRICS,
                    ),
                    _evidence("gc_ratio", row["gc_ratio"], MetricUnit.ratio, "derived"),
                ],
                threshold=_evidence("gc_ratio", 0.1, MetricUnit.ratio),
                caveat="GC time overlaps task execution and is summed across "
                "tasks, so this is a share of executor time, not of elapsed time.",
                next_investigation="Check executor heap sizing and whether the "
                "stage holds large objects (broadcast, collected results).",
            )
        )
    return (
        _assessment(rule_id, RuleStatus.evaluated, findings, entities=measured.height),
        findings,
    )


def _rule_aqe_plan_change(dfs: Dfs) -> tuple[RuleAssessment, list[Finding]]:
    rule_id = "aqe_plan_change"
    dag = dfs.dag
    if dag.height == 0 or "node_type" not in dag.columns:
        return (
            _assessment(
                rule_id, RuleStatus.unsupported, [], reason="No plan is available."
            ),
            [],
        )
    adaptive = dag.filter(pl.col("node_type") == NodeType.AdaptiveSparkPlan)
    adjustments = find_aqe_adjustments(dfs)
    if adaptive.height == 0 and adjustments.height == 0:
        return (
            _assessment(
                rule_id,
                RuleStatus.unsupported,
                [],
                reason="The plan carries no adaptive execution markers, so there "
                "is no runtime re-planning to report.",
            ),
            [],
        )

    findings: list[Finding] = []
    for row in adjustments.to_dicts():
        skewed = row["skewed_partitions"]
        partitions = row["partitions"]
        if skewed is not None and skewed > 0:
            findings.append(
                Finding(
                    rule_id=rule_id,
                    severity=FindingSeverity.warning,
                    category="AQE Skew Split",
                    observation=(
                        f"AQE split {int(skewed)} skewed partition(s) at runtime in"
                        f" query {row['query_id']}"
                    ),
                    confidence=FindingConfidence.high,
                    query_id=row["query_id"],
                    node_ids=[row["node_id"]],
                    evidence=[
                        _evidence("skewed_partitions", int(skewed), MetricUnit.items),
                        _evidence("aqe_arguments", row["arguments"]),
                    ],
                    caveat="AQE handled the skew, so the query completed. The skew "
                    "in the source data remains.",
                    next_investigation="Look at the join or aggregation key "
                    "distribution feeding this exchange.",
                )
            )
        elif partitions is not None and partitions == 1:
            findings.append(
                Finding(
                    rule_id=rule_id,
                    severity=FindingSeverity.warning,
                    category="AQE Coalesce",
                    observation=(
                        f"AQE coalesced this shuffle to a single partition in query"
                        f" {row['query_id']}, removing parallelism downstream"
                    ),
                    confidence=FindingConfidence.medium,
                    query_id=row["query_id"],
                    node_ids=[row["node_id"]],
                    evidence=[
                        _evidence("partitions", 1, MetricUnit.items),
                        _evidence("aqe_arguments", row["arguments"]),
                    ],
                    caveat="A single partition is correct when the data is small. "
                    "It only matters if expensive work follows this exchange.",
                    next_investigation="Check the size of the operators above this "
                    "exchange.",
                )
            )

    reason = (
        "Only the final plan is retained by the parser, so initial and final "
        "plans cannot be compared. Reported adjustments come from metrics the "
        "optimizer recorded on the surviving plan."
    )
    return (
        _assessment(
            rule_id,
            RuleStatus.evaluated,
            findings,
            reason=reason,
            entities=adjustments.height,
        ),
        findings,
    )


RULES: tuple[RuleFn, ...] = (
    _rule_cartesian_join,
    _rule_join_row_expansion,
    _rule_nested_loop_join,
    _rule_spill,
    _rule_repeated_scan,
    _rule_scan_efficiency,
    _rule_task_straggler,
    _rule_shuffle_volume,
    _rule_gc_overhead,
    _rule_aqe_plan_change,
)

_SEVERITY_ORDER = {FindingSeverity.critical: 0, FindingSeverity.warning: 1}


def analyze_dfs(dfs: Dfs, log_name: str = "capture") -> AnalysisReport:
    """Run every rule and return findings alongside per-rule assessment status.

    A rule that produced no findings is still listed, with the status that says
    whether it ran, could not run on this source, or lacked the data it needed.
    """
    findings: list[Finding] = []
    assessments: list[RuleAssessment] = []
    for rule in RULES:
        assessment, rule_findings = rule(dfs)
        assessments.append(assessment)
        findings.extend(rule_findings)
    findings.sort(key=lambda f: (_SEVERITY_ORDER[f.severity], f.rule_id))
    return AnalysisReport(
        schema_version=ANALYSIS_SCHEMA_VERSION,
        log_name=log_name,
        findings=findings,
        assessments=assessments,
    )


_REDACTED_EVIDENCE_NAMES = frozenset(
    {"path", "join_condition", "left_keys", "right_keys"}
)


def _workload_literals(dag: pl.DataFrame) -> list[str]:
    """Return workload-specific strings that appear in finding text.

    Paths, their basenames, join keys and conditions identify the data being
    processed, so redaction has to cover them wherever they were interpolated.
    """
    literals: set[str] = set()
    if dag.height == 0 or "details" not in dag.columns:
        return []
    for details_str in dag.get_column("details").to_list():
        detail = _detail_dict(details_str)
        if not detail:
            continue
        for path in _get_scan_paths(details_str):
            literals.add(path)
            literals.add(_path_name(path))
        for key in ("raw", "raw_name"):
            # Connect stores the server's operator name for unmapped operators.
            value = detail.get(key)
            if isinstance(value, str) and value:
                literals.add(value)
        for key in ("join_condition", "read_schema"):
            value = detail.get(key)
            if isinstance(value, str) and value:
                literals.add(value)
        for key in ("left_keys", "right_keys"):
            values = detail.get(key)
            if isinstance(values, list):
                literals.update(str(value) for value in values if value)
    return sorted(literals, key=len, reverse=True)


def to_analysis_export(
    dfs: Dfs, log_name: str = "capture", *, redact: bool = False
) -> dict[str, Any]:
    """Return the analysis report as JSON-ready data, optionally redacted.

    Redaction replaces workload literals (paths, join keys and conditions) with
    stable hashed tokens while leaving the surrounding wording intact, so a
    report stays readable when it is shared.
    """
    report = analyze_dfs(dfs, log_name)
    data = report.model_dump(mode="json")
    if not redact:
        return data

    redactor = Redactor(True)
    literals = _workload_literals(dfs.dag)
    for finding in data["findings"]:
        observation = finding["observation"]
        for literal in literals:
            if literal in observation:
                observation = observation.replace(literal, str(redactor.expr(literal)))
        finding["observation"] = observation
        for evidence in finding["evidence"]:
            if evidence["name"] in _REDACTED_EVIDENCE_NAMES:
                evidence["value"] = redactor.expr(evidence["value"])
    return data


def get_issues(dfs: Dfs) -> list[dict[str, Any]]:
    """
    Return findings as flat dicts for the dashboard and other simple consumers.

    Each issue has ``severity`` ("critical" | "warning"), ``category``,
    ``message``, ``query_id`` and ``stage_id``, plus ``rule_id``, ``confidence``
    and ``caveat`` from the underlying finding.
    """
    report = analyze_dfs(dfs)
    return [
        {
            "severity": finding.severity.value,
            "category": finding.category,
            "message": finding.observation,
            "query_id": finding.query_id,
            "stage_id": finding.stage_id,
            "rule_id": finding.rule_id,
            "confidence": finding.confidence.value,
            "caveat": finding.caveat,
        }
        for finding in report.findings
    ]


def get_coverage_notes(dfs: Dfs) -> list[dict[str, Any]]:
    """
    Return one entry per rule that could not be evaluated.

    Used by the dashboard so missing telemetry is visible rather than silently
    rendering an empty issue list.
    """
    report = analyze_dfs(dfs)
    return [
        {
            "rule_id": assessment.rule_id,
            "status": assessment.status.value,
            "reason": assessment.reason,
        }
        for assessment in report.assessments
        if assessment.status != RuleStatus.evaluated
    ]


def summarize_durations(values: Iterable[float]) -> dict[str, float | None]:
    """Return p50/p95/max for a sequence of durations, or nulls when empty."""
    data = [value for value in values if value is not None]
    if not data:
        return {"p50": None, "p95": None, "max": None}
    data.sort()
    return {
        "p50": statistics.median(data),
        "p95": data[min(len(data) - 1, int(round(0.95 * (len(data) - 1))))],
        "max": data[-1],
    }
