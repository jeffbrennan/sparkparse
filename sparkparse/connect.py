"""SparkConnectCapture — metric capture for Spark Connect (including Databricks serverless).

The adapter intercepts a small set of Spark Connect client methods:

- action boundaries (``to_table``, ``to_pandas``, ``to_table_as_iterator``,
  ``execute_command``, ``execute_command_as_iterator``) delimit one execution and
  supply the logical plan plus client-observed elapsed time;
- ``_build_metrics`` yields the per-operator ``PlanMetrics`` of the active execution;
- ``_execute_plan_request_with_metadata`` supplies the server operation ID used as
  the execution's provenance identifier.

Everything is attributed per execution and per thread: metrics are never matched to
plans by list position, and join keys are only attached when a plan-ID match (or an
unambiguous single-join mapping) supports them.
"""

from __future__ import annotations

import dataclasses
import datetime
import inspect
import json
import logging
import re
import threading
import time
from collections import defaultdict
from typing import TYPE_CHECKING, Any, Literal

import polars as pl

from sparkparse.models import CaptureDiagnostic, NodeType, ParsedLogDataFrames
from sparkparse.schemas import (
    COMBINED_SCHEMA,
    DAG_SCHEMA,
    empty_capture_dataframes,
)

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

_log = logging.getLogger(__name__)

_TS_FORMAT = "%Y-%m-%dT%H:%M:%S"

# Databricks Photon and Spark Connect operator names that differ from the canonical
# NodeType spelling. Names that already match a NodeType member are resolved without
# an entry here, so this map only carries genuine aliases.
_NODE_TYPE_ALIASES: dict[str, NodeType] = {
    "PhotonScan": NodeType.Scan,
    "PhotonGroupingAgg": NodeType.HashAggregate,
    "PhotonAgg": NodeType.HashAggregate,
    "PhotonSort": NodeType.Sort,
    "PhotonShuffleExchangeSink": NodeType.Exchange,
    "PhotonShuffleExchangeSource": NodeType.Exchange,
    "PhotonShuffleMapStage": NodeType.ShuffleQueryStage,
    "PhotonResultStage": NodeType.ResultQueryStage,
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
    "PhotonColumnarToRow": NodeType.ColumnarToRow,
}

_JOIN_NODE_TYPES: frozenset[NodeType] = frozenset(
    {
        NodeType.BroadcastHashJoin,
        NodeType.SortMergeJoin,
        NodeType.BroadcastNestedLoopJoin,
        NodeType.CartesianProduct,
    }
)

_JOIN_TYPES: frozenset[str] = frozenset(
    {
        "Inner",
        "LeftOuter",
        "RightOuter",
        "FullOuter",
        "LeftSemi",
        "LeftAnti",
        "Cross",
    }
)

# Spark Connect JoinType proto enum → display string.
# Values from spark/connect/proto/relations.proto.
_PROTO_JOIN_TYPE: dict[int, str] = {
    1: "Inner",
    2: "FullOuter",
    3: "LeftOuter",
    4: "RightOuter",
    5: "LeftAnti",
    6: "LeftSemi",
    7: "Cross",
}

_BRACKET_RE = re.compile(r"\[([^\]]*)\]")
_EXPR_ID_RE = re.compile(r"#\w+")

# Client methods that delimit one execution, mapped to how the return value is consumed.
_ACTION_METHODS: dict[str, Literal["value", "iterator"]] = {
    "to_table": "value",
    "to_pandas": "value",
    "to_table_as_iterator": "iterator",
    "execute_command": "value",
    "execute_command_as_iterator": "iterator",
}
# Action methods whose first argument is a Plan (and therefore carries a logical plan).
_PLAN_ACTION_METHODS = frozenset({"to_table", "to_pandas", "to_table_as_iterator"})


@dataclasses.dataclass(frozen=True)
class CapturedMetric:
    """One operator metric as reported by the Connect server."""

    name: str
    value: float | int
    metric_type: str


@dataclasses.dataclass(frozen=True)
class CapturedNode:
    """One physical-plan node of a single execution."""

    plan_id: int
    parent_plan_id: int
    name: str
    metrics: tuple[CapturedMetric, ...]


@dataclasses.dataclass
class ConnectExecution:
    """One intercepted action boundary and the metrics attributed to it."""

    index: int
    action: str
    thread_id: int
    start_wall: datetime.datetime
    start_monotonic: float | None
    end_wall: datetime.datetime | None = None
    elapsed_seconds: float | None = None
    operation_id: str | None = None
    plan: Any | None = None
    nodes: dict[int, CapturedNode] = dataclasses.field(default_factory=dict)
    metric_batches: int = 0
    concurrent: bool = False
    attributed: bool = True

    def summary(self) -> dict[str, Any]:
        return {
            "query_id": self.index,
            "action": self.action,
            "operation_id": self.operation_id,
            "start": self.start_wall.strftime(_TS_FORMAT),
            "elapsed_seconds": self.elapsed_seconds,
            "n_nodes": len(self.nodes),
            "metric_batches": self.metric_batches,
            "concurrent": self.concurrent,
            "attributed": self.attributed,
        }


@dataclasses.dataclass(frozen=True)
class ConnectSupport:
    """Result of probing an installed client for capture support."""

    client_version: str | None
    hookable_actions: tuple[str, ...]
    missing_actions: tuple[str, ...]
    metrics_hook: bool
    metrics_signature_verified: bool | None
    execution_info_api: bool | None
    notes: tuple[str, ...]

    def as_dict(self) -> dict[str, Any]:
        return dataclasses.asdict(self)


def probe_connect_support(spark: Any) -> ConnectSupport:
    """Report what a Spark Connect client exposes for metric capture.

    ``DataFrame.executionInfo`` is a documented Connect-only public API, but it is
    scoped to a single DataFrame that has already been executed: it cannot observe
    SQL commands, writes, or actions run through other objects. The adapter therefore
    keeps client-level interception and only records the public API's availability.
    """
    notes: list[str] = []
    client = getattr(spark, "_client", None)

    client_version: str | None = None
    try:
        import pyspark

        client_version = str(getattr(pyspark, "__version__", "") or "") or None
    except Exception:  # pragma: no cover - pyspark is a hard runtime dependency
        notes.append("pyspark version could not be read.")

    hookable: list[str] = []
    missing: list[str] = []
    for name in _ACTION_METHODS:
        if callable(getattr(client, name, None)):
            hookable.append(name)
        else:
            missing.append(name)

    build_metrics = getattr(client, "_build_metrics", None)
    metrics_hook = callable(build_metrics)
    metrics_signature_verified: bool | None = None
    if not metrics_hook:
        notes.append(
            "client._build_metrics is unavailable; operator metrics cannot be captured."
        )
    else:
        try:
            required = [
                parameter
                for parameter in inspect.signature(build_metrics).parameters.values()
                if parameter.default is parameter.empty
                and parameter.kind
                in (parameter.POSITIONAL_ONLY, parameter.POSITIONAL_OR_KEYWORD)
            ]
            metrics_signature_verified = len(required) == 1
        except (TypeError, ValueError):
            metrics_signature_verified = None
        if metrics_signature_verified is not True:
            notes.append(
                "client._build_metrics does not have the expected single-argument "
                "signature; the hook forwards arguments unchanged."
            )
    if missing:
        notes.append(
            "Actions without a hook are not covered: " + ", ".join(sorted(missing))
        )

    execution_info_api: bool | None
    try:
        from pyspark.sql.connect.dataframe import DataFrame as ConnectDataFrame

        execution_info_api = hasattr(ConnectDataFrame, "executionInfo")
    except Exception as exc:
        execution_info_api = None
        notes.append(f"DataFrame.executionInfo could not be probed: {exc}")
    if execution_info_api:
        notes.append(
            "DataFrame.executionInfo is available but covers only actions executed "
            "through that DataFrame; client interception is used for full coverage."
        )

    return ConnectSupport(
        client_version=client_version,
        hookable_actions=tuple(hookable),
        missing_actions=tuple(missing),
        metrics_hook=metrics_hook,
        metrics_signature_verified=metrics_signature_verified,
        execution_info_api=execution_info_api,
        notes=tuple(notes),
    )


def _plan_id(message: Any) -> int | None:
    try:
        common = message.common
        if hasattr(common, "HasField") and not common.HasField("plan_id"):
            return None
        plan_id = common.plan_id
    except Exception:
        return None
    return int(plan_id) if isinstance(plan_id, int) else None


def _is_relation(value: Any) -> bool:
    if not hasattr(value, "WhichOneof"):
        return False
    try:
        value.WhichOneof("rel_type")
    except Exception:
        return False
    return True


def _child_relations(message: Any) -> list[Any]:
    """Return every sub-message of ``message`` that is itself a Relation."""
    children: list[Any] = []
    try:
        fields = message.ListFields()
    except Exception:
        return children
    for _, value in fields:
        candidates = value if isinstance(value, list | tuple) else [value]
        try:
            iterator = list(candidates)
        except TypeError:
            continue
        children.extend(item for item in iterator if _is_relation(item))
    return children


def _condition_columns(expression: Any) -> list[str]:
    """Collect unresolved attribute names referenced by a join condition."""
    names: list[str] = []
    try:
        which = expression.WhichOneof("expr_type")
    except Exception:
        return names
    if which == "unresolved_attribute":
        name = getattr(expression.unresolved_attribute, "unparsed_identifier", "")
        if name:
            names.append(str(name))
        return names
    if which == "unresolved_function":
        for argument in expression.unresolved_function.arguments:
            names.extend(_condition_columns(argument))
    return names


def _extract_join_info(relation: Any) -> list[dict[str, Any]]:
    """DFS walk of a Spark Connect Relation proto returning one entry per join.

    Each entry carries the join's client-assigned ``plan_id`` when the proto supplies
    one. ``using_columns`` (equi-join on shared names) resolves left and right keys;
    a ``join_condition`` expression yields the referenced columns without asserting
    which side each belongs to.
    """
    results: list[dict[str, Any]] = []
    if not _is_relation(relation):
        return results
    which = relation.WhichOneof("rel_type")
    if which is None:
        return results

    if which == "join":
        join = relation.join
        using_columns = [str(column) for column in join.using_columns]
        entry: dict[str, Any] = {
            "plan_id": _plan_id(relation),
            "join_type": _PROTO_JOIN_TYPE.get(join.join_type, ""),
        }
        if using_columns:
            entry["left_keys"] = using_columns
            entry["right_keys"] = using_columns
            entry["join_keys_resolved"] = True
        else:
            columns = _condition_columns(getattr(join, "join_condition", None))
            entry["join_keys_resolved"] = False
            if columns:
                entry["join_condition_columns"] = columns
        results.append(entry)

    inner = getattr(relation, which, None)
    if inner is not None:
        for child in _child_relations(inner):
            results.extend(_extract_join_info(child))
    return results


def _parse_join_details(name: str) -> dict[str, Any]:
    """Extract left/right keys and join type from an OSS Spark node name string.

    OSS Spark names look like:
        BroadcastHashJoin [left_col#id], [right_col#id], Inner, BuildRight
    Photon node names on Databricks carry no key info — use _extract_join_info instead.
    """
    groups = _BRACKET_RE.findall(name)

    def clean(group: str) -> list[str]:
        return [_EXPR_ID_RE.sub("", k).strip() for k in group.split(",") if k.strip()]

    result: dict[str, Any] = {}
    if len(groups) >= 2:
        result["left_keys"] = clean(groups[0])
        result["right_keys"] = clean(groups[1])
        result["join_keys_resolved"] = True

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


def _exact_value(raw_value: float | int, metric_type: str) -> int | None:
    """Return the metric's exact integer value, when it has one.

    ``nsTiming`` metrics are rescaled to milliseconds for display, so no integer
    survives that conversion and this returns ``None``.
    """
    if metric_type == "nsTiming" or isinstance(raw_value, bool):
        return None
    if isinstance(raw_value, int):
        return raw_value
    if isinstance(raw_value, float) and raw_value.is_integer():
        return int(raw_value)
    return None


def _build_accum_struct(metric: CapturedMetric) -> dict[str, Any]:
    norm_type, value, readable_value, readable_unit, readable_str = _convert_metric(
        metric.value, metric.metric_type
    )
    return {
        "metric_name": metric.name,
        "metric_type": norm_type,
        "value": value,
        "value_exact": _exact_value(metric.value, metric.metric_type),
        "readable_value": readable_value,
        "readable_unit": readable_unit,
        "readable_str": readable_str,
    }


def _numeric(raw_value: Any) -> float | int:
    """Return a metric value as a number, keeping integers out of float64.

    Row counts arrive as integers and can exceed 2**53; converting them to float
    here would round them before anything downstream could preserve them.
    """
    if isinstance(raw_value, bool):
        raise TypeError("metric value is a boolean")
    if isinstance(raw_value, int):
        return raw_value
    if isinstance(raw_value, str):
        text = raw_value.strip()
        try:
            return int(text)
        except ValueError:
            return float(text)
    return float(raw_value)


def normalize_plan_metrics(batch: Any) -> tuple[list[CapturedNode], list[str]]:
    """Convert a ``PlanMetrics`` batch into typed nodes, reporting malformed entries.

    Accepts anything with the ``PlanMetrics`` shape, including the dictionaries
    produced by ``PlanMetrics.to_dict()`` so sanitized fixtures can be replayed.
    """
    nodes: list[CapturedNode] = []
    problems: list[str] = []
    for entry in batch:
        try:
            if isinstance(entry, dict):
                plan_id = int(entry["plan_id"])
                parent_plan_id = int(entry["parent_plan_id"])
                name = str(entry.get("name") or "")
                raw_metrics = entry.get("metrics") or []
            else:
                plan_id = int(entry.plan_id)
                parent_plan_id = int(entry.parent_plan_id)
                name = str(entry.name or "")
                raw_metrics = entry.metrics or []
        except Exception as exc:
            problems.append(f"unreadable plan node ({type(entry).__name__}): {exc}")
            continue

        metrics: list[CapturedMetric] = []
        for raw in raw_metrics:
            try:
                if isinstance(raw, dict):
                    metric_name = str(raw["name"])
                    metric_type = str(raw.get("type") or raw.get("metric_type") or "")
                    value = _numeric(raw["value"])
                else:
                    metric_name = str(raw.name)
                    metric_type = str(raw.metric_type)
                    value = _numeric(raw.value)
            except Exception as exc:
                problems.append(f"unreadable metric on plan node {plan_id}: {exc}")
                continue
            metrics.append(
                CapturedMetric(name=metric_name, value=value, metric_type=metric_type)
            )
        nodes.append(
            CapturedNode(
                plan_id=plan_id,
                parent_plan_id=parent_plan_id,
                name=name,
                metrics=tuple(metrics),
            )
        )
    return nodes, problems


def resolve_node_type(name: str, *, strict: bool = False) -> tuple[NodeType, str]:
    """Map a Connect operator name to a ``NodeType`` and report how it was resolved.

    Unrecognized operators degrade to ``NodeType.Unknown`` unless ``strict`` is set,
    matching the tolerant behavior of the event-log parser.
    """
    prefix = name.split()[0] if name else ""
    alias = _NODE_TYPE_ALIASES.get(prefix)
    if alias is not None:
        return alias, "alias"
    try:
        return NodeType(prefix), "canonical"
    except ValueError:
        pass
    if strict:
        raise ValueError(f"Unknown Spark Connect operator: {name!r}")
    return NodeType.Unknown, "unknown"


class SparkConnectCapture:
    """Context manager that captures Spark Connect (serverless) query metrics.

    Hooks are installed atomically on the client and the exact originals are restored
    on exit, on setup failure, and on execution failure. On exit a
    ``ParsedLogDataFrames`` is built with the dag schema used by event-log parsing, so
    downstream visualization and analysis work unchanged. Task-level frames stay empty:
    the Connect source exposes no task telemetry.

    Usage::

        with SparkConnectCapture(spark=spark, log_name="my_job") as cap:
            df.collect()
        dfs = cap.dfs
        plot_dag(dfs)
    """

    def __init__(
        self,
        spark: SparkSession | None,
        log_name: str | None = None,
        strict: bool = False,
    ) -> None:
        self.spark = spark
        self._log_name = log_name or "sparkconnect"
        self._strict = strict
        self._client: Any = None
        self._support: ConnectSupport | None = None
        self._executions: list[ConnectExecution] = []
        self._diagnostics: list[CaptureDiagnostic] = []
        self._dfs: ParsedLogDataFrames | None = None
        self._installed: list[tuple[str, bool, Any]] = []
        self._local = threading.local()
        self._lock = threading.RLock()
        self._unattributed: ConnectExecution | None = None

    def to_plan_metrics(self) -> list[dict[str, Any]]:
        """Export recorded executions in the shape ``from_plan_metrics`` accepts."""
        return [
            {
                "action": execution.action,
                "operation_id": execution.operation_id,
                "start": execution.start_wall.isoformat(),
                "elapsed_seconds": execution.elapsed_seconds,
                "plan_metrics": [
                    {
                        "name": node.name,
                        "plan_id": node.plan_id,
                        "parent_plan_id": node.parent_plan_id,
                        "metrics": [
                            {
                                "name": metric.name,
                                "value": metric.value,
                                "type": metric.metric_type,
                            }
                            for metric in node.metrics
                        ],
                    }
                    for node in execution.nodes.values()
                ],
            }
            for execution in self._executions
        ]

    @classmethod
    def from_plan_metrics(
        cls,
        executions: list[dict[str, Any]],
        *,
        log_name: str | None = None,
        strict: bool = False,
    ) -> SparkConnectCapture:
        """Rebuild a capture from recorded executions, without a live session.

        Each entry accepts ``action``, ``operation_id``, ``start`` (ISO-8601),
        ``elapsed_seconds``, and ``plan_metrics`` — the list produced by
        ``PlanMetrics.to_dict()``. Logical plans are not serializable, so join keys
        come from operator names only.
        """
        capture = cls(spark=None, log_name=log_name, strict=strict)
        for index, execution in enumerate(executions):
            raw_start = execution.get("start")
            start = (
                datetime.datetime.fromisoformat(raw_start)
                if isinstance(raw_start, str)
                else datetime.datetime.now(datetime.UTC)
            )
            elapsed = execution.get("elapsed_seconds")
            record = ConnectExecution(
                index=index,
                action=str(execution.get("action") or "replay"),
                thread_id=0,
                start_wall=start,
                start_monotonic=None,
                elapsed_seconds=float(elapsed) if elapsed is not None else None,
                operation_id=execution.get("operation_id"),
                end_wall=start + datetime.timedelta(seconds=float(elapsed or 0)),
            )
            nodes, problems = normalize_plan_metrics(
                execution.get("plan_metrics") or []
            )
            for problem in problems:
                capture._add_diagnostic("malformed_metrics", problem, "parse")
            record.nodes = {node.plan_id: node for node in nodes}
            record.metric_batches = 1 if nodes else 0
            capture._executions.append(record)
        capture._dfs = capture._build_dataframes()
        return capture

    # ------------------------------------------------------------------ lifecycle

    def __enter__(self) -> SparkConnectCapture:
        self._reset()
        client = getattr(self.spark, "_client", None)
        if client is None:
            raise ValueError(
                "The supplied session exposes no Spark Connect client (spark._client)."
            )
        self._client = client
        self._support = probe_connect_support(self.spark)
        if not self._support.metrics_hook:
            raise ValueError(
                "This Spark Connect client exposes no _build_metrics method; "
                "operator metrics cannot be captured with this client version "
                f"({self._support.client_version})."
            )
        if not self._support.hookable_actions:
            raise ValueError(
                "This Spark Connect client exposes no interceptable action methods "
                f"({', '.join(sorted(_ACTION_METHODS))})."
            )
        if self._support.metrics_signature_verified is not True:
            self._add_diagnostic(
                "unverified_client_api",
                "client._build_metrics does not expose the expected single-argument "
                f"signature on client version {self._support.client_version}; metric "
                "capture is best effort.",
                "capture",
            )
        for name in self._support.missing_actions:
            self._add_diagnostic(
                "action_not_covered",
                f"Client method {name!r} is unavailable; actions routed through it "
                "produce no capture coverage.",
                "capture",
                action=name,
            )
        self._install_hooks()
        return self

    def __exit__(self, exc_type: Any, *_: Any) -> None:
        self._restore_hooks()
        with self._lock:
            for execution in self._executions:
                if execution.end_wall is None:
                    self._close_execution(execution)
        try:
            self._dfs = self._build_dataframes()
        except Exception as exc:
            self._add_diagnostic("connect_conversion_failed", str(exc), "parse")
            self._dfs = empty_capture_dataframes()
            if self._strict and exc_type is None:
                raise
        _log.info(
            "SparkConnectCapture: %d execution(s), %d with operator metrics",
            len(self._executions),
            sum(1 for execution in self._executions if execution.nodes),
        )

    def _reset(self) -> None:
        self._executions = []
        self._diagnostics = []
        self._dfs = None
        self._installed = []
        self._local = threading.local()
        self._unattributed = None

    # ------------------------------------------------------------------- hooking

    def _install_hooks(self) -> None:
        client = self._client
        patches: list[tuple[str, Any]] = [
            ("_build_metrics", self._wrap_metrics(client._build_metrics))
        ]
        for name in self._support.hookable_actions if self._support else ():
            patches.append(
                (
                    name,
                    self._wrap_action(
                        name, getattr(client, name), _ACTION_METHODS[name]
                    ),
                )
            )
        request_builder = getattr(client, "_execute_plan_request_with_metadata", None)
        if callable(request_builder):
            patches.append(
                (
                    "_execute_plan_request_with_metadata",
                    self._wrap_request_builder(request_builder),
                )
            )
        response_observer = getattr(client, "_verify_response_integrity", None)
        if callable(response_observer):
            patches.append(
                (
                    "_verify_response_integrity",
                    self._wrap_response_observer(response_observer),
                )
            )

        for name, _ in patches:
            if getattr(getattr(client, name, None), "_sparkparse_hook", False):
                raise RuntimeError(
                    "This Spark Connect client is already being captured; concurrent "
                    "or nested SparkConnectCapture use on one client is not supported."
                )

        try:
            for name, patched in patches:
                patched._sparkparse_hook = True
                self._installed.append(
                    (name, name in client.__dict__, client.__dict__.get(name))
                )
                setattr(client, name, patched)
        except BaseException:
            self._restore_hooks()
            raise

    def _restore_hooks(self) -> None:
        client = self._client
        if client is None:
            return
        while self._installed:
            name, had_instance_attr, original = self._installed.pop()
            try:
                if had_instance_attr:
                    setattr(client, name, original)
                else:
                    client.__dict__.pop(name, None)
            except Exception as exc:  # pragma: no cover - exotic client objects
                self._add_diagnostic(
                    "hook_restore_failed",
                    f"Could not restore client method {name!r}: {exc}",
                    "cleanup",
                )

    def _wrap_metrics(self, original: Any) -> Any:
        def patched(*args: Any, **kwargs: Any) -> Any:
            result = original(*args, **kwargs)
            try:
                batch = list(result)
            except Exception as exc:
                self._add_diagnostic(
                    "malformed_metrics", f"Metrics batch was unreadable: {exc}", "parse"
                )
                return result
            self._record_metrics(batch)
            return iter(batch)

        return patched

    def _wrap_request_builder(self, original: Any) -> Any:
        """Record a caller-supplied operation id, on the rare path that sets one.

        ``ExecutePlanRequest.operation_id`` is populated only when the caller passes one
        into the builder, which the action paths do not do; the field is then an unset
        optional string. The id that actually identifies the operation is assigned by
        the server and arrives on the response, so this is a fallback and
        :meth:`_wrap_response_observer` is the path that normally fills the field in.
        """

        def patched(*args: Any, **kwargs: Any) -> Any:
            request = original(*args, **kwargs)
            execution = self._current_execution()
            if execution is not None and execution.operation_id is None:
                operation_id = getattr(request, "operation_id", None)
                if operation_id:
                    execution.operation_id = str(operation_id)
            return request

        return patched

    def _wrap_response_observer(self, original: Any) -> Any:
        """Record the server-assigned operation id from the first response.

        The client calls ``_verify_response_integrity`` once per ``ExecutePlanResponse``,
        before any branch on response content, so every execution that gets a response at
        all passes through here -- including commands, which carry no operator metrics.
        """

        def patched(*args: Any, **kwargs: Any) -> Any:
            result = original(*args, **kwargs)
            execution = self._current_execution()
            if execution is not None and execution.operation_id is None:
                response = args[0] if args else kwargs.get("response")
                operation_id = getattr(response, "operation_id", None)
                if operation_id:
                    execution.operation_id = str(operation_id)
            return result

        return patched

    def _wrap_action(
        self, name: str, original: Any, kind: Literal["value", "iterator"]
    ) -> Any:
        if kind == "iterator":

            def patched_iterator(*args: Any, **kwargs: Any) -> Any:
                execution = self._begin_execution(name, args)
                iterator = original(*args, **kwargs)

                def generator() -> Any:
                    try:
                        while True:
                            self._push(execution)
                            try:
                                item = next(iterator)
                            except StopIteration:
                                return
                            finally:
                                self._pop()
                            yield item
                    finally:
                        self._close_execution(execution)

                return generator()

            return patched_iterator

        def patched_value(*args: Any, **kwargs: Any) -> Any:
            execution = self._begin_execution(name, args)
            self._push(execution)
            try:
                return original(*args, **kwargs)
            finally:
                self._pop()
                self._close_execution(execution)

        return patched_value

    # ---------------------------------------------------------------- execution

    def _stack(self) -> list[ConnectExecution]:
        stack = getattr(self._local, "stack", None)
        if stack is None:
            stack = []
            self._local.stack = stack
        return stack

    def _push(self, execution: ConnectExecution) -> None:
        self._stack().append(execution)

    def _pop(self) -> None:
        stack = self._stack()
        if stack:
            stack.pop()

    def _current_execution(self) -> ConnectExecution | None:
        stack = self._stack()
        return stack[-1] if stack else None

    def _begin_execution(self, action: str, args: tuple[Any, ...]) -> ConnectExecution:
        plan = self._logical_plan(action, args)
        with self._lock:
            execution = ConnectExecution(
                index=len(self._executions),
                action=action,
                thread_id=threading.get_ident(),
                start_wall=datetime.datetime.now(datetime.UTC),
                start_monotonic=time.monotonic(),
                plan=plan,
            )
            open_on_other_threads = [
                other
                for other in self._executions
                if other.end_wall is None and other.thread_id != execution.thread_id
            ]
            self._executions.append(execution)
        if open_on_other_threads:
            execution.concurrent = True
            for other in open_on_other_threads:
                other.concurrent = True
            self._add_diagnostic(
                "concurrent_executions",
                "Actions overlapped on more than one thread; per-execution metrics are "
                "attributed by calling thread and elapsed times overlap.",
                "capture",
                action=action,
            )
        return execution

    def _close_execution(self, execution: ConnectExecution) -> None:
        if execution.end_wall is not None:
            return
        execution.end_wall = datetime.datetime.now(datetime.UTC)
        if execution.start_monotonic is not None:
            execution.elapsed_seconds = round(
                time.monotonic() - execution.start_monotonic, 3
            )

    def _logical_plan(self, action: str, args: tuple[Any, ...]) -> Any | None:
        if action not in _PLAN_ACTION_METHODS or not args:
            return None
        plan = args[0]
        try:
            if hasattr(plan, "HasField") and plan.HasField("root"):
                return plan.root
        except Exception as exc:
            self._add_diagnostic(
                "plan_unavailable",
                f"Logical plan could not be read for {action!r}: {exc}",
                "capture",
            )
        return None

    def _record_metrics(self, batch: list[Any]) -> None:
        nodes, problems = normalize_plan_metrics(batch)
        for problem in problems:
            self._add_diagnostic("malformed_metrics", problem, "parse")
        if not nodes:
            return
        execution = self._current_execution()
        if execution is None:
            execution = self._unattributed_execution()
        with self._lock:
            execution.metric_batches += 1
            # Metric batches are snapshots of the full plan, not deltas: pyspark's own
            # CollectedMetrics.extract_graph replaces a node's metrics when a plan_id
            # repeats. Summing repeats would double count.
            for node in nodes:
                execution.nodes[node.plan_id] = node

    def _unattributed_execution(self) -> ConnectExecution:
        with self._lock:
            if self._unattributed is None:
                self._unattributed = ConnectExecution(
                    index=len(self._executions),
                    action="unattributed",
                    thread_id=threading.get_ident(),
                    start_wall=datetime.datetime.now(datetime.UTC),
                    start_monotonic=None,
                    attributed=False,
                )
                self._executions.append(self._unattributed)
                unattributed = self._unattributed
            else:
                unattributed = self._unattributed
        self._add_diagnostic(
            "unattributed_metrics",
            "Operator metrics arrived outside an intercepted action boundary; they are "
            "grouped into one query without plan or timing context.",
            "parse",
            query_id=unattributed.index,
        )
        return unattributed

    def _add_diagnostic(
        self, code: str, message: str, phase: str, **details: Any
    ) -> None:
        with self._lock:
            if any(
                diagnostic.code == code
                and diagnostic.message == message
                and diagnostic.details == details
                for diagnostic in self._diagnostics
            ):
                return
            self._diagnostics.append(
                CaptureDiagnostic(
                    code=code, message=message, phase=phase, details=details
                )
            )
        _log.warning("connect capture %s: %s", code, message)

    # ------------------------------------------------------------------- results

    @property
    def dfs(self) -> ParsedLogDataFrames | None:
        return self._dfs

    @property
    def diagnostics(self) -> list[CaptureDiagnostic]:
        return list(self._diagnostics)

    @property
    def support(self) -> ConnectSupport | None:
        return self._support

    @property
    def executions(self) -> list[dict[str, Any]]:
        return [execution.summary() for execution in self._executions]

    def _build_dataframes(self) -> ParsedLogDataFrames:
        return ParsedLogDataFrames(
            dag=self._build_dag_df(), combined=pl.DataFrame(schema=COMBINED_SCHEMA)
        )

    def _resolve_join_details(
        self, execution: ConnectExecution, join_node_ids: list[int]
    ) -> dict[int, dict[str, Any]]:
        """Attach logical join keys to physical join nodes only when verifiable.

        A physical node is matched by the client-assigned plan ID it shares with the
        logical join relation. When no plan ID is available, a single logical join and
        a single physical join node still form an unambiguous mapping; anything else is
        left unresolved rather than matched by position.
        """
        if execution.plan is None or not join_node_ids:
            return {}
        logical_joins = _extract_join_info(execution.plan)
        if not logical_joins:
            return {}

        resolved: dict[int, dict[str, Any]] = {}
        by_plan_id = {
            join["plan_id"]: join
            for join in logical_joins
            if join.get("plan_id") is not None
        }
        unmatched = [node_id for node_id in join_node_ids if node_id not in by_plan_id]
        for node_id in join_node_ids:
            join = by_plan_id.get(node_id)
            if join is not None:
                resolved[node_id] = {
                    **{k: v for k, v in join.items() if k != "plan_id"},
                    "join_details_source": "logical_plan_id_match",
                }
        if len(unmatched) == 1 and len(logical_joins) == 1:
            only_join = logical_joins[0]
            if only_join.get("plan_id") is None:
                resolved[unmatched[0]] = {
                    **{k: v for k, v in only_join.items() if k != "plan_id"},
                    "join_details_source": "single_join_in_query",
                }
        for node_id in join_node_ids:
            if node_id not in resolved:
                self._add_diagnostic(
                    "join_details_unresolved",
                    "A join node could not be matched to a logical join relation; join "
                    "keys are left unresolved instead of matched by position.",
                    "parse",
                    query_id=execution.index,
                    node_id=node_id,
                )
        return resolved

    def _build_dag_df(self) -> pl.DataFrame:
        rows: list[dict[str, Any]] = []
        unknown_names: set[str] = set()

        for execution in self._executions:
            if not execution.nodes:
                self._add_diagnostic(
                    "no_operator_metrics",
                    f"Action {execution.action!r} produced no operator metrics; this "
                    "query has no plan or metric coverage.",
                    "parse",
                    query_id=execution.index,
                    action=execution.action,
                )
                continue
            rows.extend(self._execution_rows(execution, unknown_names))

        if unknown_names:
            self._add_diagnostic(
                "unknown_operators",
                "Unrecognized Connect operators were preserved as Unknown nodes: "
                + ", ".join(sorted(unknown_names)),
                "parse",
                operators=sorted(unknown_names),
            )

        if not rows:
            return pl.DataFrame(schema=DAG_SCHEMA)
        frame = pl.DataFrame(rows, schema_overrides=DAG_SCHEMA)
        return frame.select(list(DAG_SCHEMA))

    def _execution_rows(
        self, execution: ConnectExecution, unknown_names: set[str]
    ) -> list[dict[str, Any]]:
        nodes = list(execution.nodes.values())
        # Children keep the order the server reported them: the source does not
        # establish which input is the build or probe side.
        children: dict[int, list[int]] = defaultdict(list)
        for node in nodes:
            if node.parent_plan_id != node.plan_id:
                children[node.parent_plan_id].append(node.plan_id)
        child_nodes_map = {
            parent: ", ".join(str(child) for child in child_ids)
            for parent, child_ids in children.items()
        }

        root = next(
            (node for node in nodes if node.parent_plan_id == node.plan_id), None
        )
        query_header = root.name if root else f"query_{execution.index}"

        node_types: dict[int, NodeType] = {}
        for node in nodes:
            node_type, source = resolve_node_type(node.name, strict=self._strict)
            if source == "unknown":
                unknown_names.add(node.name)
            node_types[node.plan_id] = node_type

        join_node_ids = [
            node.plan_id
            for node in nodes
            if node_types[node.plan_id] in _JOIN_NODE_TYPES
        ]
        join_details = self._resolve_join_details(execution, join_node_ids)

        start = execution.start_wall.strftime(_TS_FORMAT)
        end = execution.end_wall.strftime(_TS_FORMAT) if execution.end_wall else None

        rows: list[dict[str, Any]] = []
        for node in nodes:
            node_type = node_types[node.plan_id]
            accum_totals = [_build_accum_struct(metric) for metric in node.metrics]
            node_duration_minutes: float | None = None
            detail_data: dict[str, Any] = {"raw_name": node.name}

            for metric in node.metrics:
                if metric.name == "cumulTime" and metric.metric_type == "nsTiming":
                    node_duration_minutes = metric.value / 60_000_000_000
                    detail_data["cumulative_operator_time"] = {
                        "value": metric.value,
                        "unit": "ns",
                        "semantics": "summed across the operator subtree and tasks; "
                        "not wall-clock elapsed time",
                    }

            if node_type == NodeType.Unknown:
                detail_data["raw"] = node.name
            if node_type in _JOIN_NODE_TYPES:
                detail_data["input_roles"] = "unordered"
                resolved = join_details.get(node.plan_id)
                if resolved is not None:
                    detail_data.update(resolved)
                else:
                    from_name = _parse_join_details(node.name)
                    if from_name:
                        detail_data.update(from_name)
                        detail_data["join_details_source"] = "node_name"
                    else:
                        detail_data["join_keys_resolved"] = False
                        detail_data["join_details_source"] = "unresolved"
            if node_type == NodeType.Scan:
                location = _scan_location(node.name)
                if location is not None:
                    detail_data["location"] = {"location": [location]}
                    detail_data["scan_details_source"] = "node_name"

            node_type_str = str(node_type)
            node_name = (
                f"[{node.plan_id}] {node_type_str}"
                if node_type != NodeType.Unknown
                else f"[{node.plan_id}] {node_type_str} ({node.name})"
            )

            rows.append(
                {
                    "log_name": self._log_name,
                    "parsed_log_name": self._log_name,
                    "query_id": execution.index,
                    "query_function": execution.action,
                    "query_header": query_header,
                    "query_start_timestamp": start if execution.attributed else None,
                    "query_end_timestamp": end if execution.attributed else None,
                    "query_duration_seconds": execution.elapsed_seconds,
                    "source_execution_id": execution.operation_id,
                    "node_id": node.plan_id,
                    "node_type": node_type_str,
                    "node_name": node_name,
                    "child_nodes": child_nodes_map.get(node.plan_id),
                    "whole_stage_codegen_id": None,
                    "details": json.dumps({"detail": detail_data}),
                    "accumulator_totals": accum_totals,
                    "n_accumulator_totals": len(accum_totals),
                    "node_duration_minutes": node_duration_minutes,
                    "n_accumulators": 0,
                    "node_id_adj": node.plan_id,
                }
            )
        return rows


def _scan_location(name: str) -> str | None:
    """Return the table or path a scan node reports, when its name carries one.

    Connect scan names look like ``PhotonScan parquet catalog.schema.table [cols]``.
    """
    parts = name.split() if name else []
    if len(parts) < 3:
        return None
    candidate = parts[2].split("[")[0].strip()
    if not candidate or candidate.startswith("("):
        return None
    return candidate
