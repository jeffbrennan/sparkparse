from __future__ import annotations

import datetime
import functools
import html
import inspect
import json
import logging
import tempfile
import uuid
from collections.abc import Callable
from contextvars import ContextVar
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, TypeVar, overload

import polars as pl

from sparkparse import alerts, history
from sparkparse.analyze import to_analysis_export, to_plan_summary
from sparkparse.artifact import save_capture_artifact
from sparkparse.eventlog import discover_sources
from sparkparse.models import (
    CapabilityStatus,
    CaptureCapabilities,
    CaptureCapability,
    CaptureDiagnostic,
    CaptureMetadata,
    CaptureResult,
    CaptureStatus,
    ParsedLogDataFrames,
    RunRecord,
)
from sparkparse.parse import get_parsed_metrics
from sparkparse.storage import (
    ensure_dir,
    join_path,
    list_files,
    path_exists,
    remove_dir,
    write_text,
)

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

_log = logging.getLogger(__name__)
CaptureAction = Literal["viz", "get", "analyze"]
R = TypeVar("R")
_ACTIVE_CAPTURE: ContextVar[Any] = ContextVar("sparkparse_active_capture", default=None)


def _utcnow() -> datetime.datetime:
    return datetime.datetime.now(datetime.UTC)


def _connect_like(spark: SparkSession) -> bool:
    """Identify Connect without treating arbitrary Spark errors as Connect."""
    return type(spark).__module__.startswith("pyspark.sql.connect") or hasattr(
        spark, "_client"
    )


def _empty_dfs() -> ParsedLogDataFrames:
    from sparkparse.connect import empty_capture_dataframes

    return empty_capture_dataframes()


def _first_conf(spark: SparkSession, keys: tuple[str, ...]) -> str | None:
    for key in keys:
        try:
            value = spark.conf.get(key, None)
        except Exception:
            continue
        if value not in (None, ""):
            return str(value)
    return None


def _observed_metadata(spark: SparkSession, *, connect: bool) -> dict[str, str | None]:
    """Read only identity/version fields that the active backend exposes."""
    updates: dict[str, str | None] = {
        "client_version": None,
        "runtime_version": None,
        "source_application_id": None,
        "source_session_id": None,
        "compute_type": None,
        "access_mode": None,
    }
    try:
        import pyspark

        version = getattr(pyspark, "__version__", None)
        if version is not None:
            updates["client_version"] = str(version)
    except Exception:
        pass

    if connect:
        client = getattr(spark, "_client", None)
        session_id = getattr(client, "session_id", None) or getattr(
            spark, "_session_id", None
        )
        if session_id is not None:
            updates["source_session_id"] = str(session_id)
        return updates

    try:
        application_id = spark.sparkContext.applicationId
        if application_id is not None:
            updates["source_application_id"] = str(application_id)
    except Exception:
        pass
    try:
        runtime_version = spark.version
        if runtime_version is not None:
            updates["runtime_version"] = str(runtime_version)
    except Exception:
        pass
    updates["compute_type"] = _first_conf(
        spark,
        ("spark.databricks.clusterUsageTags.clusterComputeType",),
    )
    updates["access_mode"] = _first_conf(
        spark,
        (
            "spark.databricks.clusterUsageTags.clusterAccessMode",
            "spark.databricks.clusterUsageTags.dataSecurityMode",
        ),
    )
    return updates


def _coverage(
    status: CapabilityStatus,
    *,
    source: str,
    reason: str | None = None,
    query_ids: list[Any] | None = None,
    query_coverage: dict[Any, CapabilityStatus] | None = None,
) -> CaptureCapability:
    coverage = (
        query_coverage
        if query_coverage is not None
        else {query_id: status for query_id in query_ids or []}
    )
    return CaptureCapability(
        status=status,
        source=source,
        reason=reason,
        query_coverage={str(query_id): value for query_id, value in coverage.items()},
    )


def _query_capability(
    query_ids: list[Any],
    predicate: Callable[[pl.DataFrame], CapabilityStatus],
    dag: pl.DataFrame,
) -> tuple[CapabilityStatus, dict[Any, CapabilityStatus]]:
    coverage: dict[Any, CapabilityStatus] = {}
    for query_id in query_ids:
        query_frame = dag.filter(pl.col("query_id") == query_id)
        coverage[query_id] = predicate(query_frame)
    statuses = set(coverage.values())
    if not statuses or statuses == {CapabilityStatus.unavailable}:
        overall = CapabilityStatus.unavailable
    elif statuses == {CapabilityStatus.available}:
        overall = CapabilityStatus.available
    else:
        overall = CapabilityStatus.partial
    return overall, coverage


_CONNECT_ELAPSED_REASON = (
    "Client-observed action elapsed time includes result transfer and collection; "
    "it is not server execution time."
)


def _min_status(left: CapabilityStatus, right: CapabilityStatus) -> CapabilityStatus:
    """Return the weaker of two coverage states."""
    order = [
        CapabilityStatus.unknown,
        CapabilityStatus.unavailable,
        CapabilityStatus.not_applicable,
        CapabilityStatus.partial,
        CapabilityStatus.available,
    ]
    return left if order.index(left) <= order.index(right) else right


def _capability_reason(name: str, connect: bool) -> str:
    if connect and name == "query_elapsed_time":
        return _CONNECT_ELAPSED_REASON
    return "Observed fields only; completeness is not established."


def _capabilities(dfs: ParsedLogDataFrames, backend: str) -> CaptureCapabilities:
    """Assess observed fields per query; row presence never proves completeness."""
    source = "spark_connect_plan_metrics" if backend == "spark_connect" else "event_log"
    connect = backend == "spark_connect"
    fields = {
        "plan_structure": ("node_id", "node_type"),
        "operator_metrics": ("accumulator_totals",),
        "query_elapsed_time": ("query_start_timestamp", "query_end_timestamp"),
        "task_metrics": ("task_id", "bytes_read", "bytes_written"),
        "stage_timing": ("stage_start_timestamp", "stage_end_timestamp"),
        "scan_details": ("details",),
        "join_details": ("details",),
    }
    result = CaptureCapabilities()
    for name, columns in fields.items():
        coverage = {}
        ids = dfs.dag["query_id"].unique().to_list()
        for query_id in ids:
            frame = (
                dfs.combined if name in {"task_metrics", "stage_timing"} else dfs.dag
            )
            frame = frame.filter(pl.col("query_id") == query_id)
            if name in {"scan_details", "join_details"}:
                types = (
                    ["Scan", "BatchScan", "LocalTableScan"]
                    if name == "scan_details"
                    else [
                        "BroadcastHashJoin",
                        "SortMergeJoin",
                        "BroadcastNestedLoopJoin",
                        "CartesianProduct",
                    ]
                )
                frame = frame.filter(pl.col("node_type").is_in(types))
                if frame.is_empty():
                    coverage[str(query_id)] = CapabilityStatus.not_applicable
                    continue
            if connect and name in {"task_metrics", "stage_timing"}:
                state = CapabilityStatus.unavailable
            elif frame.is_empty() or any(col not in frame.columns for col in columns):
                state = CapabilityStatus.unavailable
            else:
                observed = [frame[col].drop_nulls().len() for col in columns]
                if not all(observed):
                    state = CapabilityStatus.unavailable
                elif name == "plan_structure" or name == "query_elapsed_time":
                    state = (
                        CapabilityStatus.available
                        if all(n == frame.height for n in observed)
                        else CapabilityStatus.partial
                    )
                else:
                    state = CapabilityStatus.partial
                if connect and name == "query_elapsed_time":
                    # Connect only observes client-side action boundaries.
                    state = _min_status(state, CapabilityStatus.partial)
                if name == "operator_metrics":
                    if not any(
                        row for row in frame["accumulator_totals"].to_list() if row
                    ):
                        state = CapabilityStatus.unavailable
            coverage[str(query_id)] = state
        states = set(coverage.values())
        if len(states) == 1:
            overall = next(iter(states))
        elif not states:
            overall = CapabilityStatus.unavailable
        else:
            overall = CapabilityStatus.partial
        setattr(
            result,
            name,
            CaptureCapability(
                status=overall,
                source=source,
                query_coverage=coverage,
                reason=_capability_reason(name, connect)
                if overall == CapabilityStatus.partial
                else (
                    "No applicable operators."
                    if overall == CapabilityStatus.not_applicable
                    else (
                        "Required telemetry was not observed."
                        if overall == CapabilityStatus.unavailable
                        else None
                    )
                ),
            ),
        )
    return result


class SparkparseCapture:
    """Capture Spark execution data without taking ownership of borrowed sessions."""

    spark: Any
    _entered: bool
    _active_token: Any

    def __init__(
        self,
        action: CaptureAction,
        spark: Any = None,
        temp_dir: str | None = None,
        headless: bool = False,
        history_path: str | None = None,
        log_name: str | None = None,
        alert_config: str | None = None,
        strict: bool = False,
        owns_spark: bool = False,
        backend: str = "auto",
        log_file: str | None = None,
        capture_errors: str = "raise",
        artifact_path: str | None = None,
    ) -> None:
        if action not in ("viz", "get", "analyze"):
            raise ValueError(f"Invalid action: {action}")
        if backend not in {"auto", "classic", "connect", "event_log"}:
            raise ValueError(f"Invalid backend: {backend}")
        if capture_errors not in {"raise", "record"}:
            raise ValueError(f"Invalid capture_errors: {capture_errors}")
        if owns_spark and spark is not None:
            raise ValueError("Owned capture must create its own session.")
        if backend == "event_log" and (spark is not None or owns_spark or not log_file):
            raise ValueError("event_log requires log_file and no session.")
        self.backend = backend
        self.log_file = log_file
        self.capture_errors = capture_errors
        self._artifact_path = artifact_path
        self.report: str | None = None
        self._owned_stopped = False
        self.action = action
        self.temp_dir = temp_dir
        self.spark = spark
        self._owns_spark = owns_spark
        self._log_dir: str | None = None
        self._should_cleanup = owns_spark and temp_dir is None
        self._headless = headless
        self._analysis: dict[str, Any] | None = None
        self._history_path = history_path
        self._log_name = log_name
        self._alert_config = alert_config
        self._strict = strict
        self._last_record: RunRecord | None = None
        self._triggered_alerts: list[dict] = []
        self._connect_cap: Any = None
        self._result: CaptureResult | None = None
        self._diagnostics: list[CaptureDiagnostic] = []
        self._capture_start = _utcnow()
        self._entered: bool = False
        self._used: bool = False
        self._active_token: Any = None
        self._metadata = CaptureMetadata(
            capture_id=uuid.uuid4().hex,
            backend="unknown",
            capture_start=self._capture_start,
            workload_label=log_name,
        )

    def _reset_for_run(self) -> None:
        self._log_dir = None
        self._analysis = None
        self.report = None
        self._owned_stopped = False
        self._last_record = None
        self._triggered_alerts = []
        self._connect_cap = None
        self._result = None
        self._diagnostics = []
        self._capture_start = _utcnow()
        self._metadata = CaptureMetadata(
            capture_id=uuid.uuid4().hex,
            backend="unknown",
            capture_start=self._capture_start,
            workload_label=self._log_name,
        )

    def _result_dataframes(self) -> ParsedLogDataFrames:
        if self._result is None:
            raise RuntimeError("Capture result is not available.")
        return ParsedLogDataFrames(dag=self._result.dag, combined=self._result.combined)

    def _add_diagnostic(
        self, code: str, message: str, phase: str, **details: Any
    ) -> None:
        self._diagnostics.append(
            CaptureDiagnostic(code=code, message=message, phase=phase, details=details)
        )
        _log.warning("capture %s: %s", code, message)

    def _configure_owned_classic(self) -> None:
        from pyspark.sql import SparkSession

        self._log_dir = self.temp_dir or tempfile.mkdtemp(prefix="sparkparse_")
        ensure_dir(self._log_dir)
        if SparkSession.getActiveSession() is not None:
            raise ValueError("An owned capture requires no active SparkSession.")
        builder = SparkSession.builder.master("local[*]").appName("sparkparse_capture")
        self.spark = (
            builder.config("spark.eventLog.enabled", "true")
            .config("spark.eventLog.dir", self._log_dir)
            .config("spark.eventLog.rolling.enabled", "false")
            .config("spark.eventLog.compress", "false")
            .getOrCreate()
        )

    def _configure_borrowed_classic(self) -> None:
        try:
            enabled = (
                str(self.spark.conf.get("spark.eventLog.enabled", "false")).lower()
                == "true"
            )
            log_dir = self.spark.conf.get("spark.eventLog.dir", None)
        except Exception as exc:
            raise RuntimeError(
                "Unable to inspect the supplied Spark session's event-log configuration."
            ) from exc
        if not enabled or not log_dir:
            raise ValueError(
                "Borrowed Spark capture requires event logging to be enabled before the "
                "workload starts. Configure spark.eventLog.enabled=true and "
                "spark.eventLog.dir, or use capture_context(own_session=True)."
            )
        self._log_dir = str(log_dir).removeprefix("file:")
        self._add_diagnostic(
            "scope_attribution_ambiguous",
            "Borrowed classic capture reads an application event log; concurrent or prior "
            "work cannot be separated reliably without execution identifiers.",
            "capture",
        )

    def __enter__(self) -> SparkparseCapture:
        if self._entered:
            raise RuntimeError(
                "A SparkparseCapture instance cannot be entered twice concurrently."
            )
        if self._used and self._owns_spark:
            raise RuntimeError("An owned SparkparseCapture is single-use.")
        active = _ACTIVE_CAPTURE.get()
        if active is not None:
            raise RuntimeError("Nested Sparkparse captures are not supported.")
        self._reset_for_run()
        self._active_token = _ACTIVE_CAPTURE.set(self)
        self._entered = True
        self._used = True

        try:
            return self._start_capture()
        except BaseException:
            self._entered = False
            if self._active_token is not None:
                _ACTIVE_CAPTURE.reset(self._active_token)
                self._active_token = None
            if self._owns_spark and self.spark is not None:
                try:
                    self.spark.stop()
                except Exception:
                    pass
            if self._should_cleanup and self._log_dir:
                try:
                    if path_exists(self._log_dir):
                        remove_dir(self._log_dir)
                except Exception:
                    pass
            raise

    def _start_capture(self) -> SparkparseCapture:
        if self.backend == "event_log":
            assert self.log_file is not None
            source = Path(self.log_file).expanduser().resolve()
            if not source.is_file():
                raise ValueError(f"Event log does not exist: {source}")
            self._log_dir = str(source.parent)
            self.log_file = str(source)
            self._metadata.backend = "event_log"
            return self
        if self._owns_spark:
            if self.backend == "connect":
                raise ValueError("Owned sessions support only classic local Spark.")
            self._configure_owned_classic()
        elif self.spark is None:
            self.spark, _ = _resolve_spark(None, own_session=False)
        if self.backend == "connect" or (
            self.backend == "auto" and _connect_like(self.spark)
        ):
            from sparkparse.connect import SparkConnectCapture

            self._metadata = self._metadata.model_copy(
                update={
                    "backend": "spark_connect",
                    "transport": "spark_connect",
                    **_observed_metadata(self.spark, connect=True),
                }
            )
            _log.info("Spark Connect runtime detected — using guarded Connect capture")
            self._connect_cap = SparkConnectCapture(
                spark=self.spark, log_name=self._log_name, strict=self._strict
            )
            self._connect_cap.__enter__()
            support = self._connect_cap.support
            if support is not None:
                self._metadata = self._metadata.model_copy(
                    update={"client_version": support.client_version}
                )
            return self

        self._metadata = self._metadata.model_copy(
            update={
                "backend": "classic_event_log",
                "transport": "classic",
            }
        )
        if not self._owns_spark:
            self._configure_borrowed_classic()
        self._metadata = self._metadata.model_copy(
            update=_observed_metadata(self.spark, connect=False)
        )
        _log.info("Using existing Spark session; event log dir: %s", self._log_dir)
        return self

    def _render_report(self) -> None:
        from sparkparse.viz import plot_dag

        assert self._result is not None
        coverage = html.escape(
            json.dumps(self._result.capabilities.model_dump(mode="json"), indent=2)
        )
        graph = (
            plot_dag(self._result_dataframes())
            if self._result.dag.height
            else "<p>No plan captured.</p>"
        )
        self.report = f"<html><body><h1>Capture report</h1><pre>{coverage}</pre>{graph}</body></html>"

    def _record_history_and_alerts(self) -> None:
        if self._result is None or self._history_path is None:
            return
        effective_log_name = (
            self._log_name
            or self._result.metadata.workload_label
            or self._result.metadata.capture_id
        )
        record = history.record_from_dfs(self._result, effective_log_name)
        self._last_record = record
        history.append(record, self._history_path)
        if self._alert_config is not None:
            rules = alerts.load_alert_config(self._alert_config)
            hist_df = history.read(self._history_path, effective_log_name)
            self._triggered_alerts = alerts.check_alerts(record, hist_df, rules)

    def _write_artifact(self) -> None:
        """Persist the result so it can be reopened without the raw event logs."""
        if self._result is None or self._artifact_path is None:
            return
        target = save_capture_artifact(
            self._result, self._artifact_path, label=self._log_name
        )
        if self.report is not None:
            write_text(join_path(target, "report.html"), self.report)

    def _set_result(self, dfs: ParsedLogDataFrames) -> None:
        # A backend that observes real execution identifiers supplies them itself;
        # classic event logs identify an execution by its query id.
        source_execution_id = (
            pl.col("source_execution_id").cast(pl.String)
            if "source_execution_id" in dfs.dag.columns
            else (
                pl.col("query_id").cast(pl.String)
                if self._metadata.backend != "spark_connect"
                else pl.lit(None, dtype=pl.String)
            )
        )
        dag = dfs.dag.with_columns(
            pl.lit(self._metadata.capture_id).alias("capture_id"),
            pl.lit(0, dtype=pl.Int64).alias("plan_version"),
            source_execution_id.alias("source_execution_id"),
        )
        self._result = CaptureResult(
            dag=dag,
            combined=dfs.combined,
            metadata=self._metadata,
            capabilities=_capabilities(dfs, self._metadata.backend),
            diagnostics=self._diagnostics,
        )

    def _parse_classic(self) -> ParsedLogDataFrames:
        if self._log_dir is None:
            self._add_diagnostic(
                "missing_log_dir", "No event-log directory was configured.", "parse"
            )
            return _empty_dfs()
        if not list_files(self._log_dir):
            self._add_diagnostic(
                "no_logs",
                "No event-log file was flushed during the capture scope.",
                "parse",
                log_dir=self._log_dir,
            )
            return _empty_dfs()
        try:
            dfs = get_parsed_metrics(
                log_dir=self._log_dir,
                log_file=self._select_log(),
                out_dir=None,
                out_format=None,
                strict=self._strict,
            )
            if (
                "query_end_timestamp" in dfs.dag.columns
                and dfs.dag["query_end_timestamp"].drop_nulls().len() < dfs.dag.height
            ):
                self._add_diagnostic(
                    "incomplete_event_log",
                    "One or more captured queries has no end timestamp; the event log may "
                    "still have been flushing when capture finished.",
                    "parse",
                )
            return dfs
        except Exception as exc:
            self._add_diagnostic("parse_failed", str(exc), "parse")
            raise

    def _select_log(self) -> str:
        if self.log_file:
            return self.log_file
        assert self._log_dir is not None
        app_id = self._metadata.source_application_id
        # Rolling logs are a directory and compressed logs carry a codec
        # suffix, so match on the discovered application identity rather than
        # on an exact file name.
        candidates = [
            source
            for source in discover_sources(self._log_dir)
            if source.application_id is not None
            and app_id is not None
            and (
                source.application_id == app_id
                # Rolling dirs and multi-attempt logs append _<attemptId>.
                or source.application_id.startswith(f"{app_id}_")
            )
        ]
        if len(candidates) != 1:
            raise ValueError(
                "Cannot uniquely identify this application log; supply log_file "
                f"explicitly. Found {len(candidates)} candidate(s) for "
                f"application {app_id!r} in {self._log_dir}."
            )
        return candidates[0].root_uri or candidates[0].name

    def _finalize_capture(self, dfs: ParsedLogDataFrames) -> None:
        self._set_result(dfs)
        self._finish_metadata(
            CaptureStatus.partial if self._diagnostics else CaptureStatus.complete
        )
        if self.action == "analyze":
            try:
                result = self._result
                if result is None:
                    raise RuntimeError("Capture result was not initialized.")
                name = self._log_name or result.metadata.workload_label or "capture"
                self._analysis = to_plan_summary(result, name)
                # Raw facts and diagnostic findings stay separate keys: the
                # summary measures, the analysis interprets.
                self._analysis["analysis"] = to_analysis_export(result, name)
            except Exception as exc:
                self._add_diagnostic("analysis_failed", str(exc), "analysis")
                raise
        elif self.action == "viz":
            self._render_report()
        self._record_history_and_alerts()

    def _finish_metadata(self, status: CaptureStatus) -> None:
        self._metadata = self._metadata.model_copy(
            update={"capture_end": _utcnow(), "status": status}
        )
        if self._result is not None:
            self._result = self._result.model_copy(
                update={"metadata": self._metadata, "diagnostics": self._diagnostics}
            )
        if self._analysis is not None:
            self._analysis["metadata"] = self._metadata.model_dump(mode="json")
            self._analysis["diagnostics"] = [
                diagnostic.model_dump(mode="json") for diagnostic in self._diagnostics
            ]

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> bool:
        finalization_error: BaseException | None = None
        try:
            if self._owns_spark:
                self.spark.stop()
                self._owned_stopped = True
            if self._connect_cap is not None:
                self._connect_cap.__exit__(exc_type, exc_value, traceback)
                self._diagnostics.extend(self._connect_cap.diagnostics)
                if self._connect_cap.dfs is not None:
                    if exc_type is None:
                        self._finalize_capture(self._connect_cap.dfs)
                    else:
                        self._set_result(self._connect_cap.dfs)
            elif self._log_dir is not None:
                dfs = self._parse_classic()
                if exc_type is None:
                    self._finalize_capture(dfs)
                else:
                    self._set_result(dfs)
        except BaseException as exc:
            finalization_error = exc
            self._add_diagnostic("finalization_failed", str(exc), "finalize")
            if self._result is None:
                self._set_result(_empty_dfs())
        finally:
            if self._owns_spark and not self._owned_stopped:
                try:
                    self.spark.stop()
                except Exception as exc:
                    self._add_diagnostic("cleanup_failed", str(exc), "cleanup")
            if (
                self._should_cleanup
                and self._log_dir
                and not self._diagnostics
                and finalization_error is None
                and exc_type is None
            ):
                try:
                    if path_exists(self._log_dir):
                        remove_dir(self._log_dir)
                except Exception as exc:
                    self._add_diagnostic("cleanup_failed", str(exc), "cleanup")
            if exc_type is not None:
                self._finish_metadata(CaptureStatus.failed)
            elif finalization_error is not None or self._diagnostics:
                self._finish_metadata(CaptureStatus.partial)
            else:
                self._finish_metadata(CaptureStatus.complete)
            # Persist after the final status is known so failed workloads keep a
            # partial artifact. A write failure must not mask the workload error,
            # but it does make the capture incomplete.
            if self._artifact_path is not None and self._result is not None:
                try:
                    self._write_artifact()
                except Exception as exc:
                    self._add_diagnostic("artifact_write_failed", str(exc), "finalize")
                    if exc_type is None:
                        self._finish_metadata(CaptureStatus.partial)
                        if finalization_error is None:
                            finalization_error = exc
            self._entered = False
            if self._active_token is not None:
                _ACTIVE_CAPTURE.reset(self._active_token)
                self._active_token = None

        if exc_type is not None:
            return False
        if finalization_error is not None and self.capture_errors == "raise":
            raise finalization_error
        return False

    @property
    def dfs(self) -> ParsedLogDataFrames | None:
        if self._result is None:
            return None
        return ParsedLogDataFrames(dag=self._result.dag, combined=self._result.combined)

    @property
    def result(self) -> CaptureResult | None:
        return self._result

    @property
    def analysis(self) -> dict[str, Any] | None:
        return self._analysis

    @property
    def last_record(self) -> RunRecord | None:
        return self._last_record

    @property
    def triggered_alerts(self) -> list[dict]:
        return self._triggered_alerts


def _resolve_spark(
    spark: SparkSession | None, *, own_session: bool
) -> tuple[SparkSession, bool]:
    from pyspark.sql import SparkSession

    if spark is not None:
        if own_session:
            raise ValueError(
                "own_session=True is only valid when capture creates the session."
            )
        return spark, False
    active = SparkSession.getActiveSession()
    if active is not None:
        if own_session:
            raise ValueError(
                "own_session=True requires no active SparkSession; pass the active session "
                "without own_session or stop it before creating an owned capture."
            )
        return active, False
    if not own_session:
        raise ValueError(
            "No active SparkSession found. Supply spark=... or explicitly opt in with own_session=True."
        )
    return SparkSession.builder.appName("sparkparse_capture").getOrCreate(), True


def capture_context(
    action: CaptureAction = "viz",
    temp_dir: str | None = None,
    spark: SparkSession | None = None,
    headless: bool = False,
    history_path: str | None = None,
    log_name: str | None = None,
    alert_config: str | None = None,
    strict: bool = False,
    own_session: bool = False,
    backend: str = "auto",
    log_file: str | None = None,
    capture_errors: str = "raise",
    artifact_path: str | None = None,
) -> SparkparseCapture:
    return SparkparseCapture(
        action,
        temp_dir=temp_dir,
        spark=spark,
        headless=headless,
        history_path=history_path,
        log_name=log_name,
        alert_config=alert_config,
        strict=strict,
        owns_spark=own_session,
        backend=backend,
        log_file=log_file,
        capture_errors=capture_errors,
        artifact_path=artifact_path,
    )


@overload
def capture(
    func: Callable[..., R],
    *,
    action: CaptureAction = ...,
    temp_dir: str | None = ...,
    spark: SparkSession | None = ...,
    headless: bool = ...,
    history_path: str | None = ...,
    log_name: str | None = ...,
    alert_config: str | None = ...,
    strict: bool = ...,
    own_session: bool = ...,
    backend: str = ...,
    log_file: str | None = ...,
    capture_errors: str = ...,
    artifact_path: str | None = ...,
) -> Callable[..., tuple[R, SparkparseCapture]]: ...


@overload
def capture(
    func: None = None,
    *,
    action: CaptureAction = ...,
    temp_dir: str | None = ...,
    spark: SparkSession | None = ...,
    headless: bool = ...,
    history_path: str | None = ...,
    log_name: str | None = ...,
    alert_config: str | None = ...,
    strict: bool = ...,
    own_session: bool = ...,
    backend: str = ...,
    log_file: str | None = ...,
    capture_errors: str = ...,
    artifact_path: str | None = ...,
) -> Callable[[Callable[..., R]], Callable[..., tuple[R, SparkparseCapture]]]: ...


def capture(
    func=None,
    *,
    action: CaptureAction = "viz",
    temp_dir: str | None = None,
    spark: SparkSession | None = None,
    headless: bool = False,
    history_path: str | None = None,
    log_name: str | None = None,
    alert_config: str | None = None,
    strict: bool = False,
    own_session: bool = False,
    backend: str = "auto",
    log_file: str | None = None,
    capture_errors: str = "raise",
    artifact_path: str | None = None,
) -> Any:
    def decorator(
        func: Callable[..., R],
    ) -> Callable[..., tuple[Any, SparkparseCapture]]:
        @functools.wraps(func)
        def wrapped(*args: Any, **kwargs: Any) -> tuple[R, SparkparseCapture]:
            cap = capture_context(
                action=action,
                spark=spark,
                temp_dir=temp_dir,
                headless=headless,
                history_path=history_path,
                log_name=log_name,
                alert_config=alert_config,
                strict=strict,
                own_session=own_session,
                backend=backend,
                log_file=log_file,
                capture_errors=capture_errors,
                artifact_path=artifact_path,
            )
            with cap:
                bound = inspect.signature(func).bind_partial(*args, **kwargs)
                if "spark" in inspect.signature(func).parameters:
                    bound.arguments["spark"] = cap.spark
                result = func(*bound.args, **bound.kwargs)
            return result, cap

        return wrapped

    if func is None:
        return decorator
    return decorator(func)
