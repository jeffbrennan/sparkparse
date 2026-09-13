"""Append-only run history for tracking Spark job metrics over time.

Each call to ``record_from_dfs`` derives a compact, versioned ``RunRecord`` —
the numeric snapshot you would want to plot as a time series — from a
``ParsedLogDataFrames`` or ``CaptureResult``. ``append`` writes it to a
persistent store; ``read`` queries it back for trend analysis, baseline
cohorts, and comparison reports.

Storage selection is deterministic: an explicit ``.jsonl`` path is always
JSONL, an existing Delta table is always Delta, and ``auto`` only falls back to
the installed-package heuristic for a brand-new store. Installing ``deltalake``
therefore cannot silently change the format of an existing store. JSONL appends
are only safe for a single writer on a local filesystem; cloud object stores
have no atomic append and concurrent writers must use Delta (or per-run
immutable objects).
"""

from __future__ import annotations

import datetime
import importlib.util
import logging
import statistics
import uuid
from io import IOBase
from pathlib import Path
from typing import Any, cast

import polars as pl

from sparkparse.analyze import (
    OUTPUT_ACCOUNTING_COLUMNS,
    find_cartesian_joins,
    find_largest_scans,
    resolve_capabilities,
    retained_outputs,
    workload_fingerprint,
)
from sparkparse.models import (
    RUN_RECORD_VERSION,
    CapabilityStatus,
    CaptureResult,
    ComparisonReport,
    MetricComparison,
    ParsedLogDataFrames,
    RunRecord,
)
from sparkparse.storage import (
    append_text,
    ensure_dir,
    is_cloud_path,
    join_path,
    open_file,
    path_exists,
)

logger = logging.getLogger(__name__)

HistoryFormat = str  # "auto" | "delta" | "jsonl"

_TS_FORMAT = "%Y-%m-%dT%H:%M:%S"
_DELTA_LOG = "_delta_log"

#: Measurable fields that a comparison or alert may reference. Provenance and
#: identity fields are deliberately excluded.
MEASURE_FIELDS: tuple[str, ...] = (
    "duration_s",
    "cumulative_time_s",
    "bytes_read",
    "bytes_written",
    "records_read",
    "records_written",
    "shuffle_read_bytes",
    "shuffle_write_bytes",
    "memory_bytes_spilled",
    "disk_bytes_spilled",
    "n_queries",
    "n_stages",
    "n_tasks",
    "n_cartesian_joins",
    "max_node_duration_min",
    "max_scan_bytes",
)

#: Capability family each measure depends on. A baseline cohort only includes
#: samples whose semantics are comparable for the requested measure.
METRIC_CAPABILITY: dict[str, str] = {
    "duration_s": "query_elapsed_time",
    "cumulative_time_s": "query_elapsed_time",
    "bytes_read": "task_metrics",
    "bytes_written": "task_metrics",
    "records_read": "task_metrics",
    "records_written": "task_metrics",
    "shuffle_read_bytes": "task_metrics",
    "shuffle_write_bytes": "task_metrics",
    "memory_bytes_spilled": "task_metrics",
    "disk_bytes_spilled": "task_metrics",
    "n_stages": "task_metrics",
    "n_tasks": "task_metrics",
    "max_scan_bytes": "task_metrics",
    "max_node_duration_min": "operator_metrics",
    "n_cartesian_joins": "join_details",
    "n_queries": "plan_structure",
}


def _delta_available() -> bool:
    return importlib.util.find_spec("deltalake") is not None


def _existing_format(history_path: str) -> str | None:
    """Detect the format of an existing store, or None for a new path."""
    path = str(history_path).rstrip("/")
    if path.endswith(".jsonl"):
        return "jsonl"
    if is_cloud_path(path):
        if path_exists(join_path(path, _DELTA_LOG)):
            return "delta"
        if path_exists(path) and not path.endswith("/"):
            return "jsonl"
        return None
    local = Path(path)
    if (local / _DELTA_LOG).exists():
        return "delta"
    if local.is_file():
        return "jsonl"
    return None


def _resolve_format(format: HistoryFormat, history_path: str) -> str:
    """Choose a store format without letting installed packages flip a store."""
    if format not in ("auto", "delta", "jsonl"):
        raise ValueError(f"Unsupported history format: {format!r}")
    if format != "auto":
        if format == "delta" and not _delta_available():
            raise ImportError(
                "Delta format requested but deltalake is not installed. "
                "Install it with `uv add deltalake` or `pip install deltalake`."
            )
        return format

    existing = _existing_format(history_path)
    if existing is not None:
        if existing == "delta" and not _delta_available():
            raise ImportError(
                f"Existing Delta store at {history_path!r} requires deltalake."
            )
        return existing
    return "delta" if _delta_available() else "jsonl"


def _coverage_map(dfs: ParsedLogDataFrames | CaptureResult) -> dict[str, str]:
    capabilities = resolve_capabilities(dfs)
    return {
        name: getattr(capabilities, name).status.value
        for name in capabilities.model_fields
    }


def _cumulative_time_seconds(dag: pl.DataFrame) -> float | None:
    """Sum measured query durations, counted once per query.

    The DAG repeats each query's duration on every operator row, so the column
    must be deduplicated by ``query_id`` before summing; otherwise a query's
    time is multiplied by its operator count.
    """
    if "query_duration_seconds" not in dag.columns or dag.height == 0:
        return None
    if "query_id" not in dag.columns:
        return None
    per_query = (
        dag.select("query_id", "query_duration_seconds")
        .drop_nulls("query_duration_seconds")
        .unique(subset=["query_id"], keep="first")
    )
    if per_query.height == 0:
        return None
    return float(per_query["query_duration_seconds"].sum() or 0.0)


def record_from_dfs(
    dfs: ParsedLogDataFrames | CaptureResult, log_name: str
) -> RunRecord:
    """Derive a ``RunRecord`` from parsed DataFrames.

    Reuses ``analyze`` helpers so the metric definitions stay consistent with
    the rest of the codebase. Missing measures stay null; a measured zero is
    preserved as ``0``.
    """
    dag = dfs.dag
    combined = dfs.combined

    start_ts = None
    end_ts = None
    if (
        "query_start_timestamp" in dag.columns
        and "query_end_timestamp" in dag.columns
        and dag.height > 0
    ):
        start_ts = (
            dag["query_start_timestamp"]
            .str.to_datetime(format=_TS_FORMAT, strict=False)
            .drop_nulls()
            .min()
        )
        end_ts = (
            dag["query_end_timestamp"]
            .str.to_datetime(format=_TS_FORMAT, strict=False)
            .drop_nulls()
            .max()
        )
    if start_ts is not None and end_ts is not None:
        assert isinstance(start_ts, datetime.datetime)
        assert isinstance(end_ts, datetime.datetime)
        duration_s = (end_ts - start_ts).total_seconds()
    else:
        duration_s = None

    kept = retained_outputs(combined)

    def total(column: str) -> int | None:
        # Output totals count retained outputs only; a recomputed or raced
        # partition would otherwise report its bytes twice. Spill and time
        # count every attempt.
        frame = kept if column in OUTPUT_ACCOUNTING_COLUMNS else combined
        if column not in frame.columns or frame.height == 0:
            return None
        values = frame[column].drop_nulls()
        if len(values) == 0:
            return None
        return int(values.sum() or 0)

    n_cartesian = find_cartesian_joins(dfs).height if dag.height else None

    max_node_duration_min = None
    if "node_duration_minutes" in dag.columns and dag.height:
        max_node_dur = dag["node_duration_minutes"].drop_nulls().max()
        if isinstance(max_node_dur, int | float):
            max_node_duration_min = float(max_node_dur)

    max_scan_bytes = None
    if dag.height:
        largest_scans = find_largest_scans(dfs, n=1)
        if not largest_scans.is_empty():
            val = largest_scans["bytes_read"][0]
            max_scan_bytes = int(val) if val is not None else None

    n_queries = (
        dag["query_id"].n_unique() if "query_id" in dag.columns and dag.height else None
    )
    n_stages = (
        combined["stage_id"].n_unique()
        if "stage_id" in combined.columns and combined.height
        else None
    )
    n_tasks = combined.height if combined.height else None

    provenance: dict[str, Any] = {
        "status": None,
        "backend": None,
        "transport": None,
        "compute_type": None,
        "access_mode": None,
        "runtime_version": None,
        "client_version": None,
        "config": {},
    }
    if isinstance(dfs, CaptureResult):
        metadata = dfs.metadata
        provenance.update(
            status=metadata.status.value,
            backend=metadata.backend,
            transport=metadata.transport,
            compute_type=metadata.compute_type,
            access_mode=metadata.access_mode,
            runtime_version=metadata.runtime_version,
            client_version=metadata.client_version,
            config=dict(metadata.configuration),
        )
    else:
        provenance["backend"] = "event_log"

    record = RunRecord(
        record_version=RUN_RECORD_VERSION,
        run_id=uuid.uuid4().hex,
        run_at=datetime.datetime.now(datetime.UTC),
        log_name=log_name,
        workload_fingerprint=workload_fingerprint(dfs),
        coverage=_coverage_map(dfs),
        duration_s=duration_s,
        cumulative_time_s=_cumulative_time_seconds(dag),
        bytes_read=total("bytes_read"),
        bytes_written=total("bytes_written"),
        records_read=total("records_read"),
        records_written=total("records_written"),
        shuffle_read_bytes=total("shuffle_bytes_read"),
        shuffle_write_bytes=total("shuffle_bytes_written"),
        memory_bytes_spilled=total("memory_bytes_spilled"),
        disk_bytes_spilled=total("disk_bytes_spilled"),
        n_queries=n_queries,
        n_stages=n_stages,
        n_tasks=n_tasks,
        n_cartesian_joins=n_cartesian,
        max_node_duration_min=max_node_duration_min,
        max_scan_bytes=max_scan_bytes,
        **provenance,
    )

    # A capability that was explicitly unavailable means the measure is unknown,
    # not zero. Null it rather than recording a misleading value.
    requirements = {
        "task_metrics": [
            "bytes_read",
            "bytes_written",
            "records_read",
            "records_written",
            "shuffle_read_bytes",
            "shuffle_write_bytes",
            "memory_bytes_spilled",
            "disk_bytes_spilled",
            "n_tasks",
            "n_stages",
            "max_scan_bytes",
        ],
        # cumulative_time_s is derived from measured per-query durations and is
        # kept whenever those exist; only wall-clock duration_s needs full
        # timestamp coverage. Connect exposes aggregate time without wall clock.
        "query_elapsed_time": ["duration_s"],
        "operator_metrics": ["max_node_duration_min"],
        "join_details": ["n_cartesian_joins"],
        "plan_structure": ["n_queries"],
    }
    capabilities = resolve_capabilities(dfs)
    for capability, metrics in requirements.items():
        if getattr(capabilities, capability).status != CapabilityStatus.available:
            for metric in metrics:
                setattr(record, metric, None)

    if isinstance(dfs, CaptureResult):
        record.run_id = dfs.metadata.capture_id
        record.run_at = dfs.metadata.capture_start
    return record


def append(
    record: RunRecord, history_path: str, format: HistoryFormat = "auto"
) -> None:
    """Append a ``RunRecord`` to the history store at ``history_path``.

    For Delta, ``history_path`` is a directory. For JSONL, it is a file path.
    Local and cloud URIs are both supported via ``storage`` helpers.

    JSONL appends are only atomic for a single local writer; use Delta when
    multiple writers may append concurrently. Cloud object stores have no
    atomic append at all.
    """
    resolved = _resolve_format(format, history_path)

    if resolved == "delta":
        from deltalake import write_deltalake

        df = pl.DataFrame([record.model_dump()])
        table = df.to_arrow()
        write_deltalake(history_path, table, mode="append")
        logger.info("Appended run %s to Delta table at %s", record.run_id, history_path)
    else:
        if not is_cloud_path(history_path):
            ensure_dir(Path(history_path).parent)
        append_text(history_path, record.model_dump_json() + "\n")
        logger.info("Appended run %s to JSONL at %s", record.run_id, history_path)


def _migrate_frame(df: pl.DataFrame) -> pl.DataFrame:
    """Bring historical records up to the current shape without inventing data.

    Rows written before versioning carry ``record_version = 1``. Baseline
    selection excludes those rows rather than reading a historical zero as a
    measured value. Legacy combined ``spill_bytes`` and ``shuffle_bytes``
    columns are preserved but not split into the new distinct measures.
    """
    if df.is_empty():
        return df

    if "record_version" not in df.columns:
        df = df.with_columns(pl.lit(1).alias("record_version"))
    else:
        df = df.with_columns(
            pl.col("record_version").cast(pl.Int64, strict=False).fill_null(1)
        )

    return df


def row_to_record(row: dict[str, Any]) -> RunRecord:
    """Validate a history row, defaulting null struct columns to empty maps."""
    data = dict(row)
    for key in ("coverage", "config"):
        if data.get(key) is None:
            data[key] = {}
    return RunRecord.model_validate(data)


def read(
    history_path: str,
    log_name: str | None = None,
    last_n: int | None = None,
    format: HistoryFormat = "auto",
) -> pl.DataFrame:
    """Read history records, optionally filtered by ``log_name`` and/or limited
    to the last ``N`` runs (most recent by ``run_at``).
    """
    resolved = _resolve_format(format, history_path)

    if not path_exists(history_path):
        return pl.DataFrame()

    if resolved == "delta":
        from deltalake import DeltaTable

        dt = DeltaTable(history_path)
        df = pl.from_arrow(dt.to_pyarrow_table())
    else:
        # Infer across the whole store, not just the first 100 lines: a long
        # run of legacy records must not hide the columns added by later
        # version-2 appends (which inference would then drop).
        if is_cloud_path(history_path):
            with open_file(history_path, "rb") as f:
                df = pl.read_ndjson(cast(IOBase, f), infer_schema_length=None)
        else:
            df = pl.read_ndjson(history_path, infer_schema_length=None)

    assert isinstance(df, pl.DataFrame)
    if df.is_empty():
        return df

    df = _migrate_frame(df)

    if "run_at" in df.columns and df["run_at"].dtype == pl.String:
        # Stored timestamps may be UTC with a trailing ``Z`` or carry an offset.
        # Strip the marker and parse as UTC; already-typed columns are untouched.
        df = df.with_columns(
            pl.col("run_at")
            .str.strip_chars_end("Z")
            .str.to_datetime(time_zone="UTC", strict=False)
        )

    if log_name is not None and "log_name" in df.columns:
        df = df.filter(pl.col("log_name") == log_name)

    if "run_at" in df.columns:
        df = df.sort("run_at")

    if last_n is not None and last_n > 0:
        df = df.tail(last_n)

    return df


def select_baseline_cohort(
    history: pl.DataFrame,
    current: RunRecord,
    *,
    log_name: str,
    window: int,
    metric: str | None = None,
    required_capability: str | None = None,
    match_fingerprint: bool = True,
    match_backend: bool = False,
    match_runtime: bool = False,
) -> tuple[list[dict[str, Any]], list[str]]:
    """Return the comparable baseline samples for ``current`` and the reasons
    some rows were excluded.

    A sample is only comparable when it predates the current run, is a
    versioned record (legacy provenance is unknown), reports the requested
    measure, has compatible coverage, and matches the workload identity and any
    requested runtime/compute dimension. ``metric`` filters null measurements
    here so ``window`` counts valid samples rather than all candidates.
    """
    if history.is_empty() or "log_name" not in history.columns:
        return [], []

    reasons = {
        "legacy": 0,
        "future": 0,
        "coverage": 0,
        "fingerprint": 0,
        "dimension": 0,
        "duplicate": 0,
        "missing_measure": 0,
    }
    rows: list[dict[str, Any]] = []
    seen_run_ids: set[str] = set()
    for row in history.to_dicts():
        if row.get("log_name") != log_name:
            continue
        if row.get("run_id") == current.run_id:
            continue
        run_id = row.get("run_id")
        if run_id is not None:
            if run_id in seen_run_ids:
                reasons["duplicate"] += 1
                continue
            seen_run_ids.add(run_id)
        version = row.get("record_version")
        if version is None or int(version) < RUN_RECORD_VERSION:
            reasons["legacy"] += 1
            continue
        run_at = row.get("run_at")
        # Strictly later runs cannot inform the current run's baseline. Equal
        # timestamps are simultaneous, not later, so they stay eligible.
        if not isinstance(run_at, datetime.datetime) or run_at > current.run_at:
            reasons["future"] += 1
            continue
        if required_capability is not None:
            coverage = row.get("coverage") or {}
            if coverage.get(required_capability) not in (None, "available"):
                reasons["coverage"] += 1
                continue
        if match_fingerprint and current.workload_fingerprint:
            if row.get("workload_fingerprint") != current.workload_fingerprint:
                reasons["fingerprint"] += 1
                continue
        if match_backend and row.get("backend") != current.backend:
            reasons["dimension"] += 1
            continue
        if match_runtime and row.get("runtime_version") != current.runtime_version:
            reasons["dimension"] += 1
            continue
        if metric is not None and row.get(metric) is None:
            reasons["missing_measure"] += 1
            continue
        rows.append(row)

    rows.sort(key=lambda r: r["run_at"], reverse=True)
    limited = rows[:window]

    notes = [
        f"{count} {reason} sample(s) excluded"
        for reason, count in reasons.items()
        if count
    ]
    return limited, notes


def _plan_changed(
    history: pl.DataFrame, current: RunRecord, *, log_name: str, window: int
) -> bool | None:
    if current.workload_fingerprint is None:
        return None
    cohort, _ = select_baseline_cohort(
        history,
        current,
        log_name=log_name,
        window=window,
        required_capability=None,
        match_fingerprint=False,
    )
    fingerprints = {
        row.get("workload_fingerprint")
        for row in cohort
        if row.get("workload_fingerprint")
    }
    if not fingerprints:
        return None
    return current.workload_fingerprint not in fingerprints


def compare_runs(
    history_path: str,
    log_name: str,
    *,
    window: int = 10,
    min_samples: int = 1,
    match_fingerprint: bool = True,
    match_backend: bool = False,
    match_runtime: bool = False,
    format: HistoryFormat = "auto",
) -> ComparisonReport:
    """Compare the latest run for ``log_name`` against its comparable history.

    Only measures with a measured current value and enough comparable baseline
    samples are reported; every other measure is listed with the reason it was
    excluded so a missing comparison is never mistaken for a clean one.
    """
    if window <= 0:
        raise ValueError("window must be positive")
    if min_samples <= 0:
        raise ValueError("min_samples must be positive")

    df = read(history_path, log_name=log_name, format=format)
    if df.is_empty():
        raise ValueError(f"No history records found for log_name {log_name!r}.")

    records = [row_to_record(row) for row in df.sort("run_at").to_dicts()]
    current = records[-1]

    metrics: list[MetricComparison] = []
    excluded: list[str] = []
    for metric in MEASURE_FIELDS:
        current_value = getattr(current, metric)
        if current_value is None:
            excluded.append(f"{metric}: current value unavailable")
            continue
        capability = METRIC_CAPABILITY.get(metric)
        cohort, _ = select_baseline_cohort(
            df,
            current,
            log_name=log_name,
            window=window,
            metric=metric,
            required_capability=capability,
            match_fingerprint=match_fingerprint,
            match_backend=match_backend,
            match_runtime=match_runtime,
        )
        values = [float(row[metric]) for row in cohort]
        if len(values) < min_samples:
            excluded.append(
                f"{metric}: {len(values)} comparable sample(s), {min_samples} required"
            )
            continue
        baseline = statistics.fmean(values)
        delta = float(current_value) - baseline
        pct_change = delta / baseline if baseline != 0 else None
        metrics.append(
            MetricComparison(
                metric=metric,
                current=float(current_value),
                baseline=baseline,
                delta=delta,
                pct_change=pct_change,
                sample_count=len(values),
                cohort=current.backend,
            )
        )

    identity_cohort, _ = select_baseline_cohort(
        df,
        current,
        log_name=log_name,
        window=window,
        required_capability=None,
        match_fingerprint=False,
    )
    return ComparisonReport(
        log_name=log_name,
        current_run_id=current.run_id,
        current_run_at=current.run_at,
        cohort_size=len(identity_cohort),
        window=window,
        metrics=metrics,
        excluded=excluded,
        plan_changed=_plan_changed(df, current, log_name=log_name, window=window),
    )
