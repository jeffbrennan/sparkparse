"""Append-only run history for tracking Spark job metrics over time.

Each call to ``record_from_dfs`` derives a compact ``RunRecord`` — the numeric
snapshot you would want to plot as a time series — from a
``ParsedLogDataFrames``. ``append`` writes it to a persistent store; ``read``
queries it back for trend analysis and alert baseline computation.

Storage format is selected automatically: Delta Lake (append-only, ACID,
supports concurrent writers) when ``deltalake`` is importable, JSONL otherwise.
Format can be forced via the ``format`` parameter.
"""

from __future__ import annotations

import datetime
import importlib.util
import logging
import uuid
from io import IOBase
from pathlib import Path
from typing import cast

import polars as pl

from sparkparse.analyze import find_cartesian_joins, find_largest_scans
from sparkparse.models import (
    CapabilityStatus,
    CaptureResult,
    ParsedLogDataFrames,
    RunRecord,
)
from sparkparse.storage import (
    append_text,
    ensure_dir,
    is_cloud_path,
    open_file,
    path_exists,
)

logger = logging.getLogger(__name__)

HistoryFormat = str  # "auto" | "delta" | "jsonl"

_TS_FORMAT = "%Y-%m-%dT%H:%M:%S"


def _delta_available() -> bool:
    return importlib.util.find_spec("deltalake") is not None


def _resolve_format(format: HistoryFormat) -> str:
    if format not in ("auto", "delta", "jsonl"):
        raise ValueError(f"Unsupported history format: {format!r}")
    if format == "auto":
        return "delta" if _delta_available() else "jsonl"
    if format == "delta" and not _delta_available():
        raise ImportError(
            "Delta format requested but deltalake is not installed. "
            "Install it with `uv add deltalake` or `pip install deltalake`."
        )
    return format


def _resolve_read_format(format: HistoryFormat, history_path: str) -> str:
    """For reads, detect JSONL from the path extension before falling back to
    the deltalake availability check. This lets ``read`` work on a JSONL file
    even when deltalake is installed.
    """
    if format != "auto":
        return _resolve_format(format)
    if str(history_path).endswith(".jsonl"):
        return "jsonl"
    return "delta" if _delta_available() else "jsonl"


def record_from_dfs(
    dfs: ParsedLogDataFrames | CaptureResult, log_name: str
) -> RunRecord:
    """Derive a ``RunRecord`` from parsed DataFrames.

    Reuses ``analyze.find_cartesian_joins`` and ``analyze.find_largest_scans``
    so the metric definitions stay consistent with the rest of the codebase.
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

    def total(column: str) -> int | None:
        if column not in combined.columns or combined.height == 0:
            return None
        values = combined[column].drop_nulls()
        if len(values) == 0:
            return None
        return int(values.sum() or 0)

    bytes_read = total("bytes_read")
    bytes_written = total("bytes_written")
    shuffle_read = total("shuffle_bytes_read")
    shuffle_written = total("shuffle_bytes_written")
    memory_spilled = total("memory_bytes_spilled")
    disk_spilled = total("disk_bytes_spilled")

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

    record = RunRecord(
        run_id=uuid.uuid4().hex,
        run_at=datetime.datetime.now(datetime.UTC),
        log_name=log_name,
        duration_s=duration_s,
        bytes_read=bytes_read,
        bytes_written=bytes_written,
        shuffle_bytes=(shuffle_read + shuffle_written)
        if shuffle_read is not None and shuffle_written is not None
        else None,
        spill_bytes=(memory_spilled + disk_spilled)
        if memory_spilled is not None and disk_spilled is not None
        else None,
        n_queries=n_queries,
        n_stages=n_stages,
        n_tasks=n_tasks,
        n_cartesian_joins=n_cartesian,
        max_node_duration_min=max_node_duration_min,
        max_scan_bytes=max_scan_bytes,
    )
    if isinstance(dfs, CaptureResult):
        record.run_id = dfs.metadata.capture_id
        record.run_at = dfs.metadata.capture_start
        requirements = {
            "task_metrics": [
                "bytes_read",
                "bytes_written",
                "shuffle_bytes",
                "spill_bytes",
                "n_tasks",
                "n_stages",
                "max_scan_bytes",
            ],
            "query_elapsed_time": ["duration_s"],
            "operator_metrics": ["max_node_duration_min"],
            "join_details": ["n_cartesian_joins"],
            "plan_structure": ["n_queries"],
        }
        for capability, metrics in requirements.items():
            if (
                getattr(dfs.capabilities, capability).status
                != CapabilityStatus.available
            ):
                for metric in metrics:
                    setattr(record, metric, None)
    return record


def append(
    record: RunRecord, history_path: str, format: HistoryFormat = "auto"
) -> None:
    """Append a ``RunRecord`` to the history store at ``history_path``.

    For Delta, ``history_path`` is a directory. For JSONL, it is a file path.
    Local and cloud URIs are both supported via ``storage`` helpers.
    """
    resolved = _resolve_format(format)

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


def read(
    history_path: str,
    log_name: str | None = None,
    last_n: int | None = None,
    format: HistoryFormat = "auto",
) -> pl.DataFrame:
    """Read history records, optionally filtered by ``log_name`` and/or limited
    to the last ``N`` runs (most recent by ``run_at``).
    """
    resolved = _resolve_read_format(format, history_path)

    if resolved == "delta":
        from deltalake import DeltaTable

        dt = DeltaTable(history_path)
        df = pl.from_arrow(dt.to_pyarrow_table())
    else:
        if not path_exists(history_path):
            return pl.DataFrame()
        if is_cloud_path(history_path):
            with open_file(history_path, "rb") as f:
                df = pl.read_ndjson(cast(IOBase, f))
        else:
            df = pl.read_ndjson(history_path)

    assert isinstance(df, pl.DataFrame)
    if df.is_empty():
        return df

    if df["run_at"].dtype == pl.String:
        df = df.with_columns(pl.col("run_at").str.to_datetime(time_zone="UTC"))

    if log_name is not None:
        df = df.filter(pl.col("log_name") == log_name)

    df = df.sort("run_at")

    if last_n is not None and last_n > 0:
        df = df.tail(last_n)

    return df
