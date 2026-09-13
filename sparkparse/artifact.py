"""Portable capture artifacts.

A capture artifact is a directory (or a single JSON file) that holds everything
needed to open a capture locally without the raw Spark event logs:

- ``manifest.json`` — metadata, capabilities, and diagnostics
- ``dag.arrow`` — one row per physical plan node
- ``combined.arrow`` — one row per task attempt
- optional ``job_stage.arrow`` / ``query_stage.arrow`` relation tables

The single-file form is the Arrow-IPC-in-JSON serialization already defined by
``CaptureResult`` and is what ``model_dump_json()`` produces. These helpers make
the directory form the default and read either form.
"""

from __future__ import annotations

import datetime
import json
import uuid
from pathlib import Path

import polars as pl

from sparkparse.models import (
    CaptureCapabilities,
    CaptureDiagnostic,
    CaptureMetadata,
    CaptureResult,
    ParsedLogDataFrames,
)
from sparkparse.storage import (
    ensure_dir,
    is_cloud_path,
    join_path,
    open_file,
    path_exists,
    read_text,
    write_text,
)

ARTIFACT_SCHEMA_VERSION = "1"
_MANIFEST_NAME = "manifest.json"
_DAG_NAME = "dag.arrow"
_COMBINED_NAME = "combined.arrow"
_JOB_STAGE_NAME = "job_stage.arrow"
_QUERY_STAGE_NAME = "query_stage.arrow"


def _write_frame(path: str, frame: pl.DataFrame) -> None:
    with open_file(path, "wb") as handle:
        frame.write_ipc(handle)


def _read_frame(path: str) -> pl.DataFrame:
    if not path_exists(path):
        return pl.DataFrame()
    with open_file(path, "rb") as handle:
        return pl.read_ipc(handle)


def _coerce_result(
    result: CaptureResult | ParsedLogDataFrames, label: str | None
) -> CaptureResult:
    if isinstance(result, CaptureResult):
        return result
    return CaptureResult(
        dag=result.dag,
        combined=result.combined,
        metadata=CaptureMetadata(
            capture_id=uuid.uuid4().hex,
            backend="event_log",
            capture_start=datetime.datetime.now(datetime.UTC),
            workload_label=label,
        ),
        capabilities=CaptureCapabilities(),
        diagnostics=[
            CaptureDiagnostic(code=d.code, message=d.message, phase="parse")
            for d in result.diagnostics
        ],
    )


def save_capture_artifact(
    result: CaptureResult | ParsedLogDataFrames,
    path: str | Path,
    *,
    label: str | None = None,
) -> str:
    """Write a capture artifact directory and return its path.

    Accepted on a cloud path as well as a local one; Arrow files use the same
    storage layer as every other read/write in sparkparse.
    """
    capture = _coerce_result(result, label)
    path_str = str(path)
    ensure_dir(path_str)

    _write_frame(join_path(path_str, _DAG_NAME), capture.dag)
    _write_frame(join_path(path_str, _COMBINED_NAME), capture.combined)
    if isinstance(result, ParsedLogDataFrames):
        if result.job_stage is not None:
            _write_frame(join_path(path_str, _JOB_STAGE_NAME), result.job_stage)
        if result.query_stage is not None:
            _write_frame(join_path(path_str, _QUERY_STAGE_NAME), result.query_stage)

    manifest = {
        "schema_version": ARTIFACT_SCHEMA_VERSION,
        "metadata": capture.metadata.model_dump(mode="json"),
        "capabilities": capture.capabilities.model_dump(mode="json"),
        "diagnostics": [d.model_dump(mode="json") for d in capture.diagnostics],
        "frames": {
            "dag": _DAG_NAME,
            "combined": _COMBINED_NAME,
        },
    }
    write_text(
        join_path(path_str, _MANIFEST_NAME),
        json.dumps(manifest, indent=2, default=str),
    )
    return path_str


def is_capture_artifact(path: str | Path) -> bool:
    """Return True when ``path`` looks like a saved capture artifact."""
    path_str = str(path)
    if not path_exists(path_str):
        return False
    if path_str.endswith(".json"):
        return True
    if is_cloud_path(path_str):
        return path_exists(join_path(path_str, _MANIFEST_NAME))
    candidate = Path(path_str)
    return candidate.is_dir() and (candidate / _MANIFEST_NAME).exists()


def load_capture_artifact(path: str | Path) -> CaptureResult:
    """Read a capture artifact, in single-file JSON or directory form."""
    path_str = str(path)
    if path_str.endswith(".json"):
        return CaptureResult.model_validate_json(read_text(path_str))

    manifest = json.loads(read_text(join_path(path_str, _MANIFEST_NAME)))
    return CaptureResult(
        dag=_read_frame(join_path(path_str, _DAG_NAME)),
        combined=_read_frame(join_path(path_str, _COMBINED_NAME)),
        metadata=CaptureMetadata.model_validate(manifest["metadata"]),
        capabilities=CaptureCapabilities.model_validate(manifest["capabilities"]),
        diagnostics=[
            CaptureDiagnostic.model_validate(entry)
            for entry in manifest.get("diagnostics", [])
        ],
    )
