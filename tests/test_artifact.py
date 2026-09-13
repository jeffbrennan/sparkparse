import datetime
import io

import polars as pl

from sparkparse.artifact import (
    is_capture_artifact,
    load_capture_artifact,
    save_capture_artifact,
)
from sparkparse.models import (
    CapabilityStatus,
    CaptureCapabilities,
    CaptureCapability,
    CaptureDiagnostic,
    CaptureMetadata,
    CaptureResult,
    ParsedLogDataFrames,
)
from sparkparse.schemas import COMBINED_SCHEMA, DAG_SCHEMA


def _parsed_frames() -> ParsedLogDataFrames:
    return ParsedLogDataFrames(
        dag=pl.DataFrame(
            {
                "query_id": [0],
                "node_id": [1],
                "node_type": ["Scan"],
                "node_name": ["scan"],
            }
        ),
        combined=pl.DataFrame({"task_id": [10], "bytes_read": [1024]}),
    )


def _capture_result() -> CaptureResult:
    return CaptureResult(
        dag=pl.DataFrame({"query_id": [0], "node_id": [1]}),
        combined=pl.DataFrame({"task_id": [1]}),
        metadata=CaptureMetadata(
            capture_id="abc",
            backend="spark_connect",
            capture_start=datetime.datetime(2026, 9, 12, tzinfo=datetime.UTC),
            capture_end=datetime.datetime(2026, 9, 12, 0, 1, tzinfo=datetime.UTC),
            workload_label="job-a",
            status="partial",
        ),
        capabilities=CaptureCapabilities(
            task_metrics=CaptureCapability(
                status=CapabilityStatus.unavailable,
                reason="Connect exposes no task telemetry.",
            )
        ),
        diagnostics=[CaptureDiagnostic(code="no_logs", message="none", phase="parse")],
    )


def test_artifact_round_trip_preserves_frames_and_manifest(tmp_path):
    result = _capture_result()
    path = save_capture_artifact(result, tmp_path / "run")

    assert is_capture_artifact(path)
    assert (tmp_path / "run" / "manifest.json").exists()

    loaded = load_capture_artifact(path)
    assert loaded.dag.equals(result.dag)
    assert loaded.combined.equals(result.combined)
    assert loaded.metadata.workload_label == "job-a"
    assert loaded.metadata.backend == "spark_connect"
    assert loaded.capabilities.task_metrics.status is CapabilityStatus.unavailable
    assert loaded.diagnostics[0].code == "no_logs"


def test_artifact_from_parsed_frames_derives_metadata(tmp_path):
    path = save_capture_artifact(_parsed_frames(), tmp_path / "classic", label="my-log")
    loaded = load_capture_artifact(path)

    assert loaded.metadata.backend == "event_log"
    assert loaded.metadata.workload_label == "my-log"


def test_single_file_json_artifact_round_trips(tmp_path):
    result = _capture_result()
    path = tmp_path / "capture.json"
    path.write_text(result.model_dump_json())

    assert is_capture_artifact(path)
    loaded = load_capture_artifact(path)
    assert loaded.dag.equals(result.dag)
    assert loaded.combined.equals(result.combined)


def test_empty_typed_frames_survive_round_trip(tmp_path):
    result = CaptureResult(
        dag=pl.DataFrame(schema=DAG_SCHEMA),
        combined=pl.DataFrame(schema=COMBINED_SCHEMA),
        metadata=CaptureMetadata(
            capture_id="empty",
            backend="spark_connect",
            capture_start=datetime.datetime(2026, 9, 12, tzinfo=datetime.UTC),
        ),
        capabilities=CaptureCapabilities(),
    )
    loaded = load_capture_artifact(save_capture_artifact(result, tmp_path / "empty"))

    assert loaded.dag.schema == pl.DataFrame(schema=DAG_SCHEMA).schema
    assert loaded.combined.schema == pl.DataFrame(schema=COMBINED_SCHEMA).schema


def test_arrow_in_json_serialization_is_reversible():
    result = _capture_result()
    payload = result.model_dump(mode="json")
    assert payload["dag"]["format"] == "arrow-ipc"
    # Sanity: the base64 payload decodes to a real IPC stream.
    import base64

    stream = io.BytesIO(base64.b64decode(payload["dag"]["data"]))
    assert pl.read_ipc(stream).height == 1
