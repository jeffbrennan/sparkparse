import datetime
from pathlib import Path

import polars as pl

from sparkparse.artifact import save_capture_artifact
from sparkparse.dataset import MAX_BROWSER_TASK_ROWS, Dataset
from sparkparse.models import (
    CaptureCapabilities,
    CaptureMetadata,
    CaptureResult,
    ParsedLogDataFrames,
)
from sparkparse.models import (
    ParsedLogDataFrames as ParsedFrames,
)

FULL_LOGS = Path(__file__).parent / "data" / "full_logs"


def test_event_log_dataset_lists_sources_and_caches():
    dataset = Dataset(FULL_LOGS)

    logs = dataset.list_logs()
    assert logs
    name = logs[0]

    first = dataset.dataframes(name)
    second = dataset.dataframes(name)
    assert first is second
    assert not first.dag.is_empty()


def test_artifact_dataset_uses_metadata_label(tmp_path):
    result = CaptureResult(
        dag=pl.DataFrame({"query_id": [0], "node_id": [1]}),
        combined=pl.DataFrame({"task_id": [1], "bytes_read": [16]}),
        metadata=CaptureMetadata(
            capture_id="x",
            backend="spark_connect",
            capture_start=datetime.datetime(2026, 9, 12, tzinfo=datetime.UTC),
            workload_label="serverless-job",
        ),
        capabilities=CaptureCapabilities(),
    )
    path = save_capture_artifact(result, tmp_path / "artifact")

    dataset = Dataset(path)
    assert dataset.kind == "artifact"
    assert dataset.list_logs() == ["serverless-job"]
    frames = dataset.dataframes("serverless-job")
    assert frames.dag.height == 1
    assert dataset.metadata("serverless-job") is not None
    assert dataset.capabilities("serverless-job") is not None


def test_memory_dataset_accepts_parsed_frames():
    frames = ParsedFrames(
        dag=pl.DataFrame({"query_id": [0], "node_id": [1]}),
        combined=pl.DataFrame({"task_id": [1]}),
    )
    dataset = Dataset(frames)
    assert dataset.kind == "memory"
    assert dataset.dataframes(None).dag.height == 1


def test_combined_records_truncates_at_limit(tmp_path):
    combined = pl.DataFrame({"task_id": list(range(10)), "bytes_read": [1] * 10})
    frames = ParsedLogDataFrames(dag=pl.DataFrame({"query_id": [0]}), combined=combined)
    dataset = Dataset(frames)

    records, truncated = dataset.combined_records(None, limit=4)
    assert len(records) == 4
    assert truncated is True

    records, truncated = dataset.combined_records(None, limit=MAX_BROWSER_TASK_ROWS)
    assert len(records) == 10
    assert truncated is False
