import datetime
from types import SimpleNamespace

import polars as pl

from sparkparse.dataset import Dataset
from sparkparse.models import (
    CapabilityStatus,
    CaptureCapabilities,
    CaptureCapability,
    CaptureMetadata,
    CaptureResult,
)
from sparkparse.pages import summary as summary_module
from sparkparse.pages.summary import (
    EXECUTOR_COLS,
    SUMMARY_COLS,
    build_timeline_payload,
    get_executor_table_df,
    get_table_df,
)
from sparkparse.schemas import COMBINED_SCHEMA, empty_capture_dataframes


def _combined() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "log_name": ["log"] * 4,
            "parsed_log_name": ["parsed"] * 4,
            "query_id": [0, 0, 1, 1],
            "query_function": ["collect", "collect", "count", "count"],
            "job_id": [0, 0, 1, 1],
            "job_start_timestamp": [datetime.datetime(2026, 1, 1)] * 4,
            "job_end_timestamp": [datetime.datetime(2026, 1, 1, 0, 1)] * 4,
            "job_duration_seconds": [60.0] * 4,
            "stage_id": [0, 0, 1, 1],
            "stage_start_timestamp": [datetime.datetime(2026, 1, 1)] * 4,
            "stage_end_timestamp": [datetime.datetime(2026, 1, 1, 0, 1)] * 4,
            "stage_duration_seconds": [60.0] * 4,
            "stage_attempt_id": [0, 0, 0, 0],
            "task_id": [0, 1, 0, 1],
            "task_duration_seconds": [1.0, 2.0, 3.0, 4.0],
            "bytes_read": [10, 20, 30, 40],
            "bytes_written": [1, 2, 3, 4],
            "shuffle_bytes_read": [0, 0, 0, 0],
            "shuffle_bytes_written": [5, 5, 5, 5],
            "executor_id": ["e1", "e1", "e2", "e2"],
            "host": ["h1", "h1", "h2", "h2"],
        }
    )


def test_stage_aggregation_sums_task_metrics():
    records = _combined().to_pandas().to_dict("records")
    stage = get_table_df(records, SUMMARY_COLS.grouping)

    assert stage["tasks"].sum() == 4
    assert stage["bytes_read"].sum() == 100
    assert stage["shuffle_bytes_written"].sum() == 20


def test_executor_aggregation_groups_by_executor():
    records = _combined().to_pandas().to_dict("records")
    executor = get_executor_table_df(records, grouping_cols=EXECUTOR_COLS.grouping)

    assert set(executor["executor_id"]) == {"e1", "e2"}
    assert executor["bytes_read"].sum() == 100


def test_timeline_payload_precomputes_stage_frame():
    payload = build_timeline_payload(_combined())

    assert len(payload["stage_frame"]) == 2
    assert "job_time" in payload
    assert payload["idle_str"].startswith("idle:")


def test_timeline_payload_empty_and_untyped_frames_are_safe():
    assert build_timeline_payload(pl.DataFrame(schema=COMBINED_SCHEMA)) == {}
    assert build_timeline_payload(pl.DataFrame({"job_id": [0]})) == {}


def test_analysis_prefers_preserved_capture_result(monkeypatch):
    frames = empty_capture_dataframes()
    result = CaptureResult(
        dag=frames.dag,
        combined=frames.combined,
        metadata=CaptureMetadata(
            capture_id="x",
            backend="spark_connect",
            capture_start=datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC),
            workload_label="capture",
        ),
        capabilities=CaptureCapabilities(
            scan_details=CaptureCapability(
                status=CapabilityStatus.unavailable, reason="not observed"
            )
        ),
    )
    fake_app = SimpleNamespace(
        server=SimpleNamespace(config={"DATASET": Dataset(result)})
    )
    monkeypatch.setattr(summary_module, "get_app", lambda: fake_app)

    seen: dict[str, type] = {}
    real_analyze = summary_module.analyze_dfs

    def spy(dfs, name):
        seen["type"] = type(dfs)
        return real_analyze(dfs, name)

    monkeypatch.setattr(summary_module, "analyze_dfs", spy)
    summary_module.get_records("capture")

    # The explicit capabilities must reach analyze_dfs, not be inferred from columns.
    assert seen["type"] is CaptureResult
