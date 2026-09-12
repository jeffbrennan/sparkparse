import datetime
from pathlib import Path
from typing import Any, cast

import polars as pl
import pytest

from sparkparse.capture import SparkparseCapture, _capabilities, capture
from sparkparse.connect import empty_capture_dataframes
from sparkparse.history import record_from_dfs
from sparkparse.models import (
    CapabilityStatus,
    CaptureMetadata,
    CaptureResult,
    CaptureStatus,
)


class _FakeConf:
    def __init__(self, values):
        self.values = values

    def get(self, key, default=None):
        return self.values.get(key, default)


class _FakeSparkContext:
    def __init__(self):
        self._conf = self

    def getAll(self):
        return []


class _BorrowedSpark:
    def __init__(self, log_dir: Path):
        self.conf = _FakeConf(
            {"spark.eventLog.enabled": "true", "spark.eventLog.dir": str(log_dir)}
        )
        self.sparkContext = _FakeSparkContext()
        self.stop_calls = 0

    def stop(self):
        self.stop_calls += 1


def test_borrowed_session_is_not_stopped_and_missing_tasks_are_explicit(tmp_path: Path):
    spark = _BorrowedSpark(tmp_path / "event-log")
    cap = SparkparseCapture("get", spark=cast(Any, spark))

    with cap:
        pass

    assert spark.stop_calls == 0
    assert cap.dfs is not None
    assert cap.result is not None
    assert cap.result is not None
    assert cap.result.metadata.status == CaptureStatus.partial
    assert cap.result.metadata.backend == "classic_event_log"
    assert cap.result.metadata.client_version is not None
    assert cap.result.capabilities.task_metrics.status == CapabilityStatus.unavailable
    assert any(d.code == "no_logs" for d in cap.result.diagnostics)
    assert cap.dfs is not None
    assert cap.dfs.dag.equals(cap.result.dag)


def test_invalid_action_is_rejected_at_construction():
    with pytest.raises(ValueError, match="Invalid action"):
        SparkparseCapture(cast(Any, "unsupported"), spark=cast(Any, object()))


def test_decorator_creates_fresh_capture_state_each_invocation(tmp_path: Path):
    spark = _BorrowedSpark(tmp_path / "event-log")

    @capture(action="get", spark=cast(Any, spark))
    def run_once():
        return "done"

    first_value, first_capture = run_once()
    second_value, second_capture = run_once()

    assert first_value == second_value == "done"
    assert first_capture.result is not None
    assert second_capture.result is not None
    assert (
        first_capture.result.metadata.capture_id
        != second_capture.result.metadata.capture_id
    )


def test_nested_capture_is_rejected(tmp_path: Path):
    spark = _BorrowedSpark(tmp_path / "event-log")

    with SparkparseCapture("get", spark=cast(Any, spark)):
        with pytest.raises(RuntimeError, match="Nested Sparkparse captures"):
            with SparkparseCapture("get", spark=cast(Any, spark)):
                pass


def test_user_exception_wins_over_finalization_failure(tmp_path: Path):
    spark = _BorrowedSpark(tmp_path / "event-log")
    cap = SparkparseCapture("get", spark=cast(Any, spark))
    cap._parse_classic = lambda: (_ for _ in ()).throw(RuntimeError("finalize failed"))

    with pytest.raises(ValueError, match="user failed"):
        with cap:
            raise ValueError("user failed")


def test_capture_result_serialization_round_trip():
    metadata = CaptureMetadata(
        capture_id="capture-1",
        backend="spark_connect",
        capture_start=datetime.datetime.now(datetime.UTC),
    )
    dfs = empty_capture_dataframes()
    result = CaptureResult(
        dag=dfs.dag,
        combined=dfs.combined,
        metadata=metadata,
        capabilities=_capabilities(dfs, "spark_connect"),
    )

    restored = CaptureResult.model_validate_json(result.model_dump_json())

    assert restored.metadata.capture_id == "capture-1"
    assert restored.dag.is_empty()
    assert restored.combined.is_empty()
    assert restored.dag.schema == result.dag.schema
    assert restored.combined.schema == result.combined.schema


def test_connect_capabilities_do_not_claim_task_coverage():
    dfs = empty_capture_dataframes()
    capabilities = _capabilities(dfs, "spark_connect")

    assert capabilities.task_metrics.status == CapabilityStatus.unavailable
    assert capabilities.task_metrics.reason is not None
    assert capabilities.stage_timing.status == CapabilityStatus.unavailable


def test_empty_history_record_preserves_unavailable_values():
    record = record_from_dfs(empty_capture_dataframes(), "empty")

    assert record.duration_s is None
    assert record.bytes_read is None
    assert record.n_tasks is None


@pytest.mark.parametrize("policy", ["raise", "record"])
def test_capture_error_policy_is_separate_from_parser_strictness(tmp_path, policy):
    cap = SparkparseCapture(
        "get", spark=_BorrowedSpark(tmp_path), strict=True, capture_errors=policy
    )

    def fail():
        raise RuntimeError("parse failure")

    cap._parse_classic = fail
    if policy == "raise":
        with pytest.raises(RuntimeError, match="parse failure"):
            with cap:
                pass
    else:
        with cap:
            pass
    assert cap.result is not None
    assert cap.result.metadata.status == CaptureStatus.partial
    assert cap.result is not None
    assert cap.result.diagnostics[-1].code == "finalization_failed"


def test_selects_application_log_not_last_filename(tmp_path):
    (tmp_path / "app-1").touch()
    (tmp_path / "zzz-unrelated").touch()
    cap = SparkparseCapture("get", spark=_BorrowedSpark(tmp_path))
    cap._log_dir = str(tmp_path)
    cap._metadata = cap._metadata.model_copy(update={"source_application_id": "app-1"})
    assert cap._select_log() == "app-1"
    cap._metadata = cap._metadata.model_copy(
        update={"source_application_id": "missing"}
    )
    with pytest.raises(ValueError, match="uniquely identify"):
        cap._select_log()


def test_owned_session_stops_before_parse_and_retains_failed_logs(
    tmp_path, monkeypatch
):
    spark = _BorrowedSpark(tmp_path)
    cap = SparkparseCapture("get", owns_spark=True)

    def setup():
        cap.spark = spark
        cap._log_dir = str(tmp_path)

    def parse():
        assert spark.stop_calls == 1
        raise RuntimeError("broken log")

    monkeypatch.setattr(cap, "_configure_owned_classic", setup)
    monkeypatch.setattr(cap, "_parse_classic", parse)
    with pytest.raises(RuntimeError, match="broken log"):
        with cap:
            pass
    assert tmp_path.exists()
    assert spark.stop_calls == 1


def test_session_free_event_log_can_be_reused(tmp_path, monkeypatch):
    log = tmp_path / "event-log"
    log.touch()
    cap = SparkparseCapture("viz", backend="event_log", log_file=str(log))
    monkeypatch.setattr(cap, "_parse_classic", empty_capture_dataframes)
    for _ in range(2):
        with cap:
            assert cap.spark is None
        assert cap.result is not None
        assert cap.report is not None
        assert cap.result.metadata.backend == "event_log"
        assert "No plan captured" in cap.report
        assert "task_metrics" in cap.report
    assert log.exists()


def test_analysis_has_final_metadata(tmp_path, monkeypatch):
    cap = SparkparseCapture("analyze", spark=_BorrowedSpark(tmp_path))
    monkeypatch.setattr(cap, "_parse_classic", empty_capture_dataframes)
    with cap:
        pass
    assert cap.analysis is not None
    assert cap.result is not None
    assert cap.analysis["metadata"] == cap.result.metadata.model_dump(mode="json")
    assert cap.analysis["capabilities"]["task_metrics"]["status"] == "unavailable"


@pytest.mark.parametrize("action", ["get", "analyze", "viz"])
def test_connect_finalizes_and_restores_hooks_without_logs(action, monkeypatch):
    from types import SimpleNamespace

    from sparkparse.connect import SparkConnectCapture

    def build(metrics):
        return metrics

    def table(plan):
        return None
    client = SimpleNamespace(_build_metrics=build, to_table=table)
    spark = SimpleNamespace(_client=client)
    monkeypatch.setattr(
        SparkConnectCapture,
        "_build_dataframes",
        lambda self: empty_capture_dataframes(),
    )
    cap = SparkparseCapture(action, spark=spark, backend="connect")
    with cap:
        assert client._build_metrics is not build
    assert client._build_metrics is build
    assert client.to_table is table
    assert cap.result is not None
    assert cap.result.metadata.backend == "spark_connect"
    assert cap._log_dir is None
    if action == "analyze":
        assert cap.analysis is not None
    if action == "viz":
        assert cap.report is not None


def test_populated_result_serialization_preserves_nested_values():
    dfs = empty_capture_dataframes()
    dag = pl.DataFrame(
        {
            "query_id": [1],
            "metric": [[{"value": 0, "name": "rows"}]],
            "at": [datetime.datetime.now(datetime.UTC)],
        }
    )
    result = CaptureResult(
        dag=dag,
        combined=dfs.combined,
        metadata=CaptureMetadata(
            capture_id="nested",
            backend="event_log",
            capture_start=datetime.datetime.now(datetime.UTC),
        ),
        capabilities=_capabilities(dfs, "event_log"),
    )
    restored = CaptureResult.model_validate_json(result.model_dump_json())
    assert restored.dag.equals(dag)
    assert restored.dag.schema == dag.schema
