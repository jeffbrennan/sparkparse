import datetime
import io
import json
import math
import uuid
from pathlib import Path
from unittest.mock import patch

import polars as pl
import pytest
from pydantic import ValidationError

from sparkparse.alerts import (
    AlertConfig,
    AlertStatus,
    SparkparseAlertError,
    check_alerts,
    load_alert_config,
)
from sparkparse.models import RunRecord

_ALL_AVAILABLE = {
    "plan_structure": "available",
    "operator_metrics": "available",
    "query_elapsed_time": "available",
    "task_metrics": "available",
    "stage_timing": "available",
    "scan_details": "available",
    "join_details": "available",
}


def _make_record(**overrides) -> RunRecord:
    defaults = dict(
        run_id=uuid.uuid4().hex,
        run_at=datetime.datetime(2024, 1, 1, tzinfo=datetime.UTC),
        log_name="test_job",
        duration_s=100.0,
        bytes_read=1000,
        bytes_written=500,
        shuffle_read_bytes=100,
        shuffle_write_bytes=100,
        memory_bytes_spilled=0,
        disk_bytes_spilled=0,
        n_queries=2,
        n_stages=3,
        n_tasks=50,
        n_cartesian_joins=0,
        max_node_duration_min=1.5,
        max_scan_bytes=800,
        coverage=dict(_ALL_AVAILABLE),
    )
    defaults.update(overrides)
    return RunRecord(**defaults)


def _history_df(records: list[RunRecord]) -> pl.DataFrame:
    return pl.DataFrame([r.model_dump() for r in records])


def _only(assessments, name):
    return next(a for a in assessments if a.alert_name == name)


def test_load_alert_config_from_toml(tmp_path: Path):
    toml_content = """
[[alerts]]
name = "duration_regression"
log_name = "nightly_job"
metric = "duration_s"
condition = "pct_increase"
threshold = 0.20
window = 5
min_samples = 2
severity = "warning"
on_trigger = "log"

[[alerts]]
name = "spill_alert"
log_name = "nightly_job"
metric = "memory_bytes_spilled"
condition = "threshold"
threshold = 1073741824
severity = "critical"
on_trigger = "raise"
"""
    toml_path = tmp_path / "alerts.toml"
    toml_path.write_text(toml_content)

    alerts = load_alert_config(str(toml_path))
    assert len(alerts) == 2
    assert alerts[0].name == "duration_regression"
    assert alerts[0].condition == "pct_increase"
    assert alerts[0].window == 5
    assert alerts[0].min_samples == 2
    assert alerts[1].name == "spill_alert"
    assert alerts[1].condition == "threshold"
    assert alerts[1].severity == "critical"
    assert alerts[1].on_trigger == "raise"


def test_load_alert_config_cloud_path():
    toml_bytes = b"""
[[alerts]]
name = "cloud_alert"
log_name = "my_job"
metric = "duration_s"
condition = "threshold"
threshold = 300.0
"""
    fake_file = io.BytesIO(toml_bytes)
    with patch("sparkparse.alerts.open_file", return_value=fake_file):
        alerts = load_alert_config("s3://bucket/alerts.toml")
    assert len(alerts) == 1
    assert alerts[0].name == "cloud_alert"


def test_load_alert_config_empty():
    fake_file = io.BytesIO(b"")
    with patch("sparkparse.alerts.open_file", return_value=fake_file):
        alerts = load_alert_config("s3://bucket/empty.toml")
    assert alerts == []


@pytest.mark.parametrize(
    "overrides",
    [
        {"window": 0},
        {"min_samples": 0},
        {"threshold": math.inf},
        {"threshold": math.nan},
    ],
)
def test_invalid_alert_config_is_rejected(overrides):
    kwargs = dict(
        name="bad",
        log_name="job",
        metric="duration_s",
        condition="threshold",
        threshold=1.0,
    )
    kwargs.update(overrides)
    with pytest.raises(ValidationError):
        AlertConfig.model_validate(kwargs)


def test_check_alerts_threshold_fires():
    record = _make_record(memory_bytes_spilled=200)
    alert = AlertConfig(
        name="spill_check",
        log_name="test_job",
        metric="memory_bytes_spilled",
        condition="threshold",
        threshold=100,
        on_trigger="log",
    )
    result = check_alerts(record, pl.DataFrame(), [alert])
    assessment = _only(result, "spill_check")
    assert assessment.status == AlertStatus.triggered
    assert assessment.current == 200
    assert assessment.baseline is None


def test_check_alerts_threshold_does_not_fire():
    record = _make_record(memory_bytes_spilled=50)
    alert = AlertConfig(
        name="spill_check",
        log_name="test_job",
        metric="memory_bytes_spilled",
        condition="threshold",
        threshold=100,
        on_trigger="log",
    )
    result = check_alerts(record, pl.DataFrame(), [alert])
    assert _only(result, "spill_check").status == AlertStatus.clean


def test_check_alerts_pct_increase_fires():
    current = _make_record(duration_s=130.0)
    history = _history_df(
        [
            _make_record(duration_s=100.0),
            _make_record(duration_s=100.0),
            _make_record(duration_s=100.0),
        ]
    )
    alert = AlertConfig(
        name="duration_regression",
        log_name="test_job",
        metric="duration_s",
        condition="pct_increase",
        threshold=0.20,
        window=5,
        on_trigger="log",
    )
    result = check_alerts(current, history, [alert])
    assessment = _only(result, "duration_regression")
    assert assessment.status == AlertStatus.triggered
    assert assessment.baseline == 100.0
    assert assessment.current == 130.0


def test_check_alerts_pct_increase_does_not_fire():
    current = _make_record(duration_s=110.0)
    history = _history_df(
        [
            _make_record(duration_s=100.0),
            _make_record(duration_s=100.0),
        ]
    )
    alert = AlertConfig(
        name="duration_regression",
        log_name="test_job",
        metric="duration_s",
        condition="pct_increase",
        threshold=0.20,
        on_trigger="log",
    )
    result = check_alerts(current, history, [alert])
    assert _only(result, "duration_regression").status == AlertStatus.clean


def test_check_alerts_pct_increase_baseline_zero_fires():
    current = _make_record(memory_bytes_spilled=100)
    history = _history_df([_make_record(memory_bytes_spilled=0)])
    alert = AlertConfig(
        name="new_spill",
        log_name="test_job",
        metric="memory_bytes_spilled",
        condition="pct_increase",
        threshold=0.5,
        on_trigger="log",
    )
    result = check_alerts(current, history, [alert])
    assessment = _only(result, "new_spill")
    assert assessment.status == AlertStatus.triggered
    assert assessment.baseline == 0.0


def test_check_alerts_pct_increase_baseline_zero_no_fire():
    current = _make_record(memory_bytes_spilled=0)
    history = _history_df([_make_record(memory_bytes_spilled=0)])
    alert = AlertConfig(
        name="no_spill",
        log_name="test_job",
        metric="memory_bytes_spilled",
        condition="pct_increase",
        threshold=0.5,
        on_trigger="log",
    )
    result = check_alerts(current, history, [alert])
    assert _only(result, "no_spill").status == AlertStatus.clean


def test_check_alerts_absolute_increase_fires():
    current = _make_record(duration_s=200.0)
    history = _history_df([_make_record(duration_s=100.0)])
    alert = AlertConfig(
        name="abs_increase",
        log_name="test_job",
        metric="duration_s",
        condition="absolute_increase",
        threshold=50,
        on_trigger="log",
    )
    result = check_alerts(current, history, [alert])
    assessment = _only(result, "abs_increase")
    assert assessment.status == AlertStatus.triggered
    assert assessment.baseline == 100.0
    assert assessment.current == 200.0


def test_check_alerts_absolute_increase_does_not_fire():
    current = _make_record(duration_s=120.0)
    history = _history_df([_make_record(duration_s=100.0)])
    alert = AlertConfig(
        name="abs_increase",
        log_name="test_job",
        metric="duration_s",
        condition="absolute_increase",
        threshold=50,
        on_trigger="log",
    )
    result = check_alerts(current, history, [alert])
    assert _only(result, "abs_increase").status == AlertStatus.clean


def test_check_alerts_window_larger_than_history():
    current = _make_record(duration_s=200.0)
    history = _history_df(
        [
            _make_record(duration_s=90.0),
            _make_record(duration_s=110.0),
        ]
    )
    alert = AlertConfig(
        name="big_window",
        log_name="test_job",
        metric="duration_s",
        condition="pct_increase",
        threshold=0.20,
        window=10,
        on_trigger="log",
    )
    result = check_alerts(current, history, [alert])
    assessment = _only(result, "big_window")
    assert assessment.status == AlertStatus.triggered
    assert assessment.baseline == 100.0


def test_check_alerts_excludes_current_run():
    current = _make_record(duration_s=130.0, run_id="current_run")
    history = _history_df(
        [
            _make_record(duration_s=100.0, run_id="old_1"),
            _make_record(duration_s=130.0, run_id="current_run"),
        ]
    )
    alert = AlertConfig(
        name="exclude_current",
        log_name="test_job",
        metric="duration_s",
        condition="pct_increase",
        threshold=0.20,
        on_trigger="log",
    )
    result = check_alerts(current, history, [alert])
    assert _only(result, "exclude_current").baseline == 100.0


def test_check_alerts_excludes_later_runs():
    now = datetime.datetime(2024, 6, 1, tzinfo=datetime.UTC)
    current = _make_record(duration_s=100.0, run_at=now)
    history = _history_df(
        [
            _make_record(duration_s=100.0, run_at=now - datetime.timedelta(hours=1)),
            _make_record(duration_s=500.0, run_at=now + datetime.timedelta(hours=1)),
        ]
    )
    alert = AlertConfig(
        name="exclude_future",
        log_name="test_job",
        metric="duration_s",
        condition="absolute_increase",
        threshold=50,
        on_trigger="log",
    )
    result = check_alerts(current, history, [alert])
    assessment = _only(result, "exclude_future")
    assert assessment.sample_count == 1
    assert assessment.status == AlertStatus.clean


def test_window_counts_only_valid_measurements():
    base = datetime.datetime(2024, 1, 1, tzinfo=datetime.UTC)
    history = _history_df(
        [
            _make_record(duration_s=100.0, run_at=base, run_id="old"),
            _make_record(
                duration_s=None,
                run_at=base + datetime.timedelta(hours=1),
                run_id="recent",
            ),
        ]
    )
    current = _make_record(
        duration_s=200.0, run_at=base + datetime.timedelta(hours=2), run_id="current"
    )
    alert = AlertConfig(
        name="windowed",
        log_name="test_job",
        metric="duration_s",
        condition="pct_increase",
        threshold=0.20,
        window=1,
        on_trigger="log",
    )
    result = check_alerts(current, history, [alert])
    assessment = _only(result, "windowed")
    assert assessment.status == AlertStatus.triggered
    assert assessment.baseline == 100.0
    assert assessment.sample_count == 1


def test_check_alerts_insufficient_samples():
    current = _make_record(duration_s=200.0)
    history = _history_df([_make_record(duration_s=100.0)])
    alert = AlertConfig(
        name="needs_more",
        log_name="test_job",
        metric="duration_s",
        condition="pct_increase",
        threshold=0.20,
        min_samples=3,
        on_trigger="log",
    )
    result = check_alerts(current, history, [alert])
    assessment = _only(result, "needs_more")
    assert assessment.status == AlertStatus.insufficient_data
    assert "required" in (assessment.reason or "")


def test_first_run_does_not_fire_pct_increase():
    current = _make_record(duration_s=200.0)
    alert = AlertConfig(
        name="first_run",
        log_name="test_job",
        metric="duration_s",
        condition="pct_increase",
        threshold=0.20,
        on_trigger="log",
    )
    result = check_alerts(current, pl.DataFrame(), [alert])
    assert _only(result, "first_run").status == AlertStatus.insufficient_data


def test_check_alerts_coverage_mismatch_excluded():
    current = _make_record(duration_s=200.0)
    history = _history_df(
        [
            _make_record(
                duration_s=100.0,
                coverage={"query_elapsed_time": "unavailable"},
            )
        ]
    )
    alert = AlertConfig(
        name="coverage",
        log_name="test_job",
        metric="duration_s",
        condition="pct_increase",
        threshold=0.20,
        on_trigger="log",
    )
    result = check_alerts(current, history, [alert])
    assert _only(result, "coverage").status == AlertStatus.insufficient_data


def test_check_alerts_fingerprint_mismatch_excluded():
    current = _make_record(duration_s=200.0, workload_fingerprint="fp_a")
    history = _history_df([_make_record(duration_s=100.0, workload_fingerprint="fp_b")])
    alert = AlertConfig(
        name="fingerprint",
        log_name="test_job",
        metric="duration_s",
        condition="pct_increase",
        threshold=0.20,
        on_trigger="log",
    )
    result = check_alerts(current, history, [alert])
    assert _only(result, "fingerprint").status == AlertStatus.insufficient_data


def test_check_alerts_unavailable_current_metric_skipped():
    record = _make_record(duration_s=None)
    alert = AlertConfig(
        name="missing_current",
        log_name="test_job",
        metric="duration_s",
        condition="threshold",
        threshold=1.0,
        on_trigger="log",
    )
    result = check_alerts(record, pl.DataFrame(), [alert])
    assessment = _only(result, "missing_current")
    assert assessment.status == AlertStatus.skipped
    assert assessment.reason == "current metric unavailable"


def test_check_alerts_unknown_metric_unsupported():
    record = _make_record()
    alert = AlertConfig(
        name="bogus",
        log_name="test_job",
        metric="not_a_metric",
        condition="threshold",
        threshold=1.0,
        on_trigger="log",
    )
    result = check_alerts(record, pl.DataFrame(), [alert])
    assert _only(result, "bogus").status == AlertStatus.unsupported


def test_check_alerts_filters_by_log_name():
    record = _make_record(log_name="job_b", duration_s=500.0)
    history = _history_df([_make_record(log_name="job_a", duration_s=100.0)])
    alert = AlertConfig(
        name="job_a_alert",
        log_name="job_a",
        metric="duration_s",
        condition="threshold",
        threshold=200,
        on_trigger="log",
    )
    result = check_alerts(record, history, [alert])
    assert result == []


def test_on_trigger_raise():
    record = _make_record(memory_bytes_spilled=200)
    alert = AlertConfig(
        name="critical_spill",
        log_name="test_job",
        metric="memory_bytes_spilled",
        condition="threshold",
        threshold=100,
        on_trigger="raise",
    )
    with pytest.raises(SparkparseAlertError) as exc_info:
        check_alerts(record, pl.DataFrame(), [alert])
    assert exc_info.value.alert_name == "critical_spill"
    assert exc_info.value.metric == "memory_bytes_spilled"
    assert exc_info.value.current == 200


def test_on_trigger_log(caplog):
    record = _make_record(memory_bytes_spilled=200)
    alert = AlertConfig(
        name="spill_warning",
        log_name="test_job",
        metric="memory_bytes_spilled",
        condition="threshold",
        threshold=100,
        severity="warning",
        on_trigger="log",
    )
    with caplog.at_level("WARNING", logger="sparkparse.alerts"):
        check_alerts(record, pl.DataFrame(), [alert])
    assert any("spill_warning" in r.message for r in caplog.records)


def test_on_trigger_log_critical(caplog):
    record = _make_record(memory_bytes_spilled=200)
    alert = AlertConfig(
        name="spill_critical",
        log_name="test_job",
        metric="memory_bytes_spilled",
        condition="threshold",
        threshold=100,
        severity="critical",
        on_trigger="log",
    )
    with caplog.at_level("ERROR", logger="sparkparse.alerts"):
        check_alerts(record, pl.DataFrame(), [alert])
    assert any("spill_critical" in r.message for r in caplog.records)
    assert any(r.levelname == "ERROR" for r in caplog.records)


def test_on_trigger_file(tmp_path: Path):
    alert_path = str(tmp_path / "alerts_out.jsonl")
    record = _make_record(memory_bytes_spilled=200)
    alert = AlertConfig(
        name="file_spill",
        log_name="test_job",
        metric="memory_bytes_spilled",
        condition="threshold",
        threshold=100,
        on_trigger="file",
    )
    check_alerts(record, pl.DataFrame(), [alert], alert_output_path=alert_path)

    content = Path(alert_path).read_text().strip()
    alert_dict = json.loads(content)
    assert alert_dict["alert_name"] == "file_spill"
    assert alert_dict["current"] == 200
    assert alert_dict["status"] == "triggered"


def test_on_trigger_file_without_path(caplog):
    record = _make_record(memory_bytes_spilled=200)
    alert = AlertConfig(
        name="orphan_file",
        log_name="test_job",
        metric="memory_bytes_spilled",
        condition="threshold",
        threshold=100,
        on_trigger="file",
    )
    with caplog.at_level("ERROR", logger="sparkparse.alerts"):
        result = check_alerts(record, pl.DataFrame(), [alert])
    assert _only(result, "orphan_file").status == AlertStatus.triggered
    assert any("no alert_output_path" in r.message for r in caplog.records)


def test_multiple_alerts_preserve_all_outcomes():
    record = _make_record(duration_s=200.0, memory_bytes_spilled=50)
    history = _history_df([_make_record(duration_s=100.0)])
    alerts = [
        AlertConfig(
            name="duration_alert",
            log_name="test_job",
            metric="duration_s",
            condition="pct_increase",
            threshold=0.20,
            on_trigger="log",
        ),
        AlertConfig(
            name="spill_alert",
            log_name="test_job",
            metric="memory_bytes_spilled",
            condition="threshold",
            threshold=100,
            on_trigger="log",
        ),
    ]
    result = check_alerts(record, history, alerts)
    assert {a.alert_name: a.status for a in result} == {
        "duration_alert": AlertStatus.triggered,
        "spill_alert": AlertStatus.clean,
    }
