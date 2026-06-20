import datetime
import io
import json
import uuid
from pathlib import Path
from unittest.mock import patch

import polars as pl
import pytest

from sparkparse.alerts import (
    AlertConfig,
    SparkparseAlertError,
    check_alerts,
    load_alert_config,
)
from sparkparse.models import RunRecord


def _make_record(**overrides) -> RunRecord:
    defaults = dict(
        run_id=uuid.uuid4().hex,
        run_at=datetime.datetime.now(datetime.UTC),
        log_name="test_job",
        duration_s=100.0,
        bytes_read=1000,
        bytes_written=500,
        shuffle_bytes=200,
        spill_bytes=0,
        n_queries=2,
        n_stages=3,
        n_tasks=50,
        n_cartesian_joins=0,
        max_node_duration_min=1.5,
        max_scan_bytes=800,
    )
    defaults.update(overrides)
    return RunRecord(**defaults)


def _history_df(records: list[RunRecord]) -> pl.DataFrame:
    return pl.DataFrame([r.model_dump() for r in records])


def test_load_alert_config_from_toml(tmp_path: Path):
    toml_content = """
[[alerts]]
name = "duration_regression"
log_name = "nightly_job"
metric = "duration_s"
condition = "pct_increase"
threshold = 0.20
window = 5
severity = "warning"
on_trigger = "log"

[[alerts]]
name = "spill_alert"
log_name = "nightly_job"
metric = "spill_bytes"
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


def test_check_alerts_threshold_fires():
    record = _make_record(spill_bytes=200)
    alert = AlertConfig(
        name="spill_check",
        log_name="test_job",
        metric="spill_bytes",
        condition="threshold",
        threshold=100,
        on_trigger="log",
    )
    result = check_alerts(record, pl.DataFrame(), [alert])
    assert len(result) == 1
    assert result[0]["alert_name"] == "spill_check"
    assert result[0]["current"] == 200
    assert result[0]["baseline"] is None


def test_check_alerts_threshold_does_not_fire():
    record = _make_record(spill_bytes=50)
    alert = AlertConfig(
        name="spill_check",
        log_name="test_job",
        metric="spill_bytes",
        condition="threshold",
        threshold=100,
        on_trigger="log",
    )
    result = check_alerts(record, pl.DataFrame(), [alert])
    assert result == []


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
    assert len(result) == 1
    assert result[0]["baseline"] == 100.0
    assert result[0]["current"] == 130.0


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
    assert result == []


def test_check_alerts_pct_increase_baseline_zero_fires():
    current = _make_record(spill_bytes=100)
    history = _history_df([_make_record(spill_bytes=0)])
    alert = AlertConfig(
        name="new_spill",
        log_name="test_job",
        metric="spill_bytes",
        condition="pct_increase",
        threshold=0.5,
        on_trigger="log",
    )
    result = check_alerts(current, history, [alert])
    assert len(result) == 1
    assert result[0]["baseline"] == 0.0


def test_check_alerts_pct_increase_baseline_zero_no_fire():
    current = _make_record(spill_bytes=0)
    history = _history_df([_make_record(spill_bytes=0)])
    alert = AlertConfig(
        name="no_spill",
        log_name="test_job",
        metric="spill_bytes",
        condition="pct_increase",
        threshold=0.5,
        on_trigger="log",
    )
    result = check_alerts(current, history, [alert])
    assert result == []


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
    assert len(result) == 1
    assert result[0]["baseline"] == 100.0
    assert result[0]["current"] == 200.0


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
    assert result == []


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
    assert len(result) == 1
    assert result[0]["baseline"] == 100.0


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
    assert len(result) == 1
    assert result[0]["baseline"] == 100.0


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


def test_check_alerts_empty_history_threshold():
    record = _make_record(duration_s=300.0)
    alert = AlertConfig(
        name="threshold_only",
        log_name="test_job",
        metric="duration_s",
        condition="threshold",
        threshold=200,
        on_trigger="log",
    )
    result = check_alerts(record, pl.DataFrame(), [alert])
    assert len(result) == 1


def test_on_trigger_raise():
    record = _make_record(spill_bytes=200)
    alert = AlertConfig(
        name="critical_spill",
        log_name="test_job",
        metric="spill_bytes",
        condition="threshold",
        threshold=100,
        on_trigger="raise",
    )
    with pytest.raises(SparkparseAlertError) as exc_info:
        check_alerts(record, pl.DataFrame(), [alert])
    assert exc_info.value.alert_name == "critical_spill"
    assert exc_info.value.metric == "spill_bytes"
    assert exc_info.value.current == 200


def test_on_trigger_log(caplog):
    record = _make_record(spill_bytes=200)
    alert = AlertConfig(
        name="spill_warning",
        log_name="test_job",
        metric="spill_bytes",
        condition="threshold",
        threshold=100,
        severity="warning",
        on_trigger="log",
    )
    with caplog.at_level("WARNING", logger="sparkparse.alerts"):
        result = check_alerts(record, pl.DataFrame(), [alert])
    assert len(result) == 1
    assert any("spill_warning" in r.message for r in caplog.records)


def test_on_trigger_log_critical(caplog):
    record = _make_record(spill_bytes=200)
    alert = AlertConfig(
        name="spill_critical",
        log_name="test_job",
        metric="spill_bytes",
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
    record = _make_record(spill_bytes=200)
    alert = AlertConfig(
        name="file_spill",
        log_name="test_job",
        metric="spill_bytes",
        condition="threshold",
        threshold=100,
        on_trigger="file",
    )
    check_alerts(record, pl.DataFrame(), [alert], alert_output_path=alert_path)

    content = Path(alert_path).read_text().strip()
    alert_dict = json.loads(content)
    assert alert_dict["alert_name"] == "file_spill"
    assert alert_dict["current"] == 200


def test_on_trigger_file_without_path(caplog):
    record = _make_record(spill_bytes=200)
    alert = AlertConfig(
        name="orphan_file",
        log_name="test_job",
        metric="spill_bytes",
        condition="threshold",
        threshold=100,
        on_trigger="file",
    )
    with caplog.at_level("ERROR", logger="sparkparse.alerts"):
        result = check_alerts(record, pl.DataFrame(), [alert])
    assert len(result) == 1
    assert any("no alert_output_path" in r.message for r in caplog.records)


def test_multiple_alerts_some_fire():
    record = _make_record(duration_s=200.0, spill_bytes=50)
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
            metric="spill_bytes",
            condition="threshold",
            threshold=100,
            on_trigger="log",
        ),
    ]
    result = check_alerts(record, history, alerts)
    assert len(result) == 1
    assert result[0]["alert_name"] == "duration_alert"
