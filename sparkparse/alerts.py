"""Regression detection against run history.

Alert rules are defined in TOML (or passed as Python dicts) and evaluated
against the current ``RunRecord`` and a comparable historical cohort for the
same ``log_name``. Three condition types are supported:

- ``threshold`` — fire if the current metric exceeds a fixed value. This works
  even with no comparable history because it needs no baseline.
- ``pct_increase`` — fire if ``(current - baseline) / baseline > threshold``,
  where baseline is the mean of the last ``window`` comparable runs.
- ``absolute_increase`` — fire if ``current - baseline > threshold``.

Every rule produces an ``AlertAssessment`` — including rules that do not fire
and rules that could not be evaluated — so a silent rule is never mistaken for
a clean one. Triggered alerts dispatch via ``on_trigger``: ``"log"`` emits a
log record, ``"raise"`` raises ``SparkparseAlertError``, ``"file"`` appends a
JSON record to ``alert_output_path``.
"""

from __future__ import annotations

import datetime
import logging
import math
import statistics
import tomllib
from enum import StrEnum
from pathlib import Path
from typing import Literal

import polars as pl
from pydantic import BaseModel, field_validator

from sparkparse.history import (
    MEASURE_FIELDS,
    METRIC_CAPABILITY,
    select_baseline_cohort,
)
from sparkparse.models import RunRecord
from sparkparse.storage import append_text, ensure_dir, is_cloud_path, open_file

logger = logging.getLogger(__name__)


class AlertStatus(StrEnum):
    """Outcome of evaluating one rule, whether or not it fired."""

    triggered = "triggered"
    clean = "clean"
    insufficient_data = "insufficient_data"
    unsupported = "unsupported"
    skipped = "skipped"


class SparkparseAlertError(Exception):
    """Raised when an alert with ``on_trigger = "raise"`` fires."""

    def __init__(
        self,
        alert_name: str,
        metric: str,
        current: float,
        baseline: float | None,
    ) -> None:
        self.alert_name = alert_name
        self.metric = metric
        self.current = current
        self.baseline = baseline
        super().__init__(
            f"Alert '{alert_name}' triggered: {metric}={current} (baseline={baseline})"
        )


class AlertAssessment(BaseModel):
    """The result of evaluating one rule against one run."""

    alert_name: str
    log_name: str
    metric: str
    condition: str
    threshold: float
    severity: str
    status: AlertStatus
    current: float | None = None
    baseline: float | None = None
    sample_count: int = 0
    window: int = 0
    reason: str | None = None
    triggered_at: str | None = None


class AlertConfig(BaseModel):
    name: str
    log_name: str
    metric: str
    condition: Literal["pct_increase", "absolute_increase", "threshold"]
    threshold: float
    window: int = 10
    min_samples: int = 1
    severity: Literal["warning", "critical"] = "warning"
    on_trigger: Literal["log", "raise", "file"] = "log"
    match_fingerprint: bool = True
    match_backend: bool = False
    match_runtime: bool = False

    @field_validator("window", "min_samples")
    @classmethod
    def _positive(cls, value: int) -> int:
        if value <= 0:
            raise ValueError("window and min_samples must be positive")
        return value

    @field_validator("threshold")
    @classmethod
    def _finite(cls, value: float) -> float:
        if not math.isfinite(value):
            raise ValueError("threshold must be finite")
        return value


def load_alert_config(path: str) -> list[AlertConfig]:
    """Load alert rules from a TOML file.

    The TOML file must contain an ``[[alerts]]`` table per rule. Local paths
    and cloud URIs are both supported via ``storage.open_file``.
    """
    with open_file(path, "rb") as f:
        data = tomllib.load(f)

    raw_alerts = data.get("alerts", [])
    return [AlertConfig(**a) for a in raw_alerts]


def _dispatch(
    alert: AlertConfig, assessment: AlertAssessment, alert_output_path: str | None
) -> None:
    if alert.on_trigger == "raise":
        raise SparkparseAlertError(
            alert_name=alert.name,
            metric=alert.metric,
            current=assessment.current or 0.0,
            baseline=assessment.baseline,
        )

    if alert.on_trigger == "log":
        msg = f"Alert '{alert.name}' triggered: {alert.metric}={assessment.current}"
        if assessment.baseline is not None:
            msg += f" (baseline={assessment.baseline})"
        if alert.severity == "critical":
            logger.error(msg)
        else:
            logger.warning(msg)

    if alert.on_trigger == "file":
        if alert_output_path is None:
            logger.error(
                "Alert '%s' has on_trigger='file' but no alert_output_path set",
                alert.name,
            )
            return
        if not is_cloud_path(alert_output_path):
            ensure_dir(Path(alert_output_path).parent)
        append_text(alert_output_path, assessment.model_dump_json() + "\n")


def check_alerts(
    record: RunRecord,
    history: pl.DataFrame,
    alerts: list[AlertConfig],
    alert_output_path: str | None = None,
) -> list[AlertAssessment]:
    """Evaluate every alert rule against the current record and history.

    Fires ``on_trigger`` actions for triggered alerts. Returns one assessment
    per rule, preserving ``insufficient_data`` and ``skipped`` outcomes.
    """
    assessments: list[AlertAssessment] = []

    for alert in alerts:
        if alert.log_name != record.log_name:
            continue

        if alert.metric not in MEASURE_FIELDS:
            logger.warning(
                "Alert '%s' references unknown metric '%s', skipping",
                alert.name,
                alert.metric,
            )
            assessments.append(
                AlertAssessment(
                    alert_name=alert.name,
                    log_name=alert.log_name,
                    metric=alert.metric,
                    condition=alert.condition,
                    threshold=alert.threshold,
                    severity=alert.severity,
                    status=AlertStatus.unsupported,
                    reason=f"unknown metric {alert.metric!r}",
                    window=alert.window,
                )
            )
            continue

        current_value = getattr(record, alert.metric)
        if current_value is None:
            logger.warning(
                "Alert '%s' references unavailable metric '%s', skipping",
                alert.name,
                alert.metric,
            )
            assessments.append(
                AlertAssessment(
                    alert_name=alert.name,
                    log_name=alert.log_name,
                    metric=alert.metric,
                    condition=alert.condition,
                    threshold=alert.threshold,
                    severity=alert.severity,
                    status=AlertStatus.skipped,
                    reason="current metric unavailable",
                    window=alert.window,
                )
            )
            continue

        current = float(current_value)
        cohort, _ = select_baseline_cohort(
            history,
            record,
            log_name=alert.log_name,
            window=alert.window,
            metric=alert.metric,
            required_capability=METRIC_CAPABILITY.get(alert.metric),
            match_fingerprint=alert.match_fingerprint,
            match_backend=alert.match_backend,
            match_runtime=alert.match_runtime,
        )
        values = [float(row[alert.metric]) for row in cohort]
        baseline = statistics.fmean(values) if values else None

        assessment = AlertAssessment(
            alert_name=alert.name,
            log_name=alert.log_name,
            metric=alert.metric,
            condition=alert.condition,
            threshold=alert.threshold,
            severity=alert.severity,
            current=current,
            baseline=baseline,
            sample_count=len(values),
            window=alert.window,
            status=AlertStatus.clean,
        )

        if alert.condition == "threshold":
            assessment.status = (
                AlertStatus.triggered
                if current > alert.threshold
                else AlertStatus.clean
            )
        elif len(values) < alert.min_samples:
            assessment.status = AlertStatus.insufficient_data
            assessment.reason = (
                f"{len(values)} comparable sample(s), {alert.min_samples} required"
            )
        else:
            assert baseline is not None
            if alert.condition == "pct_increase":
                fired = (
                    current > 0
                    if baseline == 0
                    else (current - baseline) / baseline > alert.threshold
                )
            else:
                fired = current - baseline > alert.threshold
            assessment.status = AlertStatus.triggered if fired else AlertStatus.clean

        if assessment.status == AlertStatus.triggered:
            assessment.triggered_at = datetime.datetime.now(datetime.UTC).isoformat()
            _dispatch(alert, assessment, alert_output_path)

        assessments.append(assessment)

    return assessments
