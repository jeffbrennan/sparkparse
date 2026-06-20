"""Regression detection against run history.

Alert rules are defined in TOML (or passed as Python dicts) and evaluated
against the current ``RunRecord`` and historical records for the same
``log_name``. Three condition types are supported:

- ``threshold`` — fire if the current metric exceeds a fixed value.
- ``pct_increase`` — fire if ``(current - baseline) / baseline > threshold``,
  where baseline is the mean of the last ``window`` runs.
- ``absolute_increase`` — fire if ``current - baseline > threshold``.

Triggered alerts dispatch via ``on_trigger``: ``"log"`` emits a log record,
``"raise"`` raises ``SparkparseAlertError``, ``"file"`` appends a JSON record
to ``alert_output_path``.
"""

from __future__ import annotations

import datetime
import json
import logging
import tomllib
from pathlib import Path
from typing import Literal

import polars as pl
from pydantic import BaseModel

from sparkparse.models import RunRecord
from sparkparse.storage import append_text, ensure_dir, is_cloud_path, open_file

logger = logging.getLogger(__name__)

_VALID_METRICS = set(RunRecord.model_fields.keys()) - {"run_id", "run_at", "log_name"}


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


class AlertConfig(BaseModel):
    name: str
    log_name: str
    metric: str
    condition: Literal["pct_increase", "absolute_increase", "threshold"]
    threshold: float
    window: int = 10
    severity: Literal["warning", "critical"] = "warning"
    on_trigger: Literal["log", "raise", "file"] = "log"


def load_alert_config(path: str) -> list[AlertConfig]:
    """Load alert rules from a TOML file.

    The TOML file must contain an ``[[alerts]]`` table per rule. Local paths
    and cloud URIs are both supported via ``storage.open_file``.
    """
    with open_file(path, "rb") as f:
        data = tomllib.load(f)

    raw_alerts = data.get("alerts", [])
    return [AlertConfig(**a) for a in raw_alerts]


def _compute_baseline(history: pl.DataFrame, alert: AlertConfig, current_run_id: str) -> float:
    """Mean of the alert metric over the last ``window`` runs, excluding the
    current run. Returns 0.0 when no history is available.
    """
    if history.is_empty() or "run_id" not in history.columns:
        return 0.0

    hist = (
        history.filter(
            (pl.col("log_name") == alert.log_name) & (pl.col("run_id") != current_run_id)
        )
        .sort("run_at", descending=True)
        .head(alert.window)
    )

    if hist.is_empty():
        return 0.0

    mean_val = hist[alert.metric].mean()
    if mean_val is not None and isinstance(mean_val, int | float):
        return float(mean_val)
    return 0.0


def _dispatch(alert: AlertConfig, alert_dict: dict, alert_output_path: str | None) -> None:
    if alert.on_trigger == "raise":
        raise SparkparseAlertError(
            alert_name=alert.name,
            metric=alert.metric,
            current=alert_dict["current"],
            baseline=alert_dict.get("baseline"),
        )

    if alert.on_trigger == "log":
        msg = f"Alert '{alert.name}' triggered: {alert.metric}={alert_dict['current']}"
        if alert_dict.get("baseline") is not None:
            msg += f" (baseline={alert_dict['baseline']})"
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
        append_text(alert_output_path, json.dumps(alert_dict, default=str) + "\n")


def check_alerts(
    record: RunRecord,
    history: pl.DataFrame,
    alerts: list[AlertConfig],
    alert_output_path: str | None = None,
) -> list[dict]:
    """Evaluate all alert rules against the current record and history.

    Fires ``on_trigger`` actions for any triggered alerts. Returns a list of
    triggered alert dicts (empty if none triggered).
    """
    triggered: list[dict] = []

    for alert in alerts:
        if alert.log_name != record.log_name:
            continue

        if alert.metric not in _VALID_METRICS:
            logger.warning(
                "Alert '%s' references unknown metric '%s', skipping",
                alert.name,
                alert.metric,
            )
            continue

        current = float(getattr(record, alert.metric))
        baseline: float | None = None

        if alert.condition == "threshold":
            fired = current > alert.threshold
        else:
            baseline = _compute_baseline(history, alert, record.run_id)
            if alert.condition == "pct_increase":
                if baseline == 0:
                    fired = current > 0
                else:
                    fired = (current - baseline) / baseline > alert.threshold
            elif alert.condition == "absolute_increase":
                fired = current - baseline > alert.threshold
            else:
                fired = False

        if not fired:
            continue

        alert_dict = {
            "alert_name": alert.name,
            "log_name": alert.log_name,
            "metric": alert.metric,
            "condition": alert.condition,
            "threshold": alert.threshold,
            "severity": alert.severity,
            "current": current,
            "baseline": baseline,
            "triggered_at": datetime.datetime.now(datetime.UTC).isoformat(),
        }

        _dispatch(alert, alert_dict, alert_output_path)
        triggered.append(alert_dict)

    return triggered
