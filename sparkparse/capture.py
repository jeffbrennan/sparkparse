import functools
import logging
import os
import subprocess
import sys
import tempfile
import time
import webbrowser
from collections.abc import Callable
from typing import Any, Literal, TypeVar, overload

from pyspark.sql import SparkSession

from sparkparse import alerts, history
from sparkparse.analyze import to_plan_summary
from sparkparse.app import get
from sparkparse.models import ParsedLogDataFrames, RunRecord
from sparkparse.storage import (
    copy_file,
    ensure_dir,
    get_path_name,
    get_path_stem,
    join_path,
    list_files,
    path_exists,
    remove_dir,
)

_log = logging.getLogger(__name__)

CaptureAction = Literal["viz", "get", "analyze"]

R = TypeVar("R")
F = TypeVar("F", bound=Callable[..., Any])


class SparkparseCapture:
    spark: SparkSession
    parsed_logs: None | ParsedLogDataFrames

    def __init__(
        self,
        action: CaptureAction,
        spark: SparkSession,
        temp_dir: str | None = None,
        headless: bool = False,
        history_path: str | None = None,
        log_name: str | None = None,
        alert_config: str | None = None,
    ) -> None:
        self.action = action
        self.temp_dir = temp_dir
        self.spark = spark
        self._orig_log_dir = None
        self._log_dir = None
        self._should_cleanup = temp_dir is None
        self._headless = headless
        self._parsed_logs = None
        self._analysis: dict[str, Any] | None = None
        self._history_path = history_path
        self._log_name = log_name
        self._alert_config = alert_config
        self._last_record: RunRecord | None = None
        self._triggered_alerts: list[dict] = []

    def __call__(
        self, func: Callable[..., R]
    ) -> Callable[..., tuple[R, "SparkparseCapture"]]:
        @functools.wraps(func)
        def get_wrapper(*args: Any, **kwargs: Any) -> tuple[R, "SparkparseCapture"]:
            with self:
                func_params = func.__code__.co_varnames
                if "spark" in func_params:
                    kwargs["spark"] = self.spark

                result = func(*args, **kwargs)
                return result, self

        return get_wrapper

    def __enter__(self):
        if not self.spark and not SparkSession.getActiveSession():
            raise ValueError(
                "No active SparkSession found - please create one before using this context manager."
            )

        if self.temp_dir is None:
            self._log_dir = tempfile.mkdtemp(prefix="sparkparse_")
        else:
            ensure_dir(self.temp_dir)
            self._log_dir = self.temp_dir

        orig_conf: dict[str, str] = {}
        if self.spark or SparkSession.getActiveSession():
            self._orig_spark = self.spark
            self._orig_log_dir = self._orig_spark.conf.get("spark.eventLog.dir")
            orig_conf = dict(self._orig_spark.sparkContext._conf.getAll())
            self.spark.stop()

        builder = SparkSession.builder.appName("sparkparse")
        if hasattr(self, "_orig_spark"):
            for key, value in orig_conf.items():
                if key not in ["spark.eventLog.enabled", "spark.eventLog.dir"]:
                    builder = builder.config(key, value)

        builder = (
            builder.config("spark.eventLog.enabled", "true")
            .config("spark.eventLog.dir", self._log_dir)
            .config("spark.eventLog.rolling.enabled", "false")
            .config("spark.eventLog.compression.codec", "none")
        )

        self.spark = builder.getOrCreate()

        _log.info("Log enabled: %s", self.spark.conf.get("spark.eventLog.enabled"))
        _log.info("Log dir config: %s", self.spark.conf.get("spark.eventLog.dir"))
        return self

    def _run_dashboard_in_background(self):
        cmd = [
            sys.executable,
            "-m",
            "sparkparse.app",
            "viz",
            "--log-dir",
            str(self._log_dir),
        ]

        nohup_cmd = " ".join([f'"{c}"' for c in cmd])
        subprocess.Popen(
            f"nohup {nohup_cmd} > /dev/null 2>&1 &",
            shell=True,
            start_new_session=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            preexec_fn=os.setpgrp,
            close_fds=True,
        )

        if not self._headless:
            time.sleep(2)
            webbrowser.open("http://127.0.0.1:8050/")

    def _record_history_and_alerts(self) -> None:
        if self._parsed_logs is None or self._history_path is None:
            return

        if self._log_dir is None:
            _log.warning("log directory is not set, skipping history and alerts")
            return

        effective_log_name = self._log_name or get_path_name(self._log_dir)
        record = history.record_from_dfs(self._parsed_logs, effective_log_name)
        self._last_record = record

        history.append(record, self._history_path)
        _log.info("Appended run record %s to %s", record.run_id, self._history_path)

        if self._alert_config is not None:
            rules = alerts.load_alert_config(self._alert_config)
            hist_df = history.read(self._history_path, effective_log_name)
            self._triggered_alerts = alerts.check_alerts(record, hist_df, rules)
            if self._triggered_alerts:
                _log.warning(
                    "%d alert(s) triggered for %s",
                    len(self._triggered_alerts),
                    effective_log_name,
                )

    def __exit__(self, exc_type, *args):
        self.spark.stop()
        self.spark = self._orig_spark

        if exc_type:
            return

        if self._log_dir is None:
            raise ValueError("log directory is not set")

        log_dir_contents = list_files(self._log_dir)
        if not log_dir_contents:
            raise ValueError("no logs found in log directory")

        if self._orig_log_dir is not None:
            for f in log_dir_contents:
                out_path = join_path(self._orig_log_dir, get_path_stem(f))
                copy_file(f, out_path)

        if self.action == "viz":
            self._run_dashboard_in_background()
            return
        elif self.action == "get":
            if self._log_dir is None:
                raise ValueError("log directory is not set")
            result = get(log_dir=self._log_dir)
            self._parsed_logs = result
        elif self.action == "analyze":
            if self._log_dir is None:
                raise ValueError("log directory is not set")
            result = get(log_dir=self._log_dir)
            self._parsed_logs = result
            log_name = get_path_name(self._log_dir)
            self._analysis = to_plan_summary(result, log_name)
        else:
            raise ValueError(f"Invalid action: {self.action}")

        if self._history_path is not None:
            self._record_history_and_alerts()

        if (
            self._should_cleanup
            and self._log_dir is not None
            and path_exists(self._log_dir)
        ):
            remove_dir(self._log_dir)


def capture_context(
    action: CaptureAction = "viz",
    temp_dir: str | None = None,
    spark: SparkSession | None = None,
    headless: bool = False,
    history_path: str | None = None,
    log_name: str | None = None,
    alert_config: str | None = None,
) -> SparkparseCapture:
    if spark is None:
        _spark = SparkSession.builder.appName("sparkparse_capture").getOrCreate()
    else:
        _spark = spark

    return SparkparseCapture(
        action,
        temp_dir=temp_dir,
        spark=_spark,
        headless=headless,
        history_path=history_path,
        log_name=log_name,
        alert_config=alert_config,
    )


@overload
def capture(
    func: Callable[..., R],
    *,
    action: CaptureAction = ...,
    temp_dir: str | None = ...,
    spark: SparkSession | None = ...,
    headless: bool = ...,
    history_path: str | None = ...,
    log_name: str | None = ...,
    alert_config: str | None = ...,
) -> Callable[..., tuple[R, SparkparseCapture]]: ...


@overload
def capture(
    func: None = None,
    *,
    action: CaptureAction = ...,
    temp_dir: str | None = ...,
    spark: SparkSession | None = ...,
    headless: bool = ...,
    history_path: str | None = ...,
    log_name: str | None = ...,
    alert_config: str | None = ...,
) -> Callable[[Callable[..., R]], Callable[..., tuple[R, SparkparseCapture]]]: ...


def capture(
    func=None,
    *,
    action: CaptureAction = "viz",
    temp_dir: str | None = None,
    spark: SparkSession | None = None,
    headless: bool = False,
    history_path: str | None = None,
    log_name: str | None = None,
    alert_config: str | None = None,
) -> Any:
    def decorator(
        func: Callable[..., R],
    ) -> Callable[..., tuple[Any, SparkparseCapture]]:
        if spark is None:
            _spark = SparkSession.builder.appName("sparkparse_capture").getOrCreate()
        else:
            _spark = spark

        cap = SparkparseCapture(
            action,
            spark=_spark,
            temp_dir=temp_dir,
            headless=headless,
            history_path=history_path,
            log_name=log_name,
            alert_config=alert_config,
        )
        return cap(func)

    if func is None:
        return decorator

    return decorator(func)
