import functools
import os
import shutil
import subprocess
import sys
import tempfile
import time
import webbrowser
from pathlib import Path
from typing import Any, Callable, TypeVar, overload

from pyspark.sql import SparkSession

from sparkparse.app import get
from sparkparse.models import ParsedLogDataFrames

R = TypeVar("R")
F = TypeVar("F", bound=Callable[..., Any])


class SparkparseCapture:
    spark: SparkSession
    parsed_logs: None | ParsedLogDataFrames

    def __init__(
        self,
        action: str,
        spark: SparkSession,
        headless: bool = False,
        clean_log_name: str | None = None,
    ) -> None:
        self.action = action
        self.spark = spark
        self._orig_log_dir = None
        self._headless = headless
        self._parsed_logs = None
        self._clean_log_name = clean_log_name

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
        # keep orig spark session config for later
        self._orig_spark = self.spark
        orig_conf = dict(self._orig_spark.sparkContext._conf.getAll())

        log_dir = self._orig_spark.conf.get("spark.eventLog.dir")
        self._should_cleanup = log_dir is None
        if log_dir is None:
            log_dir = tempfile.mkdtemp(prefix="sparkparse_")
        self._log_dir = log_dir

        builder = SparkSession.builder.appName("sparkparse")  # type: ignore
        builder = builder.config("spark.eventLog.enabled", "true").config(
            "spark.eventLog.dir", self._log_dir
        )
        for key, value in orig_conf.items():
            if key not in ["spark.eventLog.enabled", "spark.eventLog.dir"]:
                builder = builder.config(key, value)

        self._orig_log_name = self.spark.sparkContext.applicationId
        self.spark.stop()

        self.spark = builder.getOrCreate()

        print(f"Log enabled: {self.spark.conf.get('spark.eventLog.enabled')}")
        print(f"Log dir config: {self.spark.conf.get('spark.eventLog.dir')}")
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

    def __exit__(self, exc_type, *args):
        log_name = self.spark.sparkContext.applicationId
        self.spark.stop()
        if self._clean_log_name:
            src = f"{self._log_dir}/{log_name}"
            dst = f"{self._log_dir}/{self._clean_log_name}"

            assert os.path.exists(src), f"Source file {src} does not exist"
            os.rename(src, dst)
            assert os.path.exists(dst), f"Destination file {dst} does not exist"
            os.unlink(f"{self._log_dir}/{self._orig_log_name}")

            log_name = self._clean_log_name

        self.spark = self._orig_spark

        if exc_type:
            return

        log_dir_contents = [i for i in Path(self._log_dir).glob("*")]
        if not any(log_dir_contents):
            raise ValueError("no logs found in log directory")

        if self.action == "viz":
            self._run_dashboard_in_background()
            return

        elif self.action == "get":
            result = get(log_dir=self._log_dir, log_file=log_name)
            self._parsed_logs = result

            if self._should_cleanup and os.path.exists(self._log_dir):
                shutil.rmtree(self._log_dir)
        else:
            raise ValueError(f"Invalid action: {self.action}")


def capture_context(
    action: str = "viz",
    spark: SparkSession | None = None,
    headless: bool = False,
    clean_log_name: str | None = None,
) -> SparkparseCapture:
    if spark is None:
        _spark = SparkSession.builder.appName("temp").getOrCreate()  # type: ignore
    else:
        _spark = spark

    return SparkparseCapture(
        action,
        spark=_spark,
        headless=headless,
        clean_log_name=clean_log_name,
    )


@overload
def capture(
    func: Callable[..., R],
    *,
    action: str = ...,
    spark: SparkSession | None = ...,
    headless: bool = ...,
    clean_log_name: str | None = None,
) -> Callable[..., tuple[R, SparkparseCapture]]: ...


@overload
def capture(
    func: None = None,
    *,
    action: str = ...,
    spark: SparkSession | None = ...,
    headless: bool = ...,
    clean_log_name: str | None = None,
) -> Callable[[Callable[..., R]], Callable[..., tuple[R, SparkparseCapture]]]: ...


def capture(
    func=None,
    *,
    action: str = "viz",
    spark: SparkSession | None = None,
    headless: bool = False,
    clean_log_name: str | None = None,
) -> Any:
    def decorator(
        func: Callable[..., R],
    ) -> Callable[..., tuple[Any, SparkparseCapture]]:
        if spark is None:
            _spark = SparkSession.builder.appName("temp").getOrCreate()  # type: ignore
        else:
            _spark = spark

        cap = SparkparseCapture(
            action, spark=_spark, headless=headless, clean_log_name=clean_log_name
        )
        return cap(func)

    if func is None:
        return decorator

    return decorator(func)
