import functools
import os
import shutil
import subprocess
import sys
import tempfile
import time
import webbrowser
from pathlib import Path
from typing import Any, Callable, Literal, Optional, TypeVar, cast, overload

from pyspark.sql import SparkSession

from sparkparse.app import get
from sparkparse.models import ParsedLogDataFrames

R = TypeVar("R")
F = TypeVar("F", bound=Callable[..., Any])


class SparkLogCapture:
    spark: SparkSession
    parsed_logs: None | ParsedLogDataFrames

    def __init__(
        self,
        action: str,
        spark: SparkSession,
        temp_dir: Optional[str] = None,
        headless: bool = False,
    ) -> None:
        self.action = action
        self.temp_dir = temp_dir
        self.spark = spark
        self._orig_log_dir = None
        self._log_dir = None
        self._should_cleanup = temp_dir is None
        self._headless = headless
        self._parsed_logs = None

    def __call__(
        self, func: Callable[..., R]
    ) -> Callable[..., R] | Callable[..., tuple[R, "SparkLogCapture"]]:
        if self.action == "get":

            @functools.wraps(func)
            def get_wrapper(*args: Any, **kwargs: Any) -> tuple[R, "SparkLogCapture"]:
                with self:
                    func_params = func.__code__.co_varnames
                    if "spark" in func_params:
                        kwargs["spark"] = self.spark

                    result = func(*args, **kwargs)
                    return result, self

            return get_wrapper
        else:

            @functools.wraps(func)
            def wrapper(*args: Any, **kwargs: Any) -> R:
                with self:
                    func_params = func.__code__.co_varnames
                    if "spark" in func_params:
                        kwargs["spark"] = self.spark

                    return func(*args, **kwargs)

            return wrapper

    def __enter__(self):
        if not self.spark and not SparkSession.getActiveSession():
            raise ValueError(
                "No active SparkSession found - please create one before using this context manager."
            )

        if self.temp_dir is None:
            self._log_dir = tempfile.mkdtemp(prefix="sparkparse_")
        else:
            os.makedirs(self.temp_dir, exist_ok=True)
            self._log_dir = self.temp_dir

        if self.spark or SparkSession.getActiveSession():
            self._orig_spark = self.spark
            self._orig_log_dir = self._orig_spark.conf.get("spark.eventLog.dir")
            orig_conf = dict(self._orig_spark.sparkContext._conf.getAll())
            self.spark.stop()

        builder = SparkSession.builder.appName("sparkparse")  # type: ignore
        if hasattr(self, "_orig_spark"):
            for key, value in orig_conf.items():
                if key not in ["spark.eventLog.enabled", "spark.eventLog.dir"]:
                    builder = builder.config(key, value)

        builder = builder.config("spark.eventLog.enabled", "true").config(
            "spark.eventLog.dir", self._log_dir
        )

        self.spark = builder.getOrCreate()

        print(f"Log directory: {self._log_dir}")
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
        if self.spark is None:
            raise ValueError("no SparkSession found")

        self.spark.stop()
        self.spark = self._orig_spark

        if exc_type:
            return

        if self._log_dir is None:
            raise ValueError("log directory is not set")

        log_dir_contents = [i for i in Path(self._log_dir).glob("*")]
        if not any(log_dir_contents):
            raise ValueError("no logs found in log directory")

        if self._orig_log_dir is not None:
            for f in log_dir_contents:
                out_path = Path(self._orig_log_dir) / f.stem
                shutil.copy2(f.as_posix(), out_path)

        if self.action == "viz":
            self._run_dashboard_in_background()
            return

        elif self.action == "get":
            if self._log_dir is None:
                raise ValueError("log directory is not set")
            result = get(log_dir=self._log_dir)
            self._parsed_logs = result
            return

        if (
            self._should_cleanup
            and self._log_dir is not None
            and os.path.exists(self._log_dir)
        ):
            shutil.rmtree(self._log_dir)


@overload
def capture(action_or_func: F) -> F: ...


@overload
def capture(
    action_or_func: str = "viz",
    temp_dir: str | None = None,
    spark: SparkSession | None = None,
    headless: bool = False,
) -> SparkLogCapture: ...


@overload
def capture(
    action_or_func: Literal["get"],
    temp_dir: str | None = None,
    spark: SparkSession | None = None,
    headless: bool = False,
) -> Callable[[Callable[..., R]], Callable[..., tuple[R, SparkLogCapture]]]: ...


def capture(
    action_or_func: str | F = "viz",
    temp_dir: str | None = None,
    spark: SparkSession | None = None,
    headless: bool = False,
) -> (
    SparkLogCapture
    | F
    | Callable[[Callable[..., R]], Callable[..., R]]
    | Callable[[Callable[..., R]], Callable[..., tuple[R, SparkLogCapture]]]
):
    if spark is None:
        spark = SparkSession.builder.appName("temp").getOrCreate()  # type: ignore

    if spark is None:
        raise ValueError("No SparkSession found")

    if callable(action_or_func):
        # Decorator used directly without arguments: @sparkparse.capture
        slc = SparkLogCapture("viz", spark=spark, temp_dir=temp_dir, headless=headless)
        return cast(
            F, slc(action_or_func)
        )  # Type system should handle this correctly now
    else:
        # Decorator used with arguments: @sparkparse.capture(action_or_func="get")
        return SparkLogCapture(
            action_or_func, temp_dir=temp_dir, spark=spark, headless=headless
        )
