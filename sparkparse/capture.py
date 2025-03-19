import functools
import os
import shutil
import subprocess
import sys
import tempfile
import time
import uuid
import webbrowser
from pathlib import Path
from typing import Any, Callable, Optional, TypeVar, Union, overload

from pyspark.sql import SparkSession


class SparkLogCapture:
    def __init__(
        self,
        action: str,
        temp_dir: Optional[str] = None,
        spark: Optional[SparkSession] = None,
        headless: bool = False,
    ) -> None:
        self.action = action
        self.temp_dir = temp_dir
        self.spark = spark
        self._orig_log_dir = None
        self._log_dir = None
        self._should_cleanup = temp_dir is None
        self._headless = headless

    def __call__(self, func: Callable[..., Any]) -> Callable[..., Any]:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            with self:
                func_params = func.__code__.co_varnames
                if "spark" in func_params:
                    kwargs["spark"] = self.spark

                return func(*args, **kwargs)

        return wrapper

    def __enter__(self):
        if not self.spark:
            self.spark = SparkSession.getActiveSession()
            if not self.spark:
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

        builder = SparkSession.builder().appName("sparkparse")  # type: ignore
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

        if not exc_type:
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

        if (
            self._should_cleanup
            and self._log_dir is not None
            and os.path.exists(self._log_dir)
        ):
            shutil.rmtree(self._log_dir)


F = TypeVar("F", bound=Callable[..., Any])


@overload
def capture(action_or_func: F) -> F: ...


@overload
def capture(
    action_or_func: str = "viz",
    temp_dir: Optional[str] = None,
    spark: Optional[SparkSession] = None,
    headless: bool = False,
) -> SparkLogCapture: ...


def capture(
    action_or_func: Union[str, F] = "viz",
    temp_dir: Optional[str] = None,
    spark: Optional[SparkSession] = None,
    headless: bool = False,
) -> Union[SparkLogCapture, Callable[[F], F]]:
    if callable(action_or_func):
        # Used as a bare decorator: @capture
        return SparkLogCapture("viz", temp_dir, spark, headless)(action_or_func)
    else:
        # Used with arguments: @capture("viz")
        return SparkLogCapture(action_or_func, temp_dir, spark, headless)
