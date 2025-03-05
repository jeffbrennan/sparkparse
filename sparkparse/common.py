import datetime
import time
from functools import wraps
from pathlib import Path

import polars as pl
from pyspark.sql import SparkSession

from sparkparse.models import OutputFormat


def get_current_time() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)


def timeit(func):
    # https://dev.to/kcdchennai/python-decorator-to-measure-execution-time-54hk
    @wraps(func)
    def timeit_wrapper(*args, **kwargs):
        start_time = time.perf_counter()
        result = func(*args, **kwargs)
        end_time = time.perf_counter()
        total_time = end_time - start_time
        print(
            f"{get_current_time()} -- Function {func.__name__} Took {total_time * 1000:.2f} ms"
        )
        return result

    return timeit_wrapper


def write_dataframe(
    df: pl.DataFrame, out_path: Path, out_format: OutputFormat, overwrite: bool = True
) -> None:
    if overwrite:
        out_path.unlink(missing_ok=True)

    if out_format == OutputFormat.csv:
        df.write_csv(out_path.with_suffix(".csv").as_posix(), include_header=True)
    elif out_format == OutputFormat.parquet:
        df.write_parquet(out_path.with_suffix(".parquet").as_posix())
    elif out_format == OutputFormat.delta:
        df.write_delta(out_path.as_posix())
    elif out_format == OutputFormat.json:
        df.write_json(out_path.with_suffix(".json").as_posix())


def get_spark(log_dir: Path) -> SparkSession:
    return (
        SparkSession.builder.appName("sparkparse")  # type: ignore
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.dir", log_dir.as_posix())
        .getOrCreate()
    )
