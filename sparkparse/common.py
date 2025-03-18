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
        .config("spark.history.fs.logDirectory", log_dir.as_posix())
        .config("spark.executor.memory", "12g")
        .config("spark.driver.memory", "8g")
        .config("spark.shuffle.spill", "true")
        .config("spark.sql.execution.arrow.maxRecordsPerBatch", "1000000")
        .getOrCreate()
    )


def create_header(header_length: int, title: str, center: bool, spacer: str):
    if center:
        spacer_len = (header_length - len(title)) // 2
        output = f"{spacer * spacer_len}{title}{spacer * spacer_len}"
    else:
        output = f"{title}{spacer * (header_length - len(title))}"

    if len(output) < header_length:
        output += spacer * (header_length - len(output))
    if len(output) > header_length:
        output = spacer * header_length + "\n" + output

    return output


def resolve_dir(incoming_dir: str | Path, default_nesting=2) -> Path:
    # resolves path of incoming dir_str
    # if provided path does not exist, will attempt to resolve relative to sparkparse root

    if isinstance(incoming_dir, Path):
        initial_path = incoming_dir
    else:
        initial_path = Path(incoming_dir)

    if initial_path.exists():
        return initial_path

    path = Path(__file__).parents[default_nesting] / incoming_dir
    if not path.exists():
        if not path.parent.exists():
            raise ValueError(
                f"directory {path} does not exist and parent is also missing"
            )
        path.mkdir(exist_ok=True, parents=True)

    return path
