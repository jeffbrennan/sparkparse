from pathlib import Path

import polars as pl
from pyspark.sql import SparkSession

from sparkparse.models import OutputFormat


def write_dataframe(
    df: pl.DataFrame, out_path: Path, out_format: OutputFormat, overwrite: bool = True
) -> None:
    if overwrite:
        out_path.unlink(missing_ok=True)

    if out_format == OutputFormat.csv:
        df.write_csv(out_path.as_posix(), include_header=True)
    elif out_format == OutputFormat.parquet:
        df.write_parquet(out_path.as_posix())
    elif out_format == OutputFormat.delta:
        df.write_delta(out_path.as_posix())
    elif out_format == OutputFormat.json:
        df.write_json(out_path.as_posix())


def get_spark(log_dir: Path) -> SparkSession:
    return (
        SparkSession.builder.appName("sparkparse")  # type: ignore
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.dir", log_dir.as_posix())
        .getOrCreate()
    )
