from pathlib import Path

from pyspark.sql import SparkSession


def get_spark(log_dir: Path) -> SparkSession:
    return (
        SparkSession.builder.appName("sparkparse")  # type: ignore
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.dir", log_dir.as_posix())
        .getOrCreate()
    )
