import uuid
import pyspark.sql.functions as F
from pyspark.sql import SparkSession

import sparkparse
from tests.test import config


def _run_broadcast_join(spark: SparkSession, data_path):
    run_id = uuid.uuid4()
    print(f"starting run {run_id}")
    df = spark.read.parquet(data_path.as_posix())
    base_df = df.select("id1", "v3")
    broadcast_df = df.limit(10).select("id1", "id2")
    df_after_broadcast = base_df.join(broadcast_df, how="left", on="id1")
    df_final = (
        df_after_broadcast.groupBy("id2").agg(F.sum("v3").alias("v3")).orderBy("v3")
    )

    df_final.count()
    print(f"finishing run {run_id}")


def test_basic_capture():
    spark, data_path, _ = config()
    with sparkparse.capture_context(spark=spark, headless=True) as cap:
        _run_broadcast_join(cap.spark, data_path)


def test_basic_capture_get():
    spark, data_path, _ = config()
    with sparkparse.capture_context(action="get", spark=spark) as cap:
        _run_broadcast_join(cap.spark, data_path)

    if cap._parsed_logs is None:
        raise ValueError("No logs found")

    print(cap._parsed_logs.combined.head())
    assert cap._parsed_logs.combined.shape[0] > 0


def test_capture_with_decorator(headless: bool = True):
    spark, data_path, _ = config()

    @sparkparse.capture(spark=spark, headless=headless)
    def run_broadcast_join_with_decorator(spark, data_path):
        _run_broadcast_join(spark, data_path)

    _, cap = run_broadcast_join_with_decorator(spark=spark, data_path=data_path)
    assert cap._parsed_logs is None


def test_capture_with_decorator_get():
    spark, data_path, _ = config()

    @sparkparse.capture(action="get", spark=spark)
    def run_broadcast_join_with_decorator(spark, data_path) -> dict[str, str]:
        _run_broadcast_join(spark, data_path)
        my_result = {"result": "done"}
        return my_result

    result, cap = run_broadcast_join_with_decorator(spark=spark, data_path=data_path)
    assert result == {"result": "done"}
    if cap._parsed_logs is None:
        raise ValueError("No logs found")

    print(cap._parsed_logs.combined.head())
    assert cap._parsed_logs.combined.shape[0] > 0


if __name__ == "__main__":
    test_capture_with_decorator(False)
