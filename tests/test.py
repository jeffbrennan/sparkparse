from pathlib import Path
from sparkparse.app import get_spark
import pyspark.sql.functions as F


def test_basic_transformation():
    base_dir = Path(__file__).parents[1] / "data"
    data_path = base_dir / "raw" / "G1_1e7_1e7_100_0.parquet"
    log_dir = base_dir / "logs"
    spark = get_spark(log_dir)

    df = spark.read.parquet(data_path.as_posix())

    df_clean = (
        df.limit(1000)
        .withColumn("v4", F.col("v3") * 3)
        .groupBy("v1")
        .agg(F.sum("v4").alias("v4"))
        .orderBy("v4")
    )

    output_path = base_dir / "clean"
    df_clean.write.format("csv").mode("overwrite").save(str(output_path), header=True)
