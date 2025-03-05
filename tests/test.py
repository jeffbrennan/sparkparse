from pathlib import Path

import pyspark.sql.functions as F

from sparkparse.common import get_spark


def config():
    base_dir = Path(__file__).parents[1] / "data"
    data_path = base_dir / "raw" / "G1_1e7_1e7_100_0.parquet"
    log_dir = base_dir / "logs" / "raw"
    spark = get_spark(log_dir)

    return spark, data_path, base_dir


def test_broadcast_join():
    spark, data_path, base_dir = config()
    df = spark.read.parquet(data_path.as_posix())
    base_df = df.select("id1", "v3")
    broadcast_df = df.limit(10).select("id1", "id2")
    df_after_broadcast = base_df.join(broadcast_df, how="left", on="id1")
    df_final = (
        df_after_broadcast.groupBy("id2").agg(F.sum("v3").alias("v3")).orderBy("v3")
    )

    output_path = base_dir / "clean" / "broadcast"
    df_final.write.format("csv").mode("overwrite").save(str(output_path), header=True)


def test_row_count_explosion_join():
    spark, data_path, base_dir = config()

    df = spark.read.parquet(data_path.as_posix())
    initial_count = df.count()
    print(f"inital count: {initial_count:,}")
    duplicated_df_base = df.select("id1")
    duplicated_df = (
        duplicated_df_base.unionByName(duplicated_df_base)
        .unionByName(duplicated_df_base)
        .limit(100_000)
    )

    df_exploded = df.join(duplicated_df, how="left", on="id1")
    post_join_count = df_exploded.count()

    print(
        f"post join count {post_join_count:,} [increase: {post_join_count / initial_count:.2f}x]"
    )

    df_final = df_exploded.groupBy("id1").agg(F.sum("v3").alias("v3")).orderBy("v3")

    output_path = base_dir / "clean" / "exploded"
    df_final.limit(10).write.format("csv").mode("overwrite").save(
        str(output_path), header=True
    )


def test_basic_transformation():
    spark, data_path, base_dir = config()

    df = spark.read.parquet(data_path.as_posix())

    df_clean = (
        df.limit(1000)
        .withColumn("v4", F.col("v3") * 3)
        .groupBy("id1")
        .agg(F.sum("v4").alias("v4"))
        .orderBy("v4")
    )

    output_path = base_dir / "clean" / "basic"
    df_clean.write.format("csv").mode("overwrite").save(str(output_path), header=True)
