from functools import reduce
from pathlib import Path

import pyspark
import pyspark.sql.functions as F
from pyspark.sql import Window

from sparkparse.common import get_spark


def config(data_size: str = "small"):
    data_lookup = {
        "small": "1e7_1e7",
        "medium": "1e8_1e8",
        "large": "1e9_1e9",
    }

    base_dir = Path(__file__).parents[1] / "data"
    data_path = base_dir / "raw" / f"G1_{data_lookup[data_size]}_100_0.parquet"
    log_dir = base_dir / "logs" / "raw"
    spark = get_spark(log_dir)

    return spark, data_path, base_dir


def test_broadcast_nested_loop_join():
    spark, data_path, base_dir = config()
    df = (
        spark.read.parquet(data_path.as_posix())
        .limit(1000)
        .createOrReplaceTempView("df")
    )
    df2 = spark.sql("SELECT * FROM df").createOrReplaceTempView("df2")
    df3 = spark.sql("SELECT * FROM df").createOrReplaceTempView("df3")

    query = """
    select
        df.id1,
        df2.id2,
        df3.id3,
        df.v3
    from df
    left join df2 on df.id1 = df2.id1
    left join df3 on df.id1 = df2.id1
    """

    df_final = spark.sql(query)
    df_final.count()


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


def test_complex_transformation():
    # 52 min - use sparingly
    spark, data_path, base_dir = config("medium")

    df_large = spark.read.parquet(data_path.as_posix())

    df_large_clean = (
        df_large.withColumn(
            "id1_2_3", F.concat_ws("~", F.col("id1"), F.col("id2"), F.col("id3"))
        )
        .withColumn(
            "v4", F.when(F.col("v3") > 10, F.col("v3") * 3).otherwise(F.col("v3"))
        )
        .withColumn(
            "v5", F.when(F.col("v3") > 20, F.col("v3") * 3).otherwise(F.col("v3"))
        )
    )
    df_agg = df_large_clean.groupBy("id1_2_3").agg(
        F.sum("v5").alias("v5"),
        F.sum("v4").alias("v4"),
        F.mean("v3").alias("v3_mean"),
        F.count("v3").alias("v3_count"),
    )

    df_final = (
        df_agg.join(
            df_large_clean.select("id1_2_3", "id1", "id2", "id3").distinct(),
            on="id1_2_3",
            how="left",
        )
        .withColumn(
            "rank", F.rank().over(Window.partitionBy("id1").orderBy(F.col("v5").desc()))
        )
        .withColumn("id3", F.regexp_replace(F.col("id3"), "id", "ID!!"))
        .filter(F.col("rank") < 42)
        .withColumn(
            "rn",
            F.row_number().over(Window.partitionBy("id2").orderBy(F.col("v5").desc())),
        )
        .filter(F.col("rn") > 42)
        .withColumn("current_date", F.current_date())
        .sort(F.col("v5").desc())
        .coalesce(1)
    )

    output_path = base_dir / "clean" / "complex"

    print(df_final.count())  # force full computation
    df_final.limit(1000).write.format("csv").mode("overwrite").save(
        str(output_path), header=True
    )


def _test_row_count_explosion_join():
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


def _test_basic_transformation():
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
