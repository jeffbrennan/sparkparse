import logging

import polars as pl

from sparkparse.common import resolve_dir, timeit, write_dataframe
from sparkparse.models import (
    Job,
    OutputFormat,
    ParsedLog,
    PhysicalPlan,
    QueryEvent,
    Stage,
    Task,
)


def clean_jobs(jobs: list[Job]) -> pl.DataFrame:
    jobs_df = pl.DataFrame(jobs)
    jobs_with_duration = (
        jobs_df.select("job_id", "event_type", "job_timestamp")
        .pivot("event_type", index="job_id", values="job_timestamp")
        .with_columns(
            (pl.col("end") - pl.col("start"))
            .mul(1 / 1_000)
            .alias("job_duration_seconds")
        )
        .rename({"start": "job_start_timestamp", "end": "job_end_timestamp"})
    )
    jobs_final = (
        jobs_df.select("job_id", "stages")
        .explode("stages")
        .rename({"stages": "stage_id"})
        .join(jobs_with_duration, on="job_id", how="left")
    )
    return jobs_final


def clean_stages(stages: list[Stage]) -> pl.DataFrame:
    stages_df = pl.DataFrame(stages)
    stages_final = (
        stages_df.pivot("event_type", index="stage_id", values="stage_timestamp")
        .with_columns(
            (pl.col("end") - pl.col("start"))
            .mul(1 / 1000)
            .alias("stage_duration_seconds")
        )
        .rename({"start": "stage_start_timestamp", "end": "stage_end_timestamp"})
    )
    return stages_final


def clean_tasks(tasks: list[Task]) -> pl.DataFrame:
    task_df = pl.DataFrame(tasks)
    tasks_final = task_df.with_columns(
        (pl.col("task_finish_time") - pl.col("task_start_time"))
        .mul(1 / 1_000)
        .alias("task_duration_seconds")
    ).rename(
        {
            "task_start_time": "task_start_timestamp",
            "task_finish_time": "task_end_timestamp",
        }
    )
    return tasks_final


def clean_plan(
    query_times: list[QueryEvent], queries: list[PhysicalPlan]
) -> pl.DataFrame:
    plan = pl.DataFrame()
    for query in queries:
        plan = pl.concat(
            [
                plan,
                pl.DataFrame(query.nodes).with_columns(
                    pl.lit(query.query_id).alias("query_id")
                ),
            ]
        )

    query_times_df = pl.DataFrame(query_times)
    query_times_pivoted = (
        (
            query_times_df.pivot("event_type", index="query_id", values="query_time")
            .rename({"start": "query_start_timestamp", "end": "query_end_timestamp"})
            .with_columns(
                (pl.col("query_end_timestamp") - pl.col("query_start_timestamp"))
                .mul(1 / 1_000)
                .round(2)
                .alias("query_duration_seconds")
            )
        )
        .join(
            query_times_df.filter(pl.col("event_type") == "start").select(
                "query_id", "query_function"
            ),
            on="query_id",
            how="left",
        )
        .with_columns(
            pl.concat_str(
                [
                    pl.col("query_id"),
                    pl.lit(" - "),
                    pl.col("query_function"),
                    pl.lit(" ["),
                    pl.col("query_duration_seconds")
                    .mul(1 / 60)
                    .round(2)
                    .cast(pl.String),
                    pl.lit(" min]"),
                ]
            ).alias("query_header")
        )
    )

    plan_final = plan.with_columns(
        pl.col("child_nodes")
        .cast(pl.List(pl.String))
        .list.join(", ")
        .alias("child_nodes")
    ).join(query_times_pivoted, on="query_id", how="left")
    return plan_final


def get_readable_size(value_col: pl.Expr) -> pl.Expr:
    return (
        pl.when(value_col < 1024)
        .then(
            pl.struct(
                [value_col.alias("readable_value"), pl.lit("B").alias("readable_unit")]
            )
        )
        .when(value_col < 1024**2)
        .then(
            pl.struct(
                [
                    (value_col / 1024).alias("readable_value"),
                    pl.lit("KiB").alias("readable_unit"),
                ]
            )
        )
        .when(value_col < 1024**3)
        .then(
            pl.struct(
                [
                    (value_col / 1024**2).alias("readable_value"),
                    pl.lit("MiB").alias("readable_unit"),
                ]
            )
        )
        .when(value_col < 1024**4)
        .then(
            pl.struct(
                [
                    (value_col / 1024**3).alias("readable_value"),
                    pl.lit("GiB").alias("readable_unit"),
                ]
            )
        )
        .otherwise(
            pl.struct(
                [
                    (value_col / 1024**4).alias("readable_value"),
                    pl.lit("TiB").alias("readable_unit"),
                ]
            )
        )
    )


def get_readable_timing(value_col: pl.Expr) -> pl.Expr:
    return (
        pl.when(value_col.lt(1_000))
        .then(
            pl.struct(
                [value_col.alias("readable_value"), pl.lit("ms").alias("readable_unit")]
            )
        )
        .when(value_col.lt(60_000))
        .then(
            pl.struct(
                [
                    value_col.mul(1 / 1000).alias("readable_value"),
                    pl.lit("s").alias("readable_unit"),
                ]
            )
        )
        .when(value_col.lt(3_600_000))
        .then(
            pl.struct(
                [
                    value_col.mul(1 / 60_000).alias("readable_value"),
                    pl.lit("min").alias("readable_unit"),
                ]
            )
        )
        .otherwise(
            pl.struct(
                [
                    value_col.mul(1 / 3_600_000).alias("readable_value"),
                    pl.lit("hr").alias("readable_unit"),
                ]
            )
        )
    )


def get_readable_col(
    value_col: pl.Expr, value_type: str, round_decimals: int = 2
) -> pl.Expr:
    value_type_map = {
        "timing": get_readable_timing,
        "size": get_readable_size,
    }

    calc = value_type_map[value_type](value_col)

    calc_final = calc.struct.with_fields(
        readable_str=pl.concat_str(
            [
                calc.struct.field("readable_value").round(round_decimals),
                pl.lit(" "),
                calc.struct.field("readable_unit"),
            ]
        )
    )
    return calc_final


@timeit
def parse_accumulator_metrics(dag_long: pl.DataFrame, df_type: str) -> pl.DataFrame:
    if df_type == "task":
        output_struct = "accumulators"
        value_col = "update"
    elif df_type == "total":
        output_struct = "accumulator_totals"
        value_col = "value"
    else:
        raise ValueError(f"Invalid df_type: {df_type}")

    units = {"timing": "ms", "size": "B", "sum": "", "average": "", "nsTiming": "ms"}
    id_cols = [
        "query_id",
        "node_id",
        "stage_id",
        "task_id",
        "accumulator_id",
        "metric_name",
    ]
    accumulator_cols = [
        "stage_id",
        "task_id",
        "accumulator_id",
        "metric_name",
        "metric_type",
        "value",
        "unit",
        "readable_value",
        "readable_unit",
        "readable_str",
    ]

    base = dag_long.select(*id_cols, "metric_type", value_col).filter(
        pl.col(value_col).is_not_null()
    )

    if df_type == "task":
        base = base.rename({"update": "value"})
    else:
        base = (
            base.with_columns(
                pl.col("value")
                .rank("ordinal", descending=True)
                .over("query_id", "node_id", "metric_type", "metric_name")
                .alias("rank")
            ).filter(pl.col("rank") == 1)
        ).drop("rank")

    readable_metrics = (
        base.sort(["metric_type", "metric_name"])
        .with_columns(
            pl.when(pl.col("metric_type").eq("nsTiming"))
            .then(pl.col("value").mul(1 / 1e6))
            .otherwise(pl.col("value").alias("value"))
        )
        .with_columns(
            pl.when(pl.col("metric_type").eq("nsTiming"))
            .then(pl.lit("timing"))
            .otherwise(pl.col("metric_type"))
            .alias("metric_type")
        )
        .with_columns(pl.col("metric_type").replace_strict(units).alias("unit"))
        .with_columns(
            pl.when(pl.col("metric_type") == "size")
            .then(get_readable_col(pl.col("value"), "size"))
            .when(pl.col("metric_type") == "timing")
            .then(get_readable_col(pl.col("value"), "timing"))
            .otherwise(
                pl.struct(
                    [
                        pl.col("value").alias("readable_value"),
                        pl.col("unit").alias("readable_unit"),
                        pl.col("value").round(2).cast(pl.String).alias("readable_str"),
                    ]
                )
            )
            .alias("readable")
        )
        .unnest("readable")
        .with_columns(pl.col("readable_value").round(1))
        .with_columns(
            pl.struct([pl.col(col) for col in accumulator_cols]).alias(output_struct)
        )
        .select(*id_cols, output_struct)
    )
    return readable_metrics


def get_node_metrics(
    readable_metrics: pl.DataFrame, dag_long: pl.DataFrame, dag_base_cols: list[str]
) -> tuple[pl.DataFrame, pl.DataFrame]:
    average_metrics = (
        readable_metrics.select("accumulators")
        .unnest("accumulators")
        .filter(pl.col("metric_type") == "average")
        .group_by("accumulator_id")
        .agg(pl.median("value").alias("median_of_average_value"))
    )

    # structured accumulator totals per query, node, accumulator
    readable_metrics_total = (
        parse_accumulator_metrics(dag_long, "total")
        .join(average_metrics, on="accumulator_id", how="left")
        .with_columns(
            pl.col("accumulator_totals").struct.with_fields(
                value=pl.when(pl.field("metric_type").eq("average"))
                .then(pl.col("median_of_average_value"))
                .otherwise(pl.field("value")),
                readable_value=pl.when(pl.field("metric_type").eq("average"))
                .then(pl.col("median_of_average_value"))
                .otherwise(
                    pl.field("readable_value"),
                ),
            )
        )
    )

    node_durations = (
        readable_metrics_total.with_columns(
            pl.when(
                pl.col("accumulator_totals").struct.field("metric_type").eq("timing")
            )
            .then(pl.col("accumulator_totals").struct.field("value").mul(1 / 60_000))
            .otherwise(pl.lit(0))
            .alias("node_duration_minutes")
        )
        .group_by("query_id", "node_id")
        .agg(pl.sum("node_duration_minutes").alias("node_duration_minutes"))
    )
    metric_type_order = {
        "timing": 0,
        "size": 1,
        "sum": 2,
        "average": 3,
    }

    dag_metrics = (
        dag_long.join(
            readable_metrics,
            on=["query_id", "node_id", "accumulator_id", "task_id"],
            how="inner",
        )
        .with_columns(
            pl.col("accumulators")
            .struct.field("metric_type")
            .replace_strict(metric_type_order)
            .alias("metric_order")
        )
        .sort("metric_order", "metric_name")
        .drop("metric_order")
        .group_by(*dag_base_cols)
        .agg(pl.col("accumulators"))
        .with_columns(pl.col("accumulators").list.len().alias("n_accumulators"))
        .with_columns(pl.coalesce("n_accumulators", pl.lit(0)).alias("n_accumulators"))
    )

    dag_metrics_totals = (
        dag_long.select("query_id", "node_id", "metric_name")
        .unique()
        .join(
            readable_metrics_total,
            on=["query_id", "node_id", "metric_name"],
            how="inner",
        )
        .with_columns(
            pl.col("accumulator_totals")
            .struct.field("metric_type")
            .replace_strict(metric_type_order)
            .alias("metric_order")
        )
        .sort("metric_order", "metric_name")
        .drop("metric_order")
        .group_by("query_id", "node_id")
        .agg(pl.col("accumulator_totals"))
        .with_columns(
            pl.col("accumulator_totals").list.len().alias("n_accumulator_totals")
        )
        .with_columns(
            pl.coalesce("n_accumulator_totals", pl.lit(0)).alias("n_accumulator_totals")
        )
    )

    dag_metrics_combined = (
        dag_metrics_totals.select(
            "query_id", "node_id", "accumulator_totals", "n_accumulator_totals"
        )
        .join(
            dag_metrics,
            on=["query_id", "node_id"],
            how="left",
        )
        .select(
            "query_id",
            "node_id",
            "accumulators",
            "accumulator_totals",
            "n_accumulators",
            "n_accumulator_totals",
        )
        .sort("query_id", "node_id")
    )

    return node_durations, dag_metrics_combined


@timeit
def get_dag_long(result: ParsedLog, plan: pl.DataFrame) -> pl.DataFrame:
    tasks = clean_tasks(result.tasks)

    driver_accumulators = (
        pl.DataFrame(result.driver_accum_updates)
        .rename({"update": "driver_update"})
        .sort("query_id", "accumulator_id")
        .with_columns(pl.col("driver_update").alias("driver_value"))
        .with_columns(
            pl.col("driver_update")
            .cum_sum()
            .over("query_id", "accumulator_id")
            .alias("driver_value")
        )
    )

    accumulators_long = (
        tasks.select(
            "stage_id",
            "task_id",
            "task_start_timestamp",
            "task_end_timestamp",
            "task_duration_seconds",
            "executor_id",
            "accumulators",
        )
        .rename({"task_id": "task_id_orig"})
        .explode("accumulators")
        .unnest("accumulators")
        .drop("task_id")
        .rename({"task_id_orig": "task_id"})
        .select(
            "stage_id",
            "task_id",
            "task_start_timestamp",
            "task_end_timestamp",
            "task_duration_seconds",
            "executor_id",
            "accumulator_id",
            "update",
            "value",
        )
        .sort("stage_id", "task_id")
    )

    plan_long = (
        (
            plan.rename({"node_id": "node_id_orig"})
            .explode("accumulators")
            .unnest("accumulators")
            .drop("node_id")
            .rename({"node_id_orig": "node_id"})
        )
        .select(
            [
                "query_id",
                "query_start_timestamp",
                "query_end_timestamp",
                "query_duration_seconds",
                "whole_stage_codegen_id",
                "node_id",
                "node_type",
                "child_nodes",
                "metric_name",
                "accumulator_id",
                "metric_type",
                "node_string",
            ]
        )
        .sort("query_id", "node_id")
    )

    dag_long = (
        plan_long.join(accumulators_long, on="accumulator_id", how="left")
        .join(driver_accumulators, on=["query_id", "accumulator_id"], how="left")
        .with_columns(pl.coalesce("value", "driver_value").alias("value"))
        .with_columns(pl.coalesce("update", "driver_update").alias("update"))
        .drop("driver_value", "driver_update")
    )
    return dag_long


@timeit
def log_to_dag_df(result: ParsedLog) -> pl.DataFrame:
    dag_base_cols = [
        "query_id",
        "query_start_timestamp",
        "query_end_timestamp",
        "query_duration_seconds",
        "whole_stage_codegen_id",
        "node_id",
        "node_type",
        "child_nodes",
    ]

    extra_cols = ["details", "query_function", "query_header"]

    plan = clean_plan(result.query_times, result.queries)
    dag_long = get_dag_long(result, plan)

    readable_metrics = parse_accumulator_metrics(dag_long, "task")
    node_durations, dag_metrics_combined = get_node_metrics(
        readable_metrics, dag_long, dag_base_cols
    )

    dag_final = (
        (
            plan.select(*dag_base_cols + extra_cols)
            .join(dag_metrics_combined, on=["query_id", "node_id"], how="left")
            .join(node_durations, on=["query_id", "node_id"], how="left")
            .with_columns(
                pl.coalesce("node_duration_minutes", pl.lit(0)).alias(
                    "node_duration_minutes"
                )
            )
            .sort("query_id", "node_id")
        )
        # adjust wholestagecodegen labels
        .with_columns(
            pl.when(pl.col("node_id").ge(100_000))
            .then(pl.col("node_id") - 100_000)
            .otherwise(pl.col("node_id"))
            .alias("node_id_adj")
        )
        .with_columns(
            pl.concat_str(
                [
                    pl.lit("["),
                    pl.col("node_id_adj").cast(pl.String),
                    pl.lit("] "),
                    pl.col("node_type"),
                ]
            ).alias("node_name")
        )
        .with_columns(
            [
                pl.col(col).mul(1000).cast(pl.Datetime).dt.strftime("%Y-%m-%dT%H:%M:%S")
                for col in ["query_start_timestamp", "query_end_timestamp"]
            ]
        )
    )

    total_same_as_task = dag_final.filter(
        pl.col("n_accumulators") == pl.col("n_accumulator_totals")
    )
    assert total_same_as_task.shape[0] < dag_final.shape[0]
    assert dag_final.shape[0] == plan.shape[0]

    return dag_final


@timeit
def log_to_combined_df(
    result: ParsedLog, dag: pl.DataFrame, log_name: str
) -> pl.DataFrame:
    stages_final = clean_stages(result.stages)
    jobs_final = clean_jobs(result.jobs)

    tasks = clean_tasks(result.tasks)
    query_task_lookup = (
        dag.filter(pl.col("node_id").le(100_000))
        .explode("accumulators")
        .with_columns(
            [
                pl.col("accumulators").struct.field("task_id").alias("task_id"),
                pl.col("accumulators").struct.field("stage_id").alias("stage_id"),
            ]
        )
        .filter(pl.col("task_id").is_not_null())
        .filter(pl.col("stage_id").is_not_null())
        .unique()
        .sort("query_id", "stage_id", "task_id", "node_id")
        .group_by(
            "query_id",
            "query_function",
            "query_start_timestamp",
            "query_end_timestamp",
            "query_duration_seconds",
            "stage_id",
            "task_id",
        )
        .agg(pl.col("node_name").alias("nodes"))
    )

    combined = (
        tasks.join(stages_final, on="stage_id", how="left")
        .join(jobs_final, on="stage_id", how="left")
        .sort("job_id", "stage_id", "task_id")
        .unnest("metrics")
        .unnest("task_metrics")
        .unnest("executor_metrics")
        .unnest("shuffle_read_metrics")
        .unnest("shuffle_write_metrics")
        .unnest("input_metrics")
        .unnest("output_metrics")
        .unnest("push_based_shuffle")
        .with_columns(pl.lit(log_name).alias("log_name"))
        .with_columns(pl.lit(result.name).alias("parsed_log_name"))
    )

    timestamp_cols = [col for col in combined.columns if "timestamp" in col]
    final_cols = [
        # system identifiers / run info
        "log_name",
        "parsed_log_name",
        "query_id",
        "query_function",
        "query_start_timestamp",
        "query_end_timestamp",
        "query_duration_seconds",
        "job_id",
        "stage_id",
        "job_start_timestamp",
        "job_end_timestamp",
        "job_duration_seconds",
        "stage_start_timestamp",
        "stage_end_timestamp",
        "stage_duration_seconds",
        # core task info
        "task_id",
        "task_start_timestamp",
        "task_end_timestamp",
        "task_duration_seconds",
        "nodes",
        # task metrics
        # general
        "executor_run_time_seconds",
        "executor_cpu_time_seconds",
        "executor_deserialize_time_seconds",
        "executor_deserialize_cpu_time_seconds",
        "result_size_bytes",
        "jvm_gc_time_seconds",
        "result_serialization_time_seconds",
        "memory_bytes_spilled",
        "disk_bytes_spilled",
        "peak_execution_memory_bytes",
        # input
        "bytes_read",
        "records_read",
        # output
        "bytes_written",
        "records_written",
        # shuffle read
        "shuffle_remote_blocks_fetched",
        "shuffle_local_blocks_fetched",
        "shuffle_fetch_wait_time_seconds",
        "shuffle_remote_bytes_read",
        "shuffle_remote_bytes_read_to_disk",
        "shuffle_local_bytes_read",
        "shuffle_records_read",
        "shuffle_remote_requests_duration",
        "shuffle_bytes_read",
        # shuffle write
        "shuffle_bytes_written",
        "shuffle_write_time_seconds",
        "shuffle_records_written",
        # push based shuffle
        "merged_corrupt_block_chunks",
        "merged_fetch_fallback_count",
        "merged_remote_blocks_fetched",
        "merged_local_blocks_fetched",
        "merged_remote_chunks_fetched",
        "merged_local_chunks_fetched",
        "merged_remote_bytes_read",
        "merged_local_bytes_read",
        "merged_remote_requests_duration",
        # extra task metadata
        "executor_id",
        "host",
        "index",
        "attempt",
        "failed",
        "killed",
        "speculative",
        "task_loc",
        "task_type",
    ]

    combined_clean = (
        combined.with_columns(
            [
                pl.col(col)
                .mul(1000)
                .cast(pl.Datetime)
                .dt.strftime("%Y-%m-%dT%H:%M:%S.%f")
                .alias(col)
                for col in timestamp_cols
            ]
        )
        .rename(
            {
                "result_size": "result_size_bytes",
                "peak_execution_memory": "peak_execution_memory_bytes",
            }
        )
        .with_columns(
            (
                pl.col("shuffle_remote_bytes_read") + pl.col("shuffle_local_bytes_read")
            ).alias("shuffle_bytes_read")
        )
        .with_columns(
            [
                pl.col("executor_cpu_time")
                .mul(1 / 1e9)
                .alias("executor_cpu_time_seconds"),
                pl.col("executor_run_time")
                .mul(1 / 1e6)
                .alias("executor_run_time_seconds"),
                pl.col("executor_deserialize_cpu_time")
                .mul(1 / 1e9)
                .alias("executor_deserialize_cpu_time_seconds"),
                pl.col("executor_deserialize_time")
                .mul(1 / 1e6)
                .alias("executor_deserialize_time_seconds"),
                pl.col("shuffle_write_time")
                .mul(1 / 1e9)
                .alias("shuffle_write_time_seconds"),
                pl.col("jvm_gc_time").mul(1 / 1e6).alias("jvm_gc_time_seconds"),
                pl.col("result_serialization_time")
                .mul(1 / 1e6)
                .alias("result_serialization_time_seconds"),
                pl.col("shuffle_fetch_wait_time")
                .mul(1 / 1e6)
                .alias("shuffle_fetch_wait_time_seconds"),
            ]
        )
        .join(query_task_lookup, on=["stage_id", "task_id"], how="left")
        .filter(pl.col("query_id").is_not_null())
        .sort("query_id", "stage_id", "task_id")
    )

    final = combined_clean.select(final_cols)
    return final


def get_job_idle_time(df: pl.DataFrame) -> dict:
    return (
        df.select("job_id", "job_start_timestamp", "job_end_timestamp")
        .unique()
        .melt(id_vars="job_id")
        .with_columns(
            pl.col("value").cast(pl.Datetime).dt.epoch("ms").alias("timestamp")
        )
        .sort("job_id", "timestamp")
        .with_columns(
            pl.col("timestamp")
            .shift(1)
            .over(partition_by=1, order_by=["job_id", "value"])
            .alias("prev_timestamp")
        )
        .with_columns(pl.col("timestamp").sub(pl.col("prev_timestamp")).alias("gap_ms"))
        .with_columns(
            pl.when(
                (pl.col("variable").eq("job_end_timestamp")).or_(
                    pl.col("gap_ms").lt(0), pl.col("gap_ms").is_null()
                )
            )
            .then(0)
            .otherwise(pl.col("gap_ms"))
            .alias("gap_ms")
        )
        .select(pl.sum("gap_ms").alias("idle_time_ms"))
        .with_columns(
            get_readable_col(pl.col("idle_time_ms"), "timing").alias("readable")
        )
        .to_dicts()[0]
    )


def write_parsed_log(
    df: pl.DataFrame,
    out_dir: str,
    out_format: OutputFormat,
    parsed_name: str,
    suffix: str,
) -> None:
    out_dir_path = resolve_dir(out_dir, 1)
    out_dir_path.mkdir(parents=True, exist_ok=True)

    out_path = out_dir_path / f"{parsed_name}_{suffix}"

    logging.info(f"Writing parsed log: {out_path}")
    logging.debug(f"Output format: {out_format}")
    logging.debug(f"{df.shape[0]} rows and {df.shape[1]} columns")
    logging.debug(f"{df.head()}")

    write_dataframe(df, out_path, out_format)
