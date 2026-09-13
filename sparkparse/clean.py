import logging
from pathlib import Path
from typing import Any

import polars as pl
from pydantic import BaseModel

from sparkparse.common import resolve_dir, timeit, write_dataframe
from sparkparse.models import (
    Job,
    Metrics,
    OutputFormat,
    ParsedLog,
    PhysicalPlan,
    QueryEvent,
    Stage,
    StageStatus,
    Task,
    TaskStatus,
)
from sparkparse.schemas import COMBINED_SCHEMA, DAG_SCHEMA
from sparkparse.storage import is_cloud_path, join_path

_JOB_SCHEMA: dict[str, Any] = {
    "job_id": pl.Int64,
    "stage_id": pl.Int64,
    "job_start_timestamp": pl.Int64,
    "job_end_timestamp": pl.Int64,
    "job_duration_seconds": pl.Float64,
}

_STAGE_SCHEMA: dict[str, Any] = {
    "stage_id": pl.Int64,
    "stage_attempt_id": pl.Int64,
    "stage_start_timestamp": pl.Int64,
    "stage_end_timestamp": pl.Int64,
    "stage_duration_seconds": pl.Float64,
    "stage_num_tasks": pl.Int64,
    "stage_failure_reason": pl.Utf8,
    "stage_status": pl.Utf8,
}


def clean_jobs(jobs: list[Job]) -> pl.DataFrame:
    """One row per (job, stage) declared by JobStart, with the job's timings.

    A job still running when the log ended contributes no ``end`` row, so the
    pivot has no ``end`` column at all; the duration is unknown, not zero.
    """
    if not jobs:
        return pl.DataFrame(schema=_JOB_SCHEMA)

    jobs_df = pl.DataFrame(jobs)
    jobs_with_duration = (
        _with_missing_columns(
            jobs_df.select("job_id", "event_type", "job_timestamp").pivot(
                "event_type",
                index="job_id",
                values="job_timestamp",
                aggregate_function="first",
            ),
            {"start": pl.Int64, "end": pl.Int64},
        )
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
        # JobEnd events carry no stage list; their null row is not a relation.
        .filter(pl.col("stage_id").is_not_null())
        .join(jobs_with_duration, on="job_id", how="left")
    )
    return jobs_final


def _with_missing_columns(df: pl.DataFrame, columns: dict[str, Any]) -> pl.DataFrame:
    """Add any of ``columns`` the frame lacks, typed and null-filled.

    A stage that never completed contributes no ``end`` row, so the pivot has
    no ``end`` column at all. The column has to exist and be null — its absence
    is missing data, not a zero-length stage.
    """
    missing = [
        pl.lit(None, dtype=dtype).alias(name)
        for name, dtype in columns.items()
        if name not in df.columns
    ]
    return df.with_columns(missing) if missing else df


def clean_stages(stages: list[Stage]) -> pl.DataFrame:
    """One row per stage *attempt*.

    ``stage_id`` alone repeats across retries, so the key is
    ``(stage_id, stage_attempt_id)``; keying on stage id alone would fold a
    retry's task metrics into the original attempt.
    """
    if not stages:
        return pl.DataFrame(schema=_STAGE_SCHEMA)

    stages_df = pl.DataFrame(stages)
    index = ["stage_id", "stage_attempt_id"]

    timings = _with_missing_columns(
        stages_df.pivot(
            "event_type",
            index=index,
            values="stage_timestamp",
            aggregate_function="first",
        ),
        {"start": pl.Int64, "end": pl.Int64},
    )

    attributes = stages_df.group_by(index).agg(
        pl.col("num_tasks").drop_nulls().last().alias("stage_num_tasks"),
        pl.col("failure_reason").drop_nulls().last().alias("stage_failure_reason"),
        pl.when(pl.col("event_type").eq("end"))
        .then(pl.col("status"))
        .otherwise(None)
        .drop_nulls()
        .last()
        .alias("stage_status"),
    )

    stages_final = (
        timings.with_columns(
            (pl.col("end") - pl.col("start"))
            .mul(1 / 1000)
            .alias("stage_duration_seconds")
        )
        .rename({"start": "stage_start_timestamp", "end": "stage_end_timestamp"})
        .join(attributes, on=index, how="left")
        .with_columns(
            pl.col("stage_status").fill_null(StageStatus.running.value),
        )
    )
    return stages_final


def _null_metrics_template() -> dict:
    """Nested all-null dict matching the ``Metrics`` model shape.

    Used only when *no* task in the log reported metrics: polars would infer a
    Null column and refuse to unnest it. Every leaf stays null, so the frame
    says "not measured" rather than "measured zero".
    """

    def fields(model: type[BaseModel]) -> dict:
        out: dict = {}
        for name, field in model.model_fields.items():
            annotation = field.annotation
            if isinstance(annotation, type) and issubclass(annotation, BaseModel):
                out[name] = fields(annotation)
            else:
                out[name] = None
        return out

    return fields(Metrics)


def clean_tasks(tasks: list[Task]) -> pl.DataFrame:
    """One row per task *attempt*, including failed and speculative attempts."""
    task_df = pl.DataFrame(tasks)
    if task_df.height and task_df.schema["metrics"] == pl.Null:
        task_df = task_df.with_columns(
            pl.Series("metrics", [_null_metrics_template()] * task_df.height)
        )

    tasks_final = (
        task_df.with_columns(
            (pl.col("task_finish_time") - pl.col("task_start_time"))
            .mul(1 / 1_000)
            .alias("task_duration_seconds")
        )
        .with_columns(
            pl.col("status").eq(TaskStatus.success.value).alias("task_succeeded")
        )
        .rename(
            {
                "task_start_time": "task_start_timestamp",
                "task_finish_time": "task_end_timestamp",
                "status": "task_status",
                "failure_reason": "task_failure_reason",
            }
        )
    )
    return tasks_final


def clean_plan(
    query_times: list[QueryEvent], queries: list[PhysicalPlan]
) -> pl.DataFrame:
    # One frame for every query at once: per-query frames disagree on dtype
    # whenever a column is all-null in one plan (an initial plan with no
    # codegen ids) and populated in another.
    node_rows = [
        {**node.model_dump(), "query_id": query.query_id}
        for query in queries
        for node in query.nodes
    ]
    plan = pl.DataFrame(
        node_rows,
        schema_overrides={
            "node_id": pl.Int64,
            "whole_stage_codegen_id": pl.Int64,
            "query_id": pl.Int64,
        },
    )

    query_times_df = pl.DataFrame([qt.model_dump() for qt in query_times])
    query_times_pivoted = (
        (
            _with_missing_columns(
                query_times_df.pivot(
                    "event_type",
                    index="query_id",
                    values="query_time",
                    aggregate_function="first",
                ),
                # A query that failed or was still running when the log ended
                # has no end event at all.
                {"start": pl.Int64, "end": pl.Int64},
            )
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
            # A query still running when the log ended has no duration. The
            # label has to say so; concat_str would null the whole header.
            pl.concat_str(
                [
                    pl.col("query_id"),
                    pl.lit(" - "),
                    pl.col("query_function").fill_null("unknown"),
                    pl.lit(" ["),
                    pl.col("query_duration_seconds")
                    .mul(1 / 60)
                    .round(2)
                    .cast(pl.String)
                    .fill_null("unknown"),
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
        "value_exact",
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
        # Counts are int64 on the way in; ``value`` becomes float64 below to hold
        # rescaled timings, which rounds anything above 2**53. ``value_exact``
        # keeps the original integer for every metric that is not rescaled.
        .with_columns(
            pl.when(pl.col("metric_type").eq("nsTiming"))
            .then(None)
            .otherwise(pl.col("value").cast(pl.Int64, strict=False))
            .alias("value_exact")
        )
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
                value_exact=pl.when(pl.field("metric_type").eq("average"))
                .then(None)
                .otherwise(pl.field("value_exact")),
                readable_value=pl.when(pl.field("metric_type").eq("average"))
                .then(pl.col("median_of_average_value"))
                .otherwise(
                    pl.field("readable_value"),
                ),
            )
        )
    )

    # A node with no timing metric has no measured duration. Null says that;
    # zero would claim the operator ran instantly.
    node_durations = (
        readable_metrics_total.with_columns(
            pl.when(
                pl.col("accumulator_totals").struct.field("metric_type").eq("timing")
            )
            .then(pl.col("accumulator_totals").struct.field("value").mul(1 / 60_000))
            .otherwise(None)
            .alias("node_duration_minutes")
        )
        .group_by("query_id", "node_id")
        .agg(
            pl.when(pl.col("node_duration_minutes").is_not_null().any())
            .then(pl.sum("node_duration_minutes"))
            .otherwise(None)
            .alias("node_duration_minutes")
        )
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
        # Tie-break on the physical identity so the accumulator list order is
        # reproducible: metric type and name alone leave ties, and the input
        # order shifts whenever an unrelated query is added to the log.
        .sort("metric_order", "metric_name", "stage_id", "task_id", "accumulator_id")
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
        .sort("metric_order", "metric_name", "accumulator_id")
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

    if not result.queries:
        # No SQL executions: jobs and tasks may still exist, but there is no
        # plan to hang them on. An empty typed frame says that explicitly.
        return pl.DataFrame(schema=DAG_SCHEMA)

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

    # Per-task accumulator rows should outnumber the per-node totals. That
    # holds for any query with more than one task, but not for a plan-only or
    # single-task query, so it is a signal rather than an invariant.
    total_same_as_task = dag_final.filter(
        pl.col("n_accumulators") == pl.col("n_accumulator_totals")
    )
    if total_same_as_task.shape[0] >= dag_final.shape[0]:
        logging.debug(
            "Every node has as many task accumulators as totals; the log may "
            "cover only single-task stages or carry no task metrics."
        )
    # A join that multiplied plan rows would silently double every metric.
    assert dag_final.shape[0] == plan.shape[0]

    return dag_final


_QUERY_TASK_SCHEMA: dict[str, Any] = {
    "query_id": pl.Int64,
    "query_function": pl.Utf8,
    "query_start_timestamp": pl.Utf8,
    "query_end_timestamp": pl.Utf8,
    "query_duration_seconds": pl.Float64,
    "stage_id": pl.Int64,
    "task_id": pl.Int64,
    "nodes": pl.List(pl.Utf8),
}


def _query_task_nodes(dag: pl.DataFrame) -> pl.DataFrame:
    """One row per (query, stage, task) with the plan nodes that task fed."""
    if dag.is_empty() or "accumulators" not in dag.columns:
        # No plan to attribute tasks to; every task stays unattributed rather
        # than disappearing from the frame.
        return pl.DataFrame(schema=_QUERY_TASK_SCHEMA)
    return (
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


def query_stage_associations(dag: pl.DataFrame) -> pl.DataFrame:
    """Every (query, stage) pair observed in the plan accumulators.

    A stage reused by several queries appears once per query. This is kept as
    its own table because joining it into the task frame would duplicate task
    rows and double-count their metrics.
    """
    if dag.is_empty() or "accumulators" not in dag.columns:
        return pl.DataFrame(schema={"query_id": pl.Int64, "stage_id": pl.Int64})
    return (
        dag.filter(pl.col("node_id").le(100_000))
        .explode("accumulators")
        .with_columns(pl.col("accumulators").struct.field("stage_id").alias("stage_id"))
        .filter(pl.col("stage_id").is_not_null())
        .select("query_id", "stage_id")
        .unique()
        .sort("query_id", "stage_id")
    )


def job_stage_associations(result: ParsedLog) -> pl.DataFrame:
    """Every (job, stage) pair declared by JobStart events.

    A stage skipped because its output was already available is still listed by
    the later job, so this relation is genuinely many-to-many.
    """
    jobs_with_stages = [job for job in result.jobs if job.stages]
    if not jobs_with_stages:
        return pl.DataFrame(schema={"job_id": pl.Int64, "stage_id": pl.Int64})
    return (
        pl.DataFrame(
            [{"job_id": job.job_id, "stage_id": job.stages} for job in jobs_with_stages]
        )
        .explode("stage_id")
        .unique()
        .sort("job_id", "stage_id")
    )


def _primary_job_per_stage(
    jobs_final: pl.DataFrame, stages_final: pl.DataFrame
) -> pl.DataFrame:
    """Pick one job per stage attempt so the task frame stays one row per task.

    The full relation lives in :func:`job_stage_associations`. The job chosen
    here is the lowest-numbered job whose window contains the stage's start —
    the job that actually ran it, rather than a later job that merely listed
    it as already-computed.
    """
    if jobs_final.is_empty():
        return jobs_final
    stage_starts = stages_final.select(
        "stage_id", "stage_attempt_id", "stage_start_timestamp"
    )
    ranked = (
        jobs_final.join(stage_starts, on="stage_id", how="left")
        .with_columns(
            (
                pl.col("stage_start_timestamp").is_not_null()
                & pl.col("job_start_timestamp").le(pl.col("stage_start_timestamp"))
                & (
                    pl.col("job_end_timestamp").is_null()
                    | pl.col("job_end_timestamp").ge(pl.col("stage_start_timestamp"))
                )
            ).alias("ran_in_job")
        )
        .sort(
            ["stage_id", "stage_attempt_id", "ran_in_job", "job_id"],
            descending=[False, False, True, False],
            nulls_last=True,
        )
        .unique(
            subset=["stage_id", "stage_attempt_id"], keep="first", maintain_order=True
        )
    )
    return ranked.drop("ran_in_job", "stage_start_timestamp")


@timeit
def log_to_combined_df(
    result: ParsedLog, dag: pl.DataFrame, log_name: str
) -> pl.DataFrame:
    if not result.tasks:
        return pl.DataFrame(schema=COMBINED_SCHEMA)

    stages_final = clean_stages(result.stages)
    jobs_final = clean_jobs(result.jobs)
    primary_jobs = _primary_job_per_stage(jobs_final, stages_final)

    tasks = clean_tasks(result.tasks)
    query_task_nodes = _query_task_nodes(dag)

    # One task belongs to one physical stage attempt, but a shared stage can
    # serve several queries. Attribute the task to the earliest query and keep
    # the whole relation in query_stage instead of duplicating the task row.
    query_task_lookup = (
        query_task_nodes.sort("query_id")
        .group_by("stage_id", "task_id")
        .agg(
            pl.col("query_id").first(),
            pl.col("query_function").first(),
            pl.col("query_start_timestamp").first(),
            pl.col("query_end_timestamp").first(),
            pl.col("query_duration_seconds").first(),
            # Already node_id-ordered per query; keep that order across the
            # queries a shared task serves instead of re-sorting as text.
            pl.col("nodes").flatten().unique(maintain_order=True).alias("nodes"),
            pl.col("query_id").n_unique().alias("query_count"),
        )
    )

    combined = (
        tasks.join(stages_final, on=["stage_id", "stage_attempt_id"], how="left")
        .join(primary_jobs, on=["stage_id", "stage_attempt_id"], how="left")
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
        "query_count",
        "job_id",
        "stage_id",
        "stage_attempt_id",
        "job_start_timestamp",
        "job_end_timestamp",
        "job_duration_seconds",
        "stage_start_timestamp",
        "stage_end_timestamp",
        "stage_duration_seconds",
        "stage_num_tasks",
        "stage_status",
        "stage_failure_reason",
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
        "partition_id",
        "attempt",
        # Resource usage is per attempt; output accounting is per *successful*
        # attempt. Keeping both means a retried task is not counted twice.
        "task_status",
        "task_succeeded",
        "task_failure_reason",
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
        # Tasks from jobs no SQL execution claims (schema inference, RDD work)
        # keep a null query_id. Dropping them would erase real resource usage
        # from the ledger.
        .with_columns(
            pl.col("query_count").fill_null(0),
            pl.col("nodes").fill_null(pl.lit([], dtype=pl.List(pl.Utf8))),
        )
        .sort("query_id", "stage_id", "task_id", nulls_last=True)
    )

    final = combined_clean.select(final_cols)
    return final


def get_job_idle_time(df: pl.DataFrame) -> dict:
    idle_time_df = (
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
    )
    idle_time = (
        idle_time_df.select(pl.sum("gap_ms").alias("idle_time_ms"))
        .with_columns(
            get_readable_col(pl.col("idle_time_ms"), "timing").alias("readable")
        )
        .to_dicts()[0]
    )
    return idle_time


def write_parsed_log(
    df: pl.DataFrame,
    out_dir: str,
    out_format: OutputFormat,
    parsed_name: str,
    suffix: str,
) -> None:
    if is_cloud_path(out_dir):
        out_path = join_path(out_dir, f"{parsed_name}_{suffix}")
    else:
        out_dir_path = resolve_dir(out_dir, 1)
        assert isinstance(out_dir_path, Path)
        out_dir_path.mkdir(parents=True, exist_ok=True)
        out_path = out_dir_path / f"{parsed_name}_{suffix}"

    logging.info(f"Writing parsed log: {out_path}")
    logging.debug(f"Output format: {out_format}")
    logging.debug(f"{df.shape[0]} rows and {df.shape[1]} columns")
    logging.debug(f"{df.head()}")

    write_dataframe(df, out_path, out_format)
