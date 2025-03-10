import datetime
import json
import logging
import re
from pathlib import Path

import polars as pl

from sparkparse.common import timeit, write_dataframe
from sparkparse.models import (
    Accumulator,
    EventType,
    ExecutorMetrics,
    InputMetrics,
    Job,
    Metrics,
    NodeType,
    OutputFormat,
    OutputMetrics,
    ParsedLog,
    ParsedLogDataFrames,
    PhysicalPlan,
    PhysicalPlanDetails,
    PhysicalPlanNode,
    PlanAccumulator,
    QueryEvent,
    ShuffleReadMetrics,
    ShuffleWriteMetrics,
    Stage,
    Task,
    TaskMetrics,
)

logger = logging.getLogger(__name__)


def parse_job(line_dict: dict) -> Job:
    if line_dict["Event"].endswith("Start"):
        event_type = EventType.start
        timestamp = line_dict["Submission Time"]
    else:
        event_type = EventType.end
        timestamp = line_dict["Completion Time"]
    line_dict["job_timestamp"] = timestamp
    return Job(
        event_type=event_type,
        **line_dict,
    )


def parse_stage(line_dict: dict) -> Stage:
    if line_dict["Event"].endswith("Submitted"):
        event_type = EventType.start
        timestamp = line_dict["Stage Info"]["Submission Time"]
    else:
        event_type = EventType.end
        timestamp = line_dict["Stage Info"]["Completion Time"]

    return Stage(
        stage_id=line_dict["Stage Info"]["Stage ID"],
        event_type=event_type,
        stage_timestamp=timestamp,
    )


def parse_task(line_dict: dict) -> Task:
    task_info = line_dict["Task Info"]
    task_info["Stage ID"] = line_dict["Stage ID"]
    task_info["Task Type"] = line_dict["Task Type"]

    task_id = task_info["Task ID"]
    task_metrics = line_dict["Task Metrics"]
    metrics = Metrics(
        task_metrics=TaskMetrics(**task_metrics),
        executor_metrics=ExecutorMetrics(**line_dict["Task Executor Metrics"]),
        shuffle_read_metrics=ShuffleReadMetrics(**task_metrics["Shuffle Read Metrics"]),
        shuffle_write_metrics=ShuffleWriteMetrics(
            **task_metrics["Shuffle Write Metrics"]
        ),
        input_metrics=InputMetrics(**task_metrics["Input Metrics"]),
        output_metrics=OutputMetrics(**task_metrics["Output Metrics"]),
    )
    accumulators = [
        Accumulator(task_id=task_id, **i) for i in task_info["Accumulables"]
    ]
    return Task(
        metrics=metrics,
        accumulators=accumulators,
        **line_dict["Task Info"],
    )


def get_plan_details(
    plan_lines: list[str], tree_end: int, n_nodes: int
) -> PhysicalPlanDetails:
    sources = []
    targets = []

    details_start = plan_lines[tree_end:].index("") + tree_end + 2
    details_end = len(plan_lines) - 1
    task_details = plan_lines[details_start:details_end]
    for i, line in enumerate(task_details):
        if line == "":
            task_details[i] = "\n\n"
    task_details_split = "".join(task_details).split("\n\n")
    task_details_split = [i for i in task_details_split if i != ""][0:n_nodes]
    assert len(task_details_split) == n_nodes

    for details in task_details_split:
        if "Scan " in details and "LocalTableScan" not in details:
            source = details.split("[file:")[1].split("]")[0]
            sources.append(source)
        elif "file:" in details:
            target = details.split("file:")[-1].split(",")[0]
            targets.append(target)
        else:
            continue

    codegen_lookup = {}
    for details in task_details_split:
        if "[codegen id : " not in details:
            continue

        codegen_node = int(details.split(")")[0].split("(")[-1].strip())
        codegen_id = int(details.split("[codegen id : ")[-1].split("]")[0].strip())
        codegen_lookup[codegen_node] = codegen_id

    return PhysicalPlanDetails(
        sources=sources, targets=targets, codegen_lookup=codegen_lookup
    )


def parse_node_accumulators(
    plan: dict, node_map: dict[int, PhysicalPlanNode]
) -> dict[int, list[PlanAccumulator]]:
    def process_node(node_info: dict, child_index: int):
        node_name = node_info["nodeName"]
        node_string = node_info["simpleString"]

        node_id = node_ids[child_index]
        is_excluded = any([excluded in node_name for excluded in nodes_to_exclude])

        if "metrics" in node_info and not is_excluded:
            expected_node_name = node_map[node_id].node_type
            assert (
                node_name.replace("Execute ", "").split(" ")[0]
                in expected_node_name.value
            ), print(f"{node_name} not in {expected_node_name.value}")
            metrics_parsed = [
                PlanAccumulator(
                    node_id=node_id,
                    node_name=node_name,
                    node_string=node_string,
                    child_index=child_index,
                    **metric,
                )
                for metric in node_info["metrics"]
            ]
            accumulators[node_id] = metrics_parsed

        if "children" in node_info:
            for child in node_info["children"]:
                process_node(child, child_index=len(accumulators))

    node_ids = list(node_map.keys())
    accumulators = {}
    nodes_to_exclude = ["WholeStageCodegen", "InputAdapter"]
    for i, child in enumerate(plan["sparkPlanInfo"]["children"]):
        process_node(child, i)

    return accumulators


def parse_spark_ui_tree(tree: str) -> dict[int, PhysicalPlanNode]:
    step = 3
    empty_leading_lines = 0
    node_map: dict[int, PhysicalPlanNode] = {}
    indentation_history = []

    lines = tree.split("\n")
    node_pattern = re.compile(r".*\((\d+)\)")

    for i, line in enumerate(lines):
        if line == "":
            empty_leading_lines += 1
            continue

        # remove leading spaces and nested indentation after :
        line_strip = line.lstrip().removeprefix(": ").lstrip()
        match = node_pattern.search(line)
        if not match:
            continue

        node_id = int(match.group(1))

        node_type_match = re.search(
            r"(\b\w+\b).*\(\d{1,4}\)", line.replace("Execute", "")
        )

        if node_type_match:
            node_type = NodeType(node_type_match.group(1))

        else:
            raise ValueError(f"Could not parse node type from line: {line}")

        node = PhysicalPlanNode(
            node_id=node_id,
            node_type=node_type,
            child_nodes=[],
            whole_stage_codegen_id=None,
        )
        node_map[node_id] = node

        indentation_level = len(line) - len(line_strip)

        # first non-empty line is always the leaf node
        if i == 0 + empty_leading_lines:
            indentation_history.append((indentation_level, node_id))
            continue

        prev_indentation = indentation_history[-1]
        indentation_history.append((indentation_level, node_id))
        if prev_indentation[0] > indentation_level:
            child_nodes = [
                i[1] for i in indentation_history if i[0] == indentation_level - step
            ]
            if child_nodes:
                node_map[node_id].child_nodes = child_nodes
            continue

        node_map[node_id].child_nodes = [prev_indentation[1]]
    return node_map


def parse_physical_plan(line_dict: dict) -> PhysicalPlan:
    plan_string = line_dict["physicalPlanDescription"]
    query_id = line_dict["executionId"]
    plan_lines = plan_string.split("\n")

    tree_start = plan_lines.index("+- == Final Plan ==") + 1
    tree_end = plan_lines.index("+- == Initial Plan ==")
    tree = "\n".join(plan_lines[tree_start:tree_end])

    logging.debug(tree)

    node_map = parse_spark_ui_tree(tree)
    plan_accumulators = parse_node_accumulators(line_dict, node_map)
    details = get_plan_details(
        plan_lines,
        tree_end,
        len(node_map),
    )

    if len(details.codegen_lookup) > 0:
        for k, v in details.codegen_lookup.items():
            node_map[k].whole_stage_codegen_id = v

    if len(plan_accumulators) > 0:
        for k, v in plan_accumulators.items():
            node_map[k].accumulators = v if v else None

    return PhysicalPlan(
        query_id=query_id,
        sources=details.sources,
        targets=details.targets,
        nodes=list(node_map.values()),
    )


def get_parsed_log_name(parsed_plan: PhysicalPlan, out_name: str | None) -> str:
    name_len_limit = 100
    today = datetime.datetime.now().strftime("%Y-%m-%dT%H-%M-%S")
    if out_name is not None:
        return out_name[:name_len_limit]

    parsed_paths = []
    if len(parsed_plan.targets) > 0:
        paths_to_use = parsed_plan.targets
    else:
        paths_to_use = parsed_plan.sources

    for path in set(paths_to_use):
        path_name = path.split("/")[-1].split(".")[0]
        parsed_paths.append(path_name)

    paths_final = "_".join(parsed_paths)[:name_len_limit]

    return f"{today}__{paths_final}"


@timeit
def parse_log(log_path: Path, out_name: str | None = None) -> ParsedLog:
    logger.debug(f"Starting to parse log file: {log_path}")
    with log_path.open("r") as f:
        all_contents = f.readlines()

    start_point = "SparkListenerApplicationStart"
    for i, line in enumerate(all_contents):
        line_dict = json.loads(line)
        if line_dict["Event"] == start_point:
            start_index = i + 1
            break

    contents_to_parse = all_contents[start_index:]

    jobs = []
    stages = []
    tasks = []
    queries = []
    query_times = []
    for i, line in enumerate(contents_to_parse, start_index):
        logger.debug("-" * 40)
        logger.debug(f"[line {i:04d}] parse start")
        line_dict = json.loads(line)
        event_type = line_dict["Event"]
        if event_type.startswith("SparkListenerJob"):
            job = parse_job(line_dict)
            jobs.append(job)
            logger.debug(
                f"[line {i:04d}] parse finish - job#{job.job_id}  type:{job.event_type}"
            )
        elif event_type.startswith("SparkListenerStage"):
            stage = parse_stage(line_dict)
            stages.append(stage)
            logger.debug(
                f"[line {i:04d}] parse finish - stage#{stage.stage_id} type:{stage.event_type}"
            )
        elif event_type == "SparkListenerTaskEnd":
            task = parse_task(line_dict)
            tasks.append(task)
            logger.debug(
                f"[line {i:04d}] parse finish - task#{task.task_id} stage#{task.stage_id}"
            )
        elif event_type.endswith("SparkListenerSQLAdaptiveExecutionUpdate"):
            is_final_plan = (
                line_dict["sparkPlanInfo"]["simpleString"].split("isFinalPlan=")[-1]
                == "true"
            )
            if is_final_plan:
                logger.debug(f"Found final plan at line {i}")
                queries.append(line_dict)
            logger.debug(
                f"[line {i:04d}] parse skip - unhandled event type {event_type}"
            )
        elif event_type.endswith("SparkListenerSQLExecutionStart"):
            query_times.append(
                QueryEvent(
                    query_id=line_dict["executionId"],
                    query_time=line_dict["time"],
                    event_type=EventType.start,
                )
            )
        elif event_type.endswith("SparkListenerSQLExecutionEnd"):
            query_times.append(
                QueryEvent(
                    query_id=line_dict["executionId"],
                    query_time=line_dict["time"],
                    event_type=EventType.end,
                )
            )
    if len(queries) == 0:
        raise ValueError("No queries found in log file")

    parsed_queries = [parse_physical_plan(query) for query in queries]
    logger.debug(
        f"Finished parsing log [n={len(jobs)} jobs | n={len(stages)} stages | n={len(tasks)} tasks | n={len(parsed_queries)} queries]"
    )

    parsed_log_name = get_parsed_log_name(parsed_queries[0], out_name)

    return ParsedLog(
        name=parsed_log_name,
        jobs=jobs,
        stages=stages,
        tasks=tasks,
        queries=parsed_queries,
        query_times=query_times,
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
        query_times_df.pivot("event_type", index="query_id", values="query_time")
        .rename({"start": "query_start_timestamp", "end": "query_end_timestamp"})
        .with_columns(
            (pl.col("query_end_timestamp") - pl.col("query_start_timestamp"))
            .mul(1 / 1_000)
            .alias("query_duration_seconds")
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
            .then(get_readable_size(pl.col("value")))
            .when(pl.col("metric_type") == "timing")
            .then(get_readable_timing(pl.col("value")))
            .otherwise(
                pl.struct(
                    [
                        pl.col("value").alias("readable_value"),
                        pl.col("unit").alias("readable_unit"),
                    ]
                )
            )
            .alias("readable")
        )
        .unnest("readable")
        .with_columns(pl.col("readable_value").round(3))
        .with_columns(
            pl.struct([pl.col(col) for col in accumulator_cols]).alias(output_struct)
        )
        .select(*id_cols, output_struct)
    )
    return readable_metrics


@timeit
def log_to_dag_df(result: ParsedLog) -> pl.DataFrame:
    tasks = clean_tasks(result.tasks)
    plan = clean_plan(result.query_times, result.queries)

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

    dag_long = plan_long.join(accumulators_long, on="accumulator_id", how="left")

    # structured accumulators update values per query, node, accumulator, task
    readable_metrics = parse_accumulator_metrics(dag_long, "task")

    # structured accumulator totals per query, node, accumulator
    readable_metrics_total = parse_accumulator_metrics(dag_long, "total")

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
        dag_metrics.join(
            dag_metrics_totals.select(
                "query_id", "node_id", "accumulator_totals", "n_accumulator_totals"
            ),
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

    dag_final = (
        (
            plan.select(*dag_base_cols)
            .join(dag_metrics_combined, on=["query_id", "node_id"], how="left")
            .join(node_durations, on=["query_id", "node_id"], how="left")
            .with_columns(
                pl.coalesce("node_duration_minutes", pl.lit(0)).alias(
                    "node_duration_minutes"
                )
            )
            .sort("query_id", "node_id")
        )
        .with_columns(
            pl.concat_str(
                [
                    pl.lit("["),
                    pl.col("node_id").cast(pl.String),
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
def log_to_combined_df(result: ParsedLog, log_name: str) -> pl.DataFrame:
    tasks_final = clean_tasks(result.tasks)
    stages_final = clean_stages(result.stages)
    jobs_final = clean_jobs(result.jobs)

    combined = (
        tasks_final.join(stages_final, on="stage_id", how="left")
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
        "host",
        "index",
        "attempt",
        "failed",
        "killed",
        "speculative",
        "task_loc",
        "task_type",
    ]

    final = (
        combined.with_columns(
            [
                pl.col(col)
                .mul(1000)
                .cast(pl.Datetime)
                .dt.strftime("%Y-%m-%dT%H:%M:%S")
                .alias(col)
                for col in timestamp_cols
            ]
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
        .rename(
            {
                "result_size": "result_size_bytes",
                "peak_execution_memory": "peak_execution_memory_bytes",
            }
        )
        .select(final_cols)
    )

    return final


def write_parsed_log(
    df: pl.DataFrame,
    base_dir_path: Path,
    out_dir: str,
    out_format: OutputFormat,
    parsed_name: str,
    suffix: str,
) -> None:
    out_dir_path = base_dir_path / out_dir
    out_dir_path.mkdir(parents=True, exist_ok=True)

    out_path = out_dir_path / f"{parsed_name}_{suffix}"

    logging.info(f"Writing parsed log: {out_path}")
    logging.debug(f"Output format: {out_format}")
    logging.debug(f"{df.shape[0]} rows and {df.shape[1]} columns")
    logging.debug(f"{df.head()}")

    write_dataframe(df, out_path, out_format)


def get_parsed_metrics(
    base_dir: str = "data",
    log_dir: str = "logs/raw",
    log_file: str | None = None,
    out_dir: str | None = "logs/parsed",
    out_name: str | None = None,
    out_format: OutputFormat | None = OutputFormat.csv,
    verbose: bool = False,
) -> ParsedLogDataFrames:
    base_dir_path = Path(__file__).parents[1] / base_dir
    log_dir_path = base_dir_path / log_dir

    if verbose:
        logging.basicConfig(
            level=logging.DEBUG,
            format="%(asctime)s.%(msecs)03d %(message)s",
            datefmt="%Y-%m-%dT%H:%M:%S",
        )
    else:
        logging.basicConfig(level=logging.INFO, format="%(message)s")

    if log_file is None:
        log_to_parse = sorted(log_dir_path.glob("*"))[-1]
    else:
        log_to_parse = log_dir_path / log_file

    logging.info(f"Reading log file: {log_to_parse}")

    result = parse_log(log_to_parse, out_name)
    combined_df = log_to_combined_df(result, log_to_parse.stem)
    dag_df = log_to_dag_df(result)

    output = ParsedLogDataFrames(combined=combined_df, dag=dag_df)

    if out_dir is None or out_format is None:
        logging.info("Skipping writing parsed log")
        return output

    write_parsed_log(
        df=combined_df,
        base_dir_path=base_dir_path,
        out_dir=out_dir,
        out_format=out_format,
        parsed_name=result.name,
        suffix="_combined",
    )

    write_parsed_log(
        df=dag_df,
        base_dir_path=base_dir_path,
        out_dir=out_dir,
        out_format=out_format,
        parsed_name=result.name,
        suffix="_dag",
    )
    return output
