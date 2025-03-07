from collections import defaultdict
import copy
import datetime
import json
import logging
from pathlib import Path
import re

import polars as pl

from sparkparse.common import timeit, write_dataframe
from sparkparse.models import (
    EventType,
    ExecutorMetrics,
    InputMetrics,
    Job,
    Metrics,
    NodeType,
    OutputFormat,
    OutputMetrics,
    ParsedLog,
    PhysicalPlan,
    PhysicalPlanDetails,
    PhysicalPlanNode,
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

    return Task(
        metrics=metrics,
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
        if "Scan" in details:
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
    plan_lines = plan_string.split("\n")

    tree_start = plan_lines.index("+- == Final Plan ==") + 1
    tree_end = plan_lines.index("+- == Initial Plan ==")
    tree = "\n".join(plan_lines[tree_start:tree_end])

    logging.debug(tree)

    node_map = parse_spark_ui_tree(tree)
    details = get_plan_details(
        plan_lines,
        tree_end,
        len(node_map),
    )

    if len(details.codegen_lookup) > 0:
        for k, v in details.codegen_lookup.items():
            node_map[k].whole_stage_codegen_id = v

    return PhysicalPlan(
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
    final_plan = None
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
                final_plan = copy.deepcopy(line_dict)
        else:
            logger.debug(
                f"[line {i:04d}] parse skip - unhandled event type {event_type}"
            )
            continue
    if final_plan is None:
        raise ValueError("No final physical plan found in log file")

    parsed_plan = parse_physical_plan(final_plan)

    logger.debug(
        f"Finished parsing log [n={len(jobs)} jobs | n={len(stages)} stages | n={len(tasks)} tasks]"
    )

    parsed_log_name = get_parsed_log_name(parsed_plan, out_name)
    return ParsedLog(
        name=parsed_log_name,
        jobs=jobs,
        stages=stages,
        tasks=tasks,
        plan=parsed_plan,
    )


@timeit
def log_to_df(result: ParsedLog, log_name: str) -> pl.DataFrame:
    jobs = pl.DataFrame(result.jobs)
    jobs_with_duration = (
        jobs.select("job_id", "event_type", "job_timestamp")
        .pivot("event_type", index="job_id", values="job_timestamp")
        .with_columns(
            (pl.col("end") - pl.col("start"))
            .mul(1 / 1_000)
            .alias("job_duration_seconds")
        )
        .rename({"start": "job_start_timestamp", "end": "job_end_timestamp"})
    )
    jobs_final = (
        jobs.select("job_id", "stages")
        .explode("stages")
        .rename({"stages": "stage_id"})
        .join(jobs_with_duration, on="job_id", how="left")
    )

    stages = pl.DataFrame(result.stages)
    stages_final = (
        stages.pivot("event_type", index="stage_id", values="stage_timestamp")
        .with_columns(
            (pl.col("end") - pl.col("start"))
            .mul(1 / 1000)
            .alias("stage_duration_seconds")
        )
        .rename({"start": "stage_start_timestamp", "end": "stage_end_timestamp"})
    )

    tasks = pl.DataFrame(result.tasks)
    tasks_final = tasks.with_columns(
        (pl.col("task_finish_time") - pl.col("task_start_time"))
        .mul(1 / 1_000)
        .alias("task_duration_seconds")
    ).rename(
        {
            "task_start_time": "task_start_timestamp",
            "task_finish_time": "task_end_timestamp",
        }
    )

    plan = pl.DataFrame(result.plan.nodes)
    plan_final = plan.rename({"node_id": "task_id"}).with_columns(
        pl.col("child_nodes")
        .cast(pl.List(pl.String))
        .list.join(", ")
        .alias("child_nodes")
    )

    combined = (
        tasks_final.join(plan_final, on="task_id", how="left")
        .join(stages_final, on="stage_id", how="left")
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
        "node_type",
        "node_name",
        "child_nodes",
        "whole_stage_codegen_id",
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
                .dt.strftime("%Y-%m-%dT%H:%M:%S.%f")
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
        .with_columns(
            pl.concat_str(
                [
                    pl.lit("["),
                    pl.col("task_id").cast(pl.String),
                    pl.lit("] "),
                    pl.col("node_type"),
                ]
            ).alias("node_name")
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
) -> None:
    out_dir_path = base_dir_path / out_dir
    out_dir_path.mkdir(parents=True, exist_ok=True)

    out_path = out_dir_path / f"{parsed_name}"

    logging.info(f"Writing parsed log: {out_path}")
    logging.debug(f"Output format: {out_format}")
    logging.debug(f"{df.shape[0]} rows and {df.shape[1]} columns")
    logging.debug(f"{df.head()}")

    write_dataframe(df, out_path, out_format)
