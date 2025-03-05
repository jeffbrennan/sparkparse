import copy
import json
import logging
from pathlib import Path

import polars as pl
from pydantic import BaseModel

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


def parse_physical_plan(line_dict: dict) -> PhysicalPlan:
    plan_string = line_dict["sparkPlanInfo"]["physicalPlanDescription"]
    plan_lines = plan_string.split("\n")

    plan_start = plan_lines.index("== Final Plan ==") + 1
    plan_end = plan_lines.index("== Initial Plan ==") - 1

    base_indentation = 2
    step = 2
    nodes = []

    # lookup of indentation level : node ids
    indentation_nodes: dict[int, list[int]] = {}

    # lookup of node id : indentation level
    node_indentation: dict[int, int] = {}

    for i, line in enumerate(plan_lines[plan_start:plan_end]):
        is_whole_stage_codegen = "*" in line
        node_id = int(line.split(",")[0].strip().split("(")[-1].removesuffix(")"))
        node_type_raw = line.split(",")[1].strip().split(" (")[0].split(" ")[-1]
        node_type = NodeType(node_type_raw)
        node = PhysicalPlanNode(
            node_id=node_id,
            node_type=node_type,
            is_whole_stage_codegen=is_whole_stage_codegen,
            child_nodes=[],
        )
        nodes.append(node)

        indentation = len(line) - len(line.lstrip()) - base_indentation
        node_indentation[node_id] = indentation

        if indentation not in indentation_nodes:
            indentation_nodes[indentation] = [node_id]
        else:
            indentation_nodes[indentation].append(node_id)

    for node in nodes:
        children = indentation_nodes.get(node_indentation[node.node_id] + step)
        if children:
            node.child_nodes = children

    path_start = (
        plan_lines[plan_lines.index("== Initial Plan ==") + 1 :].index("\n") + 2
    )

    path_end = len(plan_lines) - 1

    sources = []
    targets = []

    task_details = "".join(plan_lines[path_start:path_end]).split("\n\n")
    for i, details in enumerate(task_details):
        if "Scan" in details:
            source = details.split("[file:")[1].split("]")[0]
            sources.append(source)
        elif "WriteFiles" in details:
            target = details.split("file:")[-1].split(",")[0]
            targets.append(target)
        else:
            continue

    return PhysicalPlan(sources=sources, targets=targets, nodes=nodes)


@timeit
def parse_log(log_path: Path) -> ParsedLog:
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
        elif event_type == "SparkListenerSQLAdaptiveExecutionUpdate":
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
    parsed_plan = parse_physical_plan(final_plan)

    logger.debug(
        f"Finished parsing log [n={len(jobs)} jobs | n={len(stages)} stages | n={len(tasks)} tasks]"
    )

    log_name = ",".join(sorted(set(i.split("/")[-1] for i in parsed_plan.targets)))
    return ParsedLog(
        name=log_name, jobs=jobs, stages=stages, tasks=tasks, plan=parsed_plan
    )


@timeit
def log_to_df(result: ParsedLog, log_name: str) -> pl.DataFrame:
    jobs = pl.DataFrame(result.jobs)
    jobs_with_duration = (
        jobs.select("job_id", "event_type", "job_timestamp")
        .pivot("event_type", index="job_id", values="job_timestamp")
        .with_columns((pl.col("end") - pl.col("start")).alias("job_duration_ms"))
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
        .with_columns((pl.col("end") - pl.col("start")).alias("stage_duration_ms"))
        .rename({"start": "stage_start_timestamp", "end": "stage_end_timestamp"})
    )

    tasks = pl.DataFrame(result.tasks)

    combined = (
        (
            tasks.join(stages_final, on="stage_id", how="left")
            .join(jobs_final, on="stage_id", how="left")
            .sort("job_id", "stage_id", "task_id")
        )
        .unnest("metrics")
        .unnest("task_metrics")
        .unnest("executor_metrics")
        .unnest("shuffle_read_metrics")
        .unnest("shuffle_write_metrics")
        .unnest("input_metrics")
        .unnest("output_metrics")
        .unnest("push_based_shuffle")
    ).with_columns(pl.lit(log_name).alias("log_name"))

    return combined


def write_parsed_log(
    df: pl.DataFrame,
    base_dir_path: Path,
    output_dir: str,
    out_format: OutputFormat,
    parsed_name: str,
) -> None:
    out_dir = base_dir_path / output_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    out_path = out_dir / f"{parsed_name}.csv"

    logging.info(f"Writing parsed log: {out_path}")
    logging.debug(f"Output format: {out_format}")
    logging.debug(f"{df.shape[0]} rows and {df.shape[1]} columns")
    logging.debug(f"{df.head()}")

    write_dataframe(df, out_path, out_format)
