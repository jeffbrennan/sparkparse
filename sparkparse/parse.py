import json
from pathlib import Path

import polars as pl

from sparkparse.models import (
    EventType,
    ExecutorMetrics,
    InputMetrics,
    Job,
    Metrics,
    OutputMetrics,
    ParsedLog,
    ShuffleReadMetrics,
    ShuffleWriteMetrics,
    Stage,
    Task,
    TaskMetrics,
)


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


def parse_log(log_path: Path) -> ParsedLog:
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
        print("parsing line number", i)
        line_dict = json.loads(line)
        event_type = line_dict["Event"]
        if event_type.startswith("SparkListenerJob"):
            job = parse_job(line_dict)
            jobs.append(job)
            print(job)
        elif event_type.startswith("SparkListenerStage"):
            stage = parse_stage(line_dict)
            stages.append(stage)
            print(stage)
        elif event_type == "SparkListenerTaskEnd":
            task = parse_task(line_dict)
            tasks.append(task)
            print(task)
        else:
            print(event_type, "unhandled, skipping")
            continue
    return ParsedLog(jobs=jobs, stages=stages, tasks=tasks)


def log_to_df(result: ParsedLog) -> pl.DataFrame:
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
    )

    return combined
