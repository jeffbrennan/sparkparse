"""Builders for adversarial event logs.

Real Spark cannot be asked to produce a truncated file, a corrupt line or a
stage retry on demand, so these helpers derive such logs from the recorded
fixtures by slicing and mutating their events.
"""

from __future__ import annotations

import copy
import json
from collections.abc import Callable, Iterable
from pathlib import Path

FULL_LOGS = Path(__file__).parent / "data" / "full_logs"


def log_lines(name: str = "nested_loop_join") -> list[str]:
    return FULL_LOGS.joinpath(name).read_text().splitlines()


def events(name: str = "nested_loop_join") -> list[dict]:
    return [json.loads(line) for line in log_lines(name)]


def write_log(
    path: Path, lines: Iterable[str | dict], newline_at_end: bool = True
) -> Path:
    rendered = [line if isinstance(line, str) else json.dumps(line) for line in lines]
    text = "\n".join(rendered)
    if newline_at_end:
        text += "\n"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)
    return path


def drop_events(lines: list[str], predicate: Callable[[dict], bool]) -> list[str]:
    """Return ``lines`` without the events matching ``predicate``."""
    kept = []
    for line in lines:
        event = json.loads(line)
        if predicate(event):
            continue
        kept.append(line)
    return kept


def map_events(lines: list[str], fn: Callable[[dict], dict | None]) -> list[str]:
    out = []
    for line in lines:
        result = fn(json.loads(line))
        if result is not None:
            out.append(json.dumps(result))
    return out


def is_adaptive_update(event: dict) -> bool:
    return event["Event"].endswith("SparkListenerSQLAdaptiveExecutionUpdate")


def stage_attempt(event: dict, attempt: int) -> dict:
    """Return a copy of a stage event re-stamped as a later attempt."""
    out = copy.deepcopy(event)
    out["Stage Info"]["Stage Attempt ID"] = attempt
    return out


def task_attempt(
    event: dict,
    *,
    task_id: int,
    stage_attempt_id: int | None = None,
    attempt: int | None = None,
    failed: bool = False,
    killed: bool = False,
    speculative: bool = False,
    drop_metrics: bool = False,
) -> dict:
    """Return a copy of a TaskEnd event describing another attempt."""
    out = copy.deepcopy(event)
    out["Task Info"]["Task ID"] = task_id
    if stage_attempt_id is not None:
        out["Stage Attempt ID"] = stage_attempt_id
    if attempt is not None:
        out["Task Info"]["Attempt"] = attempt
    out["Task Info"]["Failed"] = failed
    out["Task Info"]["Killed"] = killed
    out["Task Info"]["Speculative"] = speculative
    if failed:
        out["Task End Reason"] = {
            "Reason": "ExceptionFailure",
            "Class Name": "java.lang.OutOfMemoryError",
            "Description": "Java heap space",
        }
    if killed:
        out["Task End Reason"] = {
            "Reason": "TaskKilled",
            "Kill Reason": "another attempt succeeded",
        }
    if drop_metrics:
        out.pop("Task Metrics", None)
    return out


def rolling_log_dir(
    root: Path,
    lines: list[str],
    app_id: str = "app-rolled-0001",
    segments: int = 2,
    in_progress: bool = False,
) -> Path:
    """Write ``lines`` split across a Spark rolling-log directory layout."""
    log_dir = root / f"eventlog_v2_{app_id}"
    log_dir.mkdir(parents=True, exist_ok=True)
    status = log_dir / f"appstatus_{app_id}"
    status.write_text("")
    if in_progress:
        status.rename(log_dir / f"appstatus_{app_id}.inprogress")

    chunk = max(1, len(lines) // segments + 1)
    for index in range(segments):
        piece = lines[index * chunk : (index + 1) * chunk]
        write_log(log_dir / f"events_{index + 1}_{app_id}", piece)
    return log_dir


def scale_task_volume(lines: list[str], copies: int) -> list[str]:
    """Return the log with each TaskEnd repeated ``copies`` extra times.

    Each copy is a distinct attempt of the same partition — unique task id,
    higher attempt number, later finish time — so the log stays a valid
    sequence of events while the volume the parser must retain grows.
    """
    if copies <= 0:
        return list(lines)

    parsed = [json.loads(line) for line in lines]
    next_id = (
        max(
            (
                event["Task Info"]["Task ID"]
                for event in parsed
                if event["Event"] == "SparkListenerTaskEnd"
            ),
            default=0,
        )
        + 1
    )

    out: list[str] = []
    for event in parsed:
        out.append(json.dumps(event))
        if event["Event"] != "SparkListenerTaskEnd":
            continue
        for copy_index in range(copies):
            duplicate = task_attempt(
                event,
                task_id=next_id,
                attempt=event["Task Info"]["Attempt"] + copy_index + 1,
                speculative=True,
            )
            duplicate["Task Info"]["Finish Time"] = (
                event["Task Info"]["Finish Time"] + copy_index + 1
            )
            out.append(json.dumps(duplicate))
            next_id += 1
    return out
