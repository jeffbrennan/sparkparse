"""Increment 1 and 2 of brief 04: partial logs, plan fallback, attempts.

Every case here is an event log Spark really produces but the fixtures do not
contain: a non-adaptive query, a log copied mid-flush, a retried stage.
"""

from __future__ import annotations

import json

import polars as pl
import pytest

from sparkparse.eventlog import EventLogNotFoundError
from sparkparse.models import TaskStatus
from sparkparse.parse import get_all_parsed_metrics, get_parsed_metrics, parse_log
from tests.eventlog_fixtures import (
    drop_events,
    is_adaptive_update,
    log_lines,
    map_events,
    stage_attempt,
    task_attempt,
    write_log,
)


def _parse(tmp_path, lines, name="app-test-0001", **kwargs):
    write_log(tmp_path / name, lines)
    return parse_log(tmp_path / name, **kwargs)


# ---------------------------------------------------------------------------
# increment 1 — plan fallback and partial parsing
# ---------------------------------------------------------------------------


def test_non_adaptive_log_falls_back_to_the_start_event_plan(tmp_path):
    """Without AQE the only plan is the one on SQLExecutionStart."""
    lines = drop_events(log_lines("nested_loop_join"), is_adaptive_update)

    parsed = _parse(tmp_path, lines)

    assert [query.query_id for query in parsed.queries] == [0, 1, 2, 3]
    assert all(query.nodes for query in parsed.queries)
    assert {d.code for d in parsed.diagnostics} == {"non_final_plan"}


def test_final_adaptive_plan_still_wins_over_the_initial_plan(tmp_path):
    """The initial plan is a fallback, not a replacement."""
    full = _parse(tmp_path, log_lines("nested_loop_join"), name="full")
    initial_only = _parse(
        tmp_path,
        drop_events(log_lines("nested_loop_join"), is_adaptive_update),
        name="initial",
    )

    final_query = next(q for q in full.queries if q.query_id == 3)
    initial_query = next(q for q in initial_only.queries if q.query_id == 3)

    assert len(final_query.nodes) != len(initial_query.nodes)
    assert not [
        d for d in full.diagnostics if d.code == "non_final_plan" and " 3 " in d.message
    ]


def test_missing_application_start_is_not_fatal(tmp_path):
    lines = drop_events(
        log_lines("nested_loop_join"),
        lambda event: event["Event"] == "SparkListenerApplicationStart",
    )

    parsed = _parse(tmp_path, lines, name="app-no-start")

    assert parsed.queries
    assert parsed.application_id == "app-no-start"
    assert any(d.code == "missing_application_start" for d in parsed.diagnostics)


def test_log_without_sql_executions_yields_typed_empty_frames(tmp_path):
    lines = drop_events(
        log_lines("nested_loop_join"),
        lambda event: "SQL" in event["Event"],
    )
    write_log(tmp_path / "app-no-sql", lines)

    dfs = get_parsed_metrics(
        log_dir=tmp_path, log_file="app-no-sql", out_dir=None, out_format=None
    )

    assert dfs.dag.is_empty()
    # Typed, not shapeless: downstream code checks columns, not just row counts.
    assert "query_id" in dfs.dag.columns
    # The application still ran tasks; they are unattributed, not absent.
    assert dfs.combined.height > 0
    assert dfs.combined["query_id"].null_count() == dfs.combined.height
    assert any(d.code == "no_queries" for d in dfs.diagnostics)


def test_query_without_end_event_keeps_its_plan_and_reports_no_duration(tmp_path):
    lines = drop_events(
        log_lines("nested_loop_join"),
        lambda event: event["Event"].endswith("SQLExecutionEnd")
        and event["executionId"] == 3,
    )
    write_log(tmp_path / "app-unfinished", lines)

    dfs = get_parsed_metrics(
        log_dir=tmp_path, log_file="app-unfinished", out_dir=None, out_format=None
    )

    query_3 = dfs.dag.filter(pl.col("query_id") == 3)
    assert query_3.height > 0
    assert query_3["query_end_timestamp"].null_count() == query_3.height
    assert query_3["query_duration_seconds"].null_count() == query_3.height
    # The label still identifies the query instead of going null with it.
    assert query_3["query_header"][0] == "3 - count [unknown min]"


def test_all_queries_unfinished_still_produces_a_frame(tmp_path):
    """A log cut off before any SQLExecutionEnd has no 'end' column to pivot."""
    lines = drop_events(
        log_lines("nested_loop_join"),
        lambda event: event["Event"].endswith("SQLExecutionEnd"),
    )
    write_log(tmp_path / "app-cut-short", lines)

    dfs = get_parsed_metrics(
        log_dir=tmp_path, log_file="app-cut-short", out_dir=None, out_format=None
    )

    assert dfs.dag.height > 0
    assert dfs.dag["query_end_timestamp"].null_count() == dfs.dag.height


def test_log_cut_off_mid_job_keeps_the_tasks_it_recorded(tmp_path):
    """Completed tasks but no JobEnd: the job duration is unknown, not zero."""
    lines = log_lines("nested_loop_join")
    cut = (
        next(
            index
            for index, line in enumerate(lines)
            if json.loads(line)["Event"] == "SparkListenerTaskEnd"
        )
        + 1
    )
    write_log(tmp_path / "app-mid-job", lines[:cut])

    dfs = get_parsed_metrics(
        log_dir=tmp_path, log_file="app-mid-job", out_dir=None, out_format=None
    )

    assert dfs.combined.height == 1
    assert dfs.combined["job_end_timestamp"].null_count() == 1
    assert dfs.combined["job_duration_seconds"].null_count() == 1
    assert dfs.combined["stage_end_timestamp"].null_count() == 1
    assert dfs.combined["stage_status"][0] == "running"


def test_truncated_tail_is_reported_as_truncation_not_corruption(tmp_path):
    lines = log_lines("nested_loop_join")
    lines.append('{"Event":"SparkListenerApplicationEnd","Timestamp":17424')

    parsed = _parse(tmp_path, lines, name="app-truncated")

    codes = {d.code for d in parsed.diagnostics}
    assert "truncated_tail" in codes
    assert "corrupt_line" not in codes
    assert parsed.queries


def test_corruption_mid_file_is_distinguished_from_a_truncated_tail(tmp_path):
    lines = log_lines("nested_loop_join")
    corrupt_index = len(lines) // 2
    lines.insert(corrupt_index, '{"Event":"SparkListenerTaskEnd", "Stage I')

    parsed = _parse(tmp_path, lines, name="app-corrupt")

    corrupt = [d for d in parsed.diagnostics if d.code == "corrupt_line"]
    assert len(corrupt) == 1
    assert corrupt[0].line == corrupt_index + 1
    assert not [d for d in parsed.diagnostics if d.code == "truncated_tail"]


@pytest.mark.parametrize("position", ["tail", "middle"])
def test_strict_mode_fails_with_source_context(tmp_path, position):
    lines = log_lines("nested_loop_join")
    broken = '{"Event":"SparkListenerTaskEnd", "Stage I'
    if position == "tail":
        lines.append(broken)
    else:
        lines.insert(len(lines) // 2, broken)

    with pytest.raises(ValueError, match=r"app-strict:\d+"):
        _parse(tmp_path, lines, name="app-strict", strict=True)


def test_task_without_metrics_is_kept_with_unknown_usage(tmp_path):
    seen = {"done": False}

    def strip_first_task_metrics(event: dict) -> dict:
        if event["Event"] == "SparkListenerTaskEnd" and not seen["done"]:
            seen["done"] = True
            event.pop("Task Metrics")
        return event

    lines = map_events(log_lines("nested_loop_join"), strip_first_task_metrics)
    parsed = _parse(tmp_path, lines, name="app-no-task-metrics")

    without_metrics = [task for task in parsed.tasks if task.metrics is None]
    assert len(without_metrics) == 1
    assert any(d.code == "tasks_missing_metrics" for d in parsed.diagnostics)


def test_every_task_missing_metrics_still_builds_a_typed_frame(tmp_path):
    def strip_metrics(event: dict) -> dict:
        if event["Event"] == "SparkListenerTaskEnd":
            event.pop("Task Metrics", None)
        return event

    lines = map_events(log_lines("nested_loop_join"), strip_metrics)
    write_log(tmp_path / "app-metricless", lines)

    dfs = get_parsed_metrics(
        log_dir=tmp_path, log_file="app-metricless", out_dir=None, out_format=None
    )

    # Missing, not zero: a null total says the metric was never reported.
    assert dfs.combined.height > 0
    assert dfs.combined["bytes_read"].null_count() == dfs.combined.height


def test_unknown_events_are_counted_not_fatal(tmp_path):
    lines = log_lines("nested_loop_join")
    lines.insert(5, json.dumps({"Event": "SparkListenerSomethingFromSpark5"}))

    parsed = _parse(tmp_path, lines, name="app-unknown-event")

    assert parsed.unknown_events["SparkListenerSomethingFromSpark5"] == 1
    assert parsed.queries


def test_empty_log_file_parses_to_nothing(tmp_path):
    (tmp_path / "app-empty").write_text("")

    dfs = get_parsed_metrics(
        log_dir=tmp_path, log_file="app-empty", out_dir=None, out_format=None
    )

    assert dfs.dag.is_empty()
    assert dfs.combined.is_empty()
    assert any(d.code == "no_queries" for d in dfs.diagnostics)


def test_empty_directory_reports_no_sources(tmp_path):
    with pytest.raises(EventLogNotFoundError):
        get_parsed_metrics(log_dir=tmp_path, out_dir=None, out_format=None)


# ---------------------------------------------------------------------------
# increment 2 — identity and attempts
# ---------------------------------------------------------------------------


def _with_stage_retry(lines: list[str], stage_id: int = 1) -> tuple[list[str], int]:
    """Append a second attempt of ``stage_id`` with one task of its own."""
    parsed = [json.loads(line) for line in lines]
    submitted = next(
        event
        for event in parsed
        if event["Event"] == "SparkListenerStageSubmitted"
        and event["Stage Info"]["Stage ID"] == stage_id
    )
    completed = next(
        event
        for event in parsed
        if event["Event"] == "SparkListenerStageCompleted"
        and event["Stage Info"]["Stage ID"] == stage_id
    )
    task = next(
        event
        for event in parsed
        if event["Event"] == "SparkListenerTaskEnd" and event["Stage ID"] == stage_id
    )
    retry_task_id = (
        max(
            event["Task Info"]["Task ID"]
            for event in parsed
            if event["Event"] == "SparkListenerTaskEnd"
        )
        + 1
    )

    extra = [
        stage_attempt(submitted, 1),
        task_attempt(task, task_id=retry_task_id, stage_attempt_id=1, attempt=1),
        stage_attempt(completed, 1),
    ]
    end_index = next(
        i
        for i, event in enumerate(parsed)
        if event["Event"] == "SparkListenerApplicationEnd"
    )
    merged = parsed[:end_index] + extra + parsed[end_index:]
    return [json.dumps(event) for event in merged], retry_task_id


def test_stage_retry_is_kept_as_a_separate_attempt(tmp_path):
    lines, retry_task_id = _with_stage_retry(log_lines("nested_loop_join"))
    write_log(tmp_path / "app-retry", lines)

    dfs = get_parsed_metrics(
        log_dir=tmp_path, log_file="app-retry", out_dir=None, out_format=None
    )

    attempts = dfs.combined.filter(pl.col("stage_id") == 1)["stage_attempt_id"]
    assert set(attempts.to_list()) == {0, 1}
    # One row per task attempt: the retry must not duplicate the original rows.
    assert (
        dfs.combined.height
        == dfs.combined.select(
            pl.struct("stage_id", "stage_attempt_id", "task_id")
        ).n_unique()
    )
    assert retry_task_id in dfs.combined["task_id"].to_list()


def test_failed_and_speculative_attempts_are_kept_out_of_output_totals(tmp_path):
    parsed = [json.loads(line) for line in log_lines("nested_loop_join")]
    template = next(
        event for event in parsed if event["Event"] == "SparkListenerTaskEnd"
    )
    next_id = (
        max(
            event["Task Info"]["Task ID"]
            for event in parsed
            if event["Event"] == "SparkListenerTaskEnd"
        )
        + 1
    )
    extras = [
        task_attempt(template, task_id=next_id, attempt=1, failed=True),
        task_attempt(
            template, task_id=next_id + 1, attempt=2, killed=True, speculative=True
        ),
    ]
    end_index = next(
        i
        for i, event in enumerate(parsed)
        if event["Event"] == "SparkListenerApplicationEnd"
    )
    merged = parsed[:end_index] + extras + parsed[end_index:]
    write_log(tmp_path / "app-attempts", [json.dumps(e) for e in merged])

    dfs = get_parsed_metrics(
        log_dir=tmp_path, log_file="app-attempts", out_dir=None, out_format=None
    )
    baseline = get_parsed_metrics(
        log_dir="tests/data/full_logs",
        log_file="nested_loop_join",
        out_dir=None,
        out_format=None,
    )

    statuses = set(dfs.combined["task_status"].to_list())
    assert statuses == {
        TaskStatus.success.value,
        TaskStatus.failed.value,
        TaskStatus.killed.value,
    }
    assert dfs.combined.height == baseline.combined.height + 2

    from sparkparse.analyze import retained_outputs

    kept = retained_outputs(dfs.combined)
    assert kept.height == baseline.combined.height
    # Output accounting ignores the discarded attempts...
    assert kept["bytes_read"].sum() == baseline.combined["bytes_read"].sum()
    # ...while the resources they burned are still on the ledger.
    assert (
        dfs.combined["executor_run_time_seconds"].sum()
        > baseline.combined["executor_run_time_seconds"].sum()
    )


def test_reused_stage_does_not_multiply_task_rows(tmp_path):
    """A stage listed by two jobs is one physical stage, not two."""
    parsed = [json.loads(line) for line in log_lines("nested_loop_join")]
    job_start = next(
        event for event in parsed if event["Event"] == "SparkListenerJobStart"
    )
    shared_stage = job_start["Stage IDs"][0]
    later_job = next(
        event
        for event in parsed
        if event["Event"] == "SparkListenerJobStart"
        and shared_stage not in event["Stage IDs"]
    )
    later_job["Stage IDs"] = [*later_job["Stage IDs"], shared_stage]
    write_log(tmp_path / "app-reuse", [json.dumps(e) for e in parsed])

    dfs = get_parsed_metrics(
        log_dir=tmp_path, log_file="app-reuse", out_dir=None, out_format=None
    )
    baseline = get_parsed_metrics(
        log_dir="tests/data/full_logs",
        log_file="nested_loop_join",
        out_dir=None,
        out_format=None,
    )

    assert dfs.combined.height == baseline.combined.height
    assert dfs.combined["bytes_read"].sum() == baseline.combined["bytes_read"].sum()

    # The relation itself is preserved, just not joined into the task frame.
    assert dfs.job_stage is not None
    jobs_for_stage = dfs.job_stage.filter(pl.col("stage_id") == shared_stage)
    assert jobs_for_stage.height == 2


def test_task_shared_by_two_queries_appears_once_with_the_relation_kept():
    dfs = get_parsed_metrics(
        log_dir="tests/data/full_logs",
        log_file="nested_final_plans",
        out_dir=None,
        out_format=None,
    )

    keys = dfs.combined.select("stage_id", "task_id")
    assert keys.n_unique() == dfs.combined.height
    assert max(dfs.combined["query_count"].to_list()) > 1

    assert dfs.query_stage is not None
    shared = (
        dfs.query_stage.group_by("stage_id")
        .agg(pl.col("query_id").n_unique().alias("queries"))
        .filter(pl.col("queries") > 1)
    )
    assert shared.height > 0


def test_repeated_query_ids_across_applications_stay_separate(tmp_path):
    for name in ("app-a", "app-b"):
        write_log(tmp_path / name, log_lines("nested_loop_join"))

    results = get_all_parsed_metrics(log_dir=tmp_path, out_dir=None, out_format=None)

    assert set(results) == {"app-a", "app-b"}
    for dfs in results.values():
        assert set(dfs.dag["query_id"].unique().to_list()) == {0, 1, 2, 3}


def test_summary_records_which_attempts_each_total_covers(tmp_path):
    from sparkparse.analyze import to_plan_summary

    dfs = get_parsed_metrics(
        log_dir="tests/data/full_logs",
        log_file="nested_loop_join",
        out_dir=None,
        out_format=None,
    )

    summary = to_plan_summary(dfs, "nested_loop_join")

    assert summary["total_basis"]["bytes_read"] == "retained_outputs"
    assert summary["total_basis"]["memory_bytes_spilled"] == "all_attempts"
    assert summary["total_basis"]["executor_run_time_seconds"] == "all_attempts"


def _append_task_events(lines: list[str], build) -> tuple[list[str], list[dict]]:
    """Insert task events built by ``build(template, next_task_id)`` before ApplicationEnd."""
    parsed = [json.loads(line) for line in lines]
    # The task that read the most: duplicating a task with no input metrics
    # would make the "counted twice" assertions vacuous.
    template = max(
        (event for event in parsed if event["Event"] == "SparkListenerTaskEnd"),
        key=lambda event: event["Task Metrics"]["Input Metrics"]["Bytes Read"],
    )
    next_id = (
        max(
            event["Task Info"]["Task ID"]
            for event in parsed
            if event["Event"] == "SparkListenerTaskEnd"
        )
        + 1
    )
    extras = build(template, next_id)
    end_index = next(
        index
        for index, event in enumerate(parsed)
        if event["Event"] == "SparkListenerApplicationEnd"
    )
    merged = parsed[:end_index] + extras + parsed[end_index:]
    return [json.dumps(event) for event in merged], extras


def _baseline():
    return get_parsed_metrics(
        log_dir="tests/data/full_logs",
        log_file="nested_loop_join",
        out_dir=None,
        out_format=None,
    )


def test_losing_speculative_copy_does_not_count_its_output_twice(tmp_path):
    """Both copies succeed; only the one that committed produced output."""

    def build(template, next_id):
        loser = task_attempt(template, task_id=next_id, attempt=1, speculative=True)
        # Finished after the original, so the commit was already awarded.
        loser["Task Info"]["Finish Time"] = template["Task Info"]["Finish Time"] + 5_000
        return [loser]

    lines, _ = _append_task_events(log_lines("nested_loop_join"), build)
    write_log(tmp_path / "app-speculative", lines)

    dfs = get_parsed_metrics(
        log_dir=tmp_path, log_file="app-speculative", out_dir=None, out_format=None
    )
    baseline = _baseline()

    from sparkparse.analyze import retained_outputs

    # Both attempts are on the resource ledger...
    assert dfs.combined.height == baseline.combined.height + 1
    assert dfs.combined["task_status"].to_list().count("success") == (
        baseline.combined.height + 1
    )
    # ...but only one of them counts as output.
    kept = retained_outputs(dfs.combined)
    assert kept.height == baseline.combined.height
    assert kept["bytes_read"].sum() == baseline.combined["bytes_read"].sum()
    assert (
        dfs.combined["executor_run_time_seconds"].sum()
        > baseline.combined["executor_run_time_seconds"].sum()
    )


def test_partition_recomputed_in_a_later_stage_attempt_counts_once(tmp_path):
    """A fetch failure recomputes a partition; the newer output supersedes."""

    def build(template, next_id):
        recomputed = task_attempt(
            template, task_id=next_id, stage_attempt_id=1, attempt=1
        )
        recomputed["Task Info"]["Finish Time"] = (
            template["Task Info"]["Finish Time"] + 10_000
        )
        return [recomputed]

    lines, extras = _append_task_events(log_lines("nested_loop_join"), build)
    write_log(tmp_path / "app-recomputed", lines)

    dfs = get_parsed_metrics(
        log_dir=tmp_path, log_file="app-recomputed", out_dir=None, out_format=None
    )
    baseline = _baseline()

    from sparkparse.analyze import retained_outputs

    kept = retained_outputs(dfs.combined)
    assert dfs.combined.height == baseline.combined.height + 1
    assert kept.height == baseline.combined.height
    # The surviving row is the recomputed one, not the superseded attempt.
    recomputed_id = extras[0]["Task Info"]["Task ID"]
    partition = extras[0]["Task Info"]["Index"]
    survivor = kept.filter(
        (pl.col("stage_id") == extras[0]["Stage ID"]) & (pl.col("index") == partition)
    )
    assert survivor["task_id"].to_list() == [recomputed_id]
    assert survivor["stage_attempt_id"].to_list() == [1]


def test_duplicate_successful_outputs_do_not_inflate_summary_or_history(tmp_path):
    def build(template, next_id):
        duplicate = task_attempt(template, task_id=next_id, attempt=1, speculative=True)
        duplicate["Task Info"]["Finish Time"] = (
            template["Task Info"]["Finish Time"] + 5_000
        )
        return [duplicate]

    lines, _ = _append_task_events(log_lines("nested_loop_join"), build)
    write_log(tmp_path / "app-dup", lines)

    from sparkparse.analyze import to_plan_summary
    from sparkparse.history import record_from_dfs

    dfs = get_parsed_metrics(
        log_dir=tmp_path, log_file="app-dup", out_dir=None, out_format=None
    )
    baseline = _baseline()

    assert (
        to_plan_summary(dfs, "dup")["totals"]["bytes_read"]
        == to_plan_summary(baseline, "baseline")["totals"]["bytes_read"]
    )
    assert (
        record_from_dfs(dfs, "dup").bytes_read
        == record_from_dfs(baseline, "baseline").bytes_read
    )
