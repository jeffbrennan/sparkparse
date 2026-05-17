from pathlib import Path

import polars as pl
import pytest

from sparkparse.clean import (
    clean_jobs,
    clean_stages,
    clean_tasks,
    get_job_idle_time,
    get_readable_size,
    get_readable_timing,
)
from sparkparse.models import EventType, Job, Stage
from sparkparse.parse import parse_log


@pytest.fixture(scope="module")
def parsed_log():
    log_path = Path(__file__).parent / "data" / "full_logs" / "nested_final_plans"
    return parse_log(log_path)


# ---------------------------------------------------------------------------
# get_readable_size
# ---------------------------------------------------------------------------


def _eval_size(values: list[int]) -> list[dict]:
    df = pl.DataFrame({"value": values})
    return (
        df.with_columns(get_readable_size(pl.col("value")).alias("r"))
        .unnest("r")
        .to_dicts()
    )


def test_get_readable_size_bytes():
    rows = _eval_size([0, 500, 1023])
    for row in rows:
        assert row["readable_unit"] == "B"


def test_get_readable_size_kib():
    rows = _eval_size([1024, 2048, 1024**2 - 1])
    for row in rows:
        assert row["readable_unit"] == "KiB"
    assert rows[0]["readable_value"] == pytest.approx(1.0)
    assert rows[1]["readable_value"] == pytest.approx(2.0)


def test_get_readable_size_mib():
    rows = _eval_size([1024**2, 1024**2 * 2])
    for row in rows:
        assert row["readable_unit"] == "MiB"
    assert rows[0]["readable_value"] == pytest.approx(1.0)


def test_get_readable_size_gib():
    rows = _eval_size([1024**3, 1024**3 * 3])
    for row in rows:
        assert row["readable_unit"] == "GiB"
    assert rows[0]["readable_value"] == pytest.approx(1.0)


def test_get_readable_size_tib():
    rows = _eval_size([1024**4, 1024**4 * 2])
    for row in rows:
        assert row["readable_unit"] == "TiB"
    assert rows[0]["readable_value"] == pytest.approx(1.0)


# ---------------------------------------------------------------------------
# get_readable_timing
# ---------------------------------------------------------------------------


def _eval_timing(values: list[int]) -> list[dict]:
    df = pl.DataFrame({"value": values})
    return (
        df.with_columns(get_readable_timing(pl.col("value")).alias("r"))
        .unnest("r")
        .to_dicts()
    )


def test_get_readable_timing_ms():
    rows = _eval_timing([0, 500, 999])
    for row in rows:
        assert row["readable_unit"] == "ms"


def test_get_readable_timing_seconds():
    rows = _eval_timing([1_000, 30_000, 59_999])
    for row in rows:
        assert row["readable_unit"] == "s"
    assert rows[0]["readable_value"] == pytest.approx(1.0)
    assert rows[1]["readable_value"] == pytest.approx(30.0)


def test_get_readable_timing_minutes():
    rows = _eval_timing([60_000, 90_000, 3_599_999])
    for row in rows:
        assert row["readable_unit"] == "min"
    assert rows[0]["readable_value"] == pytest.approx(1.0)
    assert rows[1]["readable_value"] == pytest.approx(1.5)


def test_get_readable_timing_hours():
    rows = _eval_timing([3_600_000, 7_200_000])
    for row in rows:
        assert row["readable_unit"] == "hr"
    assert rows[0]["readable_value"] == pytest.approx(1.0)
    assert rows[1]["readable_value"] == pytest.approx(2.0)


# ---------------------------------------------------------------------------
# clean_jobs
# ---------------------------------------------------------------------------


def _make_job(
    job_id: int, event_type: EventType, ts: int, stages: list[int] | None = None
) -> Job:
    return Job.model_validate(
        {
            "Job ID": job_id,
            "event_type": event_type,
            "job_timestamp": ts,
            "Stage IDs": stages,
        }
    )


def test_clean_jobs_columns():
    jobs = [
        _make_job(0, EventType.start, 1_000, [0, 1]),
        _make_job(0, EventType.end, 5_000),
    ]
    result = clean_jobs(jobs)
    assert "job_id" in result.columns
    assert "stage_id" in result.columns
    assert "job_start_timestamp" in result.columns
    assert "job_end_timestamp" in result.columns
    assert "job_duration_seconds" in result.columns


def test_clean_jobs_duration():
    jobs = [
        _make_job(0, EventType.start, 1_000, [0]),
        _make_job(0, EventType.end, 5_000),
    ]
    result = clean_jobs(jobs)
    row = result.filter(pl.col("job_id") == 0).to_dicts()[0]
    assert row["job_duration_seconds"] == pytest.approx(4.0)
    assert row["job_start_timestamp"] == 1_000
    assert row["job_end_timestamp"] == 5_000


def test_clean_jobs_stage_expansion():
    # end events produce a null-stage row; only start events carry stage IDs
    jobs = [
        _make_job(0, EventType.start, 0, [0, 1, 2]),
        _make_job(0, EventType.end, 3_000),
    ]
    result = clean_jobs(jobs)
    non_null = result.filter(pl.col("stage_id").is_not_null())
    assert non_null.shape[0] == 3
    assert sorted(non_null["stage_id"].to_list()) == [0, 1, 2]


def test_clean_jobs_multiple_jobs():
    jobs = [
        _make_job(0, EventType.start, 0, [0]),
        _make_job(0, EventType.end, 2_000),
        _make_job(1, EventType.start, 5_000, [1]),
        _make_job(1, EventType.end, 8_000),
    ]
    result = clean_jobs(jobs)
    non_null = result.filter(pl.col("stage_id").is_not_null())
    assert non_null.shape[0] == 2
    durations = dict(zip(non_null["job_id"].to_list(), non_null["job_duration_seconds"].to_list()))
    assert durations[0] == pytest.approx(2.0)
    assert durations[1] == pytest.approx(3.0)


# ---------------------------------------------------------------------------
# clean_stages
# ---------------------------------------------------------------------------


def _make_stage(stage_id: int, event_type: EventType, ts: int) -> Stage:
    return Stage(stage_id=stage_id, event_type=event_type, stage_timestamp=ts)


def test_clean_stages_columns():
    stages = [
        _make_stage(0, EventType.start, 1_000),
        _make_stage(0, EventType.end, 5_000),
    ]
    result = clean_stages(stages)
    assert "stage_id" in result.columns
    assert "stage_start_timestamp" in result.columns
    assert "stage_end_timestamp" in result.columns
    assert "stage_duration_seconds" in result.columns


def test_clean_stages_duration():
    stages = [
        _make_stage(0, EventType.start, 1_000),
        _make_stage(0, EventType.end, 5_000),
    ]
    result = clean_stages(stages)
    row = result.filter(pl.col("stage_id") == 0).to_dicts()[0]
    assert row["stage_duration_seconds"] == pytest.approx(4.0)
    assert row["stage_start_timestamp"] == 1_000
    assert row["stage_end_timestamp"] == 5_000


def test_clean_stages_multiple():
    stages = [
        _make_stage(0, EventType.start, 0),
        _make_stage(0, EventType.end, 1_000),
        _make_stage(1, EventType.start, 2_000),
        _make_stage(1, EventType.end, 4_000),
    ]
    result = clean_stages(stages)
    assert result.shape[0] == 2
    durations = dict(zip(result["stage_id"].to_list(), result["stage_duration_seconds"].to_list()))
    assert durations[0] == pytest.approx(1.0)
    assert durations[1] == pytest.approx(2.0)


# ---------------------------------------------------------------------------
# clean_tasks
# ---------------------------------------------------------------------------


def test_clean_tasks_columns(parsed_log):
    result = clean_tasks(parsed_log.tasks)
    assert "task_start_timestamp" in result.columns
    assert "task_end_timestamp" in result.columns
    assert "task_duration_seconds" in result.columns
    assert "task_start_time" not in result.columns
    assert "task_finish_time" not in result.columns


def test_clean_tasks_duration_positive(parsed_log):
    result = clean_tasks(parsed_log.tasks)
    assert (result["task_duration_seconds"] >= 0).all()


def test_clean_tasks_duration_calculation(parsed_log):
    result = clean_tasks(parsed_log.tasks)
    first = parsed_log.tasks[0]
    expected = (first.task_finish_time - first.task_start_time) / 1_000
    actual = result.filter(pl.col("task_id") == first.task_id)["task_duration_seconds"][0]
    assert actual == pytest.approx(expected)


def test_clean_tasks_row_count_preserved(parsed_log):
    result = clean_tasks(parsed_log.tasks)
    assert result.shape[0] == len(parsed_log.tasks)


# ---------------------------------------------------------------------------
# get_job_idle_time
# ---------------------------------------------------------------------------


def _make_combined_df(jobs: list[tuple[int, str, str]]) -> pl.DataFrame:
    """Build a minimal combined-style DataFrame for get_job_idle_time.

    jobs: list of (job_id, start_timestamp, end_timestamp) in strftime format
    "%Y-%m-%dT%H:%M:%S.%f"
    """
    rows = [
        {"job_id": jid, "job_start_timestamp": start, "job_end_timestamp": end}
        for jid, start, end in jobs
    ]
    return pl.DataFrame(rows)


def test_get_job_idle_time_no_gap():
    df = _make_combined_df(
        [
            (0, "2024-01-01T10:00:00.000000", "2024-01-01T10:05:00.000000"),
            (1, "2024-01-01T10:05:00.000000", "2024-01-01T10:10:00.000000"),
        ]
    )
    result = get_job_idle_time(df)
    assert result["idle_time_ms"] == 0


def test_get_job_idle_time_single_job():
    df = _make_combined_df(
        [(0, "2024-01-01T10:00:00.000000", "2024-01-01T10:05:00.000000")]
    )
    result = get_job_idle_time(df)
    assert result["idle_time_ms"] == 0


def test_get_job_idle_time_with_gap():
    df = _make_combined_df(
        [
            (0, "2024-01-01T10:00:00.000000", "2024-01-01T10:05:00.000000"),
            (1, "2024-01-01T10:07:00.000000", "2024-01-01T10:10:00.000000"),
        ]
    )
    result = get_job_idle_time(df)
    assert result["idle_time_ms"] == 2 * 60 * 1000


def test_get_job_idle_time_multiple_gaps():
    df = _make_combined_df(
        [
            (0, "2024-01-01T10:00:00.000000", "2024-01-01T10:05:00.000000"),
            (1, "2024-01-01T10:06:00.000000", "2024-01-01T10:08:00.000000"),
            (2, "2024-01-01T10:09:30.000000", "2024-01-01T10:12:00.000000"),
        ]
    )
    result = get_job_idle_time(df)
    # gap 1: 1 min, gap 2: 1.5 min = 2.5 min total = 150_000 ms
    assert result["idle_time_ms"] == 150_000


def test_get_job_idle_time_returns_readable(parsed_log):
    from sparkparse.clean import log_to_combined_df, log_to_dag_df

    dag = log_to_dag_df(parsed_log)
    combined = log_to_combined_df(parsed_log, dag, log_name="test")
    result = get_job_idle_time(combined)
    assert "idle_time_ms" in result
    assert "readable" in result
