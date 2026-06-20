import datetime
import uuid
from pathlib import Path

import pytest

from sparkparse.clean import log_to_combined_df, log_to_dag_df
from sparkparse.history import append, read, record_from_dfs
from sparkparse.models import ParsedLogDataFrames, RunRecord
from sparkparse.parse import parse_log

DATA_DIR = Path(__file__).parent / "data" / "full_logs"


@pytest.fixture(scope="module")
def dfs_nested() -> ParsedLogDataFrames:
    log_path = DATA_DIR / "nested_final_plans"
    result = parse_log(log_path)
    dag = log_to_dag_df(result)
    combined = log_to_combined_df(result, dag, log_path.stem)
    return ParsedLogDataFrames(dag=dag, combined=combined)


@pytest.fixture(scope="module")
def dfs_loop_join() -> ParsedLogDataFrames:
    log_path = DATA_DIR / "nested_loop_join"
    result = parse_log(log_path)
    dag = log_to_dag_df(result)
    combined = log_to_combined_df(result, dag, log_path.stem)
    return ParsedLogDataFrames(dag=dag, combined=combined)


@pytest.fixture(scope="module")
def dfs_complex() -> ParsedLogDataFrames:
    log_path = DATA_DIR / "complex_transformation_medium"
    result = parse_log(log_path)
    dag = log_to_dag_df(result)
    combined = log_to_combined_df(result, dag, log_path.stem)
    return ParsedLogDataFrames(dag=dag, combined=combined)


def _make_record(**overrides) -> RunRecord:
    defaults = dict(
        run_id=uuid.uuid4().hex,
        run_at=datetime.datetime.now(datetime.UTC),
        log_name="test_job",
        duration_s=100.0,
        bytes_read=1000,
        bytes_written=500,
        shuffle_bytes=200,
        spill_bytes=0,
        n_queries=2,
        n_stages=3,
        n_tasks=50,
        n_cartesian_joins=0,
        max_node_duration_min=1.5,
        max_scan_bytes=800,
    )
    defaults.update(overrides)
    return RunRecord(**defaults)


def test_record_from_dfs_valid(dfs_nested):
    record = record_from_dfs(dfs_nested, "nested_final_plans")
    assert isinstance(record, RunRecord)
    assert record.log_name == "nested_final_plans"
    assert len(record.run_id) == 32  # uuid4 hex
    assert isinstance(record.run_at, datetime.datetime)
    assert record.duration_s > 0
    assert record.bytes_read >= 0
    assert record.bytes_written >= 0
    assert record.shuffle_bytes >= 0
    assert record.spill_bytes >= 0
    assert record.n_queries > 0
    assert record.n_stages > 0
    assert record.n_tasks > 0
    assert record.n_cartesian_joins >= 0
    assert record.max_node_duration_min >= 0
    assert record.max_scan_bytes >= 0


def test_record_from_dfs_duration_wall_clock(dfs_nested):
    record = record_from_dfs(dfs_nested, "nested_final_plans")
    total_query_duration = dfs_nested.dag["query_duration_seconds"].sum()
    assert 0 < record.duration_s <= total_query_duration


def test_record_from_dfs_cartesian_count(dfs_loop_join):
    record = record_from_dfs(dfs_loop_join, "nested_loop_join")
    assert record.n_cartesian_joins > 0


def test_record_from_dfs_complex(dfs_complex):
    record = record_from_dfs(dfs_complex, "complex_transformation_medium")
    assert record.n_queries > 0
    assert record.n_tasks > 0


def test_append_read_roundtrip_jsonl(tmp_path: Path):
    history_path = str(tmp_path / "history.jsonl")
    records = [
        _make_record(log_name="job_a", duration_s=10.0),
        _make_record(log_name="job_a", duration_s=20.0),
        _make_record(log_name="job_a", duration_s=30.0),
    ]
    for r in records:
        append(r, history_path, format="jsonl")

    df = read(history_path, format="jsonl")
    assert df.height == 3
    assert df["log_name"].unique().to_list() == ["job_a"]
    durations = df.sort("run_at")["duration_s"].to_list()
    assert durations == sorted(durations)


def test_read_filter_by_log_name(tmp_path: Path):
    history_path = str(tmp_path / "history.jsonl")
    append(_make_record(log_name="job_a"), history_path, format="jsonl")
    append(_make_record(log_name="job_b"), history_path, format="jsonl")
    append(_make_record(log_name="job_a"), history_path, format="jsonl")

    df = read(history_path, log_name="job_a", format="jsonl")
    assert df.height == 2
    assert (df["log_name"] == "job_a").all()


def test_read_last_n(tmp_path: Path):
    history_path = str(tmp_path / "history.jsonl")
    base_time = datetime.datetime(2024, 1, 1, tzinfo=datetime.UTC)
    for i in range(5):
        append(
            _make_record(
                log_name="job_a",
                duration_s=float(i),
                run_at=base_time + datetime.timedelta(hours=i),
                run_id=f"run_{i:03d}",
            ),
            history_path,
            format="jsonl",
        )

    df = read(history_path, last_n=2, format="jsonl")
    assert df.height == 2
    run_ids = df.sort("run_at")["run_id"].to_list()
    assert run_ids == ["run_003", "run_004"]


def test_read_empty_path(tmp_path: Path):
    history_path = str(tmp_path / "nonexistent.jsonl")
    df = read(history_path, format="jsonl")
    assert df.is_empty()


def test_append_read_roundtrip_delta(tmp_path: Path):
    pytest.importorskip("deltalake")
    history_path = str(tmp_path / "delta_table")
    records = [
        _make_record(log_name="job_a", duration_s=10.0),
        _make_record(log_name="job_a", duration_s=20.0),
    ]
    for r in records:
        append(r, history_path, format="delta")

    df = read(history_path, format="delta")
    assert df.height == 2
    assert (df["log_name"] == "job_a").all()


def test_auto_format_falls_back_to_jsonl(tmp_path: Path, monkeypatch):
    monkeypatch.setattr("sparkparse.history._delta_available", lambda: False)
    history_path = str(tmp_path / "auto.jsonl")
    record = _make_record(log_name="job_a")
    append(record, history_path, format="auto")

    assert Path(history_path).exists()
    content = Path(history_path).read_text().strip()
    assert content.startswith("{")
    df = read(history_path, format="auto")
    assert df.height == 1


def test_auto_format_uses_delta_when_available(tmp_path: Path):
    pytest.importorskip("deltalake")
    history_path = str(tmp_path / "auto_delta")
    record = _make_record(log_name="job_a")
    append(record, history_path, format="auto")

    assert (Path(history_path) / "_delta_log").exists()
    df = read(history_path, format="auto")
    assert df.height == 1
