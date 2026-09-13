import datetime
import json
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import polars as pl
import pytest

from sparkparse.analyze import workload_fingerprint
from sparkparse.clean import log_to_combined_df, log_to_dag_df
from sparkparse.history import (
    _cumulative_time_seconds,
    append,
    compare_runs,
    read,
    record_from_dfs,
    select_baseline_cohort,
)
from sparkparse.models import ParsedLogDataFrames, RunRecord
from sparkparse.parse import parse_log

DATA_DIR = Path(__file__).parent / "data" / "full_logs"

_ALL_AVAILABLE = {
    "plan_structure": "available",
    "operator_metrics": "available",
    "query_elapsed_time": "available",
    "task_metrics": "available",
    "stage_timing": "available",
    "scan_details": "available",
    "join_details": "available",
}


def _make_record(**overrides) -> RunRecord:
    defaults = dict(
        run_id=uuid.uuid4().hex,
        run_at=datetime.datetime(2024, 1, 1, tzinfo=datetime.UTC),
        log_name="test_job",
        duration_s=100.0,
        bytes_read=1000,
        bytes_written=500,
        shuffle_read_bytes=100,
        shuffle_write_bytes=100,
        memory_bytes_spilled=0,
        disk_bytes_spilled=0,
        n_queries=2,
        n_stages=3,
        n_tasks=50,
        n_cartesian_joins=0,
        max_node_duration_min=1.5,
        max_scan_bytes=800,
        coverage=dict(_ALL_AVAILABLE),
    )
    defaults.update(overrides)
    return RunRecord(**defaults)


def test_jsonl_history_normalizes_timestamp_offsets_to_utc(tmp_path):
    instant = datetime.datetime(
        2026,
        9,
        12,
        12,
        30,
        1,
        123456,
        tzinfo=datetime.timezone(datetime.timedelta(hours=-4)),
    )
    path = str(tmp_path / "history.jsonl")
    append(_make_record(run_at=instant), path, format="jsonl")
    restored = read(path, format="jsonl")["run_at"][0]
    assert restored == instant.astimezone(datetime.UTC)
    assert restored.utcoffset() == datetime.timedelta(0)


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


def test_record_from_dfs_valid(dfs_nested):
    record = record_from_dfs(dfs_nested, "nested_final_plans")
    assert isinstance(record, RunRecord)
    assert record.record_version == 2
    assert record.log_name == "nested_final_plans"
    assert len(record.run_id) == 32  # uuid4 hex
    assert isinstance(record.run_at, datetime.datetime)
    assert record.backend == "event_log"
    assert record.workload_fingerprint is not None
    assert record.coverage
    assert record.duration_s is not None
    assert record.bytes_read is not None
    assert record.bytes_written is not None
    assert record.shuffle_read_bytes is not None
    assert record.shuffle_write_bytes is not None
    assert record.memory_bytes_spilled is not None
    assert record.disk_bytes_spilled is not None
    assert record.n_queries is not None
    assert record.n_stages is not None
    assert record.n_tasks is not None
    assert record.n_cartesian_joins is not None
    assert record.max_node_duration_min is not None
    assert record.max_scan_bytes is not None
    assert record.duration_s > 0
    assert record.n_queries > 0
    assert record.n_tasks > 0


def test_record_from_dfs_duration_wall_clock(dfs_nested):
    record = record_from_dfs(dfs_nested, "nested_final_plans")
    total_query_duration = dfs_nested.dag["query_duration_seconds"].sum()
    assert record.duration_s is not None
    assert 0 < record.duration_s <= total_query_duration


def test_record_from_dfs_cartesian_count(dfs_loop_join):
    # This log's nested loop join evaluates a condition, so it is counted as a
    # nested loop join and not as a cartesian product.
    record = record_from_dfs(dfs_loop_join, "nested_loop_join")
    assert record.n_cartesian_joins == 0


def test_record_from_dfs_complex(dfs_complex):
    record = record_from_dfs(dfs_complex, "complex_transformation_medium")
    assert record.n_queries is not None
    assert record.n_tasks is not None
    assert record.n_queries > 0
    assert record.n_tasks > 0


def test_connect_record_keeps_aggregate_time_without_wall_clock(dfs_nested):
    from sparkparse.models import (
        CapabilityStatus,
        CaptureCapabilities,
        CaptureCapability,
        CaptureMetadata,
        CaptureResult,
    )

    dag = dfs_nested.dag.with_columns(
        pl.lit(None, dtype=pl.String).alias("query_start_timestamp"),
        pl.lit(None, dtype=pl.String).alias("query_end_timestamp"),
    )
    result = CaptureResult(
        dag=dag,
        combined=dfs_nested.combined,
        metadata=CaptureMetadata(
            capture_id="connect-1",
            backend="spark_connect",
            capture_start=datetime.datetime(2024, 1, 1, tzinfo=datetime.UTC),
        ),
        capabilities=CaptureCapabilities(
            query_elapsed_time=CaptureCapability(status=CapabilityStatus.partial),
            task_metrics=CaptureCapability(status=CapabilityStatus.unavailable),
        ),
    )
    record = record_from_dfs(result, "connect_job")
    assert record.duration_s is None
    assert record.cumulative_time_s is not None
    assert record.cumulative_time_s > 0
    assert record.bytes_read is None


def test_cumulative_time_counts_each_query_once():
    dag = pl.DataFrame(
        {
            "query_id": [0, 0, 1, 1, 1],
            "query_duration_seconds": [10.0, 10.0, 5.0, 5.0, 5.0],
        }
    )
    assert _cumulative_time_seconds(dag) == 15.0


def test_long_legacy_prefix_does_not_drop_new_fields(tmp_path: Path):
    path = tmp_path / "store.jsonl"
    legacy = {
        "run_id": "legacy",
        "run_at": "2020-01-01T00:00:00",
        "log_name": "job",
        "duration_s": 42.0,
        "bytes_read": 10,
        "bytes_written": 5,
        "shuffle_bytes": 3,
        "spill_bytes": 1,
        "n_queries": 1,
        "n_stages": 1,
        "n_tasks": 1,
        "n_cartesian_joins": 0,
        "max_node_duration_min": 0.5,
        "max_scan_bytes": 10,
    }
    with path.open("w") as f:
        for i in range(100):
            row = dict(legacy, run_id=f"legacy-{i}")
            f.write(json.dumps(row) + "\n")

    append(
        _make_record(log_name="job", workload_fingerprint="fp_new"),
        str(path),
        format="jsonl",
    )

    df = read(str(path), format="jsonl")
    assert "workload_fingerprint" in df.columns
    newest = df.sort("run_at").row(-1, named=True)
    assert newest["record_version"] == 2
    assert newest["workload_fingerprint"] == "fp_new"


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
    assert df["record_version"].unique().to_list() == [2]
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


def test_jsonl_path_stays_jsonl_when_delta_available(tmp_path: Path, monkeypatch):
    monkeypatch.setattr("sparkparse.history._delta_available", lambda: True)
    history_path = str(tmp_path / "history.jsonl")
    append(_make_record(log_name="job_a"), history_path, format="auto")

    assert Path(history_path).is_file()
    assert Path(history_path).read_text().startswith("{")
    assert not (tmp_path / "history.jsonl" / "_delta_log").exists()
    assert read(history_path, format="auto").height == 1


def test_existing_jsonl_store_ignores_installed_delta(tmp_path: Path, monkeypatch):
    history_path = str(tmp_path / "store.jsonl")
    append(_make_record(log_name="job_a"), history_path, format="jsonl")

    monkeypatch.setattr("sparkparse.history._delta_available", lambda: True)
    append(_make_record(log_name="job_a"), history_path, format="auto")

    df = read(history_path, format="auto")
    assert df.height == 2


def test_legacy_records_read_with_unknown_provenance(tmp_path: Path):
    legacy = {
        "run_id": "legacy-run",
        "run_at": "2024-01-01T00:00:00",
        "log_name": "job_a",
        "duration_s": 42.0,
        "bytes_read": 10,
        "bytes_written": 5,
        "shuffle_bytes": 3,
        "spill_bytes": 1,
        "n_queries": 1,
        "n_stages": 1,
        "n_tasks": 1,
        "n_cartesian_joins": 0,
        "max_node_duration_min": 0.5,
        "max_scan_bytes": 10,
    }
    path = tmp_path / "legacy.jsonl"
    path.write_text(json.dumps(legacy) + "\n")

    df = read(str(path), format="jsonl")
    assert df["record_version"][0] == 1
    assert df["duration_s"][0] == 42.0
    # Legacy provenance is unknown, so legacy rows never enter a baseline cohort.
    current = _make_record(
        log_name="job_a", run_at=datetime.datetime(2025, 1, 1, tzinfo=datetime.UTC)
    )
    cohort, notes = select_baseline_cohort(df, current, log_name="job_a", window=10)
    assert cohort == []
    assert any("legacy" in note for note in notes)


def test_partial_record_roundtrips_missing_measures(tmp_path: Path):
    path = str(tmp_path / "history.jsonl")
    partial = _make_record(
        log_name="connect_job",
        duration_s=None,
        bytes_read=None,
        bytes_written=None,
        n_tasks=None,
        coverage={"task_metrics": "unavailable"},
    )
    append(partial, path, format="jsonl")

    restored = read(path, format="jsonl")
    row = restored.row(0, named=True)
    assert row["duration_s"] is None
    assert row["bytes_read"] is None
    assert row["n_tasks"] is None
    assert row["coverage"]["task_metrics"] == "unavailable"


def test_duplicate_baseline_run_counted_once():
    base = datetime.datetime(2024, 1, 1, tzinfo=datetime.UTC)
    duplicate = _make_record(log_name="job", run_id="dup", run_at=base)
    df = pl.DataFrame([duplicate.model_dump(), duplicate.model_dump()])
    current = _make_record(log_name="job", run_at=base + datetime.timedelta(hours=1))
    cohort, notes = select_baseline_cohort(df, current, log_name="job", window=10)
    assert len(cohort) == 1
    assert any("duplicate" in note for note in notes)


def _dag(rows: list[tuple[str, dict]], query_function: str = "collect") -> pl.DataFrame:
    return pl.DataFrame(
        {
            "query_id": [0] * len(rows),
            "query_function": [query_function] * len(rows),
            "node_id": list(range(len(rows))),
            "node_type": [node_type for node_type, _ in rows],
            "details": [json.dumps({"detail": detail}) for _, detail in rows],
        }
    )


def _dfs_with_dag(dag: pl.DataFrame) -> ParsedLogDataFrames:
    return ParsedLogDataFrames(dag=dag, combined=pl.DataFrame())


def test_fingerprint_is_stable_across_node_ids():
    scan = ("Scan", {"location": {"location": ["/data/a.parquet"]}})
    first = _dfs_with_dag(_dag([scan, ("Project", {})]))
    second_rows = [scan, ("Project", {})]
    second = _dfs_with_dag(
        pl.DataFrame(
            {
                "query_id": [0, 0],
                "query_function": ["collect", "collect"],
                "node_id": [7, 3],
                "node_type": [t for t, _ in second_rows],
                "details": [json.dumps({"detail": d}) for _, d in second_rows],
            }
        )
    )
    assert workload_fingerprint(first) == workload_fingerprint(second)


def test_fingerprint_changes_with_join_type():
    inner = _dfs_with_dag(_dag([("BroadcastHashJoin", {"join_type": "Inner"})]))
    outer = _dfs_with_dag(_dag([("BroadcastHashJoin", {"join_type": "LeftOuter"})]))
    assert workload_fingerprint(inner) != workload_fingerprint(outer)


def test_fingerprint_changes_with_scan_path():
    first = _dfs_with_dag(
        _dag([("Scan", {"location": {"location": ["/data/a.parquet"]}})])
    )
    second = _dfs_with_dag(
        _dag([("Scan", {"location": {"location": ["/data/b.parquet"]}})])
    )
    assert workload_fingerprint(first) != workload_fingerprint(second)


def test_compare_runs_reports_excluded_measures(tmp_path: Path):
    path = str(tmp_path / "history.jsonl")
    base = datetime.datetime(2024, 1, 1, tzinfo=datetime.UTC)
    for i in range(3):
        append(
            _make_record(
                log_name="job_a",
                duration_s=100.0,
                run_at=base + datetime.timedelta(hours=i),
            ),
            path,
            format="jsonl",
        )
    append(
        _make_record(
            log_name="job_a",
            duration_s=150.0,
            run_at=base + datetime.timedelta(hours=10),
        ),
        path,
        format="jsonl",
    )

    report = compare_runs(path, "job_a")
    metrics = {m.metric: m for m in report.metrics}
    assert metrics["duration_s"].baseline == pytest.approx(100.0)
    assert metrics["duration_s"].sample_count == 3
    assert any("current value unavailable" in reason for reason in report.excluded)
    assert report.cohort_size == 3


def test_compare_runs_exposes_input_size_change(tmp_path: Path):
    path = str(tmp_path / "history.jsonl")
    base = datetime.datetime(2024, 1, 1, tzinfo=datetime.UTC)
    for i in range(2):
        append(
            _make_record(
                log_name="job_a",
                bytes_read=1000,
                run_at=base + datetime.timedelta(hours=i),
            ),
            path,
            format="jsonl",
        )
    append(
        _make_record(
            log_name="job_a",
            bytes_read=2000,
            run_at=base + datetime.timedelta(hours=10),
        ),
        path,
        format="jsonl",
    )
    report = compare_runs(path, "job_a")
    metrics = {m.metric: m for m in report.metrics}
    assert metrics["bytes_read"].baseline == 1000.0
    assert metrics["bytes_read"].pct_change == pytest.approx(1.0)


def test_compare_runs_equivalent_plan_is_not_changed(tmp_path: Path):
    path = str(tmp_path / "history.jsonl")
    base = datetime.datetime(2024, 1, 1, tzinfo=datetime.UTC)
    for i in range(2):
        append(
            _make_record(
                log_name="job_a",
                workload_fingerprint="fp1",
                run_at=base + datetime.timedelta(hours=i),
            ),
            path,
            format="jsonl",
        )
    append(
        _make_record(
            log_name="job_a",
            workload_fingerprint="fp1",
            run_at=base + datetime.timedelta(hours=10),
        ),
        path,
        format="jsonl",
    )
    report = compare_runs(path, "job_a")
    assert report.plan_changed is False


def test_compare_runs_changed_plan_is_flagged(tmp_path: Path):
    path = str(tmp_path / "history.jsonl")
    base = datetime.datetime(2024, 1, 1, tzinfo=datetime.UTC)
    append(
        _make_record(
            log_name="job_a",
            workload_fingerprint="fp1",
            run_at=base,
        ),
        path,
        format="jsonl",
    )
    append(
        _make_record(
            log_name="job_a",
            workload_fingerprint="fp2",
            run_at=base + datetime.timedelta(hours=1),
        ),
        path,
        format="jsonl",
    )
    report = compare_runs(path, "job_a")
    assert report.plan_changed is True


def test_concurrent_jsonl_appends_do_not_lose_runs(tmp_path: Path):
    path = str(tmp_path / "history.jsonl")
    records = [_make_record(log_name="job_a") for _ in range(25)]

    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(lambda r: append(r, path, format="jsonl"), records))

    df = read(path, format="jsonl")
    assert df.height == 25
