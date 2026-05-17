from pathlib import Path

import polars as pl
import pytest

from sparkparse.analyze import (
    find_cartesian_joins,
    find_largest_scans,
    find_repeated_scans,
    find_spill,
    to_plan_summary,
)
from sparkparse.clean import log_to_combined_df, log_to_dag_df
from sparkparse.models import NodeType, ParsedLogDataFrames
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


# ---------------------------------------------------------------------------
# to_plan_summary
# ---------------------------------------------------------------------------


def test_to_plan_summary_top_level_keys(dfs_nested):
    summary = to_plan_summary(dfs_nested, "nested_final_plans")
    assert summary["log_name"] == "nested_final_plans"
    assert "queries" in summary
    assert "totals" in summary
    assert len(summary["queries"]) > 0


def test_to_plan_summary_query_structure(dfs_nested):
    summary = to_plan_summary(dfs_nested, "nested_final_plans")
    query = summary["queries"][0]
    for key in (
        "query_id",
        "query_function",
        "start",
        "end",
        "duration_seconds",
        "nodes",
    ):
        assert key in query
    assert len(query["nodes"]) > 0


def test_to_plan_summary_node_structure(dfs_nested):
    summary = to_plan_summary(dfs_nested, "nested_final_plans")
    node = summary["queries"][0]["nodes"][0]
    for key in ("node_id", "node_type", "node_name", "duration_minutes", "metrics"):
        assert key in node


def test_to_plan_summary_scan_details(dfs_nested):
    summary = to_plan_summary(dfs_nested, "nested_final_plans")
    scan_nodes = [
        n
        for q in summary["queries"]
        for n in q["nodes"]
        if n["node_type"] == NodeType.Scan and "details" in n
    ]
    assert len(scan_nodes) > 0
    detail = scan_nodes[0]["details"]
    assert "paths" in detail
    assert "read_schema" in detail
    assert isinstance(detail["paths"], list)


def test_to_plan_summary_totals_keys(dfs_nested):
    totals = to_plan_summary(dfs_nested, "nested_final_plans")["totals"]
    for key in (
        "bytes_read",
        "records_read",
        "memory_bytes_spilled",
        "disk_bytes_spilled",
        "shuffle_bytes_read",
        "shuffle_bytes_written",
        "executor_run_time_seconds",
        "jvm_gc_time_seconds",
    ):
        assert key in totals


def test_to_plan_summary_totals_non_negative(dfs_nested):
    totals = to_plan_summary(dfs_nested, "nested_final_plans")["totals"]
    for key, val in totals.items():
        assert val >= 0, f"{key} should be non-negative, got {val}"


def test_to_plan_summary_node_metrics_structure(dfs_nested):
    summary = to_plan_summary(dfs_nested, "nested_final_plans")
    nodes_with_metrics = [
        n for q in summary["queries"] for n in q["nodes"] if n["metrics"]
    ]
    assert len(nodes_with_metrics) > 0
    metric = nodes_with_metrics[0]["metrics"][0]
    assert "name" in metric
    assert "value" in metric


# ---------------------------------------------------------------------------
# find_cartesian_joins
# ---------------------------------------------------------------------------


def test_find_cartesian_joins_returns_dataframe(dfs_nested):
    result = find_cartesian_joins(dfs_nested)
    assert isinstance(result, pl.DataFrame)


def test_find_cartesian_joins_columns(dfs_nested):
    result = find_cartesian_joins(dfs_nested)
    assert set(result.columns) >= {
        "query_id",
        "node_id",
        "node_name",
        "node_type",
        "node_duration_minutes",
    }


def test_find_cartesian_joins_detects_bnlj(dfs_loop_join):
    result = find_cartesian_joins(dfs_loop_join)
    assert result.shape[0] > 0
    assert (result["node_type"] == NodeType.BroadcastNestedLoopJoin).any()


def test_find_cartesian_joins_only_join_types(dfs_loop_join):
    result = find_cartesian_joins(dfs_loop_join)
    allowed = {
        NodeType.BroadcastNestedLoopJoin,
        NodeType.BroadcastHashJoin,
        NodeType.SortMergeJoin,
    }
    assert set(result["node_type"].to_list()).issubset(allowed)


# ---------------------------------------------------------------------------
# find_largest_scans
# ---------------------------------------------------------------------------


def test_find_largest_scans_returns_dataframe(dfs_nested):
    result = find_largest_scans(dfs_nested)
    assert isinstance(result, pl.DataFrame)


def test_find_largest_scans_columns(dfs_nested):
    result = find_largest_scans(dfs_nested)
    assert set(result.columns) >= {
        "query_id",
        "node_id",
        "node_name",
        "paths",
        "bytes_read",
        "records_read",
        "node_duration_minutes",
    }


def test_find_largest_scans_n_limit(dfs_complex):
    for n in (1, 3, 5):
        result = find_largest_scans(dfs_complex, n=n)
        assert result.shape[0] <= n


def test_find_largest_scans_sorted_descending(dfs_complex):
    result = find_largest_scans(dfs_complex, n=10)
    if result.shape[0] > 1:
        vals = result["bytes_read"].drop_nulls().to_list()
        assert vals == sorted(vals, reverse=True)


def test_find_largest_scans_paths_are_lists(dfs_nested):
    result = find_largest_scans(dfs_nested)
    assert result["paths"].dtype == pl.List(pl.String)


# ---------------------------------------------------------------------------
# find_repeated_scans
# ---------------------------------------------------------------------------


def test_find_repeated_scans_returns_dataframe(dfs_nested):
    result = find_repeated_scans(dfs_nested)
    assert isinstance(result, pl.DataFrame)


def test_find_repeated_scans_columns(dfs_complex):
    result = find_repeated_scans(dfs_complex)
    if result.shape[0] > 0:
        assert set(result.columns) >= {"path", "scan_count", "query_ids"}


def test_find_repeated_scans_count_above_one(dfs_complex):
    result = find_repeated_scans(dfs_complex)
    if result.shape[0] > 0:
        assert (result["scan_count"] > 1).all()


def test_find_repeated_scans_sorted_descending(dfs_complex):
    result = find_repeated_scans(dfs_complex)
    if result.shape[0] > 1:
        counts = result["scan_count"].to_list()
        assert counts == sorted(counts, reverse=True)


# ---------------------------------------------------------------------------
# find_spill
# ---------------------------------------------------------------------------


def test_find_spill_returns_dataframe(dfs_nested):
    result = find_spill(dfs_nested)
    assert isinstance(result, pl.DataFrame)


def test_find_spill_columns(dfs_nested):
    result = find_spill(dfs_nested)
    assert set(result.columns) >= {
        "query_id",
        "stage_id",
        "memory_bytes_spilled",
        "disk_bytes_spilled",
        "task_count",
    }


def test_find_spill_no_zero_rows(dfs_nested):
    result = find_spill(dfs_nested)
    if result.shape[0] > 0:
        has_spill = (result["memory_bytes_spilled"] > 0) | (
            result["disk_bytes_spilled"] > 0
        )
        assert has_spill.all()
