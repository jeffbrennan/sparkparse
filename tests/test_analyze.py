from pathlib import Path

import polars as pl
import pytest

from sparkparse.analyze import (
    find_cartesian_joins,
    find_largest_scans,
    find_long_running_nodes,
    find_repeated_scans,
    find_row_count_explosions,
    find_shuffle_heavy_stages,
    find_skewed_tasks,
    find_spill,
    get_issues,
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


def test_find_skewed_tasks_returns_dataframe(dfs_complex):
    result = find_skewed_tasks(dfs_complex)
    assert isinstance(result, pl.DataFrame)


def test_find_skewed_tasks_columns(dfs_complex):
    result = find_skewed_tasks(dfs_complex)
    assert set(result.columns) >= {
        "query_id",
        "stage_id",
        "task_count",
        "median_task_s",
        "max_task_s",
        "skew_ratio",
    }


def test_find_skewed_tasks_ratio_above_threshold(dfs_complex):
    threshold = 3.0
    result = find_skewed_tasks(dfs_complex, skew_ratio=threshold)
    if result.shape[0] > 0:
        assert (result["skew_ratio"] >= threshold).all()


def test_find_skewed_tasks_sorted_descending(dfs_complex):
    result = find_skewed_tasks(dfs_complex, skew_ratio=0.0)
    if result.shape[0] > 1:
        ratios = result["skew_ratio"].to_list()
        assert ratios == sorted(ratios, reverse=True)


def test_find_skewed_tasks_high_threshold_returns_empty(dfs_complex):
    result = find_skewed_tasks(dfs_complex, skew_ratio=1e9)
    assert result.shape[0] == 0


def test_find_shuffle_heavy_stages_returns_dataframe(dfs_complex):
    result = find_shuffle_heavy_stages(dfs_complex)
    assert isinstance(result, pl.DataFrame)


def test_find_shuffle_heavy_stages_columns(dfs_complex):
    result = find_shuffle_heavy_stages(dfs_complex)
    assert set(result.columns) >= {
        "query_id",
        "stage_id",
        "shuffle_write_bytes",
        "shuffle_read_bytes",
        "total_shuffle_bytes",
    }


def test_find_shuffle_heavy_stages_total_is_sum(dfs_complex):
    result = find_shuffle_heavy_stages(dfs_complex)
    if result.shape[0] > 0:
        computed = result["shuffle_write_bytes"] + result["shuffle_read_bytes"]
        assert (computed == result["total_shuffle_bytes"]).all()


def test_find_shuffle_heavy_stages_above_threshold(dfs_complex):
    result = find_shuffle_heavy_stages(dfs_complex, threshold_bytes=0)
    if result.shape[0] > 0:
        assert (result["total_shuffle_bytes"] > 0).all()


def test_find_shuffle_heavy_stages_sorted_descending(dfs_complex):
    result = find_shuffle_heavy_stages(dfs_complex, threshold_bytes=0)
    if result.shape[0] > 1:
        totals = result["total_shuffle_bytes"].to_list()
        assert totals == sorted(totals, reverse=True)


def test_find_shuffle_heavy_stages_high_threshold_returns_empty(dfs_complex):
    result = find_shuffle_heavy_stages(dfs_complex, threshold_bytes=2**63 - 1)
    assert result.shape[0] == 0


def test_find_long_running_nodes_returns_dataframe(dfs_complex):
    result = find_long_running_nodes(dfs_complex)
    assert isinstance(result, pl.DataFrame)


def test_find_long_running_nodes_columns(dfs_complex):
    result = find_long_running_nodes(dfs_complex)
    assert set(result.columns) >= {
        "query_id",
        "node_id",
        "node_type",
        "node_name",
        "node_duration_minutes",
    }


def test_find_long_running_nodes_threshold_zero_returns_rows(dfs_complex):
    result = find_long_running_nodes(dfs_complex, threshold_min=0.0)
    assert result.shape[0] > 0


def test_find_long_running_nodes_threshold_filters(dfs_complex):
    result = find_long_running_nodes(dfs_complex, threshold_min=0.0)
    if result.shape[0] > 0:
        assert (result["node_duration_minutes"] >= 0.0).all()


def test_find_long_running_nodes_sorted_descending(dfs_complex):
    result = find_long_running_nodes(dfs_complex, threshold_min=0.0)
    if result.shape[0] > 1:
        durations = result["node_duration_minutes"].to_list()
        assert durations == sorted(durations, reverse=True)


def test_find_long_running_nodes_high_threshold_returns_empty(dfs_complex):
    result = find_long_running_nodes(dfs_complex, threshold_min=1e9)
    assert result.shape[0] == 0


def test_get_issues_returns_list(dfs_complex):
    result = get_issues(dfs_complex)
    assert isinstance(result, list)


def test_get_issues_valid_severity_values(dfs_complex):
    result = get_issues(dfs_complex)
    allowed = {"critical", "warning"}
    for issue in result:
        assert issue["severity"] in allowed


def test_get_issues_required_keys(dfs_complex):
    result = get_issues(dfs_complex)
    required = {"severity", "category", "message", "query_id", "stage_id"}
    for issue in result:
        assert required.issubset(issue.keys())


def test_get_issues_cartesian_join_fixture(dfs_loop_join):
    result = get_issues(dfs_loop_join)
    categories = [i["category"] for i in result]
    assert "Cartesian Join" in categories
    critical = [i for i in result if i["category"] == "Cartesian Join"]
    assert all(i["severity"] == "critical" for i in critical)


def test_get_issues_messages_are_strings(dfs_complex):
    result = get_issues(dfs_complex)
    for issue in result:
        assert isinstance(issue["message"], str)
        assert len(issue["message"]) > 0


# ---------------------------------------------------------------------------
# find_row_count_explosions
# ---------------------------------------------------------------------------


def test_find_row_count_explosions_returns_dataframe(dfs_loop_join):
    result = find_row_count_explosions(dfs_loop_join)
    assert isinstance(result, pl.DataFrame)


def test_find_row_count_explosions_columns(dfs_loop_join):
    result = find_row_count_explosions(dfs_loop_join)
    assert set(result.columns) >= {
        "query_id",
        "node_id",
        "node_type",
        "join_type",
        "left_keys",
        "right_keys",
        "output_rows",
        "left_input_rows",
        "right_input_rows",
        "ratio",
        "left_scan_paths",
        "right_scan_paths",
    }


def test_find_row_count_explosions_detects_bnlj(dfs_loop_join):
    result = find_row_count_explosions(dfs_loop_join)
    assert result.shape[0] > 0
    assert (
        result.filter(pl.col("node_type") == NodeType.BroadcastNestedLoopJoin).shape[0]
        > 0
    )


def test_find_row_count_explosions_above_threshold(dfs_loop_join):
    result = find_row_count_explosions(dfs_loop_join, ratio_threshold=1.1)
    if result.shape[0] > 0:
        assert (result["ratio"] >= 1.1).all()


def test_find_row_count_explosions_sorted_descending(dfs_loop_join):
    result = find_row_count_explosions(dfs_loop_join, ratio_threshold=0.0)
    if result.shape[0] > 1:
        ratios = result["ratio"].to_list()
        assert ratios == sorted(ratios, reverse=True)


def test_find_row_count_explosions_high_threshold_returns_empty(dfs_loop_join):
    result = find_row_count_explosions(dfs_loop_join, ratio_threshold=1e9)
    assert result.shape[0] == 0


def test_find_row_count_explosions_scan_paths_are_lists(dfs_loop_join):
    result = find_row_count_explosions(dfs_loop_join)
    if result.shape[0] > 0:
        assert result["left_scan_paths"].dtype == pl.List(pl.String)
        assert result["right_scan_paths"].dtype == pl.List(pl.String)


def test_get_issues_row_count_explosion_message_includes_details(dfs_loop_join):
    result = get_issues(dfs_loop_join)
    explosion_issues = [
        i for i in result if i["category"] in ("Cartesian Join", "Row Count Explosion")
    ]
    assert len(explosion_issues) > 0
    for issue in explosion_issues:
        message = issue["message"]
        assert "rows" in message
        assert "×" in message
        assert "scans:" in message or "left:" in message
