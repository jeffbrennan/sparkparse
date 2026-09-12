import json
from pathlib import Path

import polars as pl
import pytest

from sparkparse.analyze import (
    RULES,
    analyze_dfs,
    find_cartesian_joins,
    find_gc_overhead,
    find_inefficient_scans,
    find_largest_scans,
    find_long_running_nodes,
    find_nested_loop_joins,
    find_operator_spill,
    find_repeated_scans,
    find_row_count_explosions,
    find_shuffle_heavy_stages,
    find_skewed_tasks,
    find_spill,
    get_coverage_notes,
    get_issues,
    to_analysis_export,
    to_plan_summary,
)
from sparkparse.clean import log_to_combined_df, log_to_dag_df
from sparkparse.connect import SparkConnectCapture
from sparkparse.metrics import SOURCE_CONNECT_PLAN, canonical_metrics, metric_value
from sparkparse.models import NodeType, ParsedLogDataFrames
from sparkparse.parse import get_parsed_metrics, parse_log
from tests.synthetic import accum, make_combined, make_dag, make_dfs, node

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


def test_find_cartesian_joins_excludes_conditional_nested_loop(dfs_loop_join):
    # The nested loop join in this log carries a join condition, so it is a
    # nested loop join but not a cartesian product.
    result = find_cartesian_joins(dfs_loop_join)
    assert result.shape[0] == 0


def test_find_nested_loop_joins_detects_conditional_bnlj(dfs_loop_join):
    result = find_nested_loop_joins(dfs_loop_join)
    assert result.shape[0] > 0
    assert (result["node_type"] == NodeType.BroadcastNestedLoopJoin).all()
    assert all(condition for condition in result["join_condition"].to_list())


def test_find_cartesian_joins_only_join_types(dfs_loop_join):
    result = find_cartesian_joins(dfs_loop_join)
    allowed = {
        NodeType.BroadcastNestedLoopJoin,
        NodeType.BroadcastHashJoin,
        NodeType.SortMergeJoin,
        NodeType.CartesianProduct,
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
    # Ranked by the larger of the two sides; read and write are not summed.
    result = find_shuffle_heavy_stages(dfs_complex, threshold_bytes=0)
    if result.shape[0] > 1:
        sides = result["max_side_bytes"].to_list()
        assert sides == sorted(sides, reverse=True)


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


def test_get_issues_nested_loop_join_fixture(dfs_loop_join):
    result = get_issues(dfs_loop_join)
    categories = [i["category"] for i in result]
    assert "Nested Loop Join" in categories
    assert "Cartesian Join" not in categories


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


# ---------------------------------------------------------------------------
# synthetic fixtures: cases real logs in tests/data cannot cover
# ---------------------------------------------------------------------------


def _rows(n: int) -> list[dict]:
    return [accum("number of output rows", n)]


def _join_detail(**kwargs) -> dict:
    detail = {"join_type": "Inner", "join_condition": None}
    detail.update(kwargs)
    return detail


def _scan(
    node_id: int, path: str, rows: int | None = 100, schema: str = "struct<a:int>"
):
    return node(
        node_id,
        NodeType.Scan,
        metrics=_rows(rows) if rows is not None else [],
        detail={
            "output": ["a"],
            "batched": True,
            "location": {"location_type": "InMemoryFileIndex", "location": [path]},
            "read_schema": schema,
        },
    )


def test_cartesian_product_operator_is_detected():
    dag = make_dag(
        {
            0: [
                _scan(1, "/data/left", 10),
                _scan(2, "/data/right", 20),
                node(
                    3,
                    NodeType.CartesianProduct,
                    children=(1, 2),
                    metrics=_rows(200),
                    detail={"join_type": "Inner", "join_condition": None},
                ),
            ]
        }
    )
    result = find_cartesian_joins(make_dfs(dag))
    assert result["node_id"].to_list() == [3]
    assert result["reason"][0] == "CartesianProduct operator"


def test_unconditional_nested_loop_join_is_cartesian():
    dag = make_dag(
        {
            0: [
                _scan(1, "/data/left", 10),
                _scan(2, "/data/right", 20),
                node(
                    3,
                    NodeType.BroadcastNestedLoopJoin,
                    children=(1, 2),
                    metrics=_rows(200),
                    detail=_join_detail(join_condition=None),
                ),
            ]
        }
    )
    dfs = make_dfs(dag)
    assert find_cartesian_joins(dfs).height == 1
    assert find_nested_loop_joins(dfs).height == 0


def test_conditional_nested_loop_join_is_not_cartesian():
    dag = make_dag(
        {
            0: [
                _scan(1, "/data/left", 10),
                _scan(2, "/data/right", 20),
                node(
                    3,
                    NodeType.BroadcastNestedLoopJoin,
                    children=(1, 2),
                    metrics=_rows(30),
                    detail=_join_detail(
                        join_type="LeftOuter", join_condition="(a > b)"
                    ),
                ),
            ]
        }
    )
    dfs = make_dfs(dag)
    assert find_cartesian_joins(dfs).height == 0
    assert find_nested_loop_joins(dfs).height == 1


def test_cross_join_type_is_cartesian():
    dag = make_dag(
        {
            0: [
                _scan(1, "/data/left", 10),
                _scan(2, "/data/right", 20),
                node(
                    3,
                    NodeType.SortMergeJoin,
                    children=(1, 2),
                    metrics=_rows(200),
                    detail=_join_detail(join_type="Cross"),
                ),
            ]
        }
    )
    result = find_cartesian_joins(make_dfs(dag))
    assert result["node_id"].to_list() == [3]
    assert result["reason"][0] == "join_type=Cross"


def test_cross_join_detection_is_scoped_per_query():
    # Node 3 is a Cross join in query 0 and an ordinary inner join in query 1.
    cross = [
        _scan(1, "/data/left", 10),
        _scan(2, "/data/right", 20),
        node(
            3,
            NodeType.SortMergeJoin,
            children=(1, 2),
            metrics=_rows(200),
            detail=_join_detail(join_type="Cross"),
        ),
    ]
    inner = [
        _scan(1, "/data/left", 10),
        _scan(2, "/data/right", 20),
        node(
            3,
            NodeType.SortMergeJoin,
            children=(1, 2),
            metrics=_rows(10),
            detail=_join_detail(join_type="Inner"),
        ),
    ]
    result = find_cartesian_joins(make_dfs(make_dag({0: cross, 1: inner})))
    assert result.select("query_id", "node_id").rows() == [(0, 3)]


def test_expansion_uses_immediate_inputs_not_scans():
    # The filter above each scan cuts 1000 rows down to 10, so the join's real
    # input is 10 per side: a 50-row output is an expansion, even though the
    # scans read 1000 rows each.
    dag = make_dag(
        {
            0: [
                _scan(1, "/data/left", 1000),
                node(2, NodeType.Filter, children=(1,), metrics=_rows(10)),
                _scan(3, "/data/right", 1000),
                node(4, NodeType.Filter, children=(3,), metrics=_rows(10)),
                node(
                    5,
                    NodeType.SortMergeJoin,
                    children=(2, 4),
                    metrics=_rows(50),
                    detail=_join_detail(left_keys=["a"], right_keys=["b"]),
                ),
            ]
        }
    )
    result = find_row_count_explosions(make_dfs(dag))
    assert result.height == 1
    row = result.row(0, named=True)
    assert row["left_input_rows"] == 10
    assert row["right_input_rows"] == 10
    assert row["ratio"] == 5.0
    assert row["inputs_resolved"] is True
    # Scans are lineage only, but they still name the tables.
    assert row["left_scan_paths"] == ["/data/left"]


def test_expansion_descends_row_preserving_wrappers():
    dag = make_dag(
        {
            0: [
                _scan(1, "/data/left", 100),
                node(2, NodeType.BroadcastExchange, children=(1,), metrics=_rows(100)),
                node(3, NodeType.BroadcastQueryStage, children=(2,)),
                _scan(4, "/data/right", 100),
                node(
                    5,
                    NodeType.BroadcastHashJoin,
                    children=(3, 4),
                    metrics=_rows(400),
                    detail=_join_detail(left_keys=["a"], right_keys=["b"]),
                ),
            ]
        }
    )
    row = find_row_count_explosions(make_dfs(dag)).row(0, named=True)
    assert row["left_input_rows"] == 100
    assert row["inputs_resolved"] is True


def test_expansion_skips_zero_row_input():
    dag = make_dag(
        {
            0: [
                _scan(1, "/data/left", 0),
                _scan(2, "/data/right", 0),
                node(
                    3,
                    NodeType.SortMergeJoin,
                    children=(1, 2),
                    metrics=_rows(0),
                    detail=_join_detail(left_keys=["a"], right_keys=["b"]),
                ),
            ]
        }
    )
    assert find_row_count_explosions(make_dfs(dag)).height == 0


def test_zero_output_rows_is_measured_not_missing():
    dag = make_dag({0: [_scan(1, "/data/left", 0)]})
    summary = to_plan_summary(make_dfs(dag), "synthetic")
    metric = summary["queries"][0]["nodes"][0]["metrics"][0]
    assert metric["canonical"] == "output_rows"
    assert metric["value"] == 0


def test_reused_exchange_input_is_unresolved_not_invented():
    dag = make_dag(
        {
            0: [
                _scan(1, "/data/left", 100),
                node(2, NodeType.ReusedExchange, children=()),
                node(
                    3,
                    NodeType.SortMergeJoin,
                    children=(1, 2),
                    metrics=_rows(500),
                    detail=_join_detail(left_keys=["a"], right_keys=["b"]),
                ),
            ]
        }
    )
    result = find_row_count_explosions(make_dfs(dag))
    row = result.row(0, named=True)
    assert row["left_input_rows"] == 100
    assert row["right_input_rows"] is None
    assert row["inputs_resolved"] is False

    findings = [
        f
        for f in analyze_dfs(make_dfs(dag)).findings
        if f.rule_id == "join_row_expansion"
    ]
    assert len(findings) == 1
    assert findings[0].severity == "warning"
    assert findings[0].confidence == "medium"


def test_repeated_scan_within_one_query_is_reported():
    dag = make_dag(
        {
            0: [
                _scan(1, "/data/sales", 100),
                _scan(2, "/data/sales", 100),
            ]
        }
    )
    result = find_repeated_scans(make_dfs(dag))
    assert result.height == 1
    row = result.row(0, named=True)
    assert row["scan_count"] == 2
    assert row["distinct_queries"] == 1
    assert row["repeats_within_query"] is True


def test_repeated_scan_tracks_differing_projections():
    dag = make_dag(
        {
            0: [_scan(1, "/data/sales", 100, schema="struct<a:int>")],
            1: [_scan(1, "/data/sales", 100, schema="struct<a:int,b:int>")],
        }
    )
    row = find_repeated_scans(make_dfs(dag)).row(0, named=True)
    assert row["distinct_read_schemas"] == 2
    assert row["repeats_within_query"] is False
    finding = next(
        f for f in analyze_dfs(make_dfs(dag)).findings if f.rule_id == "repeated_scan"
    )
    assert finding.confidence == "medium"


def test_incomplete_scan_detail_is_skipped_not_guessed():
    dag = make_dag(
        {
            0: [
                node(1, NodeType.Scan, metrics=_rows(100)),
                node(2, NodeType.Scan, metrics=_rows(100), detail={"output": ["a"]}),
            ]
        }
    )
    dfs = make_dfs(dag)
    assert find_repeated_scans(dfs).height == 0
    scans = find_largest_scans(dfs)
    assert scans["paths"].to_list() == [[], []]


def test_largest_scans_without_size_metrics_returns_nothing():
    dag = make_dag({0: [node(1, NodeType.Scan, detail={"output": ["a"]})]})
    assert find_largest_scans(make_dfs(dag)).height == 0


def test_largest_scans_falls_back_to_operator_metrics():
    dag = make_dag(
        {
            0: [
                node(
                    1,
                    NodeType.Scan,
                    metrics=[
                        accum("number of output rows", 10),
                        accum("size of files read", 2048, "size"),
                    ],
                    detail={"output": ["a"]},
                )
            ]
        }
    )
    row = find_largest_scans(make_dfs(dag)).row(0, named=True)
    assert row["bytes_read"] == 2048
    assert row["bytes_source"] == "operator_metric"


def test_largest_scans_ranks_by_rows_when_no_bytes_exist():
    dag = make_dag(
        {
            0: [
                node(1, NodeType.Scan, metrics=_rows(10), detail={"output": ["a"]}),
                node(2, NodeType.Scan, metrics=_rows(90), detail={"output": ["a"]}),
            ]
        }
    )
    result = find_largest_scans(make_dfs(dag))
    assert result["node_id"].to_list() == [2, 1]
    assert result["bytes_read"].to_list() == [None, None]


def test_plan_spill_is_found_without_task_rows():
    dag = make_dag(
        {
            0: [
                node(
                    1,
                    NodeType.HashAggregate,
                    metrics=[accum("numBytesSpilled", 1024, "size")],
                )
            ]
        }
    )
    dfs = make_dfs(dag)
    assert dfs.combined.height == 0
    operator_spill = find_operator_spill(dfs)
    assert operator_spill["spill_bytes"].to_list() == [1024]

    report = analyze_dfs(dfs)
    spill_findings = [f for f in report.findings if f.rule_id == "spill"]
    assert len(spill_findings) == 1
    assert spill_findings[0].stage_id is None
    assert dfs.combined.height == 0


def test_task_totals_are_null_not_zero_without_tasks():
    dag = make_dag({0: [_scan(1, "/data/left", 100)]})
    totals = to_plan_summary(make_dfs(dag), "synthetic")["totals"]
    assert set(totals.values()) == {None}


def test_rules_needing_tasks_report_unsupported():
    dag = make_dag({0: [_scan(1, "/data/left", 100)]})
    statuses = {
        assessment.rule_id: assessment
        for assessment in analyze_dfs(make_dfs(dag)).assessments
    }
    for rule_id in ("task_straggler", "gc_overhead"):
        assert statuses[rule_id].status == "unsupported"
        assert statuses[rule_id].reason


def test_scan_efficiency_is_insufficient_without_row_counts():
    dag = make_dag({0: [node(1, NodeType.Scan, detail={"output": ["a"]})]})
    statuses = {
        assessment.rule_id: assessment
        for assessment in analyze_dfs(make_dfs(dag)).assessments
    }
    assert statuses["scan_efficiency"].status == "insufficient_data"


def test_scan_efficiency_uses_photon_scanned_rows():
    dag = make_dag(
        {
            0: [
                node(
                    1,
                    NodeType.Scan,
                    metrics=[
                        accum("numRowsScanned", 1_000_000),
                        accum("numOutputRows", 1_000),
                    ],
                    detail={
                        "location": {
                            "location_type": "InMemoryFileIndex",
                            "location": ["/data/events"],
                        }
                    },
                )
            ]
        }
    )
    result = find_inefficient_scans(make_dfs(dag))
    assert result.height == 1
    assert result["basis"][0] == "scanned_vs_output_rows"
    assert result["retained_fraction"][0] == 0.001


def test_scan_efficiency_uses_filter_above_scan():
    dag = make_dag(
        {
            0: [
                _scan(1, "/data/events", 1_000_000),
                node(2, NodeType.Filter, children=(1,), metrics=_rows(500)),
            ]
        }
    )
    result = find_inefficient_scans(make_dfs(dag))
    assert result["basis"].to_list() == ["filter_above_scan"]
    assert result["filter_node_id"].to_list() == [2]


def test_selective_scan_below_threshold_is_not_reported():
    dag = make_dag(
        {
            0: [
                _scan(1, "/data/events", 1_000_000),
                node(2, NodeType.Filter, children=(1,), metrics=_rows(900_000)),
            ]
        }
    )
    assert find_inefficient_scans(make_dfs(dag)).height == 0


def test_aqe_rule_is_unsupported_without_markers():
    dag = make_dag({0: [_scan(1, "/data/left", 100)]})
    statuses = {
        assessment.rule_id: assessment
        for assessment in analyze_dfs(make_dfs(dag)).assessments
    }
    assert statuses["aqe_plan_change"].status == "unsupported"


def test_aqe_skew_split_is_reported():
    dag = make_dag(
        {
            0: [
                node(
                    1,
                    NodeType.AQEShuffleRead,
                    metrics=[
                        accum("number of skewed partitions", 3),
                        accum("number of partitions", 12),
                    ],
                    detail={"input": ["a"], "arguments": "coalesced"},
                )
            ]
        }
    )
    findings = [
        f for f in analyze_dfs(make_dfs(dag)).findings if f.rule_id == "aqe_plan_change"
    ]
    assert len(findings) == 1
    assert findings[0].category == "AQE Skew Split"


def test_straggler_without_size_evidence_is_not_called_skew():
    dag = make_dag({0: [_scan(1, "/data/left", 100)]})
    combined = make_combined(
        [{"task_id": i, "task_duration_seconds": 1.0} for i in range(9)]
        + [{"task_id": 9, "task_duration_seconds": 30.0}]
    )
    findings = [
        f
        for f in analyze_dfs(make_dfs(dag, combined)).findings
        if f.rule_id == "task_straggler"
    ]
    assert len(findings) == 1
    assert findings[0].category == "Task Straggler"
    assert findings[0].confidence == "medium"


def test_straggler_with_size_evidence_is_called_skew():
    dag = make_dag({0: [_scan(1, "/data/left", 100)]})
    combined = make_combined(
        [
            {"task_id": i, "task_duration_seconds": 1.0, "bytes_read": 1_000}
            for i in range(9)
        ]
        + [{"task_id": 9, "task_duration_seconds": 30.0, "bytes_read": 100_000}]
    )
    findings = [
        f
        for f in analyze_dfs(make_dfs(dag, combined)).findings
        if f.rule_id == "task_straggler"
    ]
    assert findings[0].category == "Data Skew"
    assert findings[0].confidence == "high"


def test_gc_overhead_ignores_trivially_short_stages():
    dag = make_dag({0: [_scan(1, "/data/left", 100)]})
    short = make_combined(
        [{"executor_run_time_seconds": 0.01, "jvm_gc_time_seconds": 0.009}]
    )
    assert find_gc_overhead(make_dfs(dag, short)).height == 0

    long = make_combined(
        [{"executor_run_time_seconds": 100.0, "jvm_gc_time_seconds": 30.0}]
    )
    result = find_gc_overhead(make_dfs(dag, long))
    assert result["gc_ratio"].to_list() == [0.3]


def test_memory_and_disk_spill_are_reported_separately():
    dag = make_dag({0: [_scan(1, "/data/left", 100)]})
    combined = make_combined(
        [{"memory_bytes_spilled": 2048, "disk_bytes_spilled": 1024}]
    )
    finding = next(
        f for f in analyze_dfs(make_dfs(dag, combined)).findings if f.rule_id == "spill"
    )
    evidence = {e.name: e.value for e in finding.evidence}
    assert evidence["memory_bytes_spilled"] == 2048
    assert evidence["disk_bytes_spilled"] == 1024
    assert 3072 not in evidence.values()


def test_shuffle_rule_reports_sides_separately():
    dag = make_dag({0: [_scan(1, "/data/left", 100)]})
    combined = make_combined(
        [{"shuffle_bytes_written": 2 * 2**30, "shuffle_bytes_read": 2 * 2**30}]
    )
    finding = next(
        f
        for f in analyze_dfs(make_dfs(dag, combined)).findings
        if f.rule_id == "shuffle_volume"
    )
    evidence = {e.name: e.value for e in finding.evidence}
    assert set(evidence) == {"shuffle_write_bytes", "shuffle_read_bytes"}


# ---------------------------------------------------------------------------
# export shape
# ---------------------------------------------------------------------------


def test_summary_metrics_are_numeric_with_units(dfs_nested):
    summary = to_plan_summary(dfs_nested, "nested_final_plans")
    metrics = [m for q in summary["queries"] for n in q["nodes"] for m in n["metrics"]]
    assert metrics
    for metric in metrics:
        assert isinstance(metric["value"], int | float)
        assert metric["unit"]
        assert "aggregation" in metric and "scope" in metric


def test_summary_round_trips_through_json(dfs_nested):
    summary = to_plan_summary(dfs_nested, "nested_final_plans")
    restored = json.loads(json.dumps(summary, default=str))
    assert (
        restored["queries"][0]["nodes"][0]["metrics"]
        == (summary["queries"][0]["nodes"][0]["metrics"])
    )


def test_summary_carries_schema_version_and_coverage(dfs_nested):
    summary = to_plan_summary(dfs_nested, "nested_final_plans")
    assert summary["schema_version"]
    assert summary["coverage"]["task_metrics"]["status"] == "available"
    assert summary["total_units"]["bytes_read"] == "bytes"


def test_summary_compact_mode_reports_omitted_nodes(dfs_complex):
    summary = to_plan_summary(dfs_complex, "complex", compact=True, top_n=3)
    query = summary["queries"][0]
    assert len(query["nodes"]) == 3
    assert query["omitted_node_count"] == query["node_count"] - 3
    assert "node_name" not in query["nodes"][0]


def test_summary_redaction_hides_paths(dfs_nested):
    summary = to_plan_summary(dfs_nested, "nested_final_plans", redact=True)
    paths = [
        path
        for q in summary["queries"]
        for n in q["nodes"]
        for path in (n.get("details", {}) or {}).get("paths", [])
    ]
    assert paths
    assert all(path.startswith("path:") for path in paths)
    assert all("parquet" not in path for path in paths)


def test_analysis_export_is_json_serializable(dfs_complex):
    export = to_analysis_export(dfs_complex, "complex")
    json.dumps(export)
    assert export["schema_version"]
    assert {"rule_id", "status"} <= set(export["assessments"][0])


def test_get_coverage_notes_explains_unevaluated_rules():
    dag = make_dag({0: [_scan(1, "/data/left", 100)]})
    notes = get_coverage_notes(make_dfs(dag))
    assert notes
    assert all(note["reason"] for note in notes)
    assert {"task_straggler", "gc_overhead"} <= {note["rule_id"] for note in notes}


def test_findings_carry_evidence_and_caveats(dfs_complex):
    report = analyze_dfs(dfs_complex, "complex")
    assert report.findings
    for finding in report.findings:
        assert finding.rule_id
        assert finding.evidence
        assert finding.caveat
        assert finding.next_investigation


def test_every_rule_reports_an_assessment(dfs_complex):
    report = analyze_dfs(dfs_complex, "complex")
    assert len(report.assessments) == len(RULES)
    assert len({a.rule_id for a in report.assessments}) == len(RULES)


# ---------------------------------------------------------------------------
# Spark Connect / Photon capture (real sanitized serverless recording)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def dfs_photon() -> ParsedLogDataFrames:
    fixture = json.loads(
        (
            Path(__file__).parent / "data" / "connect" / "photon_join_execution.json"
        ).read_text()
    )
    cap = SparkConnectCapture.from_plan_metrics(
        fixture["executions"], log_name="fixture"
    )
    assert cap.dfs is not None
    return cap.dfs


def test_photon_metrics_normalize_to_canonical_names(dfs_photon):
    join = dfs_photon.dag.filter(pl.col("node_type") == NodeType.BroadcastHashJoin).row(
        0, named=True
    )
    metrics = canonical_metrics(join["accumulator_totals"], SOURCE_CONNECT_PLAN)
    assert metrics["output_rows"].value == 2000
    assert metrics["peak_memory_bytes"].value == 2101661
    assert metrics["cumulative_operator_time"].aggregation == "cumulative"


def test_unmapped_photon_metrics_are_preserved(dfs_photon):
    summary = to_plan_summary(dfs_photon, "fixture")
    metrics = [m for q in summary["queries"] for n in q["nodes"] for m in n["metrics"]]
    unmapped = [m for m in metrics if m["canonical"] is None]
    assert unmapped, "Photon emits metrics with no canonical mapping"
    assert all(m["name"] for m in unmapped)
    # numBytesRead means shuffle bytes on an exchange and file bytes on a scan,
    # so it must not be guessed onto scan_bytes.
    assert all(m["canonical"] != "scan_bytes" for m in metrics)


def test_connect_capture_reports_zero_output_rows_as_measured(dfs_photon):
    values = [
        metric_value(row["accumulator_totals"], "output_rows", SOURCE_CONNECT_PLAN)
        for row in dfs_photon.dag.filter(
            pl.col("node_type") == NodeType.ShuffleQueryStage
        ).to_dicts()
    ]
    # A shuffle query stage reporting 0 rows is a measurement, not missing data.
    assert 0 in values


def test_connect_capture_marks_task_rules_unsupported(dfs_photon):
    statuses = {
        assessment.rule_id: assessment
        for assessment in analyze_dfs(dfs_photon, "fixture").assessments
    }
    for rule_id in ("task_straggler", "gc_overhead", "shuffle_volume"):
        assert statuses[rule_id].status == "unsupported"
        assert statuses[rule_id].reason
    # Plan-based rules still run on a Connect capture.
    assert statuses["cartesian_join"].status == "evaluated"
    assert statuses["spill"].status == "evaluated"


def test_connect_capture_totals_are_null(dfs_photon):
    summary = to_plan_summary(dfs_photon, "fixture")
    assert set(summary["totals"].values()) == {None}
    assert summary["coverage"]["task_metrics"]["status"] == "unavailable"


def test_analysis_redaction_hides_literals_but_keeps_wording(dfs_loop_join):
    export = to_analysis_export(dfs_loop_join, "loop", redact=True)
    findings = [f for f in export["findings"] if f["rule_id"] == "repeated_scan"]
    assert findings
    observation = findings[0]["observation"]
    assert "parquet" not in observation
    assert "scanned" in observation
    path_evidence = next(e for e in findings[0]["evidence"] if e["name"] == "path")
    assert str(path_evidence["value"]).startswith("expr:")


def _connect_capture_with_unknown_operator(secret: str) -> ParsedLogDataFrames:
    """A Connect capture whose unmapped operator name carries a literal."""
    cap = SparkConnectCapture.from_plan_metrics(
        [
            {
                "action": "collect",
                "operation_id": "op-secret",
                "start": "2026-01-01T00:00:00+00:00",
                "elapsed_seconds": 1.0,
                "plan_metrics": [
                    {
                        "name": f"PhotonMysteryOp filter(ssn = '{secret}')",
                        "plan_id": 1,
                        "parent_plan_id": 2,
                        "metrics": [
                            {"name": "numOutputRows", "value": 10, "type": "sum"}
                        ],
                    },
                    {
                        "name": "PhotonResultStage",
                        "plan_id": 2,
                        "parent_plan_id": 2,
                        "metrics": [
                            {"name": "numOutputRows", "value": 10, "type": "sum"}
                        ],
                    },
                ],
            }
        ],
        log_name="secretive",
    )
    assert cap.dfs is not None
    return cap.dfs


def test_unknown_operator_name_survives_unredacted_export():
    secret = "123-45-6789"
    dfs = _connect_capture_with_unknown_operator(secret)
    summary = json.dumps(to_plan_summary(dfs, "secretive"))
    # Without redaction the raw operator name is deliberately preserved.
    assert secret in summary


def test_redacted_export_hides_unknown_operator_literals():
    secret = "123-45-6789"
    dfs = _connect_capture_with_unknown_operator(secret)

    summary = to_plan_summary(dfs, "secretive", redact=True)
    payload = json.dumps(summary)
    assert secret not in payload
    assert "PhotonMysteryOp" not in payload

    names = [n["node_name"] for q in summary["queries"] for n in q["nodes"]]
    unknown = [name for name in names if "Unknown" in name]
    assert unknown, "the unmapped operator should still be identifiable by type"
    assert all(name.startswith("[") for name in names)

    assert secret not in json.dumps(to_analysis_export(dfs, "secretive", redact=True))


def test_redacted_summary_keeps_known_node_names_intact(dfs_nested):
    summary = to_plan_summary(dfs_nested, "nested_final_plans", redact=True)
    plain = to_plan_summary(dfs_nested, "nested_final_plans")
    redacted_names = [n["node_name"] for q in summary["queries"] for n in q["nodes"]]
    plain_names = [n["node_name"] for q in plain["queries"] for n in q["nodes"]]
    # Classic node names are just "[id] NodeType" and carry no workload text.
    assert redacted_names == plain_names


def test_gc_rule_reports_insufficient_data_when_telemetry_is_missing():
    dag = make_dag({0: [_scan(1, "/data/left", 100)]})
    combined = make_combined(
        [
            {
                "task_id": 0,
                "executor_run_time_seconds": None,
                "jvm_gc_time_seconds": None,
            }
        ]
    )
    statuses = {
        assessment.rule_id: assessment
        for assessment in analyze_dfs(make_dfs(dag, combined)).assessments
    }
    gc = statuses["gc_overhead"]
    assert gc.status == "insufficient_data"
    assert "GC" in (gc.reason or "")
    # A null GC column must not be summed into a confident "0s of GC".
    assert find_gc_overhead(make_dfs(dag, combined)).height == 0


def test_gc_rule_counts_assessed_stages_not_just_findings():
    dag = make_dag({0: [_scan(1, "/data/left", 100)]})
    combined = make_combined(
        [
            {
                "stage_id": stage,
                "task_id": stage,
                "executor_run_time_seconds": 100.0,
                "jvm_gc_time_seconds": 0.1,
            }
            for stage in range(4)
        ]
    )
    assessment = next(
        a
        for a in analyze_dfs(make_dfs(dag, combined)).assessments
        if a.rule_id == "gc_overhead"
    )
    assert assessment.status == "evaluated"
    assert assessment.findings == 0
    assert assessment.entities_evaluated == 4


def test_task_rules_report_insufficient_data_when_counters_are_null():
    dag = make_dag({0: [_scan(1, "/data/left", 100)]})
    combined = make_combined(
        [
            {
                "task_id": 0,
                "task_duration_seconds": None,
                "memory_bytes_spilled": None,
                "disk_bytes_spilled": None,
                "shuffle_bytes_read": None,
                "shuffle_bytes_written": None,
            }
        ]
    )
    statuses = {
        assessment.rule_id: assessment
        for assessment in analyze_dfs(make_dfs(dag, combined)).assessments
    }
    for rule_id in ("spill", "shuffle_volume", "task_straggler"):
        assert statuses[rule_id].status == "insufficient_data", rule_id
        assert statuses[rule_id].reason


# ---------------------------------------------------------------------------
# integer precision, end to end through ingestion
# ---------------------------------------------------------------------------

_HUGE_COUNT = 2**53 + 1


def test_event_log_ingestion_preserves_counts_above_2_53(tmp_path):
    """Parse → clean → summary → JSON must not round a large row count.

    The float64 ``value`` column cannot hold this number, so this fails unless
    the exact integer is carried through ingestion.
    """
    source = DATA_DIR / "nested_loop_join"
    patched = source.read_text().replace(
        '"Name":"number of output rows","Update":"4096","Value":"4096"',
        f'"Name":"number of output rows","Update":"{_HUGE_COUNT}",'
        f'"Value":"{_HUGE_COUNT}"',
    )
    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    (log_dir / "nested_loop_join").write_text(patched)

    dfs = get_parsed_metrics(
        log_dir=str(log_dir),
        log_file="nested_loop_join",
        out_dir=None,
        out_format=None,
        verbose=False,
    )
    summary = json.loads(
        json.dumps(to_plan_summary(dfs, "nested_loop_join"), default=str)
    )
    values = [
        metric["value"]
        for query in summary["queries"]
        for node in query["nodes"]
        for metric in node["metrics"]
        if metric["canonical"] == "output_rows"
    ]
    assert _HUGE_COUNT in values
    assert all(value != _HUGE_COUNT - 1 for value in values)


def test_connect_ingestion_preserves_counts_above_2_53():
    cap = SparkConnectCapture.from_plan_metrics(
        [
            {
                "action": "collect",
                "operation_id": "op-huge",
                "start": "2026-01-01T00:00:00+00:00",
                "elapsed_seconds": 1.0,
                "plan_metrics": [
                    {
                        "name": "PhotonScan parquet catalog.schema.events",
                        "plan_id": 1,
                        "parent_plan_id": 1,
                        "metrics": [
                            {
                                "name": "numOutputRows",
                                "value": _HUGE_COUNT,
                                "type": "sum",
                            }
                        ],
                    }
                ],
            }
        ],
        log_name="huge",
    )
    assert cap.dfs is not None
    summary = json.loads(json.dumps(to_plan_summary(cap.dfs, "huge"), default=str))
    metric = summary["queries"][0]["nodes"][0]["metrics"][0]
    assert metric["canonical"] == "output_rows"
    assert metric["value"] == _HUGE_COUNT

    node = cap.dfs.dag.row(0, named=True)
    assert (
        metric_value(node["accumulator_totals"], "output_rows", SOURCE_CONNECT_PLAN)
        == _HUGE_COUNT
    )


def test_rescaled_timings_have_no_exact_integer():
    cap = SparkConnectCapture.from_plan_metrics(
        [
            {
                "action": "collect",
                "operation_id": "op-ns",
                "start": "2026-01-01T00:00:00+00:00",
                "elapsed_seconds": 1.0,
                "plan_metrics": [
                    {
                        "name": "PhotonResultStage",
                        "plan_id": 1,
                        "parent_plan_id": 1,
                        "metrics": [
                            {
                                "name": "cumulTime",
                                "value": 1_500_000,
                                "type": "nsTiming",
                            }
                        ],
                    }
                ],
            }
        ],
        log_name="ns",
    )
    assert cap.dfs is not None
    (metric,) = cap.dfs.dag.row(0, named=True)["accumulator_totals"]
    # 1.5 ms: an integer nanosecond count is not an integer millisecond count.
    assert metric["value"] == 1.5
    assert metric["value_exact"] is None
