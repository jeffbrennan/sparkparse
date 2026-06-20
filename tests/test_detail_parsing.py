import json
from pathlib import Path

import pytest

from sparkparse.models import (
    AppendDataDetail,
    ArrowEvalPythonDetail,
    BatchEvalPythonDetail,
    BatchScanDetail,
    CartesianProductDetail,
    ExchangeDetail,
    ExpandDetail,
    FilterDetail,
    FlatMapCoGroupsInPandasDetail,
    FlatMapGroupsInPandasDetail,
    HashAggregateDetail,
    MapInArrowDetail,
    MapInPandasDetail,
    NodeType,
    ObjectHashAggregateDetail,
    OverwriteByExpressionDetail,
    OverwritePartitionsDynamicDetail,
    RangeDetail,
    RepartitionByExpressionDetail,
    ReusedSubqueryExecDetail,
    SampleDetail,
    SortAggregateDetail,
    SortDetail,
    SubqueryBroadcastDetail,
    SubqueryExecDetail,
    TakeOrderedAndProjectDetail,
    WindowDetail,
    WindowGroupLimitDetail,
    WriteToDataSourceV2Detail,
    str_to_list,
)
from sparkparse.parse import parse_physical_plan


def test_details_parse_correctly():
    base_path = Path(__file__).parents[0] / "data"
    input_path = base_path / "test_detail_parsing" / "input.json"
    expected_path = base_path / "test_detail_parsing" / "expected.json"

    with input_path.open("r") as f:
        input_data = json.load(f)

    results = [i.details for i in parse_physical_plan(input_data).nodes]

    result_dicts = []
    for result in results:
        if result is not None:
            result_dicts.append(json.loads(result))

    result_dicts = sorted(result_dicts, key=lambda x: x["node_id"])

    result_json = json.loads(json.dumps(result_dicts))

    # with expected_path.open("w") as f:
    #     json.dump(result_json, f, indent=2)

    with expected_path.open("r") as f:
        expected_json = json.load(f)

    assert result_json == expected_json


def test_filter_parsing():
    input_dict = {
        "Input": "[v5#68, id2#85, rn#132]",
        "Condition": "(rn#132 > 42)",
    }
    expected = {"input": ["v5#68", "id2#85", "rn#132"], "condition": "(rn#132 > 42)"}

    parsed = FilterDetail.model_validate(input_dict)
    assert json.loads(parsed.model_dump_json()) == expected


def test_exchange_parsing():
    input_dict = {
        "Input": "[id1_2_3#93, id1#84, id2#85, id3#86]",
        "Arguments": "hashpartitioning(id1_2_3#93, id1#84, id2#85, id3#86, 200), ENSURE_REQUIREMENTS, [plan_id=183]",
    }

    parsed = ExchangeDetail.model_validate(input_dict)
    expected = {
        "input": ["id1_2_3#93", "id1#84", "id2#85", "id3#86"],
        "arguments": {
            "partition_cols": ["id1_2_3#93", "id1#84", "id2#85", "id3#86"],
            "n_partitions": 200,
            "exchange_type": "ENSURE_REQUIREMENTS",
            "plan_identifier": 183,
        },
    }

    parsed_json = json.loads(parsed.model_dump_json())
    assert parsed_json == expected


def test_sort_parsing():
    input_dict = {
        "Input": "[id1_2_3#18, v5#68]",
        "Arguments": "[id1_2_3#18 ASC NULLS FIRST], false, 0",
    }

    expected = {
        "input": ["id1_2_3#18", "v5#68"],
        "arguments": {
            "cols": [{"name": "id1_2_3#18", "asc": True, "nulls_first": True}],
            "global_sort": False,
            "sort_order": 0,
        },
    }

    parsed = SortDetail.model_validate(input_dict)
    assert json.loads(parsed.model_dump_json()) == expected


def test_sort_parsing2():
    input_dict = {
        "Input": "[v5#68, id1#84, id2#85]",
        "Arguments": "[id1#84 ASC NULLS FIRST, v5#68 DESC NULLS LAST], false, 0",
    }
    expected = {
        "input": ["v5#68", "id1#84", "id2#85"],
        "arguments": {
            "cols": [
                {"name": "id1#84", "asc": True, "nulls_first": True},
                {"name": "v5#68", "asc": False, "nulls_first": False},
            ],
            "global_sort": False,
            "sort_order": 0,
        },
    }

    parsed = SortDetail.model_validate(input_dict)
    assert json.loads(parsed.model_dump_json()) == expected


def test_window_parsing():
    input_dict = {
        "Input": "[v5#68, id2#85]",
        "Arguments": "[row_number() windowspecdefinition(id2#85, v5#68 DESC NULLS LAST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS rn#132], [id2#85], [v5#68 DESC NULLS LAST]",
    }
    expected = {
        "input": ["v5#68", "id2#85"],
        "arguments": {
            "window_function": {"function": "row_number", "col": None},
            "window_specification": {
                "partition_cols": ["id2#85"],
                "order_cols": [{"name": "v5#68", "asc": False, "nulls_first": False}],
                "window_frame": "specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$()) AS rn#132",
            },
        },
    }

    parsed = WindowDetail.model_validate(input_dict)
    assert json.loads(parsed.model_dump_json()) == expected


def test_window_group_limit_parsing():
    input_dict = {
        "Input": "[v5#68, id1#84, id2#85]",
        "Arguments": "[id1#84], [v5#68 DESC NULLS LAST], rank(v5#68), 41, Partial",
    }
    expected = {
        "input": ["v5#68", "id1#84", "id2#85"],
        "arguments": {
            "partition_cols": ["id1#84"],
            "order_cols": [{"name": "v5#68", "asc": False, "nulls_first": False}],
            "window_function": {"function": "rank", "col": "v5#68"},
            "limit": 41,
            "processing_stage": "Partial",
        },
    }

    parsed = WindowGroupLimitDetail.model_validate(input_dict)
    assert json.loads(parsed.model_dump_json()) == expected


def test_hash_aggregate_parsing():
    input_dict = {
        "Input": "[id1_2_3#18, sum#172]",
        "Keys": "[id1_2_3#18]",
        "Functions": "[sum(v5#42)]",
        "Aggregate Attributes": "[sum(v5#42)#67]",
        "Results": "[id1_2_3#18, sum(v5#42)#67 AS v5#68]",
    }
    expected = {
        "input": ["id1_2_3#18", "sum#172"],
        "keys": ["id1_2_3#18"],
        "functions": [{"function": "sum", "col": "v5#42"}],
        "aggregate_attributes": ["sum(v5#42)#67"],
        "results": ["id1_2_3#18", "sum(v5#42)#67 AS v5#68"],
    }

    parsed = HashAggregateDetail.model_validate(input_dict)
    assert json.loads(parsed.model_dump_json()) == expected


def test_take_ordered_and_project_parsing():
    input_dict = {
        "Input": "[id1#0, v3#99]",
        "Arguments": "10, [v3#99 ASC NULLS FIRST], [id1#0, v3#99]",
    }

    expected = {
        "input": ["id1#0", "v3#99"],
        "arguments": {
            "limit": 10,
            "cols": [{"name": "v3#99", "asc": True, "nulls_first": True}],
            "output": ["id1#0", "v3#99"],
        },
    }

    parsed = TakeOrderedAndProjectDetail.model_validate(input_dict)
    assert json.loads(parsed.model_dump_json()) == expected


def test_str_to_list_non_string_input():
    with pytest.raises(TypeError):
        str_to_list(123)


def test_str_to_list_mixed_as():
    input_str = "[concat_ws(~, id1#0, id2#1, id3#2) AS id1_2_3#18, id#1234, v#4321, CASE WHEN (v3#8 > 20.0) THEN (v3#8 * 3.0) ELSE v3#8 END AS v5#42, col3#9999]"

    expected = [
        "concat_ws(~, id1#0, id2#1, id3#2) AS id1_2_3#18",
        "id#1234",
        "v#4321",
        "CASE WHEN (v3#8 > 20.0) THEN (v3#8 * 3.0) ELSE v3#8 END AS v5#42",
        "col3#9999",
    ]

    assert str_to_list(input_str) == expected


def test_str_to_list_complex_project_output():
    project_output = "[concat_ws(~, id1#0, id2#1, id3#2) AS id1_2_3#18, CASE WHEN (v3#8 > 20.0) THEN (v3#8 * 3.0) ELSE v3#8 END AS v5#42]"

    expected = [
        "concat_ws(~, id1#0, id2#1, id3#2) AS id1_2_3#18",
        "CASE WHEN (v3#8 > 20.0) THEN (v3#8 * 3.0) ELSE v3#8 END AS v5#42",
    ]

    assert str_to_list(project_output) == expected


def test_str_to_list_empty_items():
    input_str = "[a, , c]"
    expected = ["a", "c"]
    assert str_to_list(input_str) == expected


def test_str_to_list_whitespace_only_items():
    input_str = "[a,   , c]"
    expected = ["a", "c"]
    assert str_to_list(input_str) == expected


def test_object_hash_aggregate_parsing():
    input_dict = {
        "Input": "[id1_2_3#18, sum#172]",
        "Keys": "[id1_2_3#18]",
        "Functions": "[sum(v5#42)]",
        "Aggregate Attributes": "[sum(v5#42)#67]",
        "Results": "[id1_2_3#18, sum(v5#42)#67 AS v5#68]",
    }
    expected = {
        "input": ["id1_2_3#18", "sum#172"],
        "keys": ["id1_2_3#18"],
        "functions": [{"function": "sum", "col": "v5#42"}],
        "aggregate_attributes": ["sum(v5#42)#67"],
        "results": ["id1_2_3#18", "sum(v5#42)#67 AS v5#68"],
    }

    parsed = ObjectHashAggregateDetail.model_validate(input_dict)
    assert json.loads(parsed.model_dump_json()) == expected


def test_sort_aggregate_parsing():
    input_dict = {
        "Input": "[id1_2_3#18, sum#172]",
        "Keys": "[id1_2_3#18]",
        "Functions": "[sum(v5#42)]",
        "Aggregate Attributes": "[sum(v5#42)#67]",
        "Results": "[id1_2_3#18, sum(v5#42)#67 AS v5#68]",
    }
    expected = {
        "input": ["id1_2_3#18", "sum#172"],
        "keys": ["id1_2_3#18"],
        "functions": [{"function": "sum", "col": "v5#42"}],
        "aggregate_attributes": ["sum(v5#42)#67"],
        "results": ["id1_2_3#18", "sum(v5#42)#67 AS v5#68"],
    }

    parsed = SortAggregateDetail.model_validate(input_dict)
    assert json.loads(parsed.model_dump_json()) == expected


def test_expand_parsing_list_format():
    input_dict = {
        "Input": "[id#0, v#1]",
        "Arguments": "[List(id#0, v#1, 0), List(id#0, null, 1)]",
        "Output": "[id#0, v#1, gid#2]",
    }
    expected = {
        "input": ["id#0", "v#1"],
        "projections": [["id#0", "v#1", "0"], ["id#0", "null", "1"]],
        "output": ["id#0", "v#1", "gid#2"],
    }

    parsed = ExpandDetail.model_validate(input_dict)
    assert json.loads(parsed.model_dump_json()) == expected


def test_expand_parsing_bracket_format():
    input_dict = {
        "Input": "[id#0, v#1]",
        "Arguments": "[[id#0, v#1, 0], [id#0, null, 1]]",
        "Output": "[id#0, v#1, gid#2]",
    }
    expected = {
        "input": ["id#0", "v#1"],
        "projections": [["id#0", "v#1", "0"], ["id#0", "null", "1"]],
        "output": ["id#0", "v#1", "gid#2"],
    }

    parsed = ExpandDetail.model_validate(input_dict)
    assert json.loads(parsed.model_dump_json()) == expected


def test_arrow_eval_python_parsing():
    input_dict = {
        "Input": "[id#0, v#1]",
        "Arguments": "[pythonUDF0(id#0), pythonUDF1(v#1)]",
    }
    expected = {
        "input": ["id#0", "v#1"],
        "udfs": ["pythonUDF0(id#0)", "pythonUDF1(v#1)"],
    }

    parsed = ArrowEvalPythonDetail.model_validate(input_dict)
    assert json.loads(parsed.model_dump_json()) == expected


def test_batch_eval_python_parsing():
    input_dict = {
        "Input": "[id#0, v#1]",
        "Arguments": "[pythonUDF0(id#0)]",
    }
    expected = {
        "input": ["id#0", "v#1"],
        "udfs": ["pythonUDF0(id#0)"],
    }

    parsed = BatchEvalPythonDetail.model_validate(input_dict)
    assert json.loads(parsed.model_dump_json()) == expected


def test_map_in_pandas_parsing():
    input_dict = {
        "Input": "[id#0, v#1]",
        "Arguments": "pythonUDF#0",
    }
    expected = {
        "input": ["id#0", "v#1"],
        "func": "pythonUDF#0",
    }

    parsed = MapInPandasDetail.model_validate(input_dict)
    assert json.loads(parsed.model_dump_json()) == expected


def test_map_in_arrow_parsing():
    input_dict = {
        "Input": "[id#0, v#1]",
        "Arguments": "pythonUDF#0",
    }
    expected = {
        "input": ["id#0", "v#1"],
        "func": "pythonUDF#0",
    }

    parsed = MapInArrowDetail.model_validate(input_dict)
    assert json.loads(parsed.model_dump_json()) == expected


def test_flat_map_groups_in_pandas_parsing():
    input_dict = {
        "Input": "[id#0, v#1]",
        "Arguments": "pythonUDF#0",
        "GroupingKeys": "[id#0]",
    }
    expected = {
        "input": ["id#0", "v#1"],
        "func": "pythonUDF#0",
        "grouping_keys": ["id#0"],
    }

    parsed = FlatMapGroupsInPandasDetail.model_validate(input_dict)
    assert json.loads(parsed.model_dump_json()) == expected


def test_flat_map_co_groups_in_pandas_parsing():
    input_dict = {
        "Input": "[id#0, v#1]",
        "Arguments": "pythonUDF#0",
    }
    expected = {
        "input": ["id#0", "v#1"],
        "left_output": None,
        "right_output": None,
        "func": "pythonUDF#0",
    }

    parsed = FlatMapCoGroupsInPandasDetail.model_validate(input_dict)
    assert json.loads(parsed.model_dump_json()) == expected


def test_batch_scan_parsing():
    input_dict = {
        "Output": "[id#0, v#1]",
        "Table": "my_table",
        "Filters": "[(id#0 > 10)]",
        "RuntimeFilters": "[]",
    }
    expected = {
        "output": ["id#0", "v#1"],
        "table": "my_table",
        "filters": ["(id#0 > 10)"],
        "runtime_filters": None,
    }

    parsed = BatchScanDetail.model_validate(input_dict)
    assert json.loads(parsed.model_dump_json()) == expected


def test_write_to_data_source_v2_parsing():
    input_dict = {
        "Table": "my_table",
        "WriteOptions": "[path=/tmp/out, format=parquet]",
    }
    expected = {
        "table": "my_table",
        "write_options": {"path": "/tmp/out", "format": "parquet"},
    }

    parsed = WriteToDataSourceV2Detail.model_validate(input_dict)
    assert json.loads(parsed.model_dump_json()) == expected


def test_append_data_parsing():
    input_dict = {
        "Table": "my_table",
        "Query": "[id#0, v#1]",
    }
    expected = {
        "table": "my_table",
        "query": "[id#0, v#1]",
    }

    parsed = AppendDataDetail.model_validate(input_dict)
    assert json.loads(parsed.model_dump_json()) == expected


def test_overwrite_by_expression_parsing():
    input_dict = {
        "Table": "my_table",
        "DeleteCondition": "(id#0 > 10)",
    }
    expected = {
        "table": "my_table",
        "delete_condition": "(id#0 > 10)",
    }

    parsed = OverwriteByExpressionDetail.model_validate(input_dict)
    assert json.loads(parsed.model_dump_json()) == expected


def test_overwrite_partitions_dynamic_parsing():
    input_dict = {
        "Table": "my_table",
    }
    expected = {
        "table": "my_table",
    }

    parsed = OverwritePartitionsDynamicDetail.model_validate(input_dict)
    assert json.loads(parsed.model_dump_json()) == expected


def test_subquery_exec_parsing():
    input_dict = {
        "Name": "scalar-subquery#1",
        "ChildPlanId": "42",
    }
    expected = {
        "name": "scalar-subquery#1",
        "child_plan_id": 42,
    }

    parsed = SubqueryExecDetail.model_validate(input_dict)
    assert json.loads(parsed.model_dump_json()) == expected


def test_reused_subquery_exec_parsing():
    input_dict = {
        "Output": "[id#0]",
        "reuses_node_id": 5,
    }
    expected = {
        "output": ["id#0"],
        "reuses_node_id": 5,
    }

    parsed = ReusedSubqueryExecDetail.model_validate(input_dict)
    assert json.loads(parsed.model_dump_json()) == expected


def test_subquery_broadcast_parsing():
    input_dict = {
        "Name": "dpp-subquery#1",
        "Index": "0",
        "BuildKeys": "[id#0]",
    }
    expected = {
        "name": "dpp-subquery#1",
        "index": 0,
        "build_keys": ["id#0"],
    }

    parsed = SubqueryBroadcastDetail.model_validate(input_dict)
    assert json.loads(parsed.model_dump_json()) == expected


def test_repartition_by_expression_parsing():
    input_dict = {
        "Input": "[id#0, v#1]",
        "PartitionExpressions": "[id#0]",
        "NumPartitions": "200",
    }
    expected = {
        "input": ["id#0", "v#1"],
        "partition_exprs": ["id#0"],
        "n_partitions": 200,
    }

    parsed = RepartitionByExpressionDetail.model_validate(input_dict)
    assert json.loads(parsed.model_dump_json()) == expected


def test_sample_parsing():
    input_dict = {
        "Input": "[id#0, v#1]",
        "LowerBound": "0.0",
        "UpperBound": "0.5",
        "WithReplacement": "false",
        "Seed": "42",
    }
    expected = {
        "input": ["id#0", "v#1"],
        "lower_bound": 0.0,
        "upper_bound": 0.5,
        "with_replacement": False,
        "seed": 42,
    }

    parsed = SampleDetail.model_validate(input_dict)
    assert json.loads(parsed.model_dump_json()) == expected


def test_range_parsing():
    input_dict = {
        "Output": "[id#0]",
        "Start": "0",
        "End": "100",
        "Step": "1",
        "NumPartitions": "200",
    }
    expected = {
        "output": ["id#0"],
        "start": 0,
        "end": 100,
        "step": 1,
        "n_partitions": 200,
    }

    parsed = RangeDetail.model_validate(input_dict)
    assert json.loads(parsed.model_dump_json()) == expected


def test_cartesian_product_parsing():
    input_dict = {
        "Input": "[id#0, v#1]",
        "JoinCondition": "(id#0 = id#1)",
    }
    expected = {
        "input": ["id#0", "v#1"],
        "join_type": None,
        "join_condition": "(id#0 = id#1)",
    }

    parsed = CartesianProductDetail.model_validate(input_dict)
    assert json.loads(parsed.model_dump_json()) == expected


def test_exchange_type_enum_values():
    from sparkparse.models import ExchangeType

    assert ExchangeType.ENSURE_REQUIREMENTS == "ENSURE_REQUIREMENTS"
    assert ExchangeType.REPARTITION_BY_COL == "REPARTITION_BY_COL"
    assert ExchangeType.REPARTITION_BY_NUM == "REPARTITION_BY_NUM"
    assert ExchangeType.REPARTITION == "REPARTITION"


def test_query_function_enum_values():
    from sparkparse.models import QueryFunction

    assert QueryFunction.COUNT == "count"
    assert QueryFunction.SHOW == "show"
    assert QueryFunction.COLLECT == "collect"
    assert QueryFunction.FIRST == "first"
    assert QueryFunction.HEAD == "head"
    assert QueryFunction.TAKE == "take"
    assert QueryFunction.FOREACH == "foreach"
    assert QueryFunction.FOREACH_PARTITION == "foreachPartition"
    assert QueryFunction.TO_LOCAL_ITERATOR == "toLocalIterator"


FIXTURE_DETAIL_MODELS: dict[str, type] = {
    "AppendData": AppendDataDetail,
    "ArrowEvalPython": ArrowEvalPythonDetail,
    "BatchEvalPython": BatchEvalPythonDetail,
    "BatchScan": BatchScanDetail,
    "CartesianProduct": CartesianProductDetail,
    "Expand": ExpandDetail,
    "FlatMapCoGroupsInPandas": FlatMapCoGroupsInPandasDetail,
    "FlatMapGroupsInPandas": FlatMapGroupsInPandasDetail,
    "MapInPandas": MapInPandasDetail,
    "ObjectHashAggregate": ObjectHashAggregateDetail,
    "OverwriteByExpression": OverwriteByExpressionDetail,
    "OverwritePartitionsDynamic": OverwritePartitionsDynamicDetail,
    "PythonMapInArrow": MapInArrowDetail,
    "Range": RangeDetail,
    "Sample": SampleDetail,
}


@pytest.mark.parametrize("fixture_name", sorted(FIXTURE_DETAIL_MODELS.keys()))
def test_real_fixture_parses(fixture_name: str) -> None:
    fixture_path = (
        Path(__file__).parents[0]
        / "data"
        / "test_detail_parsing"
        / f"{fixture_name}.json"
    )
    if not fixture_path.exists():
        pytest.skip(f"Fixture {fixture_name}.json not generated yet")

    with fixture_path.open("r") as f:
        event = json.load(f)

    plan = parse_physical_plan(event)
    target_node = next(
        (n for n in plan.nodes if n.node_type == NodeType(fixture_name)), None
    )
    assert target_node is not None, f"{fixture_name} node not found in parsed plan"

    detail_cls = FIXTURE_DETAIL_MODELS[fixture_name]
    assert target_node.details is not None
    detail_dict = json.loads(target_node.details)
    assert detail_dict["node_type"] == fixture_name
    assert "raw" not in detail_dict["detail"], (
        f"{fixture_name} details fell back to raw text"
    )
    detail_cls.model_validate(detail_dict["detail"])
