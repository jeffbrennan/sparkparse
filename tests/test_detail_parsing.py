import json
from pathlib import Path

import pytest

from sparkparse.models import (
    ExchangeDetail,
    FilterDetail,
    HashAggregateDetail,
    SortDetail,
    WindowDetail,
    WindowGroupLimitDetail,
    str_to_list,
)
from sparkparse.parse import parse_physical_plan


def test_details_parse_correctly():
    base_path = Path(__file__).parents[0] / "data"
    input_path = base_path / "test_detail_parsing" / "input.json"
    expected_path = base_path / "test_detail_parsing" / "expected.json"

    with input_path.open("r") as f:
        input_data = json.load(f)

    result = parse_physical_plan(input_data).details

    with expected_path.open("r") as f:
        expected_json = json.load(f)

    assert json.loads(result.model_dump_json()) == expected_json


def test_filter_parsing():
    input_dict = {
        "Input": "[v5#68, id2#85, rn#132]",
        "Condition": "(rn#132 > 42)",
    }
    expected = {
        "input": ["v5#68", "id2#85", "rn#132"],
        "condition": [
            {"condition": "AND", "col": "rn#132", "operator": ">", "value": "42"}
        ],
    }

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
