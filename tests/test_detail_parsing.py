import json
from pathlib import Path
import pytest
from sparkparse.models import str_to_list
from sparkparse.parse import parse_physical_plan


def test_details_parse_correctly():
    base_path = Path(__file__).parents[0] / "data"
    input_path = base_path / "test_detail_parsing" / "input.json"
    expected_path = base_path / "test_detail_parsing" / "expected.json"

    with input_path.open("r") as f:
        input_data = json.load(f)

    result = parse_physical_plan(input_data)
    result_json = result.details.model_dump()

    with expected_path.open("r") as f:
        expected_json = json.load(f)

    assert result_json == expected_json


def test_str_to_list_non_string_input():
    with pytest.raises(TypeError):
        str_to_list(123)


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
