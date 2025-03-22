import json
from pathlib import Path

from sparkparse.parse import parse_physical_plan, parse_spark_ui_tree


def get_paths(
    test_name: str, input_file_type: str, expected_file_type: str
) -> tuple[Path, Path]:
    base_path = Path(__file__).parents[0] / "data" / "test_tree_parsing"

    input_path = base_path / f"{test_name}_input.{input_file_type}"
    expected_path = base_path / f"{test_name}_expected.{expected_file_type}"
    return input_path, expected_path


def test_plan_parses_correctly():
    input_path, expected_path = get_paths("test_plan_parses_correctly", "json", "json")

    with input_path.open("r") as f:
        input_data = json.load(f)

    result = parse_physical_plan(input_data)

    # with expected_path.open("w") as f:
    #     json.dump(json.loads(result.model_dump_json()), f, indent=2)

    with expected_path.open("r") as f:
        expected_json = json.load(f)

    assert json.loads(result.model_dump_json()) == expected_json


def test_tree_parses_correctly():
    input_path, expected_path = get_paths("test_tree_parses_correctly", "txt", "json")

    with input_path.open("r") as f:
        spark_ui_tree = f.read()

    with expected_path.open("r") as f:
        expected = json.load(f)

    results = parse_spark_ui_tree(spark_ui_tree)
    results_formatted = {
        str(k): json.loads(v.model_dump_json()) for k, v in results.items()
    }

    assert results_formatted == expected


def test_tree_parses_correctly2():
    input_path, expected_path = get_paths("test_tree_parses_correctly2", "txt", "json")
    with input_path.open("r") as f:
        spark_ui_tree = f.read()

    with expected_path.open("r") as f:
        expected = json.load(f)

    results = parse_spark_ui_tree(spark_ui_tree)
    results_formatted = {
        str(k): json.loads(v.model_dump_json()) for k, v in results.items()
    }

    assert results_formatted == expected
