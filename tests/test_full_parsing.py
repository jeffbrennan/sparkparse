import json
from pathlib import Path

from sparkparse.parse import get_parsed_metrics


def test_full_parsing_nested_plan():
    base_path = Path(__file__).parents[0] / "data"
    expected_path = base_path / "test_full_parsing" / "expected_nested_final_plans.json"

    metrics = get_parsed_metrics(
        log_dir="tests/data/full_logs",
        log_file="nested_final_plans",
        out_dir=None,
        out_format=None,
    )

    all_results = []
    all_results.extend(metrics.dag.to_dicts())
    all_results.extend(metrics.combined.drop("parsed_log_name").to_dicts())

    result_json = json.loads(json.dumps(all_results, indent=2))

    # with expected_path.open("w") as f:
    #     json.dump(result_json, f, indent=2)

    with expected_path.open("r") as f:
        expected_json = json.load(f)

    assert result_json == expected_json


def test_full_parsing_broadcast_nested_loop_join():
    base_path = Path(__file__).parents[0] / "data"
    expected_path = base_path / "test_full_parsing" / "expected_nested_loop_join.json"

    metrics = get_parsed_metrics(
        log_dir="tests/data/full_logs",
        log_file="nested_loop_join",
        out_dir=None,
        out_format=None,
    )

    all_results = []
    all_results.extend(metrics.dag.to_dicts())
    all_results.extend(metrics.combined.drop("parsed_log_name").to_dicts())

    result_json = json.loads(json.dumps(all_results, indent=2))

    # with expected_path.open("w") as f:
    #     json.dump(result_json, f, indent=2)

    with expected_path.open("r") as f:
        expected_json = json.load(f)

    assert result_json == expected_json
