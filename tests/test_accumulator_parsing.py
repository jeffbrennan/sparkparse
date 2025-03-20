import json
from pathlib import Path

from sparkparse.parse import get_parsed_metrics


def test_accumulator_totals_parse_correctly():
    base_path = Path(__file__).parents[0] / "data"
    expected_path = base_path / "test_accumulator_parsing" / "expected.json"

    dag = get_parsed_metrics(
        log_dir="tests/data/full_logs",
        log_file="complex_transformation_medium",
        out_dir=None,
        out_format=None,
    ).dag

    result = (
        dag.select("query_id", "node_id", "node_type", "accumulator_totals")
        .explode(("accumulator_totals"))
        .unnest("accumulator_totals")
        .sort("query_id", "node_id")
    )

    result_json = result.to_dicts()

    # with expected_path.open("w") as f:
    #     json.dump(result_json, f, indent=4)

    with expected_path.open("r") as f:
        expected_json = json.load(f)

    assert result_json == expected_json
