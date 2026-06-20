import json
from pathlib import Path

from sparkparse.models import NodeType, RawDetail
from sparkparse.parse import get_plan_details, parse_physical_plan, parse_spark_ui_tree


def test_unknown_node_type_parses_as_unknown():
    base_path = Path(__file__).parents[0] / "data" / "test_unknown_type_parsing"
    tree = (base_path / "unknown_type_plan.txt").read_text()

    node_map = parse_spark_ui_tree(tree)

    assert len(node_map) == 1
    assert node_map[1].node_type == NodeType.Unknown


def test_unknown_node_type_details_stored_as_raw():
    base_path = Path(__file__).parents[0] / "data" / "test_unknown_type_parsing"
    tree = (base_path / "unknown_type_plan.txt").read_text()
    node_map = parse_spark_ui_tree(tree)

    plan_lines = [
        "== Physical Plan ==",
        "+- == Final Plan ==",
        "   UnknownNode (1)",
        "+- == Initial Plan ==",
        "",
        "",
        "(1) UnknownNode",
        "Input [id#0]",
        "",
    ]
    tree_end = plan_lines.index("+- == Initial Plan ==")

    details = get_plan_details(plan_lines, tree_end, node_map)

    assert len(details.details) == 1
    detail = details.details[0]
    assert detail.node_id == 1
    assert detail.node_type == NodeType.Unknown
    assert isinstance(detail.detail, RawDetail)

    parsed_json = json.loads(detail.model_dump_json())
    assert parsed_json["node_type"] == "Unknown"
    assert parsed_json["detail"]["raw"].startswith("(1) UnknownNode")


def test_unknown_node_type_end_to_end():
    event = {
        "Event": "org.apache.spark.sql.execution.ui.SparkListenerSQLAdaptiveExecutionUpdate",
        "executionId": 0,
        "physicalPlanDescription": (
            "== Physical Plan ==\n"
            "+- == Final Plan ==\n"
            "   UnknownNode (1)\n"
            "+- == Initial Plan ==\n"
            "\n"
            "\n"
            "(1) UnknownNode\n"
            "Input [id#0]\n"
        ),
        "sparkPlanInfo": {
            "nodeName": "AdaptiveSparkPlan",
            "simpleString": "AdaptiveSparkPlan isFinalPlan=true",
            "children": [],
        },
    }

    plan = parse_physical_plan(event)

    assert len(plan.nodes) == 1
    node = plan.nodes[0]
    assert node.node_id == 1
    assert node.node_type == NodeType.Unknown
    assert node.details is not None

    detail = RawDetail.model_validate(json.loads(node.details)["detail"])
    assert "(1) UnknownNode" in detail.raw
