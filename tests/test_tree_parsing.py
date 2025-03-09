import json
from pathlib import Path
from sparkparse.models import NodeType, PhysicalPlanNode
from sparkparse.parse import parse_physical_plan, parse_spark_ui_tree


def test_plan_parses_correctly():
    base_path = Path(__file__).parents[0] / "data" / "test_tree_parsing"
    input_path = base_path / "test_plan_parses_correctly_input.json"
    expected_path = base_path / "test_plan_parses_correctly_expected.json"

    with input_path.open("r") as f:
        input_data = json.load(f)

    result = parse_physical_plan(input_data)

    result_json = result.model_dump()

    with expected_path.open("r") as f:
        expected_json = json.load(f)

    assert result_json == expected_json


def test_tree_parses_correctly():
    spark_ui_tree = """
   Execute InsertIntoHadoopFsRelationCommand (29)
   +- WriteFiles (28)
      +- TakeOrderedAndProject (27)
         +- * HashAggregate (26)
            +- AQEShuffleRead (25)
               +- ShuffleQueryStage (24), Statistics(sizeInBytes=31.3 KiB, rowCount=1.00E+3)
                  +- Exchange (23)
                     +- * HashAggregate (22)
                        +- * Project (21)
                           +- * BroadcastHashJoin LeftOuter BuildRight (20)
                              :- * ColumnarToRow (2)
                              :  +- Scan parquet  (1)
                              +- BroadcastQueryStage (19), Statistics(sizeInBytes=12.0 MiB, rowCount=1.00E+5)
                                 +- BroadcastExchange (18)
                                    +- * Filter (17)
                                       +- * GlobalLimit (16)
                                          +- ShuffleQueryStage (15), Statistics(sizeInBytes=68.7 MiB, rowCount=3.00E+6)
                                             +- Exchange (14)
                                                +- * LocalLimit (13)
                                                   +- Union (12)
                                                      :- * LocalLimit (5)
                                                      :  +- * ColumnarToRow (4)
                                                      :     +- Scan parquet  (3)
                                                      :- * LocalLimit (8)
                                                      :  +- * ColumnarToRow (7)
                                                      :     +- Scan parquet  (6)
                                                      +- * LocalLimit (11)
                                                         +- * ColumnarToRow (10)
                                                            +- Scan parquet  (9)
"""
    result = parse_spark_ui_tree(spark_ui_tree)

    expected_result = {
        29: PhysicalPlanNode(
            node_id=29,
            node_type=NodeType.InsertIntoHadoopFsRelationCommand,
            child_nodes=[],
            whole_stage_codegen_id=None,
        ),
        28: PhysicalPlanNode(
            node_id=28,
            node_type=NodeType.WriteFiles,
            child_nodes=[29],
            whole_stage_codegen_id=None,
        ),
        27: PhysicalPlanNode(
            node_id=27,
            node_type=NodeType.TakeOrderedAndProject,
            child_nodes=[28],
            whole_stage_codegen_id=None,
        ),
        26: PhysicalPlanNode(
            node_id=26,
            node_type=NodeType.HashAggregate,
            child_nodes=[27],
            whole_stage_codegen_id=None,
        ),
        25: PhysicalPlanNode(
            node_id=25,
            node_type=NodeType.AQEShuffleRead,
            child_nodes=[26],
            whole_stage_codegen_id=None,
        ),
        24: PhysicalPlanNode(
            node_id=24,
            node_type=NodeType.ShuffleQueryStage,
            child_nodes=[25],
            whole_stage_codegen_id=None,
        ),
        23: PhysicalPlanNode(
            node_id=23,
            node_type=NodeType.Exchange,
            child_nodes=[24],
            whole_stage_codegen_id=None,
        ),
        22: PhysicalPlanNode(
            node_id=22,
            node_type=NodeType.HashAggregate,
            child_nodes=[23],
            whole_stage_codegen_id=None,
        ),
        21: PhysicalPlanNode(
            node_id=21,
            node_type=NodeType.Project,
            child_nodes=[22],
            whole_stage_codegen_id=None,
        ),
        20: PhysicalPlanNode(
            node_id=20,
            node_type=NodeType.BuildRight,
            child_nodes=[21],
            whole_stage_codegen_id=None,
        ),
        2: PhysicalPlanNode(
            node_id=2,
            node_type=NodeType.ColumnarToRow,
            child_nodes=[20],
            whole_stage_codegen_id=None,
        ),
        1: PhysicalPlanNode(
            node_id=1,
            node_type=NodeType.Scan,
            child_nodes=[2],
            whole_stage_codegen_id=None,
        ),
        19: PhysicalPlanNode(
            node_id=19,
            node_type=NodeType.BroadcastQueryStage,
            child_nodes=[20],
            whole_stage_codegen_id=None,
        ),
        18: PhysicalPlanNode(
            node_id=18,
            node_type=NodeType.BroadcastExchange,
            child_nodes=[19],
            whole_stage_codegen_id=None,
        ),
        17: PhysicalPlanNode(
            node_id=17,
            node_type=NodeType.Filter,
            child_nodes=[18],
            whole_stage_codegen_id=None,
        ),
        16: PhysicalPlanNode(
            node_id=16,
            node_type=NodeType.GlobalLimit,
            child_nodes=[17],
            whole_stage_codegen_id=None,
        ),
        15: PhysicalPlanNode(
            node_id=15,
            node_type=NodeType.ShuffleQueryStage,
            child_nodes=[16],
            whole_stage_codegen_id=None,
        ),
        14: PhysicalPlanNode(
            node_id=14,
            node_type=NodeType.Exchange,
            child_nodes=[15],
            whole_stage_codegen_id=None,
        ),
        13: PhysicalPlanNode(
            node_id=13,
            node_type=NodeType.LocalLimit,
            child_nodes=[14],
            whole_stage_codegen_id=None,
        ),
        12: PhysicalPlanNode(
            node_id=12,
            node_type=NodeType.Union,
            child_nodes=[13],
            whole_stage_codegen_id=None,
        ),
        5: PhysicalPlanNode(
            node_id=5,
            node_type=NodeType.LocalLimit,
            child_nodes=[12],
            whole_stage_codegen_id=None,
        ),
        4: PhysicalPlanNode(
            node_id=4,
            node_type=NodeType.ColumnarToRow,
            child_nodes=[5],
            whole_stage_codegen_id=None,
        ),
        3: PhysicalPlanNode(
            node_id=3,
            node_type=NodeType.Scan,
            child_nodes=[4],
            whole_stage_codegen_id=None,
        ),
        8: PhysicalPlanNode(
            node_id=8,
            node_type=NodeType.LocalLimit,
            child_nodes=[12],
            whole_stage_codegen_id=None,
        ),
        7: PhysicalPlanNode(
            node_id=7,
            node_type=NodeType.ColumnarToRow,
            child_nodes=[8],
            whole_stage_codegen_id=None,
        ),
        6: PhysicalPlanNode(
            node_id=6,
            node_type=NodeType.Scan,
            child_nodes=[7],
            whole_stage_codegen_id=None,
        ),
        11: PhysicalPlanNode(
            node_id=11,
            node_type=NodeType.LocalLimit,
            child_nodes=[12],
            whole_stage_codegen_id=None,
        ),
        10: PhysicalPlanNode(
            node_id=10,
            node_type=NodeType.ColumnarToRow,
            child_nodes=[11],
            whole_stage_codegen_id=None,
        ),
        9: PhysicalPlanNode(
            node_id=9,
            node_type=NodeType.Scan,
            child_nodes=[10],
            whole_stage_codegen_id=None,
        ),
    }
    for node_id, node in result.items():
        assert node == expected_result[node_id], print(
            f"{node}\n!=\n{expected_result[node_id]}"
        )
