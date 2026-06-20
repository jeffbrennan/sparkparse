import json
import shutil
import tempfile
from pathlib import Path

from pyspark.sql import SparkSession

from sparkparse.models import NodeType
from sparkparse.parse import parse_log


def build_spark(event_log_dir: str) -> SparkSession:
    builder = (
        SparkSession.builder.appName("sparkparse-spark4-fixture-generator")
        .master("local[2]")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.forceApply", "true")
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.rolling.enabled", "false")
        .config("spark.eventLog.compression.codec", "none")
        .config("spark.eventLog.dir", event_log_dir)
    )
    return builder.getOrCreate()


def find_event_log(event_log_dir: Path) -> Path:
    logs = sorted(event_log_dir.glob("*"))
    if not logs:
        raise FileNotFoundError("No event log found")
    return logs[-1]


def save_fixture(event: dict, target: NodeType, fixtures_dir: Path) -> Path:
    fixtures_dir.mkdir(parents=True, exist_ok=True)
    fixture_path = fixtures_dir / f"{target.value}.json"
    with fixture_path.open("w") as f:
        json.dump(event, f, indent=2)
    return fixture_path


def _raw_event_for_query(event_log: Path, target: NodeType) -> dict | None:
    with event_log.open("r") as f:
        events = [json.loads(line) for line in f if line.strip()]

    final_plan_events: dict[int, dict] = {}
    for event in events:
        event_name = event.get("Event", "")
        if not event_name.endswith("SparkListenerSQLAdaptiveExecutionUpdate"):
            continue
        plan_info = event.get("sparkPlanInfo", {})
        is_final = plan_info.get("simpleString", "").split("isFinalPlan=")[-1] == "true"
        if not is_final:
            continue
        query_id = event.get("executionId")
        if query_id is None:
            continue
        final_plan_events[query_id] = event

    try:
        parsed = parse_log(event_log)
        queries = parsed.queries
    except Exception as exc:
        print(f"[WARN] parse_log failed for {target.value}: {exc}; using raw events")
        queries = []

    for query in reversed(queries):
        if any(node.node_type == target for node in query.nodes):
            return final_plan_events.get(query.query_id)

    for event in reversed(events):
        event_name = event.get("Event", "")
        if not event_name.endswith("SparkListenerSQLExecutionStart"):
            continue
        desc = event.get("physicalPlanDescription", "")
        if target.value not in desc:
            continue
        return {
            "Event": "org.apache.spark.sql.execution.ui.SparkListenerSQLAdaptiveExecutionUpdate",
            "executionId": event.get("executionId", -1),
            "physicalPlanDescription": desc,
            "sparkPlanInfo": {
                "nodeName": "AdaptiveSparkPlan",
                "simpleString": "AdaptiveSparkPlan isFinalPlan=true",
                "children": [],
            },
        }
    return None


def generate_batch_eval_python_udtf(spark: SparkSession) -> NodeType:
    from pyspark.sql.functions import udtf

    @udtf(returnType="id: int, doubled: int")
    class MyUdtf:
        def eval(self, id: int):
            yield id, id * 2

    spark.udtf.register("my_udtf", MyUdtf)  # type: ignore[arg-type]
    df = spark.createDataFrame([(1,), (2,), (3,)], ["id"])
    df.createOrReplaceTempView("t")
    spark.sql("SELECT * FROM t, LATERAL my_udtf(id)").collect()
    return NodeType.BatchEvalPythonUDTF


def generate_arrow_eval_python_udtf(spark: SparkSession) -> NodeType:
    from pyspark.sql.functions import udtf

    @udtf(returnType="id: int, doubled: int", useArrow=True)
    class MyArrowUdtf:
        def eval(self, id: int):
            yield id, id * 2

    spark.udtf.register("my_arrow_udtf", MyArrowUdtf)  # type: ignore[arg-type]
    df = spark.createDataFrame([(1,), (2,), (3,)], ["id"])
    df.createOrReplaceTempView("t")
    spark.sql("SELECT * FROM t, LATERAL my_arrow_udtf(id)").collect()
    return NodeType.ArrowEvalPythonUDTF


def generate_flat_map_groups_in_arrow(spark: SparkSession) -> NodeType:
    df = spark.createDataFrame(
        [(1, "a", 1.0), (1, "b", 2.0), (2, "c", 3.0)], ["id", "name", "val"]
    )

    def process_batch(key, batch):
        import pyarrow.compute as pc

        result = pc.multiply(batch["val"], 2)  # type: ignore[attr-defined]
        return batch.append_column("result", result)

    result_schema = "id long, name string, val double, result double"
    df.groupBy("id").applyInArrow(process_batch, result_schema).collect()
    return NodeType.FlatMapGroupsInArrow


def generate_flat_map_co_groups_in_arrow(spark: SparkSession) -> NodeType:
    df1 = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "name"])
    df2 = spark.createDataFrame([(1, 10.0), (2, 20.0)], ["id", "score"])

    def process_cogroup(key, left, right):
        return left.join(right, "id")

    result_schema = "id long, name string, score double"
    df1.groupBy("id").cogroup(df2.groupBy("id")).applyInArrow(
        process_cogroup, result_schema
    ).collect()
    return NodeType.FlatMapCoGroupsInArrow


GENERATORS = [
    generate_batch_eval_python_udtf,
    generate_arrow_eval_python_udtf,
    generate_flat_map_groups_in_arrow,
    generate_flat_map_co_groups_in_arrow,
]


def main() -> None:
    repo_root = Path(__file__).parents[1]
    fixtures_dir = repo_root / "tests" / "data" / "test_detail_parsing"

    tmpdir = Path(tempfile.mkdtemp())
    event_log_dir = tmpdir / "event_logs"
    event_log_dir.mkdir(parents=True, exist_ok=True)

    spark = build_spark(str(event_log_dir))

    try:
        for generator in GENERATORS:
            target = generator(spark)
            event_log = find_event_log(event_log_dir)
            event = _raw_event_for_query(event_log, target)
            if event is None:
                print(f"[WARN] Could not find event for {target.value}")
                continue
            path = save_fixture(event, target, fixtures_dir)
            print(f"Saved fixture for {target.value} to {path}")
    finally:
        spark.stop()
        shutil.rmtree(tmpdir, ignore_errors=True)


if __name__ == "__main__":
    main()
