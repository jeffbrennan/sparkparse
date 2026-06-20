import json
import shutil
import tempfile
from collections.abc import Callable
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from sparkparse.models import NodeType
from sparkparse.parse import parse_log

FixtureGenerator = Callable[[SparkSession], NodeType]

# Iceberg Spark runtime is fetched via Maven so we can exercise real DataSource V2
# physical nodes (BatchScan, AppendData, OverwriteByExpression,
# OverwritePartitionsDynamic) that Delta's V1 fallback does not emit.
ICEBERG_PACKAGE = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1"


def build_spark(event_log_dir: str, work_dir: Path) -> SparkSession:
    builder = (
        SparkSession.builder.appName("sparkparse-fixture-generator")
        .master("local[2]")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.forceApply", "true")
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.dir", event_log_dir)
        .config("spark.jars.packages", ICEBERG_PACKAGE)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local.type", "hadoop")
        .config(
            "spark.sql.catalog.local.warehouse", str(work_dir / "iceberg_warehouse")
        )
        .config("spark.sql.autoBroadcastJoinThreshold", "-1")
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

    # Build a map of query_id -> final plan event
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
        # Command-only events (no Final Plan section) can fail to parse as
        # full PhysicalPlan structures. Fall back to raw execution-start events.
        print(f"[WARN] parse_log failed for {target.value}: {exc}; using raw events")
        queries = []

    for query in reversed(queries):
        if any(node.node_type == target for node in query.nodes):
            return final_plan_events.get(query.query_id)

    # Fallback: command nodes (e.g. DSv2 writes) may only appear in
    # SparkListenerSQLExecutionStart. Synthesize a sparkPlanInfo so the
    # fixture still works with parse_physical_plan for tree/detail parsing.
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


def generate_range(spark: SparkSession) -> NodeType:
    spark.range(10).collect()
    return NodeType.Range


def generate_sample(spark: SparkSession) -> NodeType:
    spark.range(100).sample(False, 0.5, 42).collect()
    return NodeType.Sample


def generate_repartition_by_expression(spark: SparkSession) -> NodeType:
    spark.range(100).repartition(10, "id").collect()
    return NodeType.RepartitionByExpression


def generate_cartesian_product(spark: SparkSession) -> NodeType:
    df1 = spark.range(10).withColumnRenamed("id", "a")
    df2 = spark.range(5).withColumnRenamed("id", "b")
    df1.crossJoin(df2).collect()
    return NodeType.CartesianProduct


def generate_expand(spark: SparkSession) -> NodeType:
    df = spark.createDataFrame([(1, "x"), (2, "y")], ["id", "v"])
    df.rollup("id", "v").count().collect()
    return NodeType.Expand


def generate_object_hash_aggregate(spark: SparkSession) -> NodeType:
    df = spark.createDataFrame([(1, "x"), (1, "y")], ["id", "v"])
    df.groupBy("id").agg(F.collect_list("v")).collect()
    return NodeType.ObjectHashAggregate


def generate_sort_aggregate(spark: SparkSession) -> NodeType:
    spark.conf.set("spark.sql.execution.aggregate.maxObjectHashAggSize", "0")
    spark.conf.set("spark.sql.execution.sortAggregate.threshold", "1000000000")
    spark.range(100).groupBy("id").count().collect()
    return NodeType.SortAggregate


def generate_arrow_eval_python(spark: SparkSession) -> NodeType:
    from pyspark.sql.functions import pandas_udf

    @pandas_udf("long")  # type: ignore[no-untyped-call]
    def times_two(x):
        return x * 2

    df = spark.range(10)
    df.select(times_two(F.col("id"))).collect()
    return NodeType.ArrowEvalPython


def generate_batch_eval_python(spark: SparkSession) -> NodeType:
    from pyspark.sql.functions import udf

    @udf("long")
    def times_two(x):
        return x * 2

    df = spark.range(10)
    df.select(times_two(F.col("id"))).collect()
    return NodeType.BatchEvalPython


def generate_map_in_pandas(spark: SparkSession) -> NodeType:
    df = spark.range(10)

    def map_fn(iterator):
        for pdf in iterator:
            yield pdf.assign(id=pdf.id * 2)

    df.mapInPandas(map_fn, schema="id long").collect()
    return NodeType.MapInPandas


def generate_map_in_arrow(spark: SparkSession) -> NodeType:
    df = spark.range(10)

    def map_fn(iterator):
        yield from iterator

    df.mapInArrow(map_fn, schema="id long").collect()
    return NodeType.PythonMapInArrow


def generate_flat_map_groups_in_pandas(spark: SparkSession) -> NodeType:
    df = spark.createDataFrame([(1, "x"), (1, "y")], ["id", "v"])

    def grouped_map(pdf):
        return pdf.assign(v=pdf.v + "_mapped")

    df.groupBy("id").applyInPandas(grouped_map, schema="id long, v string").collect()
    return NodeType.FlatMapGroupsInPandas


def generate_flat_map_co_groups_in_pandas(spark: SparkSession) -> NodeType:
    df1 = spark.createDataFrame([(1, "a")], ["id", "v1"])
    df2 = spark.createDataFrame([(1, "b")], ["id", "v2"])

    def cogroup_map(left, right):
        return left.merge(right, on="id", how="outer")

    df1.groupBy("id").cogroup(df2.groupBy("id")).applyInPandas(
        cogroup_map, schema="id long, v1 string, v2 string"
    ).collect()
    return NodeType.FlatMapCoGroupsInPandas


def _iceberg_table(spark: SparkSession, name: str) -> str:
    return f"local.db.{name}"


def generate_batch_scan(spark: SparkSession, work_dir: Path) -> NodeType:
    table = _iceberg_table(spark, "batch_scan")
    spark.sql(f"CREATE TABLE {table} (id bigint) USING iceberg")
    spark.range(10).writeTo(table).append()
    spark.read.table(table).collect()
    return NodeType.BatchScan


def generate_append_data(spark: SparkSession, work_dir: Path) -> NodeType:
    table = _iceberg_table(spark, "append_data")
    spark.sql(f"CREATE TABLE {table} (id bigint) USING iceberg")
    spark.range(10).writeTo(table).append()
    return NodeType.AppendData


def generate_overwrite_by_expression(spark: SparkSession, work_dir: Path) -> NodeType:
    table = _iceberg_table(spark, "overwrite_by_expr")
    spark.sql(f"CREATE TABLE {table} (id bigint) USING iceberg")
    # Write each side in a separate append so existing files do not straddle
    # the overwrite boundary; Iceberg requires whole-file deletes.
    spark.range(0, 6).writeTo(table).append()
    spark.range(6, 10).writeTo(table).append()
    spark.range(20, 24).writeTo(table).overwrite(F.expr("id > 5"))
    return NodeType.OverwriteByExpression


def generate_overwrite_partitions_dynamic(
    spark: SparkSession, work_dir: Path
) -> NodeType:
    table = _iceberg_table(spark, "overwrite_parts")
    spark.sql(
        f"CREATE TABLE {table} (id bigint, v string) USING iceberg PARTITIONED BY (id)"
    )
    spark.createDataFrame([(1, "a"), (2, "b")], ["id", "v"]).writeTo(table).append()
    spark.createDataFrame([(1, "c")], ["id", "v"]).writeTo(table).overwritePartitions()
    return NodeType.OverwritePartitionsDynamic


def generate_subquery_exec(spark: SparkSession) -> NodeType:
    spark.createDataFrame([(1,), (2,)], ["id"]).createOrReplaceTempView("t1")
    spark.createDataFrame([(1,), (3,)], ["id"]).createOrReplaceTempView("t2")
    spark.sql(
        """
        SELECT cnt, count(*) AS n
        FROM (
            SELECT (SELECT count(*) FROM t2 WHERE t2.id = t1.id) AS cnt FROM t1
        )
        GROUP BY cnt
        """
    ).collect()
    return NodeType.SubqueryExec


def generate_reused_subquery_exec(spark: SparkSession) -> NodeType:
    spark.createDataFrame([(1,), (2,)], ["id"]).createOrReplaceTempView("t1")
    spark.createDataFrame([(1,), (3,)], ["id"]).createOrReplaceTempView("t2")
    spark.sql(
        """
        SELECT cnt1 + cnt2 AS total, count(*) AS n
        FROM (
            SELECT
                (SELECT count(*) FROM t2 WHERE t2.id = t1.id) AS cnt1,
                (SELECT count(*) FROM t2 WHERE t2.id = t1.id) AS cnt2
            FROM t1
        )
        GROUP BY cnt1 + cnt2
        """
    ).collect()
    return NodeType.ReusedSubqueryExec


def generate_subquery_broadcast(spark: SparkSession, work_dir: Path) -> NodeType:
    # Dynamic partition pruning (DPP) emits SubqueryBroadcast when a small
    # filtered dimension is broadcast and used to prune a partitioned fact table.
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10485760")
    large_path = str(work_dir / "dpp_large")
    spark.range(1000).withColumn("v", F.lit("x")).write.partitionBy("id").parquet(
        large_path
    )
    large = spark.read.parquet(large_path)
    small = spark.range(10).filter("id < 5")
    large.join(small, "id").groupBy("id").count().collect()
    return NodeType.SubqueryBroadcast


GENERATORS: list[tuple[str, FixtureGenerator]] = [
    ("Range", generate_range),
    ("Sample", generate_sample),
    ("RepartitionByExpression", generate_repartition_by_expression),
    ("CartesianProduct", generate_cartesian_product),
    ("Expand", generate_expand),
    ("ObjectHashAggregate", generate_object_hash_aggregate),
    ("SortAggregate", generate_sort_aggregate),
    ("ArrowEvalPython", generate_arrow_eval_python),
    ("BatchEvalPython", generate_batch_eval_python),
    ("MapInPandas", generate_map_in_pandas),
    ("PythonMapInArrow", generate_map_in_arrow),
    ("FlatMapGroupsInPandas", generate_flat_map_groups_in_pandas),
    ("FlatMapCoGroupsInPandas", generate_flat_map_co_groups_in_pandas),
    ("SubqueryExec", generate_subquery_exec),
    ("ReusedSubqueryExec", generate_reused_subquery_exec),
]

WORK_DIR_GENERATORS: list[tuple[str, Callable[[SparkSession, Path], NodeType]]] = [
    ("SubqueryBroadcast", generate_subquery_broadcast),
    ("BatchScan", generate_batch_scan),
    ("AppendData", generate_append_data),
    ("OverwriteByExpression", generate_overwrite_by_expression),
    ("OverwritePartitionsDynamic", generate_overwrite_partitions_dynamic),
]


def main() -> None:
    fixtures_dir = Path(__file__).parents[0] / "data" / "test_detail_parsing"
    event_log_dir = Path(tempfile.mkdtemp(prefix="sparkparse_event_logs_"))
    work_dir = Path(tempfile.mkdtemp(prefix="sparkparse_work_"))

    spark = build_spark(str(event_log_dir), work_dir)

    found: list[str] = []
    missing: list[str] = []

    try:
        for name, generator in GENERATORS:
            target = NodeType(name)
            try:
                actual = generator(spark)
                assert actual == target
            except Exception as exc:
                print(f"[ERROR] generator {name} failed: {exc}")
                missing.append(name)
                continue

            event_log = find_event_log(event_log_dir)
            event = _raw_event_for_query(event_log, target)
            if event is None:
                print(f"[MISSING] {name}: node not found in event log")
                missing.append(name)
                continue

            fixture_path = save_fixture(event, target, fixtures_dir)
            print(f"[OK] {name} -> {fixture_path}")
            found.append(name)

        for name, generator in WORK_DIR_GENERATORS:
            target = NodeType(name)
            try:
                actual = generator(spark, work_dir)
                assert actual == target
            except Exception as exc:
                print(f"[ERROR] generator {name} failed: {exc}")
                missing.append(name)
                continue

            event_log = find_event_log(event_log_dir)
            event = _raw_event_for_query(event_log, target)
            if event is None:
                print(f"[MISSING] {name}: node not found in event log")
                missing.append(name)
                continue

            fixture_path = save_fixture(event, target, fixtures_dir)
            print(f"[OK] {name} -> {fixture_path}")
            found.append(name)
    finally:
        spark.stop()
        shutil.rmtree(event_log_dir, ignore_errors=True)
        shutil.rmtree(work_dir, ignore_errors=True)

    print("\nSummary:")
    print(f"  Found: {len(found)}/{len(GENERATORS) + len(WORK_DIR_GENERATORS)}")
    if missing:
        print(f"  Missing: {', '.join(missing)}")
        raise SystemExit(1)


if __name__ == "__main__":
    main()
