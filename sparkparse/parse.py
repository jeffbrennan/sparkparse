import datetime
import json
import logging
import re
from pathlib import Path
from typing import cast

from sparkparse.clean import log_to_combined_df, log_to_dag_df, write_parsed_log
from sparkparse.common import timeit
from sparkparse.models import (
    NODE_ID_PATTERN,
    NODE_TYPE_DETAIL_MAP,
    NODE_TYPE_PATTERN,
    Accumulator,
    DriverAccumUpdates,
    EventType,
    ExecutorMetrics,
    InputMetrics,
    InsertIntoHadoopFsRelationCommandDetail,
    Job,
    Metrics,
    NodeType,
    OutputFormat,
    OutputMetrics,
    ParsedLog,
    ParsedLogDataFrames,
    PhysicalPlan,
    PhysicalPlanDetail,
    PhysicalPlanDetails,
    PhysicalPlanNode,
    PlanAccumulator,
    QueryEvent,
    ScanDetail,
    ShuffleReadMetrics,
    ShuffleWriteMetrics,
    Stage,
    Task,
    TaskMetrics,
)

logger = logging.getLogger(__name__)


def parse_job(line_dict: dict) -> Job:
    if line_dict["Event"].endswith("Start"):
        event_type = EventType.start
        timestamp = line_dict["Submission Time"]
    else:
        event_type = EventType.end
        timestamp = line_dict["Completion Time"]
    line_dict["job_timestamp"] = timestamp
    return Job(
        event_type=event_type,
        **line_dict,
    )


def parse_stage(line_dict: dict) -> Stage:
    if line_dict["Event"].endswith("Submitted"):
        event_type = EventType.start
        timestamp = line_dict["Stage Info"]["Submission Time"]
    else:
        event_type = EventType.end
        timestamp = line_dict["Stage Info"]["Completion Time"]

    return Stage(
        stage_id=line_dict["Stage Info"]["Stage ID"],
        event_type=event_type,
        stage_timestamp=timestamp,
    )


def parse_task(line_dict: dict) -> Task:
    task_info = line_dict["Task Info"]
    task_info["Stage ID"] = line_dict["Stage ID"]
    task_info["Task Type"] = line_dict["Task Type"]

    task_id = task_info["Task ID"]
    task_metrics = line_dict["Task Metrics"]
    metrics = Metrics(
        task_metrics=TaskMetrics(**task_metrics),
        executor_metrics=ExecutorMetrics(**line_dict["Task Executor Metrics"]),
        shuffle_read_metrics=ShuffleReadMetrics(**task_metrics["Shuffle Read Metrics"]),
        shuffle_write_metrics=ShuffleWriteMetrics(
            **task_metrics["Shuffle Write Metrics"]
        ),
        input_metrics=InputMetrics(**task_metrics["Input Metrics"]),
        output_metrics=OutputMetrics(**task_metrics["Output Metrics"]),
    )
    accumulators = [
        Accumulator(task_id=task_id, **i) for i in task_info["Accumulables"]
    ]
    return Task(
        metrics=metrics,
        accumulators=accumulators,
        **line_dict["Task Info"],
    )


def get_plan_details(
    plan_lines: list[str],
    tree_end: int,
    node_map: dict[int, PhysicalPlanNode],
) -> PhysicalPlanDetails:
    details_start = plan_lines[tree_end:].index("") + tree_end + 2
    details_end = len(plan_lines) - 1
    plan_details = plan_lines[details_start:details_end]
    for i, line in enumerate(plan_details):
        if line == "":
            plan_details[i] = "\n\n"
    plan_details_split = "\n".join(plan_details).split("\n\n")
    plan_details_split = [i for i in plan_details_split if i != ""][0 : len(node_map)]
    assert len(plan_details_split) == len(node_map)

    details_parsed = []
    for detail in plan_details_split:
        node_id = re.compile(NODE_ID_PATTERN).search(detail)
        if not node_id:
            raise ValueError(f"Could not parse node id from line: {detail}")

        node_id = int(node_id.group(1))
        node_type = node_map[node_id].node_type

        detail_model = NODE_TYPE_DETAIL_MAP.get(node_type)
        if detail_model is None:
            raise ValueError(f"Could not find detail model for node type: {node_type}")

        print(detail)
        detail_dict = {}
        detail_lines = detail.split("\n")
        for i, detail_line in enumerate(detail_lines):
            if i == 0:
                continue

            detail_split = detail_line.split(":")
            key = detail_split[0]
            value = ":".join(detail_split[1:])

            # remove detail size indicators like Output [4] -> Output
            cleaned_key = re.sub(r"\s+\[\d+\]", "", key.strip())
            detail_dict[cleaned_key] = value.strip()

        detail_parsed = detail_model.model_validate(detail_dict)
        details_parsed.append(
            PhysicalPlanDetail(
                node_id=node_id,
                node_type=node_type,
                detail=detail_parsed,  # type: ignore
            )
        )

    codegen_lookup = {}
    for details in plan_details_split:
        if "[codegen id : " not in details:
            continue

        codegen_node = int(details.split(")")[0].split("(")[-1].strip())
        codegen_id = int(details.split("[codegen id : ")[-1].split("]")[0].strip())
        codegen_lookup[codegen_node] = codegen_id

    return PhysicalPlanDetails(details=details_parsed, codegen_lookup=codegen_lookup)


def parse_node_accumulators(
    plan: dict, node_map: dict[int, PhysicalPlanNode]
) -> dict[int, list[PlanAccumulator]]:
    def process_node(node_info: dict, child_index: int):
        node_name = node_info["nodeName"]
        node_string = node_info["simpleString"]

        # wholestagecodegen events are pseudo nodes that only contain a single timing metric
        if node_name.startswith("WholeStageCodegen"):
            node_id = int(node_name.split("(")[-1].split(")")[0].strip()) + 100_000
            whole_stage_codegen_accumulators[node_id] = [
                PlanAccumulator(
                    node_id=node_id,
                    node_name=node_name,
                    node_string=node_string,
                    child_index=child_index,
                    **node_info["metrics"][0],
                )
            ]

        is_excluded = any([excluded in node_name for excluded in nodes_to_exclude])
        if "metrics" in node_info and not is_excluded:
            node_id = node_ids[child_index]
            expected_node_name = node_map[node_id].node_type
            assert (
                node_name.replace("Execute ", "").split(" ")[0]
                in expected_node_name.value
            ), print(f"{node_name} not in {expected_node_name.value}")
            metrics_parsed = [
                PlanAccumulator(
                    node_id=node_id,
                    node_name=node_name,
                    node_string=node_string,
                    child_index=child_index,
                    **metric,
                )
                for metric in node_info["metrics"]
            ]
            accumulators[node_id] = metrics_parsed

        if "children" in node_info:
            for child in node_info["children"]:
                process_node(child, child_index=len(accumulators))

    node_ids = list(node_map.keys())
    accumulators = {}
    whole_stage_codegen_accumulators = {}

    nodes_to_exclude = ["WholeStageCodegen", "InputAdapter"]
    for i, child in enumerate(plan["sparkPlanInfo"]["children"]):
        process_node(child, i)

    accumulators.update(whole_stage_codegen_accumulators)
    return accumulators


def parse_spark_ui_tree(tree: str) -> dict[int, PhysicalPlanNode]:
    step = 3
    empty_leading_lines = 0
    node_map: dict[int, PhysicalPlanNode] = {}
    indentation_history = []

    lines = tree.split("\n")

    for i, line in enumerate(lines):
        if line == "":
            empty_leading_lines += 1
            continue

        # remove leading spaces and nested indentation after :
        line_strip = line.lstrip().removeprefix(": ").lstrip()
        match = re.compile(NODE_ID_PATTERN).search(line)
        if not match:
            continue

        node_id = int(match.group(1))

        node_type_match = re.search(NODE_TYPE_PATTERN, line.replace("Execute", ""))

        if node_type_match:
            node_type = NodeType(node_type_match.group(1))

        else:
            raise ValueError(f"Could not parse node type from line: {line}")

        node = PhysicalPlanNode(
            node_id=node_id,
            node_type=node_type,
            child_nodes=[],
            whole_stage_codegen_id=None,
        )
        node_map[node_id] = node

        indentation_level = len(line) - len(line_strip)

        # first non-empty line is always the leaf node
        if i == 0 + empty_leading_lines:
            indentation_history.append((indentation_level, node_id))
            continue

        prev_indentation = indentation_history[-1]
        indentation_history.append((indentation_level, node_id))
        if prev_indentation[0] > indentation_level:
            child_nodes = [
                i[1] for i in indentation_history if i[0] == indentation_level - step
            ]
            if child_nodes:
                node_map[node_id].child_nodes = child_nodes
            continue

        node_map[node_id].child_nodes = [prev_indentation[1]]
    return node_map


def parse_physical_plan(line_dict: dict) -> PhysicalPlan:
    plan_string = line_dict["physicalPlanDescription"]
    query_id = line_dict["executionId"]
    plan_lines = plan_string.split("\n")

    tree_start = plan_lines.index("+- == Final Plan ==") + 1
    tree_end = plan_lines.index("+- == Initial Plan ==")
    tree = "\n".join(plan_lines[tree_start:tree_end])

    logging.debug(tree)

    node_map = parse_spark_ui_tree(tree)
    plan_accumulators = parse_node_accumulators(line_dict, node_map)
    details = get_plan_details(plan_lines, tree_end, node_map)

    if len(details.codegen_lookup) > 0:
        for k, v in details.codegen_lookup.items():
            node_map[k].whole_stage_codegen_id = v

    if len(plan_accumulators) > 0:
        for k, v in plan_accumulators.items():
            if k not in node_map and k >= 100_000:
                node_map[k] = PhysicalPlanNode(
                    node_id=v[0].node_id,
                    node_type=NodeType.WholeStageCodegen,
                    child_nodes=None,
                    whole_stage_codegen_id=v[0].node_id - 100_000,
                    accumulators=v,
                )
            else:
                node_map[k].accumulators = v if v else None

    return PhysicalPlan(
        query_id=query_id,
        details=details,
        nodes=list(node_map.values()),
    )


def get_parsed_log_name(parsed_plan: PhysicalPlan, out_name: str | None) -> str:
    name_len_limit = 100
    today = datetime.datetime.now().strftime("%Y-%m-%dT%H-%M-%S")
    if out_name is not None:
        return out_name[:name_len_limit]
    source_models = [
        cast(ScanDetail, i.detail)
        for i in parsed_plan.details.details
        if i.node_type in [NodeType.Scan]
    ]

    target_models = [
        cast(InsertIntoHadoopFsRelationCommandDetail, i.detail)
        for i in parsed_plan.details.details
        if i.node_type in [NodeType.InsertIntoHadoopFsRelationCommand]
    ]

    sources = [i.location for i in source_models]
    targets = [i.arguments[0] for i in target_models]

    parsed_paths = []
    if len(targets) > 0:
        paths_to_use = targets
    else:
        paths_to_use = sources

    for path in set(paths_to_use):
        path_name = path.split("/")[-1].split(".")[0]
        parsed_paths.append(path_name)

    paths_final = "_".join(parsed_paths)[:name_len_limit]

    return f"{today}__{paths_final}"


def parse_driver_accum_update(line_dict: dict) -> list[DriverAccumUpdates]:
    query_id = line_dict["executionId"]
    accum_updates = []
    for i in line_dict["accumUpdates"]:
        accum_updates.append(
            DriverAccumUpdates(query_id=query_id, accumulator_id=i[0], update=i[1])
        )

    return accum_updates


@timeit
def parse_log(log_path: Path, out_name: str | None = None) -> ParsedLog:
    logger.debug(f"Starting to parse log file: {log_path}")
    with log_path.open("r") as f:
        all_contents = f.readlines()

    start_point = "SparkListenerApplicationStart"
    for i, line in enumerate(all_contents):
        line_dict = json.loads(line)
        if line_dict["Event"] == start_point:
            start_index = i + 1
            break

    contents_to_parse = all_contents[start_index:]

    jobs = []
    stages = []
    tasks = []
    queries = []
    query_times = []
    driver_accum_updates = []
    for i, line in enumerate(contents_to_parse, start_index):
        logger.debug("-" * 40)
        logger.debug(f"[line {i:04d}] parse start")
        line_dict = json.loads(line)
        event_type = line_dict["Event"]
        if event_type.startswith("SparkListenerJob"):
            job = parse_job(line_dict)
            jobs.append(job)
            logger.debug(
                f"[line {i:04d}] parse finish - job#{job.job_id}  type:{job.event_type}"
            )
        elif event_type.startswith("SparkListenerStage"):
            stage = parse_stage(line_dict)
            stages.append(stage)
            logger.debug(
                f"[line {i:04d}] parse finish - stage#{stage.stage_id} type:{stage.event_type}"
            )
        elif event_type == "SparkListenerTaskEnd":
            task = parse_task(line_dict)
            tasks.append(task)
            logger.debug(
                f"[line {i:04d}] parse finish - task#{task.task_id} stage#{task.stage_id}"
            )
        elif event_type.endswith("SparkListenerSQLAdaptiveExecutionUpdate"):
            is_final_plan = (
                line_dict["sparkPlanInfo"]["simpleString"].split("isFinalPlan=")[-1]
                == "true"
            )
            if is_final_plan:
                logger.debug(f"Found final plan at line {i}")
                queries.append(line_dict)
            logger.debug(
                f"[line {i:04d}] parse skip - unhandled event type {event_type}"
            )
        elif event_type.endswith("SparkListenerSQLExecutionStart"):
            query_times.append(
                QueryEvent(
                    query_id=line_dict["executionId"],
                    query_time=line_dict["time"],
                    event_type=EventType.start,
                )
            )
        elif event_type.endswith("SparkListenerSQLExecutionEnd"):
            query_times.append(
                QueryEvent(
                    query_id=line_dict["executionId"],
                    query_time=line_dict["time"],
                    event_type=EventType.end,
                )
            )
        elif event_type.endswith("DriverAccumUpdates"):
            driver_accum_updates.extend(parse_driver_accum_update(line_dict))

    if len(queries) == 0:
        raise ValueError("No queries found in log file")

    parsed_queries = [parse_physical_plan(query) for query in queries]
    logger.debug(
        f"Finished parsing log [n={len(jobs)} jobs | n={len(stages)} stages | n={len(tasks)} tasks | n={len(parsed_queries)} queries]"
    )

    parsed_log_name = get_parsed_log_name(parsed_queries[0], out_name)

    return ParsedLog(
        name=parsed_log_name,
        jobs=jobs,
        stages=stages,
        tasks=tasks,
        queries=parsed_queries,
        query_times=query_times,
        driver_accum_updates=driver_accum_updates,
    )


def get_parsed_metrics(
    base_dir: str = "data",
    log_dir: str = "logs/raw",
    log_file: str | None = None,
    out_dir: str | None = "logs/parsed",
    out_name: str | None = None,
    out_format: OutputFormat | None = OutputFormat.csv,
    verbose: bool = False,
) -> ParsedLogDataFrames:
    base_dir_path = Path(__file__).parents[1] / base_dir
    log_dir_path = base_dir_path / log_dir

    if verbose:
        logging.basicConfig(
            level=logging.DEBUG,
            format="%(asctime)s.%(msecs)03d %(message)s",
            datefmt="%Y-%m-%dT%H:%M:%S",
        )
    else:
        logging.basicConfig(level=logging.INFO, format="%(message)s")

    if log_file is None:
        log_to_parse = sorted(log_dir_path.glob("*"))[-1]
    else:
        log_to_parse = log_dir_path / log_file

    logging.info(f"Reading log file: {log_to_parse}")

    result = parse_log(log_to_parse, out_name)
    combined_df = log_to_combined_df(result, log_to_parse.stem)
    dag_df = log_to_dag_df(result)

    output = ParsedLogDataFrames(combined=combined_df, dag=dag_df)

    if out_dir is None or out_format is None:
        logging.info("Skipping writing parsed log")
        return output

    write_parsed_log(
        df=combined_df,
        base_dir_path=base_dir_path,
        out_dir=out_dir,
        out_format=out_format,
        parsed_name=result.name,
        suffix="_combined",
    )

    write_parsed_log(
        df=dag_df,
        base_dir_path=base_dir_path,
        out_dir=out_dir,
        out_format=out_format,
        parsed_name=result.name,
        suffix="_dag",
    )
    return output
