import datetime
import json
import logging
import re
from collections.abc import Iterator
from pathlib import Path
from typing import Any, cast

import polars as pl

from sparkparse.clean import (
    job_stage_associations,
    log_to_combined_df,
    log_to_dag_df,
    query_stage_associations,
    write_parsed_log,
)
from sparkparse.common import resolve_dir, timeit
from sparkparse.eventlog import (
    IGNORED_NAMES,
    EventLogNotFoundError,
    UnsupportedCodecError,
    discover_sources,
    iter_lines,
    resolve_source,
    single_file_source,
)
from sparkparse.models import (
    NODE_ID_PATTERN,
    NODE_TYPE_DETAIL_MAP,
    NODE_TYPE_PATTERN,
    Accumulator,
    DriverAccumUpdates,
    EventLogSource,
    EventType,
    ExecutorMetrics,
    InputMetrics,
    Job,
    LogDiagnostic,
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
    QueryFunction,
    RawDetail,
    ReusedExchangeDetail,
    ReusedSubqueryExecDetail,
    ShuffleReadMetrics,
    ShuffleWriteMetrics,
    Stage,
    StageStatus,
    Task,
    TaskMetrics,
    TaskStatus,
    deserialize_insert_into_hadoop_fs_relation_command_detail,
    deserialize_scan_detail,
)
from sparkparse.storage import get_path_name, is_cloud_path

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
    stage_info = line_dict["Stage Info"]
    failure_reason = stage_info.get("Failure Reason")
    if line_dict["Event"].endswith("Submitted"):
        event_type = EventType.start
        timestamp = stage_info.get("Submission Time")
        status = StageStatus.running
    else:
        event_type = EventType.end
        timestamp = stage_info.get("Completion Time")
        status = StageStatus.failed if failure_reason else StageStatus.succeeded

    return Stage(
        stage_id=stage_info["Stage ID"],
        # A retried stage reuses its stage id, so the attempt id is part of
        # the key. Older logs spell it "Attempt ID".
        stage_attempt_id=stage_info.get(
            "Stage Attempt ID", stage_info.get("Attempt ID", 0)
        ),
        event_type=event_type,
        stage_timestamp=timestamp,
        num_tasks=stage_info.get("Number of Tasks"),
        status=status,
        failure_reason=failure_reason,
    )


def _task_end_reason(line_dict: dict) -> tuple[TaskStatus, str | None]:
    """Map ``Task End Reason`` onto a status and a human-readable reason."""
    reason = line_dict.get("Task End Reason") or {}
    kind = reason.get("Reason", "Success")
    if kind == "Success":
        return TaskStatus.success, None
    # Prefer the short summary Spark already composed; the stack trace is a
    # last resort because the reason is stored, not just logged.
    class_name = reason.get("Class Name")
    description = reason.get("Description")
    if class_name and description:
        detail = f"{class_name}: {description}"
    else:
        detail = (
            class_name
            or description
            or reason.get("Kill Reason")
            or reason.get("Full Stack Trace")
            or kind
        )
    status = TaskStatus.killed if kind == "TaskKilled" else TaskStatus.failed
    return status, str(detail)[:500]


def parse_task(line_dict: dict) -> Task:
    task_info = line_dict["Task Info"]
    task_info["Stage ID"] = line_dict["Stage ID"]
    task_info["Stage Attempt ID"] = line_dict.get("Stage Attempt ID", 0)
    task_info["Task Type"] = line_dict["Task Type"]

    task_id = task_info["Task ID"]
    task_metrics = line_dict.get("Task Metrics")
    # A task that failed before its metrics were serialized reports none. That
    # is missing data, not a task that consumed nothing.
    if task_metrics is None:
        metrics = None
    else:
        metrics = Metrics(
            task_metrics=TaskMetrics(**task_metrics),
            executor_metrics=ExecutorMetrics(**line_dict["Task Executor Metrics"]),
            shuffle_read_metrics=ShuffleReadMetrics(
                **task_metrics["Shuffle Read Metrics"]
            ),
            shuffle_write_metrics=ShuffleWriteMetrics(
                **task_metrics["Shuffle Write Metrics"]
            ),
            input_metrics=InputMetrics(**task_metrics["Input Metrics"]),
            output_metrics=OutputMetrics(**task_metrics["Output Metrics"]),
        )
    accumulators = [
        Accumulator(task_id=task_id, **i) for i in task_info.get("Accumulables", [])
    ]
    status, failure_reason = _task_end_reason(line_dict)
    return Task(
        metrics=metrics,
        accumulators=accumulators,
        status=status,
        failure_reason=failure_reason,
        **task_info,
    )


@timeit
def get_plan_details(
    plan_lines: list[str],
    tree_end: int,
    node_map: dict[int, PhysicalPlanNode],
    strict: bool = False,
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
    null_detail_types = [NodeType.Union, NodeType.WholeStageCodegen]

    for detail in plan_details_split:
        node_id = re.compile(NODE_ID_PATTERN).search(detail)
        if not node_id:
            raise ValueError(f"Could not parse node id from line: {detail}")

        node_id = int(node_id.group(1))
        if node_id not in node_map:
            continue

        node_type = node_map[node_id].node_type

        if node_type in null_detail_types:
            details_parsed.append(
                PhysicalPlanDetail(
                    node_id=node_id,
                    node_type=node_type,
                    detail=None,
                )
            )
            continue

        if node_type not in NODE_TYPE_DETAIL_MAP:
            msg = f"No detail model for node type: {node_type}"
            if strict:
                raise ValueError(msg)
            logger.warning(f"{msg} — storing raw text")
            details_parsed.append(
                PhysicalPlanDetail(
                    node_id=node_id,
                    node_type=node_type,
                    detail=RawDetail(raw=detail),
                )
            )
            continue

        detail_model = NODE_TYPE_DETAIL_MAP[node_type]
        detail_dict: dict[str, Any] = {}
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

        if node_type == NodeType.ReusedExchange:
            reused_id = detail_lines[0].split(": ")[-1].removesuffix("]").strip()
            detail_dict["reuses_node_id"] = int(reused_id)

        if node_type == NodeType.ReusedSubqueryExec:
            reused_id = detail_lines[0].split(": ")[-1].removesuffix("]").strip()
            detail_dict["reuses_node_id"] = int(reused_id)

        try:
            detail_parsed = detail_model.model_validate(detail_dict)
        except Exception as exc:
            msg = f"Failed to parse details for node type: {node_type}"
            if strict:
                raise ValueError(msg) from exc
            logger.warning(f"{msg} — storing raw text")
            detail_parsed = RawDetail(raw=detail)

        details_parsed.append(
            PhysicalPlanDetail(
                node_id=node_id,
                node_type=node_type,
                detail=detail_parsed,
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

        is_excluded = any(
            [excluded in node_name for excluded in accumulators_missing_from_tree_nodes]
        )
        if "metrics" in node_info and not is_excluded:
            if child_index >= len(node_ids):
                logger.warning(
                    f"Accumulator child_index {child_index} out of bounds for plan with {len(node_ids)} nodes; skipping accumulators for {node_name}"
                )
                return
            node_id = node_ids[child_index]
            expected_node_name = node_map[node_id].node_type
            if expected_node_name != NodeType.Unknown:
                accumulator_node_type = node_name.replace("Execute ", "").split(" ")[0]
                if accumulator_node_type not in expected_node_name.value:
                    logger.warning(
                        f"Accumulator node name mismatch: {node_name} not in {expected_node_name.value}"
                    )
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

        # reusedexchange/reusedsubquery nodes repeat the metrics of the node they are reusing
        if "children" in node_info and not any(
            reused in node_name for reused in ("ReusedExchange", "ReusedSubquery")
        ):
            for child in node_info["children"]:
                process_node(child, child_index=len(accumulators))

    tree_nodes_missing_from_accumulators = [NodeType.InMemoryRelation]
    accumulators_missing_from_tree_nodes = ["WholeStageCodegen", "InputAdapter"]
    node_ids = [
        k
        for k, v in node_map.items()
        if v.node_type not in tree_nodes_missing_from_accumulators
    ]

    accumulators = {}
    whole_stage_codegen_accumulators = {}

    all_children = plan["sparkPlanInfo"]["children"]
    for i, child in enumerate(all_children):
        process_node(child, i)

    accumulators.update(whole_stage_codegen_accumulators)
    return accumulators


def _get_tree_indentation(line: str) -> int:
    """Return the column of the node's branch marker.

    Spark plan trees use ASCII art like::

        +- Node            <- branch marker '+-'
        :- Node            <- branch marker ':-'
        :  +- Child        <- ':' verticals + '+-' child marker
        :  :  +- Grandchild

    The logical indentation of a node is the column where its '+-' or ':-'
    marker starts. Everything before that marker is branch art or leading
    spaces. For the root line (no marker), the content column is returned.
    """
    for i in range(len(line) - 1):
        two = line[i : i + 2]
        if two in ("+-", ":-"):
            prefix = line[:i]
            if all(c == " " or c == ":" for c in prefix):
                return i
    # Root line with no branch marker (may be indented inside a larger dump).
    return len(line) - len(line.lstrip())


def parse_spark_ui_tree(tree: str, strict: bool = False) -> dict[int, PhysicalPlanNode]:
    """Parse a Spark ASCII plan tree into a node_id -> node map.

    ``PhysicalPlanNode.child_nodes`` contains the node's actual children in
    execution order (data flows from children to parent).

    When ``strict`` is True, an unrecognized node type raises instead of
    falling back to ``NodeType.Unknown``.
    """
    step = 3
    lines = tree.split("\n")

    node_map: dict[int, PhysicalPlanNode] = {}
    # Stack of (relative_indentation, node_id) for currently open parent nodes.
    stack: list[tuple[int, int]] = []
    root_content_col: int | None = None

    for line in lines:
        if not line.strip():
            continue

        match = re.compile(NODE_ID_PATTERN).search(line)
        if not match:
            continue

        node_id = int(match.group(1))
        node_type_match = re.search(NODE_TYPE_PATTERN, line.replace("Execute", ""))

        if node_type_match:
            try:
                node_type = NodeType(node_type_match.group(1))
            except ValueError:
                msg = f"Unknown node type: {node_type_match.group(1)}"
                if strict:
                    raise ValueError(msg)
                logger.warning(f"{msg} — storing as Unknown")
                node_type = NodeType.Unknown
        else:
            raise ValueError(f"Could not parse node type from line: {line}")

        raw_indentation = _get_tree_indentation(line)
        if root_content_col is None:
            # The first node line is the root. Its content column anchors the tree.
            root_content_col = raw_indentation
            indentation_level = 0
        else:
            # Child markers are aligned with the parent's content column, so a
            # marker at the root's content column is level 1.
            indentation_level = (raw_indentation - root_content_col) // step + 1

        if indentation_level < 0:
            logger.warning(f"Negative indentation {indentation_level} for line: {line}")
            continue

        node = PhysicalPlanNode(
            node_id=node_id,
            node_type=node_type,
            child_nodes=[],
            whole_stage_codegen_id=None,
        )
        node_map[node_id] = node

        # Pop the stack until we find the parent at the next lower level.
        while stack and stack[-1][0] >= indentation_level:
            stack.pop()

        if stack:
            parent_id = stack[-1][1]
            parent = node_map[parent_id]
            if parent.child_nodes is None:
                parent.child_nodes = []
            parent.child_nodes.append(node_id)

        stack.append((indentation_level, node_id))

    all_child_nodes = {
        child_id for node in node_map.values() for child_id in (node.child_nodes or [])
    }
    roots = [node_id for node_id in node_map.keys() if node_id not in all_child_nodes]
    assert len(roots) == 1, (
        f"Expected exactly one root node, found {len(roots)}: {roots}"
    )
    return node_map


def parse_physical_plan(line_dict: dict, strict: bool = False) -> PhysicalPlan:
    plan_string = line_dict["physicalPlanDescription"]
    query_id = line_dict["executionId"]

    plan_lines = plan_string.split("\n")
    final_plan_indicator = "+- == Final Plan =="
    current_plan_indicator = "+- == Current Plan =="
    initial_plan_indicator = "+- == Initial Plan =="
    physical_plan_indicator = "== Physical Plan =="

    # Adaptive plans expose Final/Initial Plan sections; command-only plans
    # (e.g. DSv2 writes) have a single tree under "== Physical Plan ==".
    if final_plan_indicator in plan_lines:
        tree_start = plan_lines.index(final_plan_indicator) + 1
    elif current_plan_indicator in plan_lines:
        tree_start = plan_lines.index(current_plan_indicator) + 1
    elif physical_plan_indicator in plan_lines:
        tree_start = plan_lines.index(physical_plan_indicator) + 1
    else:
        tree_start = 0

    if initial_plan_indicator in plan_lines:
        tree_end = plan_lines.index(initial_plan_indicator)
    else:
        # Command plans (e.g. DSv2 writes) have a single tree followed by
        # details and no Initial Plan section. The tree ends at the first
        # blank line after the tree starts.
        tree_end = len(plan_lines)
        for i in range(tree_start, len(plan_lines)):
            if plan_lines[i].strip() == "":
                tree_end = i
                break

    tree = "\n".join(plan_lines[tree_start:tree_end])

    max_attempts = 3
    attempts = 0
    while initial_plan_indicator in tree and attempts < max_attempts:
        attempts += 1
        # remove initial indicator
        tree_split = tree.split(initial_plan_indicator)

        # remove final/current indicator
        for indicator in (final_plan_indicator, current_plan_indicator):
            if indicator in tree_split[0]:
                tree_split_final = tree_split[0].split(indicator)
                tree = tree_split_final[0].strip() + tree_split_final[1]
                break

    if (
        initial_plan_indicator in tree
        or final_plan_indicator in tree
        or current_plan_indicator in tree
    ):
        raise ValueError(
            "could not remove initial plan after", max_attempts, "attempts"
        )

    node_map = parse_spark_ui_tree(tree, strict=strict)
    plan_accumulators = parse_node_accumulators(line_dict, node_map)
    details = get_plan_details(plan_lines, tree_end, node_map, strict=strict)

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

    detail_list = details.details
    for detail in detail_list:
        node_map[detail.node_id].details = detail.model_dump_json()

        # reused nodes are the children of the node they are reusing in the spark ui
        if detail.node_type == NodeType.ReusedExchange:
            reused_detail = cast(ReusedExchangeDetail, detail.detail)
            reuses_node = node_map[reused_detail.reuses_node_id]

            if reuses_node.child_nodes is None:
                reuses_node.child_nodes = [detail.node_id]
            else:
                reuses_node.child_nodes.append(detail.node_id)

        if detail.node_type == NodeType.ReusedSubqueryExec:
            reused_detail = cast(ReusedSubqueryExecDetail, detail.detail)
            reuses_node = node_map[reused_detail.reuses_node_id]

            if reuses_node.child_nodes is None:
                reuses_node.child_nodes = [detail.node_id]
            else:
                reuses_node.child_nodes.append(detail.node_id)

    return PhysicalPlan(
        query_id=query_id,
        nodes=list(node_map.values()),
    )


def get_parsed_log_name(
    parsed_plan: PhysicalPlan | None,
    out_name: str | None,
    source: EventLogSource | None = None,
) -> str:
    """Derive the output identity for a parsed log.

    Query ids restart at zero in every application, so the name carries the
    timestamp and, when no plan paths are available, the application identity.
    """
    name_len_limit = 100
    today = datetime.datetime.now().strftime("%Y-%m-%dT%H-%M-%S")
    if out_name is not None:
        return out_name[:name_len_limit]

    if parsed_plan is None:
        fallback = (source.application_id or source.name) if source else "no_queries"
        return f"{today}__{fallback}"[: name_len_limit + 21]

    source_model_strings = []
    target_model_strings = []

    for node in parsed_plan.nodes:
        if node.node_type == NodeType.Scan and node.details is not None:
            source_model_strings.append(node.details)
        elif (
            node.node_type == NodeType.InsertIntoHadoopFsRelationCommand
            and node.details is not None
        ):
            target_model_strings.append(node.details)

    sources = []
    for scan_detail in source_model_strings:
        locations = deserialize_scan_detail(scan_detail).location.location
        sources.extend(locations)

    targets = []
    for target in target_model_strings:
        path = deserialize_insert_into_hadoop_fs_relation_command_detail(
            target
        ).arguments.file_path
        targets.append(path)
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


def check_if_log_has_queries(log_path: str | Path) -> bool:
    """Return True if the log contains at least one SQL execution.

    Reads incrementally and stops at the first match, so a multi-gigabyte log
    is not copied into memory to answer a yes/no question.
    """
    log_path_str = str(log_path)
    if get_path_name(log_path_str) in IGNORED_NAMES:
        return False

    return source_has_queries(single_file_source(log_path_str))


def source_has_queries(source: EventLogSource) -> bool:
    """Return True if ``source`` contains at least one SQL execution."""
    try:
        for _, _, line in iter_lines(source):
            if "SparkListenerSQLExecutionStart" in line:
                return True
    except (UnsupportedCodecError, OSError) as exc:
        logger.warning("Could not read %s: %s", source.root_uri, exc)
        return False
    return False


# Plans arrive strongest-last: the initial plan is available at
# SQLExecutionStart, adaptive updates refine it, and the final adaptive plan is
# authoritative. A query that never reaches AQE keeps its initial plan.
PLAN_RANK_INITIAL = 0
PLAN_RANK_ADAPTIVE = 1
PLAN_RANK_FINAL = 2


def _is_final_adaptive_plan(line_dict: dict) -> bool:
    simple_string = line_dict.get("sparkPlanInfo", {}).get("simpleString", "")
    return simple_string.split("isFinalPlan=")[-1] == "true"


def iter_events(
    source: EventLogSource,
    strict: bool = False,
    diagnostics: list[LogDiagnostic] | None = None,
) -> Iterator[tuple[str, int, dict]]:
    """Yield decoded events from ``source``, one line at a time.

    A line that fails to decode is corruption when more lines follow it and a
    truncated tail when it is the last line of the last segment — a log that
    was still being written. Tolerant mode records both as diagnostics; strict
    mode raises with the source URI and line number.
    """
    sink = diagnostics if diagnostics is not None else []
    pending: tuple[str, int, str] | None = None

    for uri, line_number, line in iter_lines(source):
        if pending is not None:
            bad_uri, bad_line, bad_msg = pending
            pending = None
            message = f"Corrupt event at {bad_uri}:{bad_line} ({bad_msg})"
            if strict:
                raise ValueError(message)
            logger.warning(message)
            sink.append(
                LogDiagnostic(
                    code="corrupt_line", message=message, uri=bad_uri, line=bad_line
                )
            )
        if not line.strip():
            continue
        try:
            yield uri, line_number, json.loads(line)
        except json.JSONDecodeError as exc:
            pending = (uri, line_number, str(exc))

    if pending is not None:
        bad_uri, bad_line, bad_msg = pending
        message = (
            f"Truncated final event at {bad_uri}:{bad_line} ({bad_msg}); "
            "the log was still being written or was copied mid-flush"
        )
        if strict:
            raise ValueError(message)
        logger.warning(message)
        sink.append(
            LogDiagnostic(
                code="truncated_tail", message=message, uri=bad_uri, line=bad_line
            )
        )


@timeit
def parse_source(
    source: EventLogSource, out_name: str | None = None, strict: bool = False
) -> ParsedLog:
    """Parse one logical event log into a :class:`ParsedLog`.

    Neither ``SparkListenerApplicationStart`` nor an adaptive execution update
    is required: a log missing either is incomplete, not invalid.
    """
    logger.debug("Starting to parse event log source: %s", source.uris)

    diagnostics: list[LogDiagnostic] = []
    jobs: list[Job] = []
    stages: list[Stage] = []
    tasks: list[Task] = []
    query_times: list[QueryEvent] = []
    driver_accum_updates: list[DriverAccumUpdates] = []
    unknown_events: dict[str, int] = {}

    # query_id -> (rank, event) so a later, stronger plan replaces a weaker one
    # without keeping every intermediate plan in memory.
    plans: dict[int, tuple[int, dict]] = {}

    spark_version: str | None = None
    application_id: str | None = None
    application_name: str | None = None
    events_read = 0
    tasks_missing_metrics = 0

    for uri, line_number, line_dict in iter_events(source, strict, diagnostics):
        events_read += 1
        event_type = line_dict.get("Event")
        if event_type is None:
            message = f"Event with no 'Event' field at {uri}:{line_number}"
            if strict:
                raise ValueError(message)
            diagnostics.append(
                LogDiagnostic(
                    code="malformed_event",
                    message=message,
                    uri=uri,
                    line=line_number,
                )
            )
            continue

        if event_type == "SparkListenerLogStart":
            spark_version = line_dict.get("Spark Version")
        elif event_type == "SparkListenerApplicationStart":
            application_id = line_dict.get("App ID")
            application_name = line_dict.get("App Name")
        elif event_type.startswith("SparkListenerJob"):
            job = parse_job(line_dict)
            jobs.append(job)
            # %-style so the message is only built when debug logging is on;
            # this runs once per event.
            logger.debug(
                "[%s:%d] job#%d %s", uri, line_number, job.job_id, job.event_type
            )
        elif event_type.startswith("SparkListenerStage"):
            stage = parse_stage(line_dict)
            stages.append(stage)
            logger.debug(
                "[%s:%d] stage#%d.%d %s",
                uri,
                line_number,
                stage.stage_id,
                stage.stage_attempt_id,
                stage.event_type,
            )
        elif event_type == "SparkListenerTaskEnd":
            task = parse_task(line_dict)
            if task.metrics is None:
                tasks_missing_metrics += 1
            tasks.append(task)
            logger.debug(
                "[%s:%d] task#%d stage#%d.%d %s",
                uri,
                line_number,
                task.task_id,
                task.stage_id,
                task.stage_attempt_id,
                task.status,
            )
        elif event_type.endswith("SparkListenerSQLAdaptiveExecutionUpdate"):
            rank = (
                PLAN_RANK_FINAL
                if _is_final_adaptive_plan(line_dict)
                else PLAN_RANK_ADAPTIVE
            )
            _record_plan(plans, line_dict, rank)
        elif event_type.endswith("SparkListenerSQLExecutionStart"):
            # The initial physical plan ships with the start event. Without it
            # a non-AQE workload would have no plan at all.
            if "physicalPlanDescription" in line_dict:
                _record_plan(plans, line_dict, PLAN_RANK_INITIAL)
            description = line_dict.get("description", "")
            try:
                query_function = QueryFunction(description.split(" ")[0])
            except ValueError:
                msg = f"Unknown query function: {description.split(' ')[0]}"
                if strict:
                    raise ValueError(msg)
                logger.warning(msg)
                query_function = None
            query_times.append(
                QueryEvent(
                    query_id=line_dict["executionId"],
                    query_function=query_function,
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
        else:
            unknown_events[event_type] = unknown_events.get(event_type, 0) + 1

    if application_id is None and source.application_id is not None:
        # No ApplicationStart record: fall back to the identity in the path.
        application_id = source.application_id
        diagnostics.append(
            LogDiagnostic(
                code="missing_application_start",
                message=(
                    "No SparkListenerApplicationStart event; application identity "
                    "was taken from the log file name."
                ),
                uri=source.uris[0] if source.uris else None,
            )
        )

    if tasks_missing_metrics:
        diagnostics.append(
            LogDiagnostic(
                code="tasks_missing_metrics",
                message=(
                    f"{tasks_missing_metrics} task(s) reported no Task Metrics; "
                    "their resource usage is unknown, not zero."
                ),
            )
        )

    parsed_queries: list[PhysicalPlan] = []
    for query_id, (rank, plan_event) in sorted(plans.items()):
        try:
            parsed_queries.append(parse_physical_plan(plan_event, strict=strict))
        except Exception as exc:
            message = f"Could not parse physical plan for query {query_id}: {exc}"
            if strict:
                raise
            logger.warning(message)
            diagnostics.append(LogDiagnostic(code="plan_parse_failed", message=message))
            continue
        if rank < PLAN_RANK_FINAL:
            diagnostics.append(
                LogDiagnostic(
                    code="non_final_plan",
                    message=(
                        f"Query {query_id} has no final adaptive plan; using the "
                        f"{'initial' if rank == PLAN_RANK_INITIAL else 'in-progress adaptive'}"
                        " plan. Node metrics may be incomplete."
                    ),
                )
            )

    if not parsed_queries:
        diagnostics.append(
            LogDiagnostic(
                code="no_queries",
                message=(
                    "No SQL executions found in the event log. Non-SQL "
                    "applications produce jobs and tasks but no query plans."
                ),
                uri=source.uris[0] if source.uris else None,
            )
        )

    if not source.complete:
        diagnostics.append(
            LogDiagnostic(
                code="incomplete_source",
                message=(
                    "The event log is still in progress; results cover only the "
                    "events written so far."
                ),
                uri=source.uris[-1] if source.uris else None,
            )
        )

    logger.debug(
        "Finished parsing log [n=%d jobs | n=%d stages | n=%d tasks | n=%d queries]",
        len(jobs),
        len(stages),
        len(tasks),
        len(parsed_queries),
    )

    parsed_log_name = get_parsed_log_name(
        _plan_for_naming(parsed_queries), out_name, source
    )

    return ParsedLog(
        name=parsed_log_name,
        jobs=jobs,
        stages=stages,
        tasks=tasks,
        queries=parsed_queries,
        query_times=query_times,
        driver_accum_updates=driver_accum_updates,
        source=source,
        diagnostics=diagnostics,
        spark_version=spark_version,
        application_id=application_id,
        application_name=application_name,
        unknown_events=unknown_events,
        events_read=events_read,
    )


_NAMING_NODE_TYPES = (NodeType.Scan, NodeType.InsertIntoHadoopFsRelationCommand)


def _plan_for_naming(queries: list[PhysicalPlan]) -> PhysicalPlan | None:
    """Pick the query whose plan names the data the run touched.

    The first query is often a schema probe with no scan detail; naming the
    output after it loses the identity the paths would have given.
    """
    for query in queries:
        if any(
            node.node_type in _NAMING_NODE_TYPES and node.details is not None
            for node in query.nodes
        ):
            return query
    return queries[0] if queries else None


def _record_plan(
    plans: dict[int, tuple[int, dict]], line_dict: dict, rank: int
) -> None:
    """Keep the strongest plan seen for a query; later ties win."""
    query_id = line_dict["executionId"]
    existing = plans.get(query_id)
    if existing is None or rank >= existing[0]:
        plans[query_id] = (rank, line_dict)


def parse_log(
    log_path: str | Path, out_name: str | None = None, strict: bool = False
) -> ParsedLog:
    """Parse a single event-log file. See :func:`parse_source` for directories
    of rolled segments."""
    return parse_source(single_file_source(str(log_path)), out_name, strict=strict)


def get_parsed_metrics(
    log_dir: str | Path = "data/logs/raw",
    log_file: str | None = None,
    out_dir: str | None = "data/logs/parsed",
    out_name: str | None = None,
    out_format: OutputFormat | None = OutputFormat.csv,
    verbose: bool = False,
    strict: bool = False,
) -> ParsedLogDataFrames:
    """Parse one event-log source from ``log_dir`` into DataFrames.

    ``log_file`` selects a source explicitly (file name, rolling-log directory
    name, application id, or full path). Without it the newest source is used.
    Use :func:`get_all_parsed_metrics` to parse every application in a
    directory.
    """
    _configure_logging(verbose)
    source = resolve_source(_resolve_log_dir(log_dir), log_file)
    return _parse_and_write(source, out_dir, out_name, out_format, strict)


def get_all_parsed_metrics(
    log_dir: str | Path = "data/logs/raw",
    out_dir: str | None = "data/logs/parsed",
    out_format: OutputFormat | None = OutputFormat.csv,
    verbose: bool = False,
    strict: bool = False,
) -> dict[str, ParsedLogDataFrames]:
    """Parse every application under ``log_dir``, keyed by source name.

    Query ids restart at zero in each application, so results are kept per
    source rather than concatenated.
    """
    _configure_logging(verbose)
    resolved_dir = _resolve_log_dir(log_dir)
    sources = discover_sources(resolved_dir)
    if not sources:
        raise EventLogNotFoundError(f"No event log files found in: {resolved_dir}")

    results: dict[str, ParsedLogDataFrames] = {}
    for source in sorted(sources, key=lambda item: item.name):
        results[source.name] = _parse_and_write(
            source, out_dir, None, out_format, strict, disambiguate=True
        )
    return results


def _configure_logging(verbose: bool) -> None:
    if verbose:
        logging.basicConfig(
            level=logging.DEBUG,
            format="%(asctime)s.%(msecs)03d %(message)s",
            datefmt="%Y-%m-%dT%H:%M:%S",
        )
    else:
        logging.basicConfig(level=logging.INFO, format="%(message)s")


def _resolve_log_dir(log_dir: str | Path) -> str:
    if is_cloud_path(str(log_dir)):
        return str(log_dir)
    return str(resolve_dir(log_dir))


def _parse_and_write(
    source: EventLogSource,
    out_dir: str | None,
    out_name: str | None,
    out_format: OutputFormat | None,
    strict: bool,
    disambiguate: bool = False,
) -> ParsedLogDataFrames:
    logging.info(f"Reading event log: {source.root_uri or source.uris}")

    result = parse_source(source, out_name, strict=strict)
    for diagnostic in result.diagnostics:
        logging.info(f"[{diagnostic.code}] {diagnostic.message}")

    dag_df = log_to_dag_df(result)
    combined_df = log_to_combined_df(result, dag_df, source.name)

    output = ParsedLogDataFrames(
        combined=combined_df,
        dag=dag_df,
        job_stage=job_stage_associations(result),
        query_stage=query_stage_associations(dag_df),
        diagnostics=result.diagnostics,
    )

    if out_dir is None or out_format is None:
        logging.info("Skipping writing parsed log")
        return output

    parsed_name = result.name
    if disambiguate and out_name is None:
        # Two applications reading the same paths in the same second derive the
        # same name; the application identity keeps their outputs apart.
        parsed_name = f"{parsed_name}__{source.name}"[:160]

    if out_format == OutputFormat.csv:
        dag_df = (
            dag_df.explode("accumulators")
            .with_columns(
                pl.col("accumulators")
                .name.map_fields(lambda x: "accumulators_" + x)
                .alias("accumulators")
            )
            .unnest("accumulators")
            .explode("accumulator_totals")
            .with_columns(
                pl.col("accumulator_totals")
                .name.map_fields(lambda x: "accumulator_totals_" + x)
                .alias("accumulator_totals")
            )
            .unnest("accumulator_totals")
        )

        combined_df = combined_df.with_columns(
            pl.col("nodes").list.join(", ").alias("nodes")
        )

    write_parsed_log(
        df=dag_df,
        out_dir=out_dir,
        out_format=out_format,
        parsed_name=parsed_name,
        suffix="_dag",
    )

    write_parsed_log(
        df=combined_df,
        out_dir=out_dir,
        out_format=out_format,
        parsed_name=parsed_name,
        suffix="_combined",
    )

    return output
