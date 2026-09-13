"""Pure normalization of raw Databricks responses into a :class:`RunReport`.

Nothing here spawns a process or touches the network; it only transforms the
raw dictionaries produced by :mod:`sparkparse.databricks`. That keeps the
report contract testable offline and independent of the CLI.

Measure semantics are preserved per source. Workflow elapsed time is never
derived from summed task durations, and a Query History subtotal over only the
queries that reported a metric is labelled an *observed subtotal*, never a
complete total.
"""

from __future__ import annotations

import datetime
import statistics
from dataclasses import dataclass
from typing import Any

from sparkparse.databricks import RawRunCollection
from sparkparse.models import (
    ComputeReference,
    MetricAggregate,
    MetricCompleteness,
    MetricUnit,
    OutputAttachment,
    QueryMetrics,
    QueryObservation,
    ReportDiagnostic,
    ReportObservation,
    RevisionEvidence,
    RunReport,
    TaskTiming,
    WorkflowRunIdentity,
    WorkflowRunStatus,
    WorkflowTask,
    WorkflowTiming,
)

QUERY_METRIC_SOURCE_VERSION = "query_history.v1"
MAX_OUTPUT_CHARS = 4000
SHORT_EXCERPT_CHARS = 280


@dataclass(frozen=True)
class _MetricSpec:
    canonical: str
    raw: str
    unit: MetricUnit
    aggregation: str
    detail: str | None = None


QUERY_METRIC_SPECS: tuple[_MetricSpec, ...] = (
    _MetricSpec("read_bytes", "read_bytes", MetricUnit.bytes, "sum"),
    _MetricSpec("read_remote_bytes", "read_remote_bytes", MetricUnit.bytes, "sum"),
    _MetricSpec("read_cache_bytes", "read_cache_bytes", MetricUnit.bytes, "sum"),
    _MetricSpec(
        "written_bytes_remote",
        "write_remote_bytes",
        MetricUnit.bytes,
        "sum",
        "remote persistent bytes written",
    ),
    _MetricSpec("disk_spill_bytes", "spill_to_disk_bytes", MetricUnit.bytes, "sum"),
    _MetricSpec(
        "aggregate_task_time_ms",
        "task_total_time_ms",
        MetricUnit.milliseconds,
        "sum",
        "sum of query task execution time, not elapsed",
    ),
    _MetricSpec(
        "query_execution_ms", "execution_time_ms", MetricUnit.milliseconds, "sum"
    ),
    _MetricSpec("query_total_ms", "total_time_ms", MetricUnit.milliseconds, "sum"),
    _MetricSpec("rows_read", "rows_read_count", MetricUnit.rows, "sum"),
    _MetricSpec("rows_produced", "rows_produced_count", MetricUnit.rows, "sum"),
    _MetricSpec(
        "shuffle_network_bytes",
        "network_sent_bytes",
        MetricUnit.bytes,
        "sum",
    ),
    _MetricSpec("read_files", "read_files_count", MetricUnit.items, "sum"),
    _MetricSpec("read_partitions", "read_partitions_count", MetricUnit.items, "sum"),
)

_TASK_KIND_KEYS = (
    "notebook_task",
    "spark_python_task",
    "python_wheel_task",
    "sql_task",
    "dbt_task",
    "pipeline_task",
    "run_job_task",
    "for_each_task",
    "condition_task",
    "spark_jar_task",
    "spark_submit_task",
)


def _first_present(mapping: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        value = mapping.get(key)
        if value is not None:
            return value
    return None


def normalize_identity(
    run: dict[str, Any], workspace_host: str | None
) -> WorkflowRunIdentity:
    run_page_url = run.get("run_page_url")
    return WorkflowRunIdentity(
        run_id=str(run.get("run_id")),
        job_id=_as_str(run.get("job_id")),
        run_name=run.get("run_name"),
        run_type=run.get("run_type"),
        number_in_job=run.get("number_in_job"),
        original_attempt_run_id=_as_str(run.get("original_attempt_run_id")),
        run_page_url=run_page_url,
        workspace_host=workspace_host or _host_from_url(run_page_url),
        creator_user_name=run.get("creator_user_name"),
    )


def normalize_status(run: dict[str, Any]) -> WorkflowRunStatus:
    state = run.get("state") or {}
    termination = (run.get("status") or {}).get("termination_details") or {}
    return WorkflowRunStatus(
        life_cycle_state=state.get("life_cycle_state"),
        result_state=state.get("result_state"),
        state_message=state.get("state_message"),
        termination_code=termination.get("code"),
        termination_type=termination.get("type"),
        user_cancelled_or_timedout=state.get("user_cancelled_or_timedout"),
    )


def normalize_timing(
    run: dict[str, Any], tasks: list[dict[str, Any]]
) -> WorkflowTiming:
    summed = [t.get("execution_duration") for t in tasks]
    measured = [int(v) for v in summed if isinstance(v, int | float)]
    return WorkflowTiming(
        workflow_elapsed_ms=_as_int(run.get("run_duration")),
        setup_ms=_as_int(run.get("setup_duration")),
        execution_ms=_as_int(run.get("execution_duration")),
        cleanup_ms=_as_int(run.get("cleanup_duration")),
        summed_task_execution_ms=sum(measured) if measured else None,
    )


def normalize_tasks(
    raw: RawRunCollection,
) -> tuple[list[WorkflowTask], list[ComputeReference]]:
    tasks: list[WorkflowTask] = []
    compute: list[ComputeReference] = []
    for task in raw.tasks:
        state = task.get("state") or {}
        task_key = str(task.get("task_key", "unknown"))
        kind_key = next((k for k in _TASK_KIND_KEYS if task.get(k) is not None), None)
        reference = _compute_reference(task_key, task, raw)
        if reference is not None:
            compute.append(reference)
        attachment = _attachment_for(task, raw)
        tasks.append(
            WorkflowTask(
                task_key=task_key,
                run_id=_as_str(task.get("run_id")),
                attempt_number=task.get("attempt_number"),
                original_attempt_run_id=_as_str(task.get("original_attempt_run_id")),
                run_if=task.get("run_if"),
                state=state.get("life_cycle_state"),
                result_state=state.get("result_state"),
                timing=TaskTiming(
                    setup_ms=_as_int(task.get("setup_duration")),
                    execution_ms=_as_int(task.get("execution_duration")),
                    cleanup_ms=_as_int(task.get("cleanup_duration")),
                    start_time_ms=_as_int(task.get("start_time")),
                    end_time_ms=_as_int(task.get("end_time")),
                ),
                compute=reference,
                task_kind=kind_key.removesuffix("_task") if kind_key else None,
                is_nested_job=kind_key == "run_job_task",
                output_path=attachment.path,
                output_status=attachment.status,
                error_excerpt=attachment.excerpt,
            )
        )
    return tasks, compute


def _compute_reference(
    task_key: str, task: dict[str, Any], raw: RawRunCollection
) -> ComputeReference | None:
    new_cluster = task.get("new_cluster")
    if isinstance(new_cluster, dict):
        return ComputeReference(
            task_key=task_key,
            environment_key=task.get("environment_key"),
            cluster_id=_as_str(new_cluster.get("cluster_id")),
            source="run_snapshot",
            runtime_engine=new_cluster.get("runtime_engine"),
            spark_version=new_cluster.get("spark_version"),
            node_type_id=new_cluster.get("node_type_id"),
            num_workers=new_cluster.get("num_workers"),
            data_security_mode=new_cluster.get("data_security_mode"),
            performance_target=task.get("effective_performance_target"),
        )
    existing_cluster_id = task.get("existing_cluster_id")
    if existing_cluster_id is not None:
        lookup = raw.cluster_lookups.get(str(existing_cluster_id), {})
        return ComputeReference(
            task_key=task_key,
            environment_key=task.get("environment_key"),
            cluster_id=str(existing_cluster_id),
            source="current_lookup",
            runtime_engine=lookup.get("runtime_engine"),
            spark_version=lookup.get("spark_version"),
            node_type_id=(
                lookup.get("node_type_id") or lookup.get("driver_node_type_id")
            ),
            num_workers=lookup.get("num_workers"),
            data_security_mode=lookup.get("data_security_mode"),
            performance_target=task.get("effective_performance_target"),
        )
    if task.get("environment_key") is not None:
        return ComputeReference(
            task_key=task_key,
            environment_key=task.get("environment_key"),
            source="run_snapshot",
            performance_target=task.get("effective_performance_target"),
        )
    return None


def _attachment_for(task: dict[str, Any], raw: RawRunCollection) -> OutputAttachment:
    task_key = str(task.get("task_key", "unknown"))
    task_run_id = task.get("run_id")
    if task_run_id is None:
        return OutputAttachment(task_key=task_key, status="missing")
    response = raw.outputs.get(str(task_run_id))
    if response is None:
        return OutputAttachment(
            task_key=task_key, run_id=str(task_run_id), status="not_requested"
        )
    error = response.get("error")
    if error:
        return OutputAttachment(
            task_key=task_key,
            run_id=str(task_run_id),
            status="error",
            error=str(error),
            excerpt=_excerpt(str(error)),
        )
    notebook_output = response.get("notebook_output") or {}
    result = notebook_output.get("result")
    if result is not None:
        text, truncated = _bounded(str(result))
        return OutputAttachment(
            task_key=task_key,
            run_id=str(task_run_id),
            status="truncated"
            if notebook_output.get("truncated") or truncated
            else "saved",
            excerpt=text,
        )
    logs = response.get("logs")
    if logs:
        text, truncated = _bounded(str(logs))
        return OutputAttachment(
            task_key=task_key,
            run_id=str(task_run_id),
            status="truncated" if truncated else "saved",
            excerpt=text,
        )
    return OutputAttachment(
        task_key=task_key, run_id=str(task_run_id), status="unsupported"
    )


def normalize_revision(
    run: dict[str, Any], asserted_revision: str | None, artifact_digest: str | None
) -> RevisionEvidence:
    sources: list[dict[str, Any]] = []
    if isinstance(run.get("git_source"), dict):
        sources.append(run["git_source"])
    for task in run.get("tasks") or []:
        if isinstance(task.get("git_source"), dict):
            sources.append(task["git_source"])

    commits = {
        str((source.get("git_snapshot") or {}).get("used_commit"))
        for source in sources
        if (source.get("git_snapshot") or {}).get("used_commit")
    }
    first = sources[0] if sources else {}
    snapshot = first.get("git_snapshot") or {}
    confidence = "unknown"
    executed_commit: str | None = None
    scope: str | None = None
    if len(commits) == 1:
        executed_commit = next(iter(commits))
        confidence = "executed"
        scope = "run" if len(sources) == 1 else "mixed"
    elif len(commits) > 1:
        confidence = "mixed"
        scope = "mixed"
    elif asserted_revision is not None:
        confidence = "asserted"

    return RevisionEvidence(
        executed_commit=executed_commit,
        repository_url=first.get("git_url"),
        repository_provider=first.get("git_provider"),
        branch=first.get("git_branch"),
        commit_timestamp_ms=_as_int(snapshot.get("commit_timestamp")),
        scope=scope,
        asserted_revision=asserted_revision,
        artifact_digest=artifact_digest,
        confidence=confidence,
    )


def normalize_outputs(raw: RawRunCollection) -> list[OutputAttachment]:
    return [_attachment_for(task, raw) for task in raw.tasks]


def aggregate_queries(raw: RawRunCollection, tasks: list[WorkflowTask]) -> QueryMetrics:
    task_run_to_key = {
        task.run_id: task.task_key for task in tasks if task.run_id is not None
    }
    run_id = str(raw.run.get("run_id"))
    attribution_counts: dict[str, int] = {}
    observations: dict[str, QueryObservation] = {}
    collected_at = datetime.datetime.now(datetime.UTC)

    for query in raw.queries:
        source = query.get("query_source") or {}
        job_info = source.get("job_info") or {}
        task_run_id = job_info.get("job_task_run_id")
        parent_run_id = job_info.get("job_run_id")
        if task_run_id is not None and str(task_run_id) in task_run_to_key:
            attribution = "task_run"
        elif parent_run_id is not None and str(parent_run_id) == run_id:
            attribution = "run_parent"
        elif task_run_id is not None or parent_run_id is not None:
            attribution = "unrelated"
        else:
            attribution = "unattributed"
        attribution_counts[attribution] = attribution_counts.get(attribution, 0) + 1
        if attribution not in ("task_run", "run_parent"):
            continue
        observation = QueryObservation(
            query_id=str(query.get("query_id")),
            status=query.get("status"),
            is_final=query.get("is_final"),
            execution_end_time_ms=_as_int(query.get("execution_end_time_ms")),
            collected_at=collected_at,
            job_id=_as_str(job_info.get("job_id")),
            job_run_id=_as_str(parent_run_id),
            job_task_run_id=_as_str(task_run_id),
            attribution=attribution,
            task_key=task_run_to_key.get(str(task_run_id)),
            warehouse_id=_as_str(query.get("warehouse_id")),
            metrics=_numeric_metrics(query.get("metrics")),
        )
        existing = observations.get(observation.query_id)
        if existing is None or _observation_is_fresher(observation, existing):
            observations[observation.query_id] = observation

    kept = list(observations.values())
    aggregates = _aggregate_metrics(kept, raw.query_discovery_complete)
    unavailable = [
        "memory_bytes_spilled",
        "per_executor_distributions",
        "query_profiles",
    ]
    return QueryMetrics(
        discovered_query_count=raw.discovered_query_count,
        matched_query_count=len(kept),
        attribution_counts=attribution_counts,
        aggregates=aggregates,
        discovery_complete=raw.query_discovery_complete,
        denied=raw.query_denied,
        observations=kept,
        unavailable_measures=unavailable,
    )


def _aggregate_metrics(
    observations: list[QueryObservation], discovery_complete: bool
) -> list[MetricAggregate]:
    total = len(observations)
    aggregates: list[MetricAggregate] = []
    for spec in QUERY_METRIC_SPECS:
        values: list[int] = []
        for observation in observations:
            value = observation.metrics.get(spec.raw)
            if isinstance(value, bool):
                continue
            if isinstance(value, int | float):
                values.append(int(value))
        if not values:
            completeness = MetricCompleteness.unavailable
            value_exact: int | None = None
            value: float | None = None
        else:
            value_exact = sum(values)
            value = float(value_exact)
            completeness = (
                MetricCompleteness.complete
                if len(values) == total and discovery_complete and total > 0
                else MetricCompleteness.observed_subtotal
            )
        aggregates.append(
            MetricAggregate(
                metric=spec.canonical,
                unit=spec.unit,
                source="query_history",
                source_version=QUERY_METRIC_SOURCE_VERSION,
                aggregation=spec.aggregation,
                observed_query_count=total,
                counted_query_count=len(values),
                value=value,
                value_exact=value_exact,
                completeness=completeness,
                detail=spec.detail,
            )
        )
    cached = sum(
        1
        for observation in observations
        if observation.metrics.get("result_from_cache")
    )
    cached_counted = sum(
        1 for observation in observations if "result_from_cache" in observation.metrics
    )
    if total == 0 or cached_counted == 0:
        cache_value: float | None = None
        cache_exact: int | None = None
        cache_completeness = MetricCompleteness.unavailable
    else:
        cache_value = float(cached)
        cache_exact = cached
        cache_completeness = (
            MetricCompleteness.complete
            if cached_counted == total and discovery_complete
            else MetricCompleteness.observed_subtotal
        )
    aggregates.append(
        MetricAggregate(
            metric="queries_result_from_cache",
            unit=MetricUnit.items,
            source="query_history",
            source_version=QUERY_METRIC_SOURCE_VERSION,
            aggregation="count_true",
            observed_query_count=total,
            counted_query_count=cached_counted,
            value=cache_value,
            value_exact=cache_exact,
            completeness=cache_completeness,
            detail="count of queries whose result was served from cache",
        )
    )
    return aggregates


def build_report(
    raw: RawRunCollection,
    *,
    asserted_revision: str | None = None,
    artifact_digest: str | None = None,
    workspace_host: str | None = None,
) -> RunReport:
    """Normalize a raw collection into a versioned report envelope."""
    identity = normalize_identity(raw.run, workspace_host)
    tasks, compute = normalize_tasks(raw)
    query_metrics = aggregate_queries(raw, tasks)
    revision = normalize_revision(raw.run, asserted_revision, artifact_digest)
    diagnostics = list(raw.diagnostics)
    if revision.confidence == "mixed":
        diagnostics.append(
            ReportDiagnostic(
                code="mixed_revision",
                message="tasks report different Git commits; no single SHA represents the run",
                severity="info",
            )
        )
    observations, next_checks = _derive_observations(
        tasks, raw, query_metrics, diagnostics
    )
    coverage = _coverage(raw, tasks, query_metrics)
    return RunReport(
        collection_status=raw.collection_status,
        collected_at=datetime.datetime.now(datetime.UTC),
        collection_time_ms=raw.collection_time_ms,
        identity=identity,
        status=normalize_status(raw.run),
        timing=normalize_timing(raw.run, raw.tasks),
        tasks=tasks,
        query_metrics=query_metrics,
        compute=compute,
        revision=revision,
        coverage=coverage,
        limits=raw.limits,
        diagnostics=diagnostics,
        observations=observations,
        next_checks=next_checks,
        outputs=normalize_outputs(raw),
    )


def _coverage(
    raw: RawRunCollection, tasks: list[WorkflowTask], query_metrics: QueryMetrics
) -> dict[str, str]:
    def status(condition: bool) -> str:
        return "available" if condition else "unavailable"

    return {
        "jobs": "available" if raw.run else "unavailable",
        "tasks": status(bool(tasks)),
        "task_timing": status(any(t.timing.execution_ms is not None for t in tasks)),
        "query_history": (
            "denied"
            if query_metrics.denied
            else (
                "available"
                if query_metrics.matched_query_count
                else ("unavailable" if not raw.queries else "unattributed")
            )
        ),
        "query_metrics": (
            "complete"
            if query_metrics.discovery_complete and query_metrics.matched_query_count
            else "partial"
            if query_metrics.matched_query_count
            else "unavailable"
        ),
        "git_revision": "available" if raw.run.get("git_source") else "unknown",
        "compute": status(bool(raw.run.get("tasks"))),
        "child_tasks": (
            "incomplete"
            if any(t.is_nested_job or t.task_kind == "for_each" for t in tasks)
            else "not_applicable"
        ),
        "memory_spill": "unavailable",
    }


def _derive_observations(
    tasks: list[WorkflowTask],
    raw: RawRunCollection,
    query_metrics: QueryMetrics,
    diagnostics: list[ReportDiagnostic],
) -> tuple[list[ReportObservation], list[str]]:
    observations: list[ReportObservation] = []
    next_checks: list[str] = []

    failed = [t for t in tasks if t.result_state in ("FAILED", "TIMEDOUT", "CANCELED")]
    if failed:
        observations.append(
            ReportObservation(
                kind="failed_tasks",
                summary=f"{len(failed)} task(s) did not succeed",
                evidence={"task_keys": [t.task_key for t in failed]},
            )
        )
        next_checks.append(
            "Inspect the failed task output excerpts for the root error."
        )

    nested = [t for t in tasks if t.is_nested_job or t.task_kind == "for_each"]
    if nested:
        diagnostics.append(
            ReportDiagnostic(
                code="incomplete_child_coverage",
                message=(
                    f"{len(nested)} for-each/nested-job task(s) were not unrolled in v1; "
                    "their child runs are counted as one task and excluded from "
                    "aggregates that require child detail"
                ),
                severity="info",
            )
        )

    executions = [t.timing.execution_ms for t in tasks if t.timing.execution_ms]
    if len(executions) >= 3:
        median = statistics.median(executions)
        slow = sorted(
            (t for t in tasks if (t.timing.execution_ms or 0) > 2 * median),
            key=lambda t: t.timing.execution_ms or 0,
            reverse=True,
        )
        if slow:
            observations.append(
                ReportObservation(
                    kind="slow_tasks",
                    summary=f"{len(slow)} task(s) exceeded twice the median execution time",
                    evidence={
                        "median_execution_ms": median,
                        "tasks": {t.task_key: t.timing.execution_ms for t in slow[:5]},
                    },
                    hypothesis="The slow tasks may reflect data skew or an under-provisioned stage.",
                )
            )
            next_checks.append(
                "Compare the slow tasks' input sizes and partition counts."
            )

    spill = _aggregate_value(query_metrics, "disk_spill_bytes")
    if spill:
        observations.append(
            ReportObservation(
                kind="disk_spill",
                summary="Query History reports bytes temporarily spilled to disk",
                evidence={"disk_spill_bytes": spill},
                hypothesis="Spill may indicate insufficient memory or shuffle skew.",
            )
        )
        next_checks.append(
            "Open the query profile for the spilling query to find the operator."
        )

    read_bytes = _aggregate_value(query_metrics, "read_bytes")
    if read_bytes:
        observations.append(
            ReportObservation(
                kind="read_bytes",
                summary="Query History reports bytes read",
                evidence={"read_bytes": read_bytes},
            )
        )

    if query_metrics.attribution_counts.get("unattributed"):
        diagnostics.append(
            ReportDiagnostic(
                code="unattributed_queries",
                message=(
                    f"{query_metrics.attribution_counts['unattributed']} query/queries in the "
                    "run window had no source identity and were not attributed"
                ),
                severity="info",
            )
        )
    if not query_metrics.discovery_complete:
        diagnostics.append(
            ReportDiagnostic(
                code="query_discovery_incomplete",
                message="Query History pagination did not complete; metrics may be partial",
                severity="warning",
            )
        )
    return observations, next_checks


def _aggregate_value(metrics: QueryMetrics, canonical: str) -> int | None:
    for aggregate in metrics.aggregates:
        if aggregate.metric == canonical and aggregate.value_exact is not None:
            return aggregate.value_exact
    return None


def _numeric_metrics(metrics: Any) -> dict[str, Any]:
    if not isinstance(metrics, dict):
        return {}
    return {
        key: value
        for key, value in metrics.items()
        if isinstance(value, int | float | bool) and not isinstance(value, dict)
    }


def _observation_is_fresher(
    candidate: QueryObservation, existing: QueryObservation
) -> bool:
    if candidate.is_final and not existing.is_final:
        return True
    candidate_end = candidate.execution_end_time_ms or 0
    existing_end = existing.execution_end_time_ms or 0
    return candidate_end >= existing_end


def _bounded(text: str) -> tuple[str, bool]:
    if len(text) <= MAX_OUTPUT_CHARS:
        return text, False
    return text[:MAX_OUTPUT_CHARS], True


def _excerpt(text: str) -> str:
    text = " ".join(text.split())
    if len(text) <= SHORT_EXCERPT_CHARS:
        return text
    return text[:SHORT_EXCERPT_CHARS] + "…"


def _as_int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int | float):
        return int(value)
    return None


def _as_str(value: Any) -> str | None:
    if value is None:
        return None
    return str(value)


def _host_from_url(url: str | None) -> str | None:
    if not url:
        return None
    return url.split("//", 1)[-1].split("/", 1)[0]


def format_report_text(report: RunReport) -> str:
    """Human-readable rendering of the same facts the JSON envelope carries."""
    lines: list[str] = []
    identity = report.identity
    lines.append(
        f"Run {identity.run_id}  job={identity.job_id}  {identity.run_name or ''}"
    )
    status = report.status
    lines.append(
        f"Status: {status.life_cycle_state or '?'} / {status.result_state or '?'}"
        f"  collection={report.collection_status.value}"
    )
    timing = report.timing
    lines.append(
        "Timing: "
        f"elapsed={_fmt_ms(timing.workflow_elapsed_ms)} "
        f"setup={_fmt_ms(timing.setup_ms)} "
        f"execution={_fmt_ms(timing.execution_ms)} "
        f"cleanup={_fmt_ms(timing.cleanup_ms)} "
        f"summed_task_execution={_fmt_ms(timing.summed_task_execution_ms)}"
    )
    lines.append(
        f"Revision: {report.revision.confidence} {report.revision.executed_commit or ''}"
    )
    lines.append(f"Tasks ({len(report.tasks)}):")
    for task in report.tasks:
        lines.append(
            f"  {task.task_key}\t{task.result_state or task.state or '?'}"
            f"\texecution={_fmt_ms(task.timing.execution_ms)}"
            f"\tsetup={_fmt_ms(task.timing.setup_ms)}"
            + (f"\t{task.error_excerpt}" if task.error_excerpt else "")
        )
    if report.query_metrics is not None:
        metrics = report.query_metrics
        lines.append(
            f"Queries: discovered={metrics.discovered_query_count} "
            f"matched={metrics.matched_query_count} "
            f"discovery_complete={metrics.discovery_complete} "
            f"denied={metrics.denied}"
        )
        for aggregate in metrics.aggregates:
            if aggregate.value is None:
                continue
            lines.append(
                f"  {aggregate.metric}={aggregate.value:g} {aggregate.unit.value}"
                f" [{aggregate.completeness.value}"
                f" {aggregate.counted_query_count}/{aggregate.observed_query_count}]"
            )
    if report.observations:
        lines.append("Observations:")
        for observation in report.observations:
            lines.append(f"  [{observation.kind}] {observation.summary}")
            if observation.hypothesis:
                lines.append(f"    hypothesis: {observation.hypothesis}")
    if report.next_checks:
        lines.append("Next checks:")
        for check in report.next_checks:
            lines.append(f"  - {check}")
    if report.diagnostics:
        lines.append("Diagnostics:")
        for diagnostic in report.diagnostics:
            lines.append(
                f"  [{diagnostic.severity}] {diagnostic.code}: {diagnostic.message}"
            )
    lines.append(
        f"Coverage: {', '.join(f'{k}={v}' for k, v in sorted(report.coverage.items()))}"
    )
    return "\n".join(lines)


def _fmt_ms(value: int | None) -> str:
    if value is None:
        return "n/a"
    return f"{value}ms"
