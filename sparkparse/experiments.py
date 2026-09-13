"""Explicit performance experiments: portable snapshots and comparisons.

An experiment is a local directory holding an editable ``experiment.json``
manifest plus immutable per-run snapshots. Collection is append-only; a
recollection of the same run and variant appends a snapshot and updates the
trial reference rather than counting as a new trial. Comparisons consume saved
snapshots only and never require the network.

Storage is local by design. Cloud URIs are rejected clearly, and manifest
updates use atomic local file replacement (one writer per experiment).
"""

from __future__ import annotations

import datetime
import os
import statistics
import uuid
from pathlib import Path
from typing import Any

from sparkparse.models import (
    ComparabilityNote,
    ComparisonGroup,
    ExperimentComparison,
    ExperimentManifest,
    MetricCompleteness,
    MetricDelta,
    MetricUnit,
    OutputAttachment,
    RunReport,
    TaskDelta,
    TrialMetadata,
    TrialRef,
    TrialSnapshot,
)
from sparkparse.storage import is_cloud_path, list_files

MANIFEST_NAME = "experiment.json"
RUNS_DIR = "runs"
SNAPSHOT_SUFFIX = ".json"

_RUN_METRICS = (
    "workflow_elapsed_ms",
    "task_execution_ms",
)


class ExperimentError(Exception):
    """A user-facing experiment store or comparison problem."""


def _utcnow() -> datetime.datetime:
    return datetime.datetime.now(datetime.UTC)


def new_snapshot_id(now: datetime.datetime | None = None) -> str:
    stamp = (now or _utcnow()).strftime("%Y%m%dT%H%M%S")
    return f"{stamp}-{uuid.uuid4().hex[:8]}"


def _require_local(path: str, label: str) -> str:
    if is_cloud_path(path):
        raise ExperimentError(
            f"{label} must be a local path; cloud URIs are not supported for experiments"
        )
    return path


def _atomic_write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(text)
    os.replace(tmp, path)


def snapshot_path(base_dir: str | Path, run_id: str, snapshot_id: str) -> Path:
    return Path(base_dir) / RUNS_DIR / str(run_id) / f"{snapshot_id}{SNAPSHOT_SUFFIX}"


def save_snapshot(base_dir: str | Path, snapshot: TrialSnapshot) -> Path:
    """Write one immutable snapshot, plus any bounded output attachments."""
    base = Path(_require_local(str(base_dir), "snapshot directory"))
    # Attachments are stored relative to the snapshot directory so copying the
    # experiment preserves offline access.
    for attachment in snapshot.report.outputs:
        if attachment.excerpt is None:
            continue
        safe_key = "".join(
            c if c.isalnum() or c in "-_." else "_" for c in attachment.task_key
        )
        rel = (
            f"{RUNS_DIR}/{snapshot.run_id}/"
            f"{snapshot.snapshot_id}.outputs/{safe_key}.txt"
        )
        _atomic_write(base / rel, attachment.excerpt)
        attachment.path = rel
    for task in snapshot.report.tasks:
        attachment = _attachment_for_task(snapshot.report.outputs, task.run_id)
        if attachment is not None:
            task.output_path = attachment.path
    path = snapshot_path(base, snapshot.run_id, snapshot.snapshot_id)
    _atomic_write(path, snapshot.model_dump_json(indent=2) + "\n")
    return path


def _attachment_for_task(
    attachments: list[OutputAttachment], run_id: str | None
) -> OutputAttachment | None:
    if run_id is None:
        return None
    for attachment in attachments:
        if attachment.run_id == run_id:
            return attachment
    return None


def load_snapshot(path: str | Path) -> TrialSnapshot:
    path_obj = Path(path)
    try:
        return TrialSnapshot.model_validate_json(path_obj.read_text())
    except (OSError, ValueError) as exc:
        raise ExperimentError(f"invalid snapshot {path}: {exc}") from exc


def list_snapshot_paths(base_dir: str | Path) -> list[str]:
    base = Path(base_dir)
    if not base.exists():
        return []
    return sorted(list_files(base, f"{RUNS_DIR}/*/*{SNAPSHOT_SUFFIX}"))


def trial_snapshot_path(base_dir: str | Path, run_id: str, snapshot_id: str) -> Path:
    return snapshot_path(base_dir, run_id, snapshot_id)


def manifest_path(exp_dir: str | Path) -> Path:
    return Path(exp_dir) / MANIFEST_NAME


def load_manifest(exp_dir: str | Path) -> ExperimentManifest:
    path = manifest_path(exp_dir)
    if not path.exists():
        raise ExperimentError(f"no experiment manifest at {path}")
    try:
        manifest = ExperimentManifest.model_validate_json(path.read_text())
    except ValueError as exc:
        raise ExperimentError(f"invalid experiment manifest {path}: {exc}") from exc
    if (
        manifest.schema_version
        != ExperimentManifest.model_fields["schema_version"].default
    ):
        raise ExperimentError(
            f"unsupported experiment schema {manifest.schema_version!r}"
        )
    return manifest


def save_manifest(exp_dir: str | Path, manifest: ExperimentManifest) -> Path:
    manifest.updated_at = _utcnow()
    path = manifest_path(exp_dir)
    _atomic_write(path, manifest.model_dump_json(indent=2) + "\n")
    return path


def build_snapshot(
    report: RunReport,
    *,
    trial: TrialMetadata,
    snapshot_id: str | None = None,
) -> TrialSnapshot:
    return TrialSnapshot(
        snapshot_id=snapshot_id or new_snapshot_id(),
        collected_at=report.collected_at,
        run_id=report.identity.run_id,
        workspace_host=report.identity.workspace_host,
        job_id=report.identity.job_id,
        trial=trial,
        report=report,
    )


def record_trial(
    exp_dir: str | Path,
    report: RunReport,
    *,
    variant: str,
    revision: str | None = None,
    artifact_digest: str | None = None,
    config_fingerprint: str | None = None,
    input_snapshot: str | None = None,
    warmup: bool | None = None,
    correctness: str | None = None,
    hypothesis: str | None = None,
    notes: str | None = None,
    snapshot_id: str | None = None,
) -> tuple[TrialRef, Path]:
    """Save a snapshot and add or update its trial in the manifest.

    A trial is identified by its run. Recollecting a run updates its existing
    trial instead of adding another, so relabeling a run with a new variant
    moves the trial rather than duplicating the execution. Metadata supplied on
    a recollection overrides the stored value; omitted metadata is preserved.
    """
    exp = Path(_require_local(str(exp_dir), "experiment directory"))
    if manifest_path(exp).exists():
        manifest = load_manifest(exp)
        _check_compatible(manifest, report)
    else:
        now = _utcnow()
        manifest = ExperimentManifest(
            experiment_id=uuid.uuid4().hex,
            workspace_host=report.identity.workspace_host,
            job_id=report.identity.job_id,
            created_at=now,
            updated_at=now,
        )
    existing = _find_trial(manifest, report.identity.run_id)

    derived_config = _compute_fingerprint(report)
    if config_fingerprint is not None:
        config_value = config_fingerprint
        config_derived = False
    elif (
        existing is not None
        and not existing.config_fingerprint_derived
        and existing.config_fingerprint
    ):
        # An explicitly asserted fingerprint is authoritative and preserved.
        config_value = existing.config_fingerprint
        config_derived = False
    else:
        # Derived identity is recomputed so late enrichment cannot leave a
        # stale fingerprint attached to an updated report.
        config_value = derived_config
        config_derived = True
    trial = TrialMetadata(
        variant=variant,
        revision=revision if revision is not None else _existing(existing, "revision"),
        artifact_digest=artifact_digest,
        config_fingerprint=config_value,
        config_fingerprint_derived=config_derived,
        input_snapshot=(
            input_snapshot
            if input_snapshot is not None
            else _existing(existing, "input_snapshot")
        ),
        warmup=warmup if warmup is not None else _existing(existing, "warmup", False),
        correctness=(
            correctness
            if correctness is not None
            else _existing(existing, "correctness")
        ),
        hypothesis=hypothesis,
        notes=notes,
        workspace_host=report.identity.workspace_host,
        job_id=report.identity.job_id,
    )
    snapshot = build_snapshot(report, trial=trial, snapshot_id=snapshot_id)
    path = save_snapshot(exp, snapshot)

    if existing is not None:
        existing.variant = variant
        existing.snapshot_id = snapshot.snapshot_id
        existing.revision = trial.revision
        existing.config_fingerprint = trial.config_fingerprint
        existing.config_fingerprint_derived = trial.config_fingerprint_derived
        existing.input_snapshot = trial.input_snapshot
        existing.warmup = trial.warmup
        existing.correctness = trial.correctness
        ref = existing
    else:
        ref = TrialRef(
            trial_id=uuid.uuid4().hex[:12],
            variant=variant,
            run_id=report.identity.run_id,
            snapshot_id=snapshot.snapshot_id,
            revision=trial.revision,
            config_fingerprint=trial.config_fingerprint,
            config_fingerprint_derived=trial.config_fingerprint_derived,
            input_snapshot=trial.input_snapshot,
            workspace_host=report.identity.workspace_host,
            job_id=report.identity.job_id,
            created_at=snapshot.collected_at,
            warmup=trial.warmup,
            correctness=trial.correctness,
        )
        manifest.trials.append(ref)
    if manifest.baseline_trial_id is None:
        manifest.baseline_trial_id = ref.trial_id
    save_manifest(exp, manifest)
    return ref, path


def _existing(trial: TrialRef | None, field: str, default: Any = None) -> Any:
    return getattr(trial, field) if trial is not None else default


def _check_compatible(manifest: ExperimentManifest, report: RunReport) -> None:
    workspace = report.identity.workspace_host
    job_id = report.identity.job_id
    if manifest.workspace_host and workspace and manifest.workspace_host != workspace:
        raise ExperimentError(
            f"run is from workspace {workspace!r}, experiment is pinned to "
            f"{manifest.workspace_host!r}"
        )
    if manifest.job_id and job_id and manifest.job_id != job_id:
        raise ExperimentError(
            f"run is for job {job_id!r}, experiment is pinned to {manifest.job_id!r}"
        )


def _find_trial(manifest: ExperimentManifest, run_id: str) -> TrialRef | None:
    for trial in manifest.trials:
        if trial.run_id == run_id:
            return trial
    return None


def load_experiment(
    exp_dir: str | Path,
) -> tuple[ExperimentManifest, dict[str, TrialSnapshot]]:
    """Load a manifest and every snapshot it references, keyed by trial ID."""
    manifest = load_manifest(exp_dir)
    snapshots: dict[str, TrialSnapshot] = {}
    for trial in manifest.trials:
        path = trial_snapshot_path(exp_dir, trial.run_id, trial.snapshot_id)
        if not path.exists():
            raise ExperimentError(
                f"trial {trial.trial_id} references missing snapshot {path}"
            )
        snapshots[trial.trial_id] = load_snapshot(path)
    return manifest, snapshots


def _select_group(
    manifest: ExperimentManifest,
    snapshots: dict[str, TrialSnapshot],
    *,
    run_id: str | None,
    variant: str | None,
    label: str,
) -> ComparisonGroup:
    if bool(run_id) == bool(variant):
        raise ExperimentError(
            f"{label}: pass exactly one of a run ID or a variant label"
        )
    if run_id is not None:
        trials = [t for t in manifest.trials if t.run_id == str(run_id)]
        if not trials:
            raise ExperimentError(
                f"{label}: run {run_id} is not recorded in this experiment"
            )
    else:
        trials = [t for t in manifest.trials if t.variant == variant]
        if not trials:
            raise ExperimentError(f"{label}: no trials with variant {variant!r}")
        fingerprints = {
            t.config_fingerprint for t in trials if t.config_fingerprint is not None
        }
        if len(fingerprints) > 1:
            raise ExperimentError(
                f"{label}: variant {variant!r} mixes {len(fingerprints)} "
                "configurations; record runs of one configuration under one "
                "variant or compare explicit run IDs"
            )

    group = ComparisonGroup(label=label, variant=variant)
    for trial in trials:
        snapshot = snapshots.get(trial.trial_id)
        if snapshot is None:
            continue
        status = _trial_status(snapshot)
        if status == "failed":
            group.excluded_failed += 1
            continue
        if status == "active":
            group.excluded_active += 1
            continue
        if trial.warmup:
            group.excluded_warmup += 1
            continue
        if _is_incomplete_objective(trial):
            group.excluded_incomplete += 1
            continue
        group.trial_ids.append(trial.trial_id)
        group.run_ids.append(trial.run_id)
        group.snapshot_ids.append(snapshot.snapshot_id)
        group.sample_count += 1
    return group


def _trial_status(snapshot: TrialSnapshot) -> str:
    result = snapshot.report.status.result_state or ""
    if snapshot.report.collection_status.value == "partial" and result in (
        "",
        "RUNNING",
        "PENDING",
    ):
        return "active"
    if result in ("FAILED", "TIMEDOUT", "CANCELED", "UPSTREAM_FAILED"):
        return "failed"
    return "ok"


def _is_incomplete_objective(trial: TrialRef) -> bool:
    return trial.correctness in ("pending", "incomplete")


def compare_experiment(
    exp_dir: str | Path,
    *,
    baseline_run_id: str | None = None,
    candidate_run_id: str | None = None,
    baseline_variant: str | None = None,
    candidate_variant: str | None = None,
) -> ExperimentComparison:
    """Compare a pinned baseline group with a candidate group from saved data."""
    manifest, snapshots = load_experiment(exp_dir)
    baseline = _select_group(
        manifest,
        snapshots,
        run_id=baseline_run_id,
        variant=baseline_variant,
        label="baseline",
    )
    candidate = _select_group(
        manifest,
        snapshots,
        run_id=candidate_run_id,
        variant=candidate_variant,
        label="candidate",
    )
    if not baseline.trial_ids and not candidate.trial_ids:
        raise ExperimentError(
            "both groups have no eligible trials; see excluded counts and notes"
        )

    metrics, partial_metrics = _compare_metrics(baseline, candidate, snapshots)
    tasks = _compare_tasks(baseline, candidate, snapshots)
    comparability, revision_confidence = _compare_context(
        manifest, baseline, candidate, snapshots
    )
    if partial_metrics:
        comparability.append(
            ComparabilityNote(
                aspect="metric_completeness",
                status="partial",
                detail=(
                    "partial observations retained separately from eligible "
                    "statistics: " + ", ".join(sorted(partial_metrics))
                ),
            )
        )
    if any(
        not _task_coverage_complete(snapshots[trial_id].report)
        for trial_id in baseline.trial_ids + candidate.trial_ids
    ):
        comparability.append(
            ComparabilityNote(
                aspect="task_coverage",
                status="partial",
                detail=(
                    "task list was page-limited; summed task time is a partial "
                    "subtotal and added/removed classification is unreliable"
                ),
            )
        )
    notes = _group_notes(baseline, candidate)
    return ExperimentComparison(
        experiment_id=manifest.experiment_id,
        baseline=baseline,
        candidate=candidate,
        metrics=metrics,
        tasks=tasks,
        comparability=comparability,
        revision_confidence=revision_confidence,
        notes=notes,
    )


def _task_coverage_complete(report: RunReport) -> bool:
    """Whether the collected task list is complete for the run.

    A page-limited run reports only the tasks that were fetched; its summed task
    duration is a partial subtotal, not a comparable total.
    """
    return report.coverage.get("task_list", "complete") != "partial"


def _measure_values(
    group: ComparisonGroup, snapshots: dict[str, TrialSnapshot]
) -> tuple[dict[str, list[float]], dict[str, list[float]]]:
    """Collect per-trial values split into complete and partial observations.

    Eligible sample statistics use the complete observations only: an observed
    subtotal over queries that merely reported the metric is not a total. The
    partial observations are still returned so a raw delta can be reported with
    a coverage caveat rather than the metric disappearing.
    """
    complete: dict[str, list[float]] = {}
    partial: dict[str, list[float]] = {}
    for trial_id in group.trial_ids:
        snapshot = snapshots[trial_id]
        report = snapshot.report
        raw: dict[str, float | None] = {
            "workflow_elapsed_ms": report.timing.workflow_elapsed_ms,
        }
        raw_partial: dict[str, float] = {}
        if report.timing.summed_task_execution_ms is not None:
            if _task_coverage_complete(report):
                raw["task_execution_ms"] = report.timing.summed_task_execution_ms
            else:
                raw_partial["task_execution_ms"] = (
                    report.timing.summed_task_execution_ms
                )
        if report.query_metrics is not None:
            for aggregate in report.query_metrics.aggregates:
                if aggregate.value is None:
                    continue
                if aggregate.completeness is MetricCompleteness.complete:
                    raw[aggregate.metric] = aggregate.value
                else:
                    raw_partial[aggregate.metric] = aggregate.value
        for metric, value in raw.items():
            if value is not None:
                complete.setdefault(metric, []).append(float(value))
        for metric, value in raw_partial.items():
            partial.setdefault(metric, []).append(float(value))
    return complete, partial


def _compare_metrics(
    baseline: ComparisonGroup,
    candidate: ComparisonGroup,
    snapshots: dict[str, TrialSnapshot],
) -> tuple[list[MetricDelta], set[str]]:
    b_complete, b_partial = _measure_values(baseline, snapshots)
    c_complete, c_partial = _measure_values(candidate, snapshots)
    partial = set(b_partial) | set(c_partial)
    deltas: list[MetricDelta] = []
    for metric in sorted(set(b_complete) | set(c_complete) | partial):
        bc = b_complete.get(metric, [])
        cc = c_complete.get(metric, [])
        bp = b_partial.get(metric, [])
        cp = c_partial.get(metric, [])
        # Prefer complete observations for eligible statistics; fall back to a
        # partial side so a partially observed metric is still reported.
        b = bc or bp
        c = cc or cp
        if not b or not c:
            continue
        b_median = statistics.median(b)
        c_median = statistics.median(c)
        delta = c_median - b_median
        is_partial = bool(bp or cp)
        if is_partial and not (bc and cc):
            caveat = "raw delta uses observed subtotals, not complete totals"
        elif is_partial:
            caveat = (
                "statistics use complete observations; partial observations "
                "are retained separately"
            )
        else:
            caveat = _metric_caveat(metric, snapshots)
        deltas.append(
            MetricDelta(
                metric=metric,
                unit=_metric_unit(metric, snapshots),
                aggregation=_metric_aggregation(metric, snapshots),
                baseline_values=b,
                candidate_values=c,
                baseline_partial_values=bp,
                candidate_partial_values=cp,
                baseline_median=b_median,
                candidate_median=c_median,
                baseline_min=min(b),
                baseline_max=max(b),
                candidate_min=min(c),
                candidate_max=max(c),
                delta=delta,
                pct_change=None if b_median == 0 else delta / b_median,
                single_observation=len(b) == 1 and len(c) == 1,
                partial=is_partial,
                caveat=caveat,
            )
        )
    return deltas, partial


def _metric_unit(metric: str, snapshots: dict[str, TrialSnapshot]) -> MetricUnit:
    if metric in ("workflow_elapsed_ms", "task_execution_ms"):
        return MetricUnit.milliseconds
    for snapshot in snapshots.values():
        if snapshot.report.query_metrics is None:
            continue
        for aggregate in snapshot.report.query_metrics.aggregates:
            if aggregate.metric == metric:
                return aggregate.unit
    return MetricUnit.none


def _metric_aggregation(metric: str, snapshots: dict[str, TrialSnapshot]) -> str:
    if metric in ("workflow_elapsed_ms", "task_execution_ms"):
        return "value"
    for snapshot in snapshots.values():
        if snapshot.report.query_metrics is None:
            continue
        for aggregate in snapshot.report.query_metrics.aggregates:
            if aggregate.metric == metric:
                return aggregate.aggregation
    return "unknown"


def _metric_caveat(metric: str, snapshots: dict[str, TrialSnapshot]) -> str | None:
    if metric in _RUN_METRICS:
        return None
    for snapshot in snapshots.values():
        if snapshot.report.query_metrics is None:
            continue
        for aggregate in snapshot.report.query_metrics.aggregates:
            if (
                aggregate.metric == metric
                and aggregate.completeness.value != "complete"
            ):
                return (
                    "observed subtotal over queries that reported the metric; "
                    "not a complete total"
                )
    return None


def _task_values(report: RunReport) -> dict[str, dict[str, float | None]]:
    values: dict[str, dict[str, float | None]] = {}
    for task in report.tasks:
        values[task.task_key] = {
            "execution_ms": float(task.timing.execution_ms)
            if task.timing.execution_ms is not None
            else None,
            "setup_ms": float(task.timing.setup_ms)
            if task.timing.setup_ms is not None
            else None,
            "cleanup_ms": float(task.timing.cleanup_ms)
            if task.timing.cleanup_ms is not None
            else None,
        }
    return values


def _task_samples(
    group: ComparisonGroup, snapshots: dict[str, TrialSnapshot]
) -> dict[str, dict[str, list[float]]]:
    """Per-task, per-measure samples across every eligible trial in a group."""
    samples: dict[str, dict[str, list[float]]] = {}
    for trial_id in group.trial_ids:
        for key, measures in _task_values(snapshots[trial_id].report).items():
            for measure, value in measures.items():
                if value is None:
                    continue
                samples.setdefault(key, {}).setdefault(measure, []).append(value)
    return samples


def _median_map(samples: dict[str, list[float]]) -> dict[str, float]:
    return {measure: statistics.median(values) for measure, values in samples.items()}


def _compare_tasks(
    baseline: ComparisonGroup,
    candidate: ComparisonGroup,
    snapshots: dict[str, TrialSnapshot],
) -> list[TaskDelta]:
    base = _task_samples(baseline, snapshots)
    cand = _task_samples(candidate, snapshots)
    coverage_partial = any(
        not _task_coverage_complete(snapshots[trial_id].report)
        for trial_id in baseline.trial_ids + candidate.trial_ids
    )
    deltas: list[TaskDelta] = []
    for key in sorted(set(base) | set(cand)):
        b_samples = base.get(key, {})
        c_samples = cand.get(key, {})
        b = _median_map(b_samples)
        c = _median_map(c_samples)
        if key not in base:
            change = "unknown" if coverage_partial else "added"
        elif key not in cand:
            change = "unknown" if coverage_partial else "removed"
        elif _measures_equal(b, c):
            change = "same"
        else:
            change = "changed"
        delta_map: dict[str, float | None] = {}
        for measure in set(b) | set(c):
            base_value = b.get(measure)
            cand_value = c.get(measure)
            if base_value is None or cand_value is None:
                delta_map[measure] = None
            else:
                delta_map[measure] = cand_value - base_value
        deltas.append(
            TaskDelta(
                task_key=key,
                change=change,
                baseline=b,
                candidate=c,
                deltas=delta_map,
                baseline_values=b_samples,
                candidate_values=c_samples,
                baseline_min={
                    measure: min(values) for measure, values in b_samples.items()
                },
                baseline_max={
                    measure: max(values) for measure, values in b_samples.items()
                },
                candidate_min={
                    measure: min(values) for measure, values in c_samples.items()
                },
                candidate_max={
                    measure: max(values) for measure, values in c_samples.items()
                },
            )
        )
    return deltas


def _measures_equal(a: dict[str, float], b: dict[str, float]) -> bool:
    return all(a.get(k) == b.get(k) for k in set(a) | set(b))


def _compute_fingerprint(report: RunReport) -> str:
    """Derive configuration identity from measured run facts, not user labels.

    Compute snapshot, run environments, job parameters, task parameters,
    cluster Spark configuration and the effective performance target all
    participate so that two different configurations cannot be mistaken for one.
    """
    compute_parts = [
        "|".join(
            str(value)
            for value in (
                reference.cluster_id,
                reference.environment_key,
                reference.runtime_engine,
                reference.spark_version,
                reference.node_type_id,
                reference.num_workers,
                reference.performance_target,
            )
        )
        for reference in sorted(report.compute, key=lambda r: r.task_key)
    ]
    environment_parts = [
        "|".join(
            [
                environment.environment_key,
                environment.client or "",
                ",".join(sorted(environment.dependencies)),
            ]
        )
        for environment in sorted(report.environments, key=lambda e: e.environment_key)
    ]
    parameter_parts = [
        f"{name}={value}" for name, value in sorted(report.job_parameters.items())
    ]
    task_parts = [
        "|".join(
            [
                task.task_key,
                ",".join(f"{k}={v}" for k, v in sorted(task.parameters.items())),
                ",".join(f"{k}={v}" for k, v in sorted(task.spark_conf.items())),
            ]
        )
        for task in sorted(report.tasks, key=lambda t: t.task_key)
    ]
    return "||".join(
        [
            ";".join(compute_parts),
            "&".join(environment_parts),
            "&".join(parameter_parts),
            ";".join(task_parts),
            report.effective_performance_target or "",
        ]
    )


def _compare_context(
    manifest: ExperimentManifest,
    baseline: ComparisonGroup,
    candidate: ComparisonGroup,
    snapshots: dict[str, TrialSnapshot],
) -> tuple[list[ComparabilityNote], str]:
    notes: list[ComparabilityNote] = []

    def side(group: ComparisonGroup) -> list[TrialSnapshot]:
        return [snapshots[t] for t in group.trial_ids]

    b_snaps = side(baseline)
    c_snaps = side(candidate)

    b_rev = {
        s.report.revision.executed_commit
        for s in b_snaps
        if s.report.revision.executed_commit
    }
    c_rev = {
        s.report.revision.executed_commit
        for s in c_snaps
        if s.report.revision.executed_commit
    }
    if b_rev and c_rev:
        status = "different" if b_rev != c_rev else "match"
        detail = f"baseline={sorted(b_rev)} candidate={sorted(c_rev)}"
    else:
        status = "unknown"
        detail = "one or both sides lack an executed Git snapshot"
    notes.append(ComparabilityNote(aspect="revision", status=status, detail=detail))

    b_inputs = {s.trial.input_snapshot for s in b_snaps}
    c_inputs = {s.trial.input_snapshot for s in c_snaps}
    if None in b_inputs or None in c_inputs:
        input_status = "unknown"
    elif b_inputs != c_inputs:
        input_status = "different"
    else:
        input_status = "match"
    notes.append(
        ComparabilityNote(
            aspect="input_snapshot",
            status=input_status,
            detail=f"baseline={sorted(x for x in b_inputs if x)} candidate={sorted(x for x in c_inputs if x)}",
        )
    )

    b_config = {s.trial.config_fingerprint for s in b_snaps}
    c_config = {s.trial.config_fingerprint for s in c_snaps}
    if None in b_config or None in c_config:
        config_status = "unknown"
    elif b_config != c_config:
        config_status = "different"
    else:
        config_status = "match"
    notes.append(
        ComparabilityNote(
            aspect="configuration",
            status=config_status,
            detail="configuration differences may be the intended experiment variable",
        )
    )

    b_compute = {_compute_fingerprint(s.report) for s in b_snaps}
    c_compute = {_compute_fingerprint(s.report) for s in c_snaps}
    compute_status = (
        "match" if b_compute == c_compute and b_compute != {""} else "different"
    )
    notes.append(
        ComparabilityNote(
            aspect="compute",
            status=compute_status,
            detail="changed compute can be the intended experiment variable",
        )
    )

    b_correct = {s.trial.correctness for s in b_snaps}
    c_correct = {s.trial.correctness for s in c_snaps}
    correctness_status = (
        "unknown"
        if None in b_correct or None in c_correct
        else "different"
        if b_correct != c_correct
        else "match"
    )
    notes.append(
        ComparabilityNote(
            aspect="correctness",
            status=correctness_status,
            detail="job success alone is not proof of correctness",
        )
    )

    if len(b_rev) == 1 and len(c_rev) == 1 and b_rev == c_rev:
        confidence = "executed"
    elif b_rev and c_rev:
        confidence = "mixed"
    elif any(s.report.revision.confidence == "asserted" for s in b_snaps + c_snaps):
        confidence = "asserted"
    else:
        confidence = "unknown"
    return notes, confidence


def _group_notes(baseline: ComparisonGroup, candidate: ComparisonGroup) -> list[str]:
    notes: list[str] = []
    for group in (baseline, candidate):
        excluded = {
            "failed": group.excluded_failed,
            "active": group.excluded_active,
            "warmup": group.excluded_warmup,
            "incomplete": group.excluded_incomplete,
        }
        for reason, count in excluded.items():
            if count:
                notes.append(
                    f"{group.label}: {count} {reason} trial(s) excluded from aggregates"
                )
    if baseline.sample_count == 1 and candidate.sample_count == 1:
        notes.append(
            "single observation per group; treat deltas as indicative, not significant"
        )
    return notes


def _compute_summary(report: RunReport) -> str:
    parts: list[str] = []
    for reference in sorted(report.compute, key=lambda r: r.task_key):
        bits = [
            reference.runtime_engine or "?",
            reference.spark_version or "?",
            reference.node_type_id or "?",
            f"{reference.num_workers}w" if reference.num_workers is not None else "?w",
        ]
        suffix = " (current lookup)" if reference.source == "current_lookup" else ""
        parts.append(f"{reference.task_key}: {'/'.join(bits)}{suffix}")
    if report.effective_performance_target:
        parts.append(f"perf_target={report.effective_performance_target}")
    return "; ".join(parts)


def _top_queries(report: RunReport, limit: int = 5) -> list[dict[str, Any]]:
    if report.query_metrics is None:
        return []
    queries: list[dict[str, Any]] = []
    for observation in report.query_metrics.observations:
        total = observation.metrics.get("total_time_ms")
        if not isinstance(total, int | float) or isinstance(total, bool):
            total = observation.metrics.get("execution_time_ms")
        if not isinstance(total, int | float) or isinstance(total, bool):
            continue
        queries.append(
            {
                "query_id": observation.query_id,
                "status": observation.status,
                "total_ms": int(total),
                "read_bytes": observation.metrics.get("read_bytes"),
            }
        )
    queries.sort(key=lambda q: q["total_ms"], reverse=True)
    return queries[:limit]


def _failure_excerpts(report: RunReport) -> list[dict[str, Any]]:
    return [
        {"task_key": task.task_key, "excerpt": task.error_excerpt}
        for task in report.tasks
        if task.result_state in ("FAILED", "TIMEDOUT", "CANCELED")
        and task.error_excerpt
    ]


def _eligibility(status: str, warmup: bool, correctness: str | None) -> str:
    if status == "failed":
        return "failed"
    if status == "active":
        return "active"
    if warmup:
        return "warmup"
    if correctness in ("pending", "incomplete"):
        return "incomplete"
    return "ok"


def trial_rows(
    manifest: ExperimentManifest, snapshots: dict[str, TrialSnapshot]
) -> list[dict[str, Any]]:
    """Per-trial metric rows for tabular and plotted views, in execution order."""
    rows: list[dict[str, Any]] = []
    for trial in manifest.trials:
        snapshot = snapshots.get(trial.trial_id)
        if snapshot is None:
            continue
        report = snapshot.report
        values: dict[str, float] = {}
        if report.timing.workflow_elapsed_ms is not None:
            values["workflow_elapsed_ms"] = float(report.timing.workflow_elapsed_ms)
        if report.timing.summed_task_execution_ms is not None:
            values["task_execution_ms"] = float(report.timing.summed_task_execution_ms)
        partial: list[str] = []
        if report.query_metrics is not None:
            for aggregate in report.query_metrics.aggregates:
                if aggregate.value is None:
                    continue
                values[aggregate.metric] = float(aggregate.value)
                if aggregate.completeness.value != "complete":
                    partial.append(aggregate.metric)
        status = _trial_status(snapshot)
        task_values = {
            task.task_key: (
                float(task.timing.execution_ms)
                if task.timing.execution_ms is not None
                else None
            )
            for task in report.tasks
        }
        rows.append(
            {
                "trial_id": trial.trial_id,
                "run_id": trial.run_id,
                "variant": trial.variant,
                "revision": trial.revision or report.revision.executed_commit,
                "short_revision": (
                    report.revision.executed_commit or trial.revision or "unknown"
                )[:8],
                "collected_at": trial.created_at.isoformat(),
                "snapshot_collected_at": snapshot.collected_at.isoformat(),
                "status": status,
                "eligibility": _eligibility(status, trial.warmup, trial.correctness),
                "warmup": trial.warmup,
                "correctness": trial.correctness,
                "config_fingerprint": trial.config_fingerprint,
                "input_snapshot": trial.input_snapshot,
                "run_page_url": report.identity.run_page_url,
                "values": values,
                "task_values": task_values,
                "partial_metrics": partial,
                "coverage": dict(report.coverage),
                "compute_summary": _compute_summary(report),
                "top_queries": _top_queries(report),
                "failure_excerpts": _failure_excerpts(report),
                "failed_tasks": [
                    t.task_key for t in report.tasks if t.result_state == "FAILED"
                ],
            }
        )
    rows.sort(key=lambda row: row["collected_at"])
    return rows
