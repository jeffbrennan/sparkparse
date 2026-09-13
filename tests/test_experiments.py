import json
from pathlib import Path
from typing import Any

import pytest

from sparkparse import experiments as exp
from sparkparse.databricks import RawRunCollection
from sparkparse.models import RunReport
from sparkparse.runreport import build_report

FIXTURES = Path(__file__).parent / "data" / "databricks"


def load_fixture(name: str) -> Any:
    return json.loads((FIXTURES / name).read_text())


def make_report(
    run_id: str = "1",
    *,
    execution_ms: int = 1000,
    read_bytes: int = 100,
    result_state: str = "SUCCESS",
    commit: str = "aaaabbbbcccc",
    variant_git: bool = True,
    config: str | None = None,
    query_final: bool = True,
) -> RunReport:
    run = load_fixture("job_run_multi.json")
    run["run_id"] = int(run_id)
    run["job_run_id"] = int(run_id)
    run["state"] = {"life_cycle_state": "TERMINATED", "result_state": result_state}
    if variant_git:
        run["git_source"] = {
            "git_url": "https://example/repo",
            "git_snapshot": {"used_commit": commit},
        }
    else:
        run.pop("git_source", None)
    tasks = run["tasks"]
    for task in tasks:
        task["execution_duration"] = execution_ms
    queries = [
        {
            "query_id": f"q-{run_id}",
            "status": "FINISHED",
            "is_final": query_final,
            "execution_end_time_ms": 2000,
            "query_source": {"job_info": {"job_task_run_id": "1001"}},
            "metrics": {"read_bytes": read_bytes, "task_total_time_ms": execution_ms},
        }
    ]
    raw = RawRunCollection(
        run=run,
        tasks=tasks,
        queries=queries,
        discovered_query_count=1,
    )
    report = build_report(raw)
    if config is not None:
        report.coverage["config"] = config
    return report


def record(
    exp_dir: Path, report: RunReport, variant: str, **kwargs: Any
) -> tuple[Any, Path]:
    return exp.record_trial(exp_dir, report, variant=variant, **kwargs)


def test_recollection_updates_reference_not_trial_count(tmp_path):
    exp_dir = tmp_path / "exp"
    record(exp_dir, make_report("1"), "baseline")
    record(exp_dir, make_report("1"), "baseline")
    ref3, _ = record(exp_dir, make_report("2"), "baseline")
    manifest, _ = exp.load_experiment(exp_dir)
    assert [t.run_id for t in manifest.trials] == ["1", "2"]
    assert len(manifest.trials) == 2
    # Three snapshots remain on disk; the recollection did not erase the first.
    assert len(exp.list_snapshot_paths(exp_dir)) == 3
    assert ref3.snapshot_id


def test_incompatible_job_is_rejected(tmp_path):
    exp_dir = tmp_path / "exp"
    record(exp_dir, make_report("1"), "baseline")
    other = make_report("9")
    other.identity.job_id = "999"
    with pytest.raises(exp.ExperimentError):
        record(exp_dir, other, "baseline")


def test_cloud_experiment_path_is_rejected():
    with pytest.raises(exp.ExperimentError):
        exp.save_snapshot(
            "s3://bucket/exp", exp.build_snapshot(make_report(), trial=_trial())
        )


def _trial():
    from sparkparse.models import TrialMetadata

    return TrialMetadata(variant="baseline")


def test_corrupt_manifest_is_an_error(tmp_path):
    exp_dir = tmp_path / "exp"
    exp_dir.mkdir()
    (exp_dir / exp.MANIFEST_NAME).write_text("{not json")
    with pytest.raises(exp.ExperimentError):
        exp.load_manifest(exp_dir)


def test_zero_baseline_has_undefined_pct_change(tmp_path):
    exp_dir = tmp_path / "exp"
    record(exp_dir, make_report("1", read_bytes=0), "baseline")
    record(exp_dir, make_report("2", read_bytes=500), "candidate")
    comparison = exp.compare_experiment(
        exp_dir, baseline_run_id="1", candidate_run_id="2"
    )
    read = next(m for m in comparison.metrics if m.metric == "read_bytes")
    assert read.baseline_median == 0
    assert read.pct_change is None
    assert read.delta == 500


def test_failed_and_warmup_trials_are_excluded_but_counted(tmp_path):
    exp_dir = tmp_path / "exp"
    record(exp_dir, make_report("1"), "baseline")
    record(exp_dir, make_report("2", result_state="FAILED"), "baseline")
    record(exp_dir, make_report("3", execution_ms=50), "baseline", warmup=True)
    comparison = exp.compare_experiment(
        exp_dir, baseline_variant="baseline", candidate_variant="baseline"
    )
    assert comparison.baseline.excluded_failed == 1
    assert comparison.baseline.excluded_warmup == 1
    assert comparison.baseline.sample_count == 1
    assert any("failed" in note for note in comparison.notes)


def test_changed_compute_is_displayed_not_forbidden(tmp_path):
    exp_dir = tmp_path / "exp"
    record(exp_dir, make_report("1"), "baseline")
    changed = make_report("2")
    for reference in changed.compute:
        reference.num_workers = 16
    record(exp_dir, changed, "candidate")
    comparison = exp.compare_experiment(
        exp_dir, baseline_run_id="1", candidate_run_id="2"
    )
    compute_note = next(n for n in comparison.comparability if n.aspect == "compute")
    assert compute_note.status == "different"


def test_added_and_removed_tasks_are_shown(tmp_path):
    exp_dir = tmp_path / "exp"
    baseline = make_report("1")
    candidate = make_report("2")
    candidate.tasks = candidate.tasks[:-1]
    record(exp_dir, baseline, "baseline")
    record(exp_dir, candidate, "candidate")
    comparison = exp.compare_experiment(
        exp_dir, baseline_run_id="1", candidate_run_id="2"
    )
    changes = {t.task_key: t.change for t in comparison.tasks}
    assert changes["unknown_type"] == "removed"


def test_reduced_read_bytes_is_not_treated_as_incomparable(tmp_path):
    exp_dir = tmp_path / "exp"
    record(exp_dir, make_report("1", read_bytes=1000), "baseline")
    record(exp_dir, make_report("2", read_bytes=100), "candidate")
    comparison = exp.compare_experiment(
        exp_dir, baseline_run_id="1", candidate_run_id="2"
    )
    read = next(m for m in comparison.metrics if m.metric == "read_bytes")
    assert read.delta == -900
    assert read.pct_change == pytest.approx(-0.9)


def test_same_sha_different_configurations_are_separate_variants(tmp_path):
    exp_dir = tmp_path / "exp"
    record(exp_dir, make_report("1"), "baseline", config_fingerprint="c1")
    record(exp_dir, make_report("2"), "broadcast", config_fingerprint="c2")
    manifest, _ = exp.load_experiment(exp_dir)
    assert {t.variant for t in manifest.trials} == {"baseline", "broadcast"}
    comparison = exp.compare_experiment(
        exp_dir, baseline_variant="baseline", candidate_variant="broadcast"
    )
    config_note = next(
        n for n in comparison.comparability if n.aspect == "configuration"
    )
    assert config_note.status == "different"


def test_relabeling_a_run_does_not_duplicate_the_sample(tmp_path):
    exp_dir = tmp_path / "exp"
    record(exp_dir, make_report("1"), "baseline")
    record(exp_dir, make_report("1"), "broadcast")
    manifest, _ = exp.load_experiment(exp_dir)
    assert len(manifest.trials) == 1
    assert manifest.trials[0].variant == "broadcast"
    # An explicit run-ID comparison selects the one execution, not two.
    comparison = exp.compare_experiment(
        exp_dir, baseline_run_id="1", candidate_run_id="1"
    )
    assert comparison.baseline.sample_count == 1
    assert comparison.candidate.sample_count == 1


def test_recollection_preserves_existing_metadata(tmp_path):
    exp_dir = tmp_path / "exp"
    record(
        exp_dir,
        make_report("1"),
        "baseline",
        warmup=True,
        correctness="verified",
        input_snapshot="snapshot-a",
        config_fingerprint="cfg-explicit",
    )
    record(exp_dir, make_report("1"), "baseline")
    manifest, _ = exp.load_experiment(exp_dir)
    trial = manifest.trials[0]
    assert trial.warmup is True
    assert trial.correctness == "verified"
    assert trial.input_snapshot == "snapshot-a"
    assert trial.config_fingerprint == "cfg-explicit"


def test_variant_with_mixed_configurations_is_rejected(tmp_path):
    exp_dir = tmp_path / "exp"
    record(exp_dir, make_report("1"), "baseline", config_fingerprint="c1")
    record(exp_dir, make_report("2"), "baseline", config_fingerprint="c2")
    with pytest.raises(exp.ExperimentError):
        exp.compare_experiment(
            exp_dir, baseline_variant="baseline", candidate_variant="baseline"
        )


def test_config_fingerprint_is_derived_when_not_supplied(tmp_path):
    exp_dir = tmp_path / "exp"
    ref, _ = record(exp_dir, make_report("1"), "baseline")
    assert ref.config_fingerprint


def test_partial_metric_is_reported_with_caveat_not_dropped(tmp_path):
    exp_dir = tmp_path / "exp"
    record(exp_dir, make_report("1", read_bytes=1000), "baseline")
    record(exp_dir, make_report("2", read_bytes=500, query_final=False), "candidate")
    comparison = exp.compare_experiment(
        exp_dir, baseline_run_id="1", candidate_run_id="2"
    )
    # The raw delta survives so the metric does not disappear entirely.
    read = next(m for m in comparison.metrics if m.metric == "read_bytes")
    assert read.partial is True
    assert read.delta == -500
    assert read.candidate_partial_values == [500.0]
    assert read.caveat and "subtotal" in read.caveat
    note = next(
        n for n in comparison.comparability if n.aspect == "metric_completeness"
    )
    assert note.status == "partial"


def test_execution_parameters_change_config_fingerprint(tmp_path):
    exp_dir = tmp_path / "exp"
    baseline = make_report("1")
    baseline.job_parameters = {"shuffle_partitions": "200"}
    candidate = make_report("2")
    candidate.job_parameters = {"shuffle_partitions": "800"}
    ref_baseline, _ = record(exp_dir, baseline, "baseline")
    ref_candidate, _ = record(exp_dir, candidate, "candidate")
    assert ref_baseline.config_fingerprint != ref_candidate.config_fingerprint


def test_recollection_keeps_first_trial_ordering(tmp_path):
    exp_dir = tmp_path / "exp"
    record(exp_dir, make_report("1"), "baseline")
    record(exp_dir, make_report("2"), "candidate")
    record(exp_dir, make_report("1"), "baseline")
    manifest, snapshots = exp.load_experiment(exp_dir)
    rows = exp.trial_rows(manifest, snapshots)
    assert [row["run_id"] for row in rows] == ["1", "2"]
    assert rows[0]["snapshot_collected_at"] is not None


def test_per_task_comparison_uses_medians_and_keeps_spread(tmp_path):
    exp_dir = tmp_path / "exp"
    record(exp_dir, make_report("1", execution_ms=1000), "baseline")
    record(exp_dir, make_report("2", execution_ms=3000), "baseline")
    record(exp_dir, make_report("3", execution_ms=500), "candidate")
    comparison = exp.compare_experiment(
        exp_dir, baseline_variant="baseline", candidate_variant="candidate"
    )
    task = next(t for t in comparison.tasks if t.task_key == "ingest")
    assert task.baseline["execution_ms"] == 2000
    assert task.baseline_values["execution_ms"] == [1000.0, 3000.0]
    assert task.baseline_min["execution_ms"] == 1000
    assert task.baseline_max["execution_ms"] == 3000
    assert task.candidate["execution_ms"] == 500
    assert task.deltas["execution_ms"] == -1500


def test_unequal_sample_counts_use_medians(tmp_path):
    exp_dir = tmp_path / "exp"
    record(exp_dir, make_report("1", execution_ms=1000), "baseline")
    record(exp_dir, make_report("2", execution_ms=3000), "baseline")
    record(exp_dir, make_report("3", execution_ms=500), "candidate")
    comparison = exp.compare_experiment(
        exp_dir, baseline_variant="baseline", candidate_variant="candidate"
    )
    task = next(m for m in comparison.metrics if m.metric == "aggregate_task_time_ms")
    assert task.baseline_median == 2000
    assert task.candidate_median == 500
    assert len(task.baseline_values) == 2
    assert len(task.candidate_values) == 1


def test_missing_input_snapshot_is_a_caveat_not_a_refusal(tmp_path):
    exp_dir = tmp_path / "exp"
    record(exp_dir, make_report("1"), "baseline")
    record(exp_dir, make_report("2"), "candidate", input_snapshot="user-declared")
    comparison = exp.compare_experiment(
        exp_dir, baseline_run_id="1", candidate_run_id="2"
    )
    input_note = next(
        n for n in comparison.comparability if n.aspect == "input_snapshot"
    )
    assert input_note.status == "unknown"
    assert comparison.metrics  # raw deltas are still shown


def test_trial_rows_are_in_execution_order(tmp_path):
    exp_dir = tmp_path / "exp"
    record(exp_dir, make_report("2"), "baseline")
    record(exp_dir, make_report("1"), "candidate")
    manifest, snapshots = exp.load_experiment(exp_dir)
    rows = exp.trial_rows(manifest, snapshots)
    assert len(rows) == 2
    timestamps = [row["collected_at"] for row in rows]
    assert timestamps == sorted(timestamps)
    assert all("workflow_elapsed_ms" in row["values"] for row in rows)


def test_out_of_order_runs_compare_by_selector(tmp_path):
    exp_dir = tmp_path / "exp"
    record(exp_dir, make_report("2", read_bytes=200), "baseline")
    record(exp_dir, make_report("1", read_bytes=100), "baseline")
    comparison = exp.compare_experiment(
        exp_dir, baseline_run_id="2", candidate_run_id="1"
    )
    read = next(m for m in comparison.metrics if m.metric == "read_bytes")
    assert read.baseline_median == 200
    assert read.candidate_median == 100


def test_manifest_rejects_unknown_schema(tmp_path):
    exp_dir = tmp_path / "exp"
    record(exp_dir, make_report("1"), "baseline")
    manifest_data = json.loads((exp_dir / exp.MANIFEST_NAME).read_text())
    manifest_data["schema_version"] = "999"
    (exp_dir / exp.MANIFEST_NAME).write_text(json.dumps(manifest_data))
    with pytest.raises(exp.ExperimentError):
        exp.load_manifest(exp_dir)


def test_error_text_is_preserved_as_data(tmp_path):
    from sparkparse.models import OutputAttachment

    exp_dir = tmp_path / "exp"
    report = make_report("1")
    report.outputs = [
        OutputAttachment(
            task_key="join",
            run_id="1001",
            status="error",
            error="<script>alert(1)</script>",
            excerpt="<script>alert(1)</script>",
        )
    ]
    path = exp.save_snapshot(
        exp_dir, exp.build_snapshot(report, trial=exp.TrialMetadata(variant="v"))
    )
    saved = path.read_text()
    assert "<script>" in saved
    attachment = exp.load_snapshot(path).report.outputs[0]
    assert attachment.excerpt == "<script>alert(1)</script>"
    assert attachment.path is not None
    assert (exp_dir / attachment.path).read_text() == "<script>alert(1)</script>"


def test_snapshot_records_identity_and_schema(tmp_path):
    exp_dir = tmp_path / "exp"
    record(exp_dir, make_report("1"), "baseline")
    paths = exp.list_snapshot_paths(exp_dir)
    snapshot = exp.load_snapshot(paths[0])
    assert snapshot.run_id == "1"
    assert snapshot.schema_version == "1"
    assert snapshot.workspace_host is not None


# --- CLI contracts -------------------------------------------------------

from typer.testing import CliRunner  # noqa: E402

from sparkparse.app import app  # noqa: E402

runner = CliRunner()


def test_compare_cli_requires_one_selector_per_side(tmp_path):
    exp_dir = tmp_path / "exp"
    record(exp_dir, make_report("1"), "baseline")
    result = runner.invoke(app, ["experiments", "compare", str(exp_dir)])
    assert result.exit_code == 2


def test_compare_cli_json(tmp_path):
    exp_dir = tmp_path / "exp"
    record(exp_dir, make_report("1"), "baseline")
    record(exp_dir, make_report("2"), "candidate")
    result = runner.invoke(
        app,
        [
            "experiments",
            "compare",
            str(exp_dir),
            "--baseline",
            "1",
            "--candidate",
            "2",
        ],
    )
    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    assert payload["baseline"]["run_ids"] == ["1"]
    assert payload["candidate"]["run_ids"] == ["2"]


def test_compare_cli_missing_experiment_exits_1(tmp_path):
    result = runner.invoke(
        app,
        [
            "experiments",
            "compare",
            str(tmp_path / "nope"),
            "--baseline",
            "1",
            "--candidate",
            "2",
        ],
    )
    assert result.exit_code == 1


def test_comparison_records_pinned_snapshot_ids(tmp_path):
    exp_dir = tmp_path / "exp"
    record(exp_dir, make_report("1"), "baseline")
    record(exp_dir, make_report("2"), "candidate")
    comparison = exp.compare_experiment(
        exp_dir, baseline_run_id="1", candidate_run_id="2"
    )
    assert len(comparison.baseline.snapshot_ids) == 1
    assert len(comparison.candidate.snapshot_ids) == 1
    # Snapshot IDs are stable references, not the mutable latest pointer.
    manifest, _ = exp.load_experiment(exp_dir)
    by_run = {t.run_id: t.snapshot_id for t in manifest.trials}
    assert comparison.baseline.snapshot_ids[0] == by_run["1"]
