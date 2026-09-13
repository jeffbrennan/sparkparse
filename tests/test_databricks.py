import json
import subprocess
from pathlib import Path
from typing import Any

import pytest

from sparkparse.databricks import (
    DatabricksCLI,
    DatabricksError,
    RawRunCollection,
    collect_run,
    fetch_full_run,
)
from sparkparse.models import WorkflowCollectionLimits
from sparkparse.runreport import build_report

FIXTURES = Path(__file__).parent / "data" / "databricks"


def load_fixture(name: str) -> Any:
    return json.loads((FIXTURES / name).read_text())


def _proc(args: Any, returncode: int = 0, stdout: str = "{}", stderr: str = "") -> Any:
    return subprocess.CompletedProcess(list(args), returncode, stdout, stderr)


class ScriptedRunner:
    """Returns canned responses based on the command path."""

    def __init__(
        self,
        run: dict[str, Any] | None = None,
        run_pages: list[dict[str, Any]] | None = None,
        query_pages: list[dict[str, Any]] | None = None,
        outputs: dict[str, Any] | None = None,
        clusters: dict[str, Any] | None = None,
        rate_limit_once: bool = False,
        auth_fail: bool = False,
        runs_list: dict[str, Any] | None = None,
    ) -> None:
        self.run = run
        self.run_pages = list(run_pages or [])
        self.query_pages = list(query_pages or [])
        self.outputs = outputs or {}
        self.clusters = clusters or {}
        self.rate_limit_once = rate_limit_once
        self.auth_fail = auth_fail
        self.runs_list = runs_list
        self.calls: list[list[str]] = []

    def __call__(self, args: Any, timeout: float) -> Any:
        args = list(args)
        self.calls.append(args)
        joined = " ".join(args)
        if self.auth_fail:
            return _proc(args, 1, "", "Invalid access token")
        if self.rate_limit_once:
            self.rate_limit_once = False
            return _proc(args, 1, "", "429 Too Many Requests")
        if "runs/list" in joined:
            if isinstance(self.runs_list, list):
                page = self.runs_list.pop(0) if self.runs_list else {"runs": []}
                return _proc(args, 0, json.dumps(page))
            return _proc(args, 0, json.dumps(self.runs_list or {"runs": []}))
        if "runs/get" in joined:
            if self.run_pages:
                page = self.run_pages.pop(0)
                return _proc(args, 0, json.dumps(page))
            return _proc(args, 0, json.dumps(self.run or {}))
        if "history/queries" in joined:
            if self.query_pages:
                return _proc(args, 0, json.dumps(self.query_pages.pop(0)))
            return _proc(args, 0, json.dumps({"has_next_page": False, "res": []}))
        if "get-run-output" in joined:
            index = args.index("get-run-output")
            run_id = args[index + 1]
            return _proc(
                args, 0, json.dumps(self.outputs.get(str(run_id), {"logs": "line"}))
            )
        if "clusters/get" in joined:
            cluster_id = joined.split("cluster_id=")[-1]
            return _proc(args, 0, json.dumps(self.clusters.get(cluster_id, {})))
        return _proc(args, 0, "{}")


def make_runner(**kwargs: Any) -> ScriptedRunner:
    return ScriptedRunner(**kwargs)


def client(runner: ScriptedRunner, **limit_overrides: Any) -> DatabricksCLI:
    limits = WorkflowCollectionLimits(
        deadline_seconds=limit_overrides.get("deadline_seconds", 30.0),
        max_pages=limit_overrides.get("max_pages", 10),
        max_requests=limit_overrides.get("max_requests", 20),
        max_retries=limit_overrides.get("max_retries", 2),
        retry_backoff_seconds=0.0,
    )
    return DatabricksCLI(profile="free", runner=runner, limits=limits)


def test_multi_task_normalization_keeps_timing_measures_separate():
    runner = make_runner(run=load_fixture("job_run_multi.json"))
    raw = collect_run(client(runner), run_id="123", outputs="none")
    report = build_report(raw, asserted_revision=None)

    assert [t.task_key for t in report.tasks] == [
        "ingest",
        "join",
        "optimize",
        "foreach_copy",
        "unknown_type",
    ]
    assert report.timing.workflow_elapsed_ms == 60000
    assert report.timing.summed_task_execution_ms == 56500
    # Workflow elapsed is never the summed task time.
    assert report.timing.workflow_elapsed_ms != report.timing.summed_task_execution_ms
    # Repair/retry identity and skip state survive.
    join = next(t for t in report.tasks if t.task_key == "join")
    assert join.original_attempt_run_id == "1000"
    assert join.attempt_number == 1
    optimize = next(t for t in report.tasks if t.task_key == "optimize")
    assert optimize.result_state == "SKIPPED"
    foreach = next(t for t in report.tasks if t.task_key == "foreach_copy")
    assert foreach.task_kind == "for_each"
    unknown = next(t for t in report.tasks if t.task_key == "unknown_type")
    assert unknown.task_kind is None
    assert unknown.compute is None


def test_paginated_get_run_merges_array_properties():
    first = {"run_id": 123, "tasks": [{"task_key": "a"}], "next_page_token": "t1"}
    second = {"run_id": 123, "tasks": [{"task_key": "b"}]}
    runner = make_runner(run_pages=[first, second])
    merged, complete = fetch_full_run(client(runner), "123")
    assert complete is True
    assert [t["task_key"] for t in merged["tasks"]] == ["a", "b"]


def test_run_pagination_exhaustion_keeps_partial_tasks():
    first = {
        "run_id": 123,
        "tasks": [{"task_key": "a"}],
        "next_page_token": "t1",
        "state": {"life_cycle_state": "TERMINATED"},
    }
    runner = make_runner(
        run_pages=[first, {"run_id": 123, "tasks": [{"task_key": "b"}]}]
    )
    # max_pages=0 leaves no room for the follow-up page.
    merged, complete = fetch_full_run(client(runner, max_pages=0), "123")
    assert complete is False
    assert [t["task_key"] for t in merged["tasks"]] == ["a"]


def test_executed_git_snapshot_beats_asserted_revision():
    raw = collect_run(
        client(make_runner(run=load_fixture("job_run_multi.json"))),
        run_id="123",
        outputs="none",
    )
    report = build_report(raw, asserted_revision="zzz")
    assert report.revision.confidence == "executed"
    assert (report.revision.executed_commit or "").startswith("abc123")


def test_asserted_and_unknown_revision():
    run = load_fixture("job_run_single.json")
    run.pop("git_source", None)
    raw = collect_run(client(make_runner(run=run)), run_id="1", outputs="none")
    assert build_report(raw).revision.confidence == "unknown"
    assert (
        build_report(raw, asserted_revision="deadbeef").revision.confidence
        == "asserted"
    )


def test_mixed_task_revisions_are_not_attributed_to_one_sha():
    run = load_fixture("job_run_multi.json")
    run["tasks"][0]["git_source"] = {
        "git_url": "https://example/repo",
        "git_snapshot": {"used_commit": "othercommit"},
    }
    raw = collect_run(client(make_runner(run=run)), run_id="123", outputs="none")
    report = build_report(raw)
    assert report.revision.confidence == "mixed"
    assert report.revision.executed_commit is None
    assert any(d.code == "mixed_revision" for d in report.diagnostics)


def test_task_run_id_reports_parent():
    run = {"run_id": 999, "job_run_id": 123, "tasks": []}
    with pytest.raises(DatabricksError) as excinfo:
        collect_run(client(make_runner(run=run)), run_id="999", outputs="none")
    assert excinfo.value.code == "task_run_id"
    assert "123" in str(excinfo.value)


def test_resolve_latest_terminal_run_including_failures():
    runs = {
        "runs": [
            {"run_id": 2, "state": {"life_cycle_state": "RUNNING"}},
            {
                "run_id": 3,
                "state": {"life_cycle_state": "TERMINATED", "result_state": "FAILED"},
            },
        ]
    }
    from sparkparse.databricks import resolve_run_id

    selected, active = resolve_run_id(client(make_runner(runs_list=runs)), "42")
    assert selected == "3"
    assert active is False


def test_active_run_yields_labeled_partial_snapshot():
    run = load_fixture("job_run_multi.json")
    run["state"] = {"life_cycle_state": "RUNNING"}
    run["end_time"] = None
    raw = collect_run(client(make_runner(run=run)), run_id="123", outputs="none")
    assert raw.collection_status.value == "partial"
    report = build_report(raw)
    assert any(d.code == "active_run" for d in report.diagnostics)


def test_rate_limit_retry_then_success():
    runner = make_runner(run=load_fixture("job_run_multi.json"), rate_limit_once=True)
    raw = collect_run(client(runner), run_id="123", outputs="none")
    assert raw.run["run_id"] == 123


def test_auth_failure_is_clear():
    with pytest.raises(DatabricksError) as excinfo:
        collect_run(
            client(make_runner(run=load_fixture("job_run_multi.json"), auth_fail=True)),
            run_id="123",
            outputs="none",
        )
    assert excinfo.value.code == "auth_error"


def test_resolve_run_id_finds_terminal_run_on_a_later_page():
    runs_pages = [
        {
            "runs": [{"run_id": 2, "state": {"life_cycle_state": "RUNNING"}}],
            "next_page_token": "p2",
        },
        {
            "runs": [
                {
                    "run_id": 3,
                    "state": {
                        "life_cycle_state": "TERMINATED",
                        "result_state": "SUCCESS",
                    },
                }
            ]
        },
    ]
    from sparkparse.databricks import resolve_run_id

    selected, active = resolve_run_id(client(make_runner(runs_list=runs_pages)), "42")
    assert selected == "3"
    assert active is False


def test_query_pagination_exhaustion_preserves_collected_queries():
    first = {
        "has_next_page": True,
        "next_page_token": "t",
        "res": [{"query_id": "q1"}],
    }
    second = {"has_next_page": False, "res": [{"query_id": "q2"}]}
    runner = make_runner(
        run=load_fixture("job_run_multi.json"), query_pages=[first, second]
    )
    raw = collect_run(client(runner, max_requests=2), run_id="123", outputs="none")
    # The first page was fetched and must survive the follow-up request failure.
    assert [q["query_id"] for q in raw.queries] == ["q1"]
    assert raw.discovered_query_count == 1
    assert raw.query_discovery_complete is False
    assert raw.collection_status.value == "partial"
    assert any(d.code == "collection_budget_exhausted" for d in raw.diagnostics)


def test_query_budget_exhaustion_is_partial_not_fatal():
    pages = [
        {"has_next_page": True, "next_page_token": "t", "res": [{"query_id": "q1"}]},
        {"has_next_page": False, "res": [{"query_id": "q2"}]},
    ]
    runner = make_runner(run=load_fixture("job_run_multi.json"), query_pages=pages)
    raw = collect_run(client(runner, max_requests=2), run_id="123", outputs="none")
    assert raw.collection_status.value == "partial"
    assert any(d.code == "collection_budget_exhausted" for d in raw.diagnostics)


def test_bounded_outputs_are_truncated_and_recorded():
    run = load_fixture("job_run_multi.json")
    raw = collect_run(client(make_runner(run=run)), run_id="123", outputs="all")
    report = build_report(raw)
    assert len(report.outputs) == len(report.tasks)
    assert all(o.status in ("saved", "truncated") for o in report.outputs)


def test_output_truncation_flag_is_preserved():
    run = load_fixture("job_run_multi.json")
    outputs = {
        "1001": {"notebook_output": {"result": "x" * 10, "truncated": True}},
    }
    raw = collect_run(
        client(make_runner(run=run, outputs=outputs)), run_id="123", outputs="all"
    )
    report = build_report(raw)
    first = next(o for o in report.outputs if o.run_id == "1001")
    assert first.status == "truncated"


def test_cluster_lookup_is_cached_and_labeled_current():
    run = load_fixture("job_run_multi.json")
    run["tasks"][0].pop("new_cluster")
    run["tasks"][0]["existing_cluster_id"] = "cluster-1"
    run["tasks"][1].pop("new_cluster")
    run["tasks"][1]["existing_cluster_id"] = "cluster-1"
    clusters = {"cluster-1": {"spark_version": "15.4", "runtime_engine": "PHOTON"}}
    runner = make_runner(run=run, clusters=clusters)
    raw = collect_run(client(runner), run_id="123", outputs="none")
    lookup_calls = [c for c in runner.calls if "clusters/get" in " ".join(c)]
    assert len(lookup_calls) == 1
    report = build_report(raw)
    references = [c for c in report.compute if c.cluster_id == "cluster-1"]
    assert references and all(c.source == "current_lookup" for c in references)


def _report_with_queries(queries: list[dict[str, Any]]):
    run = load_fixture("job_run_multi.json")
    raw = RawRunCollection(
        run=run,
        tasks=run["tasks"],
        queries=queries,
        discovered_query_count=len(queries),
    )
    return build_report(raw)


def test_query_metrics_observed_subtotal_not_complete_total():
    report = _report_with_queries(load_fixture("query_history_mixed.json")["res"])
    metrics = report.query_metrics
    assert metrics is not None
    assert metrics.matched_query_count == 2
    assert metrics.attribution_counts == {
        "task_run": 2,
        "unattributed": 1,
        "unrelated": 1,
    }
    read = next(a for a in metrics.aggregates if a.metric == "read_bytes")
    assert read.value_exact == 3000
    assert read.completeness.value == "complete"
    cache = next(a for a in metrics.aggregates if a.metric == "read_cache_bytes")
    assert cache.value_exact == 400
    assert cache.completeness.value == "observed_subtotal"
    assert cache.counted_query_count == 1
    assert cache.observed_query_count == 2


def test_missing_metric_is_none_not_zero():
    report = _report_with_queries(load_fixture("query_history_mixed.json")["res"])
    metrics = report.query_metrics
    assert metrics is not None
    remote = next(a for a in metrics.aggregates if a.metric == "read_remote_bytes")
    assert remote.value is None
    assert remote.value_exact is None
    assert remote.completeness.value == "unavailable"
    # A measured zero survives as zero.
    spill = next(a for a in metrics.aggregates if a.metric == "disk_spill_bytes")
    assert spill.value_exact == 900


def test_missing_parent_id_with_valid_task_id_is_attributed():
    queries = [
        {
            "query_id": "q-valid",
            "status": "FINISHED",
            "execution_end_time_ms": 2000,
            "query_source": {"job_info": {"job_task_run_id": "1001"}},
            "metrics": {"read_bytes": 10},
        }
    ]
    report = _report_with_queries(queries)
    metrics = report.query_metrics
    assert metrics is not None
    assert metrics.matched_query_count == 1
    observation = metrics.observations[0]
    assert observation.attribution == "task_run"
    assert observation.task_key == "ingest"


def test_unrelated_queries_are_not_persisted():
    queries = [
        {
            "query_id": "q-other",
            "query_source": {"job_info": {"job_task_run_id": "999"}},
            "metrics": {"read_bytes": 1},
        }
    ]
    report = _report_with_queries(queries)
    metrics = report.query_metrics
    assert metrics is not None
    assert metrics.matched_query_count == 0
    assert metrics.observations == []


def test_duplicate_query_pages_keep_the_fresher_observation():
    queries = [
        {
            "query_id": "q-1",
            "is_final": False,
            "execution_end_time_ms": 1000,
            "query_source": {"job_info": {"job_task_run_id": "1001"}},
            "metrics": {"read_bytes": 5},
        },
        {
            "query_id": "q-1",
            "is_final": True,
            "execution_end_time_ms": 2000,
            "query_source": {"job_info": {"job_task_run_id": "1001"}},
            "metrics": {"read_bytes": 7},
        },
    ]
    report = _report_with_queries(queries)
    metrics = report.query_metrics
    assert metrics is not None
    assert len(metrics.observations) == 1
    assert metrics.observations[0].metrics["read_bytes"] == 7


def test_incomplete_discovery_prevents_complete_coverage():
    run = load_fixture("job_run_multi.json")
    raw = RawRunCollection(
        run=run,
        tasks=run["tasks"],
        queries=[
            {
                "query_id": "q-1",
                "query_source": {"job_info": {"job_task_run_id": "1001"}},
                "metrics": {"read_bytes": 5},
            }
        ],
        discovered_query_count=1,
        query_discovery_complete=False,
    )
    report = build_report(raw)
    metrics = report.query_metrics
    assert metrics is not None
    read = next(a for a in metrics.aggregates if a.metric == "read_bytes")
    assert read.completeness.value == "observed_subtotal"
    assert any(d.code == "query_discovery_incomplete" for d in report.diagnostics)


# --- CLI contracts -------------------------------------------------------

from typer.testing import CliRunner  # noqa: E402

from sparkparse.app import app  # noqa: E402

runner = CliRunner()


def _stub_collect(monkeypatch: pytest.MonkeyPatch) -> None:
    run = load_fixture("job_run_multi.json")

    def fake_collect(cli: Any, **kwargs: Any) -> RawRunCollection:
        return RawRunCollection(
            run=run,
            tasks=run["tasks"],
            queries=load_fixture("query_history_mixed.json")["res"],
            discovered_query_count=4,
        )

    monkeypatch.setattr("sparkparse.app.collect_run", fake_collect)
    monkeypatch.setattr("sparkparse.app.DatabricksCLI", lambda **kwargs: object())


def test_analyze_requires_exactly_one_id():
    assert runner.invoke(app, ["databricks", "analyze"]).exit_code == 2
    both = runner.invoke(
        app, ["databricks", "analyze", "--run-id", "1", "--job-id", "2"]
    )
    assert both.exit_code == 2


def test_analyze_rejects_out_and_experiment_together():
    result = runner.invoke(
        app,
        ["databricks", "analyze", "--run-id", "1", "--out", "a", "--experiment", "b"],
    )
    assert result.exit_code == 2


def test_analyze_stdout_is_pure_json(monkeypatch, tmp_path):
    _stub_collect(monkeypatch)
    out_dir = tmp_path / "runs"
    separate = CliRunner(mix_stderr=False)
    result = separate.invoke(
        app, ["databricks", "analyze", "--run-id", "123", "--out", str(out_dir)]
    )
    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    assert payload["identity"]["run_id"] == "123"
    assert payload["query_metrics"]["matched_query_count"] == 2
    assert result.stderr  # progress goes to stderr
    assert list(out_dir.glob("runs/*/*.json"))


def test_analyze_records_experiment_trial(monkeypatch, tmp_path):
    _stub_collect(monkeypatch)
    exp_dir = tmp_path / "exp"
    result = runner.invoke(
        app,
        [
            "databricks",
            "analyze",
            "--run-id",
            "123",
            "--experiment",
            str(exp_dir),
            "--variant",
            "broadcast",
            "--revision",
            "deadbeef",
        ],
    )
    assert result.exit_code == 0, result.output
    manifest = json.loads((exp_dir / "experiment.json").read_text())
    assert manifest["trials"][0]["variant"] == "broadcast"


def test_analyze_collection_failure_exits_1(monkeypatch):
    def boom(cli: Any, **kwargs: Any) -> RawRunCollection:
        raise DatabricksError("no runs", code="no_runs")

    monkeypatch.setattr("sparkparse.app.collect_run", boom)
    monkeypatch.setattr("sparkparse.app.DatabricksCLI", lambda **kwargs: object())
    result = runner.invoke(app, ["databricks", "analyze", "--job-id", "9"])
    assert result.exit_code == 1


def test_analyze_text_format(monkeypatch):
    _stub_collect(monkeypatch)
    result = runner.invoke(
        app, ["databricks", "analyze", "--run-id", "123", "--format", "text"]
    )
    assert result.exit_code == 0, result.output
    assert "Run 123" in result.stdout


def test_nested_and_foreach_tasks_mark_incomplete_child_coverage():
    raw = collect_run(
        client(make_runner(run=load_fixture("job_run_multi.json"))),
        run_id="123",
        outputs="none",
    )
    report = build_report(raw)
    assert report.coverage["child_tasks"] == "incomplete"
    assert any(d.code == "incomplete_child_coverage" for d in report.diagnostics)


def test_retained_serverless_query_history_fixture_is_complete():
    run = load_fixture("job_run_single.json")
    queries = load_fixture("query_history_window.json")["res"]
    raw = RawRunCollection(
        run=run,
        tasks=run["tasks"],
        queries=queries,
        discovered_query_count=len(queries),
    )
    report = build_report(raw)
    metrics = report.query_metrics
    assert metrics is not None
    assert metrics.discovered_query_count == 9
    assert metrics.matched_query_count == 9
    assert report.tasks[0].run_id == "60603881665866"
    task_time = next(
        a for a in metrics.aggregates if a.metric == "aggregate_task_time_ms"
    )
    assert task_time.completeness.value == "complete"
    assert task_time.counted_query_count == 9
