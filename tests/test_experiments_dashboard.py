from pathlib import Path
from typing import Any

import pytest

from sparkparse import experiments as exp
from sparkparse.experiments_dashboard import (
    _context_table,
    _filter_rows,
    _task_trend_figure,
    _trend_figure,
    build_layout,
    default_runs,
    init_experiments_dashboard,
)
from tests.test_experiments import make_report, record

pytest.importorskip("dash")


def _experiment(tmp_path: Path) -> Path:
    exp_dir = tmp_path / "exp"
    record(exp_dir, make_report("1", execution_ms=1000), "baseline")
    record(exp_dir, make_report("2", execution_ms=3000), "baseline")
    record(exp_dir, make_report("3", execution_ms=500), "candidate")
    return exp_dir


def test_default_runs_honor_pinned_baseline(tmp_path):
    exp_dir = _experiment(tmp_path)
    manifest, snapshots = exp.load_experiment(exp_dir)
    rows = exp.trial_rows(manifest, snapshots)
    baseline, candidate = default_runs(manifest, rows)
    assert manifest.baseline_trial_id is not None
    assert baseline == "1"
    assert candidate == "3"


def test_filter_rows_by_variant_commit_and_eligibility(tmp_path):
    exp_dir = _experiment(tmp_path)
    manifest, snapshots = exp.load_experiment(exp_dir)
    rows = exp.trial_rows(manifest, snapshots)
    assert len(_filter_rows(rows, ["baseline"], None, None)) == 2
    assert len(_filter_rows(rows, ["candidate"], None, None)) == 1
    assert _filter_rows(rows, None, None, ["failed"]) == []


def test_trend_figure_keeps_missing_context_as_a_gap():
    rows: list[dict[str, Any]] = [
        {
            "run_id": "1",
            "variant": "v",
            "short_revision": "abc",
            "values": {"workflow_elapsed_ms": 1000.0},
            "partial_metrics": [],
        }
    ]
    figure = _trend_figure(rows, "workflow_elapsed_ms", None, None)
    context_trace = figure.data[1]
    assert list(context_trace.y) == [None]  # type: ignore[missing-attribute]


def test_trend_figure_marks_partial_values_distinctly():
    rows: list[dict[str, Any]] = [
        {
            "run_id": "1",
            "variant": "v",
            "short_revision": "abc",
            "values": {"read_bytes": 100.0},
            "partial_metrics": ["read_bytes"],
        }
    ]
    figure = _trend_figure(rows, "read_bytes", None, None)
    partial = [
        trace
        for trace in figure.data
        if "partial" in (trace.name or "")  # type: ignore[missing-attribute]
    ]
    assert partial
    assert partial[0].marker.symbol == "circle-open"  # type: ignore[missing-attribute,union-attr]


def test_context_table_surfaces_compute_and_revision(tmp_path):
    exp_dir = _experiment(tmp_path)
    manifest, snapshots = exp.load_experiment(exp_dir)
    rows = exp.trial_rows(manifest, snapshots)
    comparison = exp.compare_experiment(
        exp_dir, baseline_run_id="1", candidate_run_id="3"
    )
    baseline_row = next(r for r in rows if r["run_id"] == "1")
    candidate_row = next(r for r in rows if r["run_id"] == "3")
    table = _context_table(comparison, baseline_row, candidate_row)
    aspects = {row["aspect"] for row in table}
    assert {"revision", "compute", "configuration"} <= aspects
    compute = next(row for row in table if row["aspect"] == "compute")
    assert compute["baseline"] == baseline_row["compute_summary"]


def test_trial_rows_expose_detail_fields(tmp_path):
    exp_dir = _experiment(tmp_path)
    manifest, snapshots = exp.load_experiment(exp_dir)
    rows = exp.trial_rows(manifest, snapshots)
    row = rows[0]
    assert row["eligibility"] == "ok"
    assert "compute_summary" in row
    assert "top_queries" in row
    assert "failure_excerpts" in row
    assert "ingest" in row["task_values"]


def test_trend_figure_context_aligns_with_all_runs():
    rows: list[dict[str, Any]] = [
        {
            "run_id": "1",
            "variant": "v",
            "short_revision": "abc",
            "values": {"read_bytes": 100.0, "workflow_elapsed_ms": 1000.0},
            "partial_metrics": ["read_bytes"],
        },
        {
            "run_id": "2",
            "variant": "v",
            "short_revision": "abc",
            "values": {"read_bytes": 200.0, "workflow_elapsed_ms": 2000.0},
            "partial_metrics": [],
        },
    ]
    figure = _trend_figure(rows, "read_bytes", None, None)
    context = next(
        t
        for t in figure.data
        if t.type == "bar"  # type: ignore[missing-attribute]
    )
    assert list(context.x) == ["1", "2"]  # type: ignore[missing-attribute]
    assert len(context.y) == 2  # type: ignore[missing-attribute]


def test_task_spread_excludes_ineligible_trials(tmp_path):
    exp_dir = tmp_path / "exp"
    record(exp_dir, make_report("1", execution_ms=1000), "baseline")
    record(exp_dir, make_report("2", execution_ms=3000), "baseline")
    record(
        exp_dir,
        make_report("4", execution_ms=50000, result_state="FAILED"),
        "baseline",
    )
    record(exp_dir, make_report("3", execution_ms=500), "candidate")
    manifest, snapshots = exp.load_experiment(exp_dir)
    rows = exp.trial_rows(manifest, snapshots)
    figure = _task_trend_figure(rows, "ingest", "1", "3")
    spread = [
        trace
        for trace in figure.data
        if trace.name and "spread" in trace.name  # type: ignore[missing-attribute,operator]
    ]
    baseline_spread = spread[0]
    # Median of 1000 and 3000 is 2000; max - median is 1000, not 48000.
    assert baseline_spread.error_y.array[0] == 1000  # type: ignore[missing-attribute]


def test_flatten_trend_rows_supports_csv():
    pl = pytest.importorskip("polars")
    from sparkparse.experiments_dashboard import flatten_trend_rows

    rows = [
        {"run_id": "1", "workflow_elapsed_ms": 1.0, "partial_metrics": ["read_bytes"]},
        {"run_id": "2", "workflow_elapsed_ms": 2.0, "partial_metrics": []},
    ]
    frame = pl.DataFrame(flatten_trend_rows(rows))
    assert frame.height == 2
    assert "read_bytes" in frame["partial_metrics"].to_list()[0]


def test_task_trend_figure_has_group_spread_error_bars(tmp_path):
    exp_dir = _experiment(tmp_path)
    manifest, snapshots = exp.load_experiment(exp_dir)
    rows = exp.trial_rows(manifest, snapshots)
    figure = _task_trend_figure(rows, "ingest", "1", "3")
    spread = [
        trace
        for trace in figure.data
        if trace.name and "spread" in trace.name  # type: ignore[missing-attribute,operator]
    ]
    assert spread
    # Run 1 is in the baseline variant with samples 1000ms and 3000ms.
    assert spread[0].error_y.array[0] > 0  # type: ignore[missing-attribute]


def test_dashboard_builds_without_caching_rows(tmp_path):
    exp_dir = _experiment(tmp_path)
    app = init_experiments_dashboard(str(exp_dir))
    assert app.layout is not None
    assert "EXPERIMENT_ROWS" not in app.server.config
    layout = build_layout(exp.trial_rows(*exp.load_experiment(exp_dir)), "1", "3")
    assert layout is not None
