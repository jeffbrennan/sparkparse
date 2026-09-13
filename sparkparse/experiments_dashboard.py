"""Dash/Plotly view for a saved experiment directory.

This is a standalone, read-only app: it loads the manifest and immutable
snapshots once, binds to loopback on launch, and reloads from disk on request so
a refresh reflects newly recorded trials. The same comparison functions used by
the CLI and JSON output back every number shown here.
"""

from __future__ import annotations

import statistics
from typing import Any

import dash
import dash_bootstrap_components as dbc
import plotly.graph_objects as go
from dash import Input, Output, State, dcc, html
from plotly.subplots import make_subplots

from sparkparse import experiments as exp
from sparkparse.models import ExperimentComparison, MetricUnit

_TIME_METRICS = {
    "workflow_elapsed_ms": "Workflow elapsed",
    "task_execution_ms": "Aggregate task time",
    "aggregate_task_time_ms": "Query task time",
    "query_execution_ms": "Query execution",
    "query_total_ms": "Query total",
}
_BYTE_METRICS = {
    "read_bytes": "Read bytes",
    "read_remote_bytes": "Read remote bytes",
    "read_cache_bytes": "Read cache bytes",
    "written_bytes_remote": "Written bytes (remote)",
    "disk_spill_bytes": "Disk spill",
    "shuffle_network_bytes": "Shuffle network bytes",
}
_OTHER_METRICS = {
    "rows_read": "Rows read",
    "rows_produced": "Rows produced",
    "queries_result_from_cache": "Queries served from cache",
    "read_files": "Files read",
    "read_partitions": "Partitions read",
}
_ALL_METRICS = {**_TIME_METRICS, **_BYTE_METRICS, **_OTHER_METRICS}

_ELIGIBILITY = ("ok", "warmup", "failed", "active", "incomplete")

_SCALE = {
    MetricUnit.milliseconds: ("seconds", 1000.0),
    MetricUnit.bytes: ("MB", 1024.0 * 1024.0),
    MetricUnit.rows: ("rows", 1.0),
    MetricUnit.items: ("count", 1.0),
    MetricUnit.none: ("value", 1.0),
}


def _unit_for(metric: str) -> MetricUnit:
    if metric in _TIME_METRICS:
        return MetricUnit.milliseconds
    if metric in _BYTE_METRICS:
        return MetricUnit.bytes
    return MetricUnit.rows


def _run_options(rows: list[dict[str, Any]]) -> list[dict[str, str]]:
    return [
        {
            "label": f"{row['variant']} @ {row['short_revision']} ({row['run_id']})",
            "value": row["run_id"],
        }
        for row in rows
    ]


def _multi_options(values: list[str]) -> list[dict[str, str]]:
    return [{"label": value, "value": value} for value in values]


def _filter_rows(
    rows: list[dict[str, Any]],
    variants: list[str] | None,
    revisions: list[str] | None,
    eligibility: list[str] | None,
) -> list[dict[str, Any]]:
    return [
        row
        for row in rows
        if (not variants or row["variant"] in variants)
        and (not revisions or row["short_revision"] in revisions)
        and (not eligibility or row["eligibility"] in eligibility)
    ]


def _trend_figure(
    rows: list[dict[str, Any]],
    metric: str,
    baseline_run: str | None,
    candidate_run: str | None,
) -> go.Figure:
    unit = _unit_for(metric)
    scale_label, scale = _SCALE[unit]
    fig = make_subplots(
        rows=2,
        cols=1,
        shared_xaxes=True,
        subplot_titles=(_ALL_METRICS.get(metric, metric), f"Context ({scale_label})"),
    )
    xs = [row["run_id"] for row in rows]
    primary = [
        row["values"].get(metric) / scale if metric in row["values"] else None
        for row in rows
    ]
    fig.add_trace(
        go.Scatter(
            x=xs,
            y=primary,
            mode="markers+lines",
            name=_ALL_METRICS.get(metric, metric),
            text=[f"{row['variant']} {row['short_revision']}" for row in rows],
            hovertemplate="%{text}<br>%{y:.3f} " + scale_label + "<extra></extra>",
            connectgaps=False,
        ),
        row=1,
        col=1,
    )
    context_metric = "workflow_elapsed_ms" if metric in _BYTE_METRICS else "read_bytes"
    if context_metric in _ALL_METRICS:
        _, context_scale = _SCALE[
            MetricUnit.milliseconds
            if context_metric in _TIME_METRICS
            else MetricUnit.bytes
        ]
        context_values = [
            row["values"].get(context_metric) / context_scale
            if context_metric in row["values"]
            else None
            for row in rows
        ]
        context_label = (
            "seconds"
            if context_metric in _TIME_METRICS
            else "MB"
            if context_metric in _BYTE_METRICS
            else "value"
        )
        fig.add_trace(
            go.Bar(
                x=xs,
                y=context_values,
                name=_ALL_METRICS.get(context_metric, context_metric),
                hovertemplate="%{y:.3f} " + context_label + "<extra></extra>",
            ),
            row=2,
            col=1,
        )
    for group, run_id, color in (
        ("baseline", baseline_run, "#1f77b4"),
        ("candidate", candidate_run, "#ff7f0e"),
    ):
        if not run_id:
            continue
        fig.add_vline(x=run_id, line_dash="dot", line_color=color, row=1, col=1)
        _add_median_line(fig, rows, metric, run_id, color, scale)
    fig.update_layout(
        margin={"l": 40, "r": 20, "t": 40, "b": 30},
        height=460,
        legend={"orientation": "h"},
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
    )
    return fig


def _add_median_line(
    fig: go.Figure,
    rows: list[dict[str, Any]],
    metric: str,
    run_id: str | None,
    color: str,
    scale: float,
) -> None:
    if run_id is None:
        return
    values = [
        row["values"][metric] / scale
        for row in rows
        if row["run_id"] == run_id and metric in row["values"]
    ]
    if not values:
        return
    median = statistics.median(values)
    fig.add_hline(y=median, line_dash="dash", line_color=color, row=1, col=1)


def _task_samples_for_variant(
    rows: list[dict[str, Any]], task_key: str, variant: str
) -> list[float]:
    return [
        row["task_values"][task_key]
        for row in rows
        if row["variant"] == variant and row["task_values"].get(task_key) is not None
    ]


def _task_trend_figure(
    rows: list[dict[str, Any]],
    task_key: str | None,
    baseline_run: str | None,
    candidate_run: str | None,
) -> go.Figure:
    fig = go.Figure()
    if task_key is None:
        return fig
    xs = [row["run_id"] for row in rows]
    ys = [
        row["task_values"].get(task_key)
        if row["task_values"].get(task_key) is not None
        else None
        for row in rows
    ]
    fig.add_trace(
        go.Scatter(
            x=xs,
            y=ys,
            mode="markers+lines",
            name=f"{task_key} execution",
            text=[f"{row['variant']} {row['short_revision']}" for row in rows],
            hovertemplate="%{text}<br>%{y:.0f} ms<extra></extra>",
            connectgaps=False,
        )
    )
    variant_by_run = {row["run_id"]: row["variant"] for row in rows}
    for run_id, color, label in (
        (baseline_run, "#1f77b4", "baseline"),
        (candidate_run, "#ff7f0e", "candidate"),
    ):
        if not run_id:
            continue
        variant = variant_by_run.get(run_id)
        if variant is None:
            continue
        samples = _task_samples_for_variant(rows, task_key, variant)
        if not samples:
            continue
        median = statistics.median(samples)
        fig.add_trace(
            go.Scatter(
                x=[run_id],
                y=[median],
                mode="markers",
                marker={"size": 14, "color": color, "symbol": "diamond"},
                error_y={
                    "type": "data",
                    "symmetric": False,
                    "array": [max(samples) - median],
                    "arrayminus": [median - min(samples)],
                },
                name=f"{label} variant spread (n={len(samples)})",
                hovertemplate=(
                    f"{label} variant median %{{y:.0f}} ms"
                    f"<br>min {min(samples):.0f} / max {max(samples):.0f}"
                    "<extra></extra>"
                ),
            )
        )
    fig.update_layout(
        margin={"l": 40, "r": 20, "t": 40, "b": 30},
        height=360,
        yaxis_title="ms",
        legend={"orientation": "h"},
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
    )
    return fig


def _metric_table(comparison: ExperimentComparison) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for metric in comparison.metrics:
        rows.append(
            {
                "metric": metric.metric,
                "baseline": metric.baseline_median,
                "candidate": metric.candidate_median,
                "delta": metric.delta,
                "pct_change": metric.pct_change,
                "baseline_n": len(metric.baseline_values),
                "candidate_n": len(metric.candidate_values),
                "caveat": metric.caveat,
            }
        )
    return rows


def _fmt_samples(values: list[float] | None) -> str:
    if not values:
        return ""
    return ", ".join(f"{value:.0f}" for value in values)


def _task_table(comparison: ExperimentComparison) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for task in comparison.tasks:
        rows.append(
            {
                "task_key": task.task_key,
                "change": task.change,
                "baseline_execution_ms": task.baseline.get("execution_ms"),
                "candidate_execution_ms": task.candidate.get("execution_ms"),
                "delta_execution_ms": task.deltas.get("execution_ms"),
                "baseline_samples": _fmt_samples(
                    task.baseline_values.get("execution_ms")
                ),
                "candidate_samples": _fmt_samples(
                    task.candidate_values.get("execution_ms")
                ),
                "baseline_range": _fmt_range(
                    task.baseline_min.get("execution_ms"),
                    task.baseline_max.get("execution_ms"),
                ),
                "candidate_range": _fmt_range(
                    task.candidate_min.get("execution_ms"),
                    task.candidate_max.get("execution_ms"),
                ),
            }
        )
    return rows


def _fmt_range(low: float | None, high: float | None) -> str:
    if low is None or high is None:
        return ""
    return f"{low:.0f}–{high:.0f}"


def _detail_table(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "run_id": row["run_id"],
            "variant": row["variant"],
            "revision": row["short_revision"],
            "collected_at": row["collected_at"],
            "status": row["status"],
            "eligibility": row["eligibility"],
            "config": row["config_fingerprint"],
            "input": row["input_snapshot"],
            "compute": row["compute_summary"],
            "coverage": ", ".join(
                f"{key}={value}" for key, value in sorted(row["coverage"].items())
            ),
            "top_queries": " | ".join(
                f"{q['query_id'][:8]} {q['total_ms']}ms" for q in row["top_queries"]
            ),
            "failure_excerpts": " || ".join(
                f"{f['task_key']}: {f['excerpt']}" for f in row["failure_excerpts"]
            ),
            "run_url": row["run_page_url"],
        }
        for row in rows
    ]


_CONTEXT_KEYS = {
    "revision": "short_revision",
    "input_snapshot": "input_snapshot",
    "configuration": "config_fingerprint",
    "compute": "compute_summary",
}


def _context_table(
    comparison: ExperimentComparison,
    baseline_row: dict[str, Any] | None,
    candidate_row: dict[str, Any] | None,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for note in comparison.comparability:
        key = _CONTEXT_KEYS.get(note.aspect)
        rows.append(
            {
                "aspect": note.aspect,
                "status": note.status,
                "baseline": (baseline_row or {}).get(key) if key else None,
                "candidate": (candidate_row or {}).get(key) if key else None,
                "detail": note.detail,
            }
        )
    return rows


def _table(columns: list[str], data: list[dict[str, Any]], table_id: str) -> html.Div:
    if not data:
        return html.Div("No data.", style={"padding": "8px"})
    return html.Div(
        dash.dash_table.DataTable(
            id=table_id,
            columns=[{"name": c.replace("_", " "), "id": c} for c in columns],
            data=data,
            sort_action="native",
            page_action="native",
            page_size=12,
            style_table={"overflowX": "auto"},
        )
    )


def build_layout(
    rows: list[dict[str, Any]],
    default_baseline: str | None = None,
    default_candidate: str | None = None,
) -> html.Div:
    options = _run_options(rows)
    variants = sorted({row["variant"] for row in rows})
    revisions = sorted({row["short_revision"] for row in rows})
    task_keys = sorted({key for row in rows for key in row["task_values"]})
    metric_options = [
        {"label": _ALL_METRICS[m], "value": m}
        for m in _ALL_METRICS
        if any(m in row["values"] for row in rows)
    ] or [{"label": "Workflow elapsed", "value": "workflow_elapsed_ms"}]
    return dbc.Container(
        [
            html.H3("Experiment"),
            html.Div(id="experiment-summary"),
            dbc.Row(
                [
                    dbc.Col(
                        [
                            html.Label("Variants"),
                            dcc.Dropdown(
                                id="variant-filter",
                                options=_multi_options(variants),
                                value=variants,
                                multi=True,
                            ),
                        ],
                        width=4,
                    ),
                    dbc.Col(
                        [
                            html.Label("Commits"),
                            dcc.Dropdown(
                                id="revision-filter",
                                options=_multi_options(revisions),
                                value=revisions,
                                multi=True,
                            ),
                        ],
                        width=4,
                    ),
                    dbc.Col(
                        [
                            html.Label("Eligibility"),
                            dcc.Dropdown(
                                id="eligibility-filter",
                                options=_multi_options(list(_ELIGIBILITY)),
                                value=list(_ELIGIBILITY),
                                multi=True,
                            ),
                        ],
                        width=4,
                    ),
                ]
            ),
            html.Br(),
            dbc.Row(
                [
                    dbc.Col(
                        [
                            html.Label("Baseline run"),
                            dcc.Dropdown(
                                id="baseline-select",
                                options=options,
                                value=default_baseline,
                                clearable=False,
                            ),
                        ],
                        width=3,
                    ),
                    dbc.Col(
                        [
                            html.Label("Candidate run"),
                            dcc.Dropdown(
                                id="candidate-select",
                                options=options,
                                value=default_candidate,
                                clearable=False,
                            ),
                        ],
                        width=3,
                    ),
                    dbc.Col(
                        [
                            html.Label("Metric"),
                            dcc.Dropdown(
                                id="metric-select",
                                options=metric_options,
                                value=metric_options[0]["value"],
                                clearable=False,
                            ),
                        ],
                        width=3,
                    ),
                    dbc.Col(
                        [
                            html.Label("Task"),
                            dcc.Dropdown(
                                id="task-select",
                                options=_multi_options(task_keys),
                                value=task_keys[0] if task_keys else None,
                                clearable=False,
                            ),
                        ],
                        width=3,
                    ),
                ]
            ),
            html.Br(),
            dcc.Graph(id="trend-figure"),
            html.H5("Task trend and group spread"),
            dcc.Graph(id="task-trend-figure"),
            html.Div(id="comparison-notes", style={"padding": "8px"}),
            html.H5("Metrics"),
            html.Div(id="metrics-table-container"),
            html.H5("Tasks"),
            html.Div(id="tasks-table-container"),
            html.H5("Run context"),
            html.Div(id="context-table-container"),
            html.H5("Run detail"),
            html.Div(id="detail-table-container"),
            html.Br(),
            dbc.ButtonGroup(
                [
                    dbc.Button("Download CSV", id="download-csv", n_clicks=0),
                    dbc.Button("Download JSON", id="download-json", n_clicks=0),
                ]
            ),
            dcc.Store(id="export-payload"),
            dcc.Download(id="download"),
        ],
        fluid=True,
    )


def default_runs(
    manifest: exp.ExperimentManifest, rows: list[dict[str, Any]]
) -> tuple[str | None, str | None]:
    """Default selectors: the manifest's pinned baseline, and the newest trial."""
    pinned = next(
        (t.run_id for t in manifest.trials if t.trial_id == manifest.baseline_trial_id),
        None,
    )
    return pinned or (rows[0]["run_id"] if rows else None), (
        rows[-1]["run_id"] if rows else None
    )


def init_experiments_dashboard(exp_dir: str) -> dash.Dash:
    manifest, snapshots = exp.load_experiment(exp_dir)
    rows = exp.trial_rows(manifest, snapshots)
    default_baseline, default_candidate = default_runs(manifest, rows)

    app = dash.Dash(
        __name__,
        external_stylesheets=[dbc.themes.BOOTSTRAP, dbc.icons.FONT_AWESOME],
        suppress_callback_exceptions=True,
    )
    app.layout = build_layout(rows, default_baseline, default_candidate)
    app.server.config["EXPERIMENT_DIR"] = exp_dir

    @app.callback(  # type: ignore[missing-attribute]
        [
            Output("trend-figure", "figure"),
            Output("task-trend-figure", "figure"),
            Output("metrics-table-container", "children"),
            Output("tasks-table-container", "children"),
            Output("context-table-container", "children"),
            Output("detail-table-container", "children"),
            Output("comparison-notes", "children"),
            Output("experiment-summary", "children"),
            Output("export-payload", "data"),
        ],
        [
            Input("baseline-select", "value"),
            Input("candidate-select", "value"),
            Input("metric-select", "value"),
            Input("task-select", "value"),
            Input("variant-filter", "value"),
            Input("revision-filter", "value"),
            Input("eligibility-filter", "value"),
        ],
    )
    def refresh(
        baseline_run: str | None,
        candidate_run: str | None,
        metric: str,
        task_key: str | None,
        variants: list[str] | None,
        revisions: list[str] | None,
        eligibility: list[str] | None,
    ):
        manifest, snapshots = exp.load_experiment(exp_dir)
        rows = exp.trial_rows(manifest, snapshots)
        filtered = _filter_rows(rows, variants, revisions, eligibility)

        comparison: ExperimentComparison | None = None
        error: str | None = None
        if baseline_run and candidate_run:
            try:
                comparison = exp.compare_experiment(
                    exp_dir,
                    baseline_run_id=baseline_run,
                    candidate_run_id=candidate_run,
                )
            except exp.ExperimentError as exc:
                error = str(exc)

        baseline_row = next(
            (row for row in rows if row["run_id"] == baseline_run), None
        )
        candidate_row = next(
            (row for row in rows if row["run_id"] == candidate_run), None
        )
        figure = _trend_figure(filtered, metric, baseline_run, candidate_run)
        task_figure = _task_trend_figure(
            filtered, task_key, baseline_run, candidate_run
        )

        if comparison is None:
            notes = html.Div(error or "Select two runs to compare.")
            return (
                figure,
                task_figure,
                html.Div("No comparison.", style={"padding": "8px"}),
                html.Div("No comparison.", style={"padding": "8px"}),
                html.Div("No comparison.", style={"padding": "8px"}),
                _table(
                    [
                        "run_id",
                        "variant",
                        "revision",
                        "collected_at",
                        "status",
                        "eligibility",
                        "config",
                        "input",
                        "compute",
                        "coverage",
                        "top_queries",
                        "failure_excerpts",
                        "run_url",
                    ],
                    _detail_table(filtered),
                    "detail-table",
                ),
                notes,
                html.Div(error or ""),
                {
                    "trend": [
                        {
                            "run_id": row["run_id"],
                            "variant": row["variant"],
                            **row["values"],
                        }
                        for row in filtered
                    ],
                    "error": error,
                },
            )

        notes_children = (
            html.Ul([html.Li(note) for note in comparison.notes])
            if comparison.notes
            else ""
        )
        summary = html.Div(
            [
                html.Span(f"experiment_id: {comparison.experiment_id}  "),
                html.Span(f"revision confidence: {comparison.revision_confidence}  "),
                html.Span(
                    f"baseline n={comparison.baseline.sample_count} "
                    f"candidate n={comparison.candidate.sample_count}  "
                ),
                html.Span(f"showing {len(filtered)}/{len(rows)} trial(s)"),
            ]
        )
        payload = {
            "trend": [
                {"run_id": row["run_id"], "variant": row["variant"], **row["values"]}
                for row in filtered
            ],
            "metrics": [m.model_dump(mode="json") for m in comparison.metrics],
            "tasks": [t.model_dump(mode="json") for t in comparison.tasks],
            "comparability": [
                note.model_dump(mode="json") for note in comparison.comparability
            ],
        }
        return (
            figure,
            task_figure,
            _table(
                [
                    "metric",
                    "baseline",
                    "candidate",
                    "delta",
                    "pct_change",
                    "baseline_n",
                    "candidate_n",
                    "caveat",
                ],
                _metric_table(comparison),
                "metrics-table",
            ),
            _table(
                [
                    "task_key",
                    "change",
                    "baseline_execution_ms",
                    "candidate_execution_ms",
                    "delta_execution_ms",
                    "baseline_samples",
                    "candidate_samples",
                    "baseline_range",
                    "candidate_range",
                ],
                _task_table(comparison),
                "tasks-table",
            ),
            _table(
                ["aspect", "status", "baseline", "candidate", "detail"],
                _context_table(comparison, baseline_row, candidate_row),
                "context-table",
            ),
            _table(
                [
                    "run_id",
                    "variant",
                    "revision",
                    "collected_at",
                    "status",
                    "eligibility",
                    "config",
                    "input",
                    "compute",
                    "coverage",
                    "top_queries",
                    "failure_excerpts",
                    "run_url",
                ],
                _detail_table(filtered),
                "detail-table",
            ),
            notes_children,
            summary,
            payload,
        )

    @app.callback(  # type: ignore[missing-attribute]
        Output("download", "data"),
        [
            Input("download-csv", "n_clicks"),
            Input("download-json", "n_clicks"),
        ],
        State("export-payload", "data"),
        prevent_initial_call=True,
    )
    def download(csv_clicks: int, json_clicks: int, payload: dict[str, Any]):
        triggered = dash.callback_context.triggered_id
        if triggered == "download-json":
            return dcc.send_string(
                __import__("json").dumps(payload, indent=2), "experiment.json"
            )
        import polars as pl

        frame = pl.DataFrame(payload.get("trend", []))
        return dcc.send_string(frame.write_csv(), "experiment-trend.csv")

    return app
