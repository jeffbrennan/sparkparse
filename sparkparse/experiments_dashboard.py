"""Dash/Plotly view for a saved experiment directory.

This is a standalone, read-only app: it loads the manifest and immutable
snapshots once, binds to loopback on launch, and refreshes only on request.
The same comparison functions used by the CLI and JSON output back every
number shown here.
"""

from __future__ import annotations

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

_SCALE = {
    MetricUnit.milliseconds: ("seconds", 1000.0),
    MetricUnit.bytes: ("MB", 1024.0 * 1024.0),
    MetricUnit.rows: ("rows", 1.0),
    MetricUnit.items: ("count", 1.0),
    MetricUnit.none: ("value", 1.0),
}


def _scaled(metric: str, value: float) -> tuple[str, float]:
    if metric in _TIME_METRICS:
        return "seconds", value / 1000.0
    if metric in _BYTE_METRICS:
        return "MB", value / (1024.0 * 1024.0)
    return "value", value


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


def _trend_figure(
    rows: list[dict[str, Any]], metric: str, baseline_run: str, candidate_run: str
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
            row["values"].get(context_metric, 0.0) / context_scale for row in rows
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
    median = sorted(values)[len(values) // 2]
    fig.add_hline(y=median, line_dash="dash", line_color=color, row=1, col=1)


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
                "baseline_setup_ms": task.baseline.get("setup_ms"),
                "candidate_setup_ms": task.candidate.get("setup_ms"),
                "baseline_cleanup_ms": task.baseline.get("cleanup_ms"),
                "candidate_cleanup_ms": task.candidate.get("cleanup_ms"),
            }
        )
    return rows


def _detail_table(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "run_id": row["run_id"],
            "variant": row["variant"],
            "revision": row["short_revision"],
            "collected_at": row["collected_at"],
            "status": row["status"],
            "config": row["config_fingerprint"],
            "input": row["input_snapshot"],
            "coverage": ", ".join(
                f"{key}={value}" for key, value in sorted(row["coverage"].items())
            ),
            "failed_tasks": ", ".join(row["failed_tasks"]),
            "run_url": row["run_page_url"],
        }
        for row in rows
    ]


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


def build_layout(rows: list[dict[str, Any]]) -> html.Div:
    options = _run_options(rows)
    default_baseline = rows[0]["run_id"] if rows else None
    default_candidate = rows[-1]["run_id"] if rows else None
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
                            html.Label("Baseline run"),
                            dcc.Dropdown(
                                id="baseline-select",
                                options=options,
                                value=default_baseline,
                                clearable=False,
                            ),
                        ],
                        width=4,
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
                        width=4,
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
                        width=4,
                    ),
                ]
            ),
            html.Br(),
            dcc.Graph(id="trend-figure"),
            html.Div(id="comparison-notes", style={"padding": "8px"}),
            html.H5("Metrics"),
            html.Div(id="metrics-table-container"),
            html.H5("Tasks"),
            html.Div(id="tasks-table-container"),
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


def init_experiments_dashboard(exp_dir: str) -> dash.Dash:
    manifest, snapshots = exp.load_experiment(exp_dir)
    rows = exp.trial_rows(manifest, snapshots)

    app = dash.Dash(
        __name__,
        external_stylesheets=[dbc.themes.BOOTSTRAP, dbc.icons.FONT_AWESOME],
        suppress_callback_exceptions=True,
    )
    app.layout = build_layout(rows)
    app.server.config["EXPERIMENT_DIR"] = exp_dir
    app.server.config["EXPERIMENT_ROWS"] = rows
    app.server.config["EXPERIMENT_MANIFEST"] = manifest

    @app.callback(  # type: ignore[missing-attribute]
        [
            Output("trend-figure", "figure"),
            Output("metrics-table-container", "children"),
            Output("tasks-table-container", "children"),
            Output("detail-table-container", "children"),
            Output("comparison-notes", "children"),
            Output("experiment-summary", "children"),
            Output("export-payload", "data"),
        ],
        [
            Input("baseline-select", "value"),
            Input("candidate-select", "value"),
            Input("metric-select", "value"),
        ],
    )
    def refresh(baseline_run: str, candidate_run: str, metric: str):
        result = exp.compare_experiment(
            exp_dir,
            baseline_run_id=baseline_run,
            candidate_run_id=candidate_run,
        )
        figure = _trend_figure(rows, metric, baseline_run, candidate_run)
        notes = (
            html.Ul([html.Li(note) for note in result.notes]) if result.notes else ""
        )
        summary = html.Div(
            [
                html.Span(f"experiment_id: {result.experiment_id}  "),
                html.Span(f"revision confidence: {result.revision_confidence}  "),
                html.Span(
                    f"baseline n={result.baseline.sample_count} "
                    f"candidate n={result.candidate.sample_count}"
                ),
            ]
        )
        comparability = [
            {"aspect": note.aspect, "status": note.status, "detail": note.detail}
            for note in result.comparability
        ]
        payload = {
            "trend": [
                {"run_id": row["run_id"], "variant": row["variant"], **row["values"]}
                for row in rows
            ],
            "metrics": [m.model_dump(mode="json") for m in result.metrics],
            "tasks": [t.model_dump(mode="json") for t in result.tasks],
            "comparability": comparability,
        }
        return (
            figure,
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
                _metric_table(result),
                "metrics-table",
            ),
            _table(
                [
                    "task_key",
                    "change",
                    "baseline_execution_ms",
                    "candidate_execution_ms",
                    "delta_execution_ms",
                    "baseline_setup_ms",
                    "candidate_setup_ms",
                    "baseline_cleanup_ms",
                    "candidate_cleanup_ms",
                ],
                _task_table(result),
                "tasks-table",
            ),
            _table(
                [
                    "run_id",
                    "variant",
                    "revision",
                    "collected_at",
                    "status",
                    "config",
                    "input",
                    "coverage",
                    "failed_tasks",
                    "run_url",
                ],
                _detail_table(rows),
                "detail-table",
            ),
            notes,
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
