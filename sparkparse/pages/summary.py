import dash_bootstrap_components as dbc
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import polars as pl
from dash import Input, Output, callback, dash_table, dcc, get_app, html
from pydantic import BaseModel

from sparkparse.analyze import analyze_dfs
from sparkparse.clean import get_job_idle_time, get_readable_col
from sparkparse.common import timeit
from sparkparse.styling import get_dt_style
from sparkparse.viz import style_fig


class SummaryCols(BaseModel):
    core: list[str]
    numeric: list[str]
    hidden: list[str]
    small: list[str]
    grouping: list[str]


def get_executor_table_df(data: list[dict], grouping_cols: list[str]) -> pd.DataFrame:
    if not data:
        # Connect captures have no task telemetry; the panel is unavailable, not empty.
        return pd.DataFrame()
    df_summary = (
        pl.DataFrame(data)
        .group_by(*grouping_cols)
        .agg(
            [
                pl.col("task_id").count().alias("tasks"),
                pl.col("task_duration_seconds").sum().alias("task_duration_seconds"),
                pl.col("bytes_read").sum().alias("bytes_read"),
                pl.col("bytes_written").sum().alias("bytes_written"),
                pl.col("shuffle_bytes_read").sum().alias("shuffle_bytes_read"),
                pl.col("shuffle_bytes_written").sum().alias("shuffle_bytes_written"),
            ]
        )
    )

    struct_cols = {
        "task_duration_seconds": "task_duration",
        "bytes_read": "input",
        "bytes_written": "output",
        "shuffle_bytes_read": "shuffle_read",
        "shuffle_bytes_written": "shuffle_write",
    }

    timing_conversions = [
        get_readable_col(pl.col(col).mul(1000), "timing").alias(
            f"{struct_cols[col]}_struct"
        )
        for col in struct_cols.keys()
        if "duration" in col
    ]

    size_conversions = [
        get_readable_col(pl.col(col), "size").alias(f"{struct_cols[col]}_struct")
        for col in struct_cols.keys()
        if "duration" not in col
    ]

    df_final = (
        df_summary.with_columns(timing_conversions + size_conversions)
        .with_columns(
            [
                pl.col(f"{col}_struct").struct.field("readable_str").alias(col)
                for col in struct_cols.values()
            ]
        )
        .sort("executor_id")
        .to_pandas()
    )

    return df_final


def get_table_df(data: list[dict], grouping_cols: list[str]) -> pd.DataFrame:
    if not data:
        return pd.DataFrame()
    df_summary = (
        pl.DataFrame(data)
        .group_by(*grouping_cols)
        .agg(
            [
                pl.col("task_id").count().alias("tasks"),
                pl.col("task_duration_seconds").sum().alias("task_duration_seconds"),
                pl.col("bytes_read").sum().alias("bytes_read"),
                pl.col("bytes_written").sum().alias("bytes_written"),
                pl.col("shuffle_bytes_read").sum().alias("shuffle_bytes_read"),
                pl.col("shuffle_bytes_written").sum().alias("shuffle_bytes_written"),
            ]
        )
    )

    struct_cols = {
        "stage_duration_seconds": "stage_duration",
        "task_duration_seconds": "task_duration",
        "bytes_read": "input",
        "bytes_written": "output",
        "shuffle_bytes_read": "shuffle_read",
        "shuffle_bytes_written": "shuffle_write",
    }

    timing_conversions = [
        get_readable_col(pl.col(col).mul(1000), "timing").alias(
            f"{struct_cols[col]}_struct"
        )
        for col in struct_cols.keys()
        if "duration" in col
    ]

    size_conversions = [
        get_readable_col(pl.col(col), "size").alias(f"{struct_cols[col]}_struct")
        for col in struct_cols.keys()
        if "duration" not in col
    ]

    df_final = (
        df_summary.rename(
            {"stage_start_timestamp": "submitted", "query_function": "query_func"}
        )
        .with_columns(timing_conversions + size_conversions)
        .with_columns(
            [
                pl.col(f"{col}_struct").struct.field("readable_str").alias(col)
                for col in struct_cols.values()
            ]
        )
        .with_columns(
            pl.col("submitted")
            .cast(pl.Datetime)
            .dt.strftime("%Y-%m-%d %H:%M:%S")
            .alias("submitted")
        )
        .sort("query_id", "job_id", "stage_id")
        .to_pandas()
    )

    return df_final


def apply_table_style(cols: SummaryCols, dark_mode: bool):
    metrics_style = get_dt_style(dark_mode)
    width_mapping = {col: 150 if col not in cols.small else 80 for col in cols.core}
    width_adjustment = [
        {
            "if": {"column_id": i},
            "minWidth": width_mapping[i],
            "maxWidth": width_mapping[i],
        }
        for i in width_mapping
    ]
    metrics_style["style_cell_conditional"].extend(width_adjustment)

    return metrics_style


def get_table_cols(df: pd.DataFrame, cols: SummaryCols):
    if df.empty:
        return [], []
    tbl_cols = []
    col_mapping = {}

    for col in cols.core:
        if col in cols.numeric:
            col_mapping[col] = {
                "type": "numeric",
                "format": {"specifier": ",d"},
                "id": col,
                "name": col.replace("_", " "),
            }
        else:
            col_mapping[col] = {"name": col.replace("_", " "), "id": col}

    core_df = df[cols.core + cols.hidden]
    core_records = core_df.to_dict("records")

    for col in core_df.columns:
        if col not in cols.hidden:
            tbl_cols.append(
                {**col_mapping[col], "id": col, "name": col.replace("_", " ")}
            )

    return core_records, tbl_cols


SUMMARY_COLS = SummaryCols(
    numeric=["query_id", "job_id", "stage_id", "tasks"],
    core=[
        "query_id",
        "query_func",
        "job_id",
        "stage_id",
        "submitted",
        "stage_duration",
        "tasks",
        "task_duration",
        "input",
        "output",
        "shuffle_read",
        "shuffle_write",
    ],
    hidden=[
        "task_duration_seconds",
        "bytes_read",
        "bytes_written",
        "shuffle_bytes_read",
        "shuffle_bytes_written",
    ],
    small=["query_id", "job_id", "stage_id", "tasks", "query_func"],
    grouping=[
        "query_id",
        "query_function",
        "job_id",
        "stage_id",
        "stage_start_timestamp",
        "stage_end_timestamp",
        "stage_duration_seconds",
    ],
)

EXECUTOR_COLS = SummaryCols(
    numeric=["executor_id", "tasks"],
    core=[
        "executor_id",
        "host",
        "tasks",
        "task_duration",
        "input",
        "output",
        "shuffle_read",
        "shuffle_write",
    ],
    hidden=[
        "task_duration_seconds",
        "bytes_read",
        "bytes_written",
        "shuffle_bytes_read",
        "shuffle_bytes_written",
    ],
    small=["executor_id", "tasks"],
    grouping=["executor_id", "host"],
)


def _compute_job_time(df_raw: pl.DataFrame) -> dict:
    return (
        df_raw.select(
            "job_id", "job_start_timestamp", "job_end_timestamp", "job_duration_seconds"
        )
        .unique()
        .select(
            pl.sum("job_duration_seconds").alias("job_cpu_time_seconds"),
            pl.min("job_start_timestamp").alias("first_job_start_timestamp"),
            pl.max("job_end_timestamp").alias("last_job_end_timestamp"),
        )
        .with_columns(
            pl.col("last_job_end_timestamp")
            .cast(pl.Datetime)
            .dt.epoch("ms")
            .sub(pl.col("first_job_start_timestamp").cast(pl.Datetime).dt.epoch("ms"))
            .alias("job_clock_time_ms")
        )
        .with_columns(
            get_readable_col(pl.col("job_clock_time_ms"), "timing").alias(
                "job_clock_time_struct"
            )
        )
        .with_columns(
            get_readable_col(pl.col("job_cpu_time_seconds").mul(1000), "timing").alias(
                "job_cpu_time_struct"
            )
        )
        .select(
            pl.col("job_clock_time_ms"),
            pl.col("job_cpu_time_seconds").mul(1000).alias("job_cpu_time_ms"),
            pl.col("job_cpu_time_struct")
            .struct.field("readable_str")
            .alias("cpu_time_str"),
            pl.col("job_clock_time_struct")
            .struct.field("readable_str")
            .alias("clock_time_str"),
        )
        .to_dicts()[0]
    )


def build_timeline_payload(combined: pl.DataFrame) -> dict:
    """Precompute the timeline so the browser store never holds raw task rows."""
    required = {
        "job_id",
        "job_start_timestamp",
        "job_end_timestamp",
        "job_duration_seconds",
        "stage_id",
        "stage_start_timestamp",
        "stage_end_timestamp",
        "stage_duration_seconds",
        "log_name",
        "parsed_log_name",
    }
    if combined.height == 0 or not required.issubset(combined.columns):
        return {}

    job_time = _compute_job_time(combined)
    stage_rank = (
        combined.select("stage_id")
        .unique()
        .sort("stage_id")
        .with_columns(pl.col("stage_id").rank().alias("stage_rank"))
    )
    stage_frame = (
        combined.select(
            "log_name",
            "parsed_log_name",
            "job_id",
            "stage_id",
            pl.col("stage_start_timestamp")
            .cast(pl.Datetime)
            .alias("stage_start_timestamp"),
            pl.col("stage_end_timestamp")
            .cast(pl.Datetime)
            .alias("stage_end_timestamp"),
            "stage_duration_seconds",
        )
        .unique()
        .with_columns(
            get_readable_col(
                pl.col("stage_duration_seconds").mul(1000), "timing"
            ).alias("stage_duration_struct")
        )
        .with_columns(
            pl.concat_str(
                [
                    pl.lit("stage #"),
                    pl.col("stage_id"),
                    pl.lit(" ["),
                    pl.col("stage_duration_struct").struct.field("readable_str"),
                    pl.lit("]"),
                ]
            ).alias("stage_label")
        )
        .join(stage_rank, on="stage_id")
        .sort("stage_id")
        .to_pandas()
    )
    if stage_frame.empty:
        return {}

    idle_time = get_job_idle_time(combined)
    clock = job_time.get("job_clock_time_ms") or 0
    pct_active = idle_time["idle_time_ms"] / clock * 100 if clock else 0.0
    return {
        "job_time": job_time,
        "idle_str": f"idle: {idle_time['readable']['readable_str']} [{pct_active:.2f}%]",
        "stage_frame": stage_frame.to_dict("records"),
    }


@callback(
    [
        Output("summary-table", "children"),
        Output("summary-table", "style"),
    ],
    [
        Input("stage-metrics-data", "data"),
        Input("color-mode-switch", "value"),
    ],
)
@timeit
def get_styled_metrics_table(df_data: list[dict], dark_mode: bool):
    cols = SUMMARY_COLS
    df = pd.DataFrame(df_data)
    metrics_style = apply_table_style(cols, dark_mode)
    metrics_style["style_table"]["maxHeight"] = "50vh"
    del metrics_style["style_table"]["height"]
    metrics_style["page_action"] = "native"

    core_records, tbl_cols = get_table_cols(df, cols)

    children = [
        html.H5(children="Summary Metrics", className="table-title"),
        dash_table.DataTable(
            data=core_records,
            id="summary-table",
            columns=tbl_cols,
            sort_by=[],
            sort_action="custom",
            page_size=25,
            **metrics_style,
        ),
    ]
    return children, {}


@callback(
    Output("summary-table", "data"),
    [
        Input("summary-table", "data"),
        Input("summary-table", "sort_by"),
    ],
)
def update_summary_table(data: list, sort_by: list):
    return update_table(data, sort_by)


@callback(
    Output("executor-table", "data"),
    [
        Input("executor-table", "data"),
        Input("executor-table", "sort_by"),
    ],
)
def update_executor_table(data: list, sort_by: list):
    return update_table(data, sort_by)


def update_table(data: list, sort_by: list):
    if not sort_by:
        return data

    df = pd.DataFrame(data)
    col_mapping = {
        "input": "bytes_read",
        "output": "bytes_written",
        "shuffle_read": "shuffle_bytes_read",
        "shuffle_write": "shuffle_bytes_written",
        "stage_duration": "stage_duration_seconds",
        "task_duration": "task_duration_seconds",
    }

    df_sorted = df
    for col in sort_by:
        if col["column_id"] in col_mapping:
            df_sorted = df_sorted.sort_values(
                col_mapping[col["column_id"]],
                ascending=col["direction"] == "asc",
                inplace=False,
            )
        else:
            df_sorted = df_sorted.sort_values(
                col["column_id"],
                ascending=col["direction"] == "asc",
                inplace=False,
            )

    return df_sorted.to_dict("records")


@callback(
    [
        Output("executor-table", "children"),
        Output("executor-table", "style"),
    ],
    [
        Input("executor-metrics-data", "data"),
        Input("color-mode-switch", "value"),
    ],
)
def get_styled_executor_table(df_data: list[dict], dark_mode: bool):
    cols = EXECUTOR_COLS
    df = pd.DataFrame(df_data)
    metrics_style = apply_table_style(cols, dark_mode)
    metrics_style["style_table"]["maxHeight"] = "25vh"
    del metrics_style["style_table"]["height"]
    metrics_style["page_action"] = "native"

    core_records, tbl_cols = get_table_cols(df, cols)

    children = [
        html.H5(children="Executors", className="table-title"),
        dash_table.DataTable(
            data=core_records,
            id="executor-table",
            columns=tbl_cols,
            sort_by=[],
            sort_action="custom",
            page_size=15,
            **metrics_style,
        ),
    ]
    return children, {}


@callback(
    [
        Output("stage-timeline", "figure"),
        Output("stage-timeline", "style"),
        Output("metrics-graph-fade", "is_in"),
    ],
    [
        Input("stage-timeline-data", "data"),
        Input("color-mode-switch", "value"),
    ],
)
def get_stage_timeline(payload: dict | None, dark_mode: bool):
    if not payload or not payload.get("stage_frame"):
        # Connect/serverless captures expose no task or stage telemetry.
        empty = go.Figure()
        empty.add_annotation(
            text="Stage timeline unavailable: no task or stage telemetry was captured.",
            showarrow=False,
            font={"size": 16},
        )
        empty.update_layout(
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            xaxis={"visible": False},
            yaxis={"visible": False},
        )
        return empty, {}, True

    df = pd.DataFrame(payload["stage_frame"])
    job_time = payload["job_time"]
    idle_str = payload["idle_str"]

    log_title = f"<b>{df['log_name'].iloc[0]}</b>"
    log_subtitle = f"<sup>{job_time['clock_time_str']} | {idle_str}</sup>"

    title = f"{log_title}<br>{log_subtitle}"

    n_stages = len(df["stage_id"].unique())
    fig = px.timeline(
        data_frame=df,
        x_start="stage_start_timestamp",
        x_end="stage_end_timestamp",
        y="stage_rank",
        title=title,
        height=500 + (25 * n_stages),
        text="stage_label",
    )

    fig = style_fig(
        fig=fig,
        dark_mode=dark_mode,
        min_x=min(df["stage_start_timestamp"]),
        max_x=max(df["stage_end_timestamp"]),
    )

    return fig, {}, True


@callback(
    [
        Output("stage-metrics-data", "data"),
        Output("executor-metrics-data", "data"),
        Output("stage-timeline-data", "data"),
        Output("issues-data", "data"),
    ],
    Input("log-name", "data"),
)
def get_records(log_name: str, **kwargs):
    dataset = get_app().server.config["DATASET"]
    dfs = dataset.dataframes(log_name)
    combined = dfs.combined
    # An artifact or in-memory capture carries explicit capability status; use it
    # so findings agree with the saved coverage. Raw logs have no preserved
    # result and fall back to the frames.
    analysis_input = dataset.result(log_name) or dfs
    report = analyze_dfs(analysis_input, log_name)

    # Aggregate server-side: the browser never receives the raw task table.
    stage_columns = {"task_id", *SUMMARY_COLS.grouping, "task_duration_seconds"}
    stage_columns |= {
        "bytes_read",
        "bytes_written",
        "shuffle_bytes_read",
        "shuffle_bytes_written",
    }
    executor_columns = stage_columns | {"executor_id", "host"}
    combined_records = (
        combined.to_pandas().to_dict("records") if combined.height else []
    )
    stage_records = (
        get_table_df(combined_records, SUMMARY_COLS.grouping).to_dict("records")
        if combined_records and stage_columns.issubset(combined.columns)
        else []
    )
    executor_records = (
        get_executor_table_df(
            combined_records, grouping_cols=EXECUTOR_COLS.grouping
        ).to_dict("records")
        if combined_records and executor_columns.issubset(combined.columns)
        else []
    )

    issues = {
        "findings": [finding.model_dump(mode="json") for finding in report.findings],
        "not_evaluated": [
            assessment.model_dump(mode="json")
            for assessment in report.assessments
            if assessment.status != "evaluated"
        ],
    }
    capabilities = dataset.capabilities(log_name)
    if capabilities is not None:
        issues["unavailable"] = [
            {
                "name": name,
                "status": capability["status"],
                "reason": capability["reason"],
            }
            for name, capability in capabilities.model_dump(mode="json").items()
            if capability["status"] not in ("available", "not_applicable")
            and capability["reason"]
        ]
    return (
        stage_records,
        executor_records,
        build_timeline_payload(combined),
        issues,
    )


@callback(
    Output("issues-panel", "children"),
    Input("issues-data", "data"),
)
def render_issues_panel(issues: dict | None):
    if not issues:
        return []

    findings = issues.get("findings", [])
    not_evaluated = issues.get("not_evaluated", [])
    unavailable = issues.get("unavailable", [])
    if not findings and not not_evaluated and not unavailable:
        return []

    severity_color = {"critical": "danger", "warning": "warning"}

    rows = [
        dbc.ListGroupItem(
            [
                dbc.Badge(
                    finding["category"],
                    color=severity_color.get(finding["severity"], "secondary"),
                    className="me-2",
                ),
                finding["observation"],
                html.Small(
                    f" ({finding['confidence']} confidence)", className="text-muted"
                ),
                html.Div(
                    finding["caveat"],
                    className="text-muted small fst-italic",
                )
                if finding.get("caveat")
                else None,
            ],
            color=severity_color.get(finding["severity"], "secondary"),
        )
        for finding in findings
    ]

    children = []
    if findings:
        children.extend(
            [
                html.H5(f"Issues ({len(findings)} found)", className="table-title"),
                dbc.ListGroup(rows, flush=True),
            ]
        )

    if not_evaluated:
        # Missing telemetry must be visible: an empty issue list is not a clean bill.
        children.extend(
            [
                html.H6("Checks not run", className="table-title mt-3"),
                dbc.ListGroup(
                    [
                        dbc.ListGroupItem(
                            [
                                dbc.Badge(
                                    assessment["status"].replace("_", " "),
                                    color="secondary",
                                    className="me-2",
                                ),
                                f"{assessment['rule_id']}: {assessment['reason']}",
                            ],
                            color="light",
                        )
                        for assessment in not_evaluated
                    ],
                    flush=True,
                ),
            ]
        )

    if unavailable:
        # Panels with no telemetry are explained here rather than rendered blank.
        children.extend(
            [
                html.H6("Unavailable panels", className="table-title mt-3"),
                dbc.ListGroup(
                    [
                        dbc.ListGroupItem(
                            [
                                dbc.Badge(
                                    capability["status"].replace("_", " "),
                                    color="secondary",
                                    className="me-2",
                                ),
                                f"{capability['name']}: {capability['reason']}",
                            ],
                            color="light",
                        )
                        for capability in unavailable
                    ],
                    flush=True,
                ),
            ]
        )

    children.append(html.Br())
    return children


def layout(log_name: str, **kwargs):
    return [
        dcc.Store("log-name", data=log_name),
        dcc.Store("stage-metrics-data"),
        dcc.Store("executor-metrics-data"),
        dcc.Store("stage-timeline-data"),
        dcc.Store("issues-data"),
        dbc.Fade(
            id="metrics-graph-fade",
            children=[
                html.Div(id="issues-panel"),
                dcc.Graph(
                    "stage-timeline",
                    style={"visibility": "hidden"},
                    config={"displayModeBar": False},
                ),
                html.Div(id="executor-table", style={"visibility": "hidden"}),
                html.Br(),
                html.Div(id="summary-table", style={"visibility": "hidden"}),
            ],
            style={"transition": "opacity 200ms ease-in", "minHeight": "100vh"},
            is_in=False,
        ),
    ]
