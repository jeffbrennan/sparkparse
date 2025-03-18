import dash_bootstrap_components as dbc
import pandas as pd
import plotly.express as px
import polars as pl
from dash import Input, Output, callback, dash_table, dcc, html
from plotly.graph_objs import Figure
from pydantic import BaseModel

from sparkparse.clean import get_readable_col
from sparkparse.common import timeit
from sparkparse.parse import get_parsed_metrics
from sparkparse.styling import get_dt_style, get_site_colors
from sparkparse.viz import style_fig


class SummaryCols(BaseModel):
    core: list[str]
    numeric: list[str]
    hidden: list[str]
    small: list[str]
    grouping: list[str]


@callback(
    [
        Output("metrics", "figure"),
        Output("metrics", "style"),
        Output("metrics-graph-fade", "is_in"),
    ],
    [
        Input("summary-metrics-df", "data"),
        Input("color-mode-switch", "value"),
    ],
)
@timeit
def get_metrics_viz(
    df_data: list[dict],
    dark_mode: bool,
) -> tuple[Figure, dict, bool]:
    bg_color, font_color = get_site_colors(dark_mode, False)
    if dark_mode:
        template = "plotly_dark"
    else:
        template = "plotly"

    cols = [
        "log_name",
        "parsed_log_name",
        "job_id",
        "stage_id",
        "task_id",
        "task_duration_seconds",
    ]
    df = pd.DataFrame(df_data)[cols].sort_values("task_id")
    df["task_duration_minutes"] = (df["task_duration_seconds"] / 60).round(3)
    df["cumulative_runtime"] = df["task_duration_minutes"].cumsum()

    raw_log_subtitle = f"<br><sup>raw log: {df['log_name'].iloc[0]}"
    parsed_log_subtitle = f"| parsed log: {df['parsed_log_name'].iloc[0]}</sup>"
    title = f"<b>Cumulative Task Runtime (minutes)</b>{raw_log_subtitle}{parsed_log_subtitle}"

    fig = px.scatter(
        df,
        x="task_id",
        y="cumulative_runtime",
        color="job_id",
        title=title,
        template=template,
        custom_data=["job_id", "stage_id", "task_duration_minutes"],
    )
    fig.update_traces(
        hovertemplate=(
            "Job #%{customdata[0]}<br>Stage #%{customdata[1]}<br>Task #%{x}<br><br>Task runtime: %{customdata[2]} min<br>Cumulative runtime: %{y} min<extra></extra>"
        ),
    )

    x_min = df["task_id"].min()
    x_max = df["task_id"].max()

    fig = style_fig(fig, dark_mode, x_min, x_max)
    return fig, {}, True


def get_executor_table_df(data: list[dict], grouping_cols: list[str]) -> pd.DataFrame:
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
        .with_columns(pl.col("submitted").str.replace("T", " ").alias("submitted"))
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


@callback(
    [
        Output("metrics-table", "children"),
        Output("metrics-table", "style"),
    ],
    [
        Input("summary-metrics-df", "data"),
        Input("color-mode-switch", "value"),
    ],
)
@timeit
def get_styled_metrics_table(df_data: list[dict], dark_mode: bool):
    cols = SummaryCols(
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

    df = get_table_df(df_data, cols.grouping)
    metrics_style = apply_table_style(cols, dark_mode)
    metrics_style["style_table"]["maxHeight"] = "50vh"
    del metrics_style["style_table"]["height"]

    core_records, tbl_cols = get_table_cols(df, cols)

    children = [
        html.H5(children="Summary Metrics", className="table-title"),
        dash_table.DataTable(
            data=core_records,
            id="summary-table",
            columns=tbl_cols,
            sort_by=[],
            sort_action="custom",
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
        Input("summary-metrics-df", "data"),
        Input("color-mode-switch", "value"),
    ],
)
def get_styled_executor_table(df_data: list[dict], dark_mode: bool):
    cols = SummaryCols(
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

    df = get_executor_table_df(df_data, grouping_cols=["executor_id", "host"])
    metrics_style = apply_table_style(cols, dark_mode)

    metrics_style["style_table"]["maxHeight"] = "25vh"
    del metrics_style["style_table"]["height"]

    core_records, tbl_cols = get_table_cols(df, cols)

    children = [
        html.H5(children="Executors", className="table-title"),
        dash_table.DataTable(
            data=core_records,
            id="executor-table",
            columns=tbl_cols,
            sort_by=[],
            sort_action="custom",
            **metrics_style,
        ),
    ]
    return children, {}


@callback(
    Output("summary-metrics-df", "data"),
    Input("log-name", "data"),
)
def get_records(log_name: str, **kwargs):
    df = get_parsed_metrics(log_file=log_name, out_dir=None, out_format=None).combined
    return df.to_pandas().to_dict("records")


def layout(log_name: str, **kwargs):
    return [
        dcc.Store("log-name", data=log_name),
        dcc.Store("summary-metrics-df"),
        dbc.Fade(
            id="metrics-graph-fade",
            children=[
                dcc.Graph(
                    "metrics",
                    style={"visibility": "hidden"},
                    config={"displayModeBar": False},
                ),
                html.Div(id="executor-table", style={"visibility": "hidden"}),
                html.Br(),
                html.Div(id="metrics-table", style={"visibility": "hidden"}),
            ],
            style={"transition": "opacity 200ms ease-in", "minHeight": "100vh"},
            is_in=False,
        ),
    ]
