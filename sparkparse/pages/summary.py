import dash
import dash_bootstrap_components as dbc
import pandas as pd
import plotly.express as px
import polars as pl
from dash import Input, Output, callback, dash_table, dcc, html
from plotly.graph_objs import Figure

from sparkparse.common import timeit
from sparkparse.parse import get_parsed_metrics
from sparkparse.styling import get_dt_style
from sparkparse.viz import style_fig


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
    if dark_mode:
        template = "plotly_dark"
    else:
        template = "plotly"

    df = pd.DataFrame(df_data).sort_values("task_id")
    df["cumulative_runtime"] = df["task_duration_seconds"].cumsum()

    raw_log_subtitle = f"<br><sup>raw log: {df['log_name'].iloc[0]}"
    parsed_log_subtitle = f"| parsed log: {df['parsed_log_name'].iloc[0]}</sup>"
    title = f"<b>Cumulative Task Runtime</b>{raw_log_subtitle}{parsed_log_subtitle}"

    fig = px.line(
        df,
        x="task_id",
        y="cumulative_runtime",
        title=title,
        template=template,
    )
    scatter = px.scatter(
        df,
        x="task_id",
        y="cumulative_runtime",
        color="node_type",
        template=template,
    )

    for trace in scatter.data:
        fig.add_trace(trace)

    fig.data[0].line.color = "black"  # type: ignore

    x_min = df["task_id"].min()
    x_max = df["task_id"].max()

    fig = style_fig(fig, dark_mode, x_min, x_max)
    return fig, {}, True


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
def get_styled_metrics_table(df_data: list[dict], dark_mode: bool):
    df = pd.DataFrame(df_data)
    metrics_style = get_dt_style(dark_mode)
    metrics_style["style_table"]["height"] = "60vh"
    tbl_cols = []
    # float_style = {"type": "numeric", "format": {"specifier": ".1f"}}
    int_style = {"type": "numeric", "format": {"specifier": ",d"}}

    core_cols = [
        "job_id",
        "stage_id",
        "task_id",
        "node_type",
        "task_duration_seconds",
        "executor_run_time_seconds",
        "result_size_bytes",
        "records_read",
        "records_written",
        "bytes_read",
        "bytes_written",
        "shuffle_write_time_seconds",
        "shuffle_bytes_written",
    ]

    col_mapping = {col: int_style for col in core_cols if col in df.columns}
    width_mapping = {col: 150 for col in core_cols if col in df.columns}
    width_adjustment = [
        {
            "if": {"column_id": i},
            "minWidth": width_mapping[i],
            "maxWidth": width_mapping[i],
        }
        for i in width_mapping
    ]
    metrics_style["style_cell_conditional"].extend(width_adjustment)
    core_df = df[core_cols]
    for col in core_df.columns:
        tbl_cols.append({**col_mapping[col], "id": col, "name": col.replace("_", " ")})

    core_records = core_df.to_dict("records")

    tbl = dash_table.DataTable(
        core_records,
        columns=tbl_cols,
        **metrics_style,
    )

    return tbl, {}


@callback(
    Output("summary-metrics-df", "data"),
    Input("log-name", "data"),
)
def get_records(log_name: str, **kwargs):
    df = get_parsed_metrics(log_file=log_name, out_dir=None, out_format=None).filter(
        pl.col("node_type").is_not_null()
    )
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
                html.Div(
                    id="metrics-table",
                    style={"visibility": "hidden"},
                ),
            ],
            style={"transition": "opacity 200ms ease-in", "minHeight": "100vh"},
            is_in=False,
        ),
    ]
