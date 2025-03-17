import dash_bootstrap_components as dbc
import pandas as pd
import plotly.express as px
import polars as pl
from dash import Input, Output, callback, dash_table, dcc, html
from plotly.graph_objs import Figure

from sparkparse.clean import get_readable_size, get_readable_timing
from sparkparse.common import timeit
from sparkparse.parse import get_parsed_metrics
from sparkparse.styling import get_dt_style, get_site_colors
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
        # hoveron="points",
        hovertemplate=(
            "Job #%{customdata[0]}<br>Stage #%{customdata[1]}<br>Task #%{x}<br><br>Task runtime: %{customdata[2]} min<br>Cumulative runtime: %{y} min<extra></extra>"
        ),
    )

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
@timeit
def get_styled_metrics_table(df_data: list[dict], dark_mode: bool):
    metrics_style = get_dt_style(dark_mode)
    metrics_style["style_table"]["height"] = "60vh"
    tbl_cols = []
    int_style = {"type": "numeric", "format": {"specifier": ",d"}}

    df = (
        pl.DataFrame(df_data)
        .select(
            "query_id",
            "query_function",
            "job_id",
            "stage_id",
            "stage_start_timestamp",
            "stage_end_timestamp",
            "stage_duration_seconds",
            "task_id",
            "task_duration_seconds",
            "bytes_read",
            "bytes_written",
            "shuffle_bytes_read",
            "shuffle_bytes_written",
        )
        .group_by(
            "query_id",
            "query_function",
            "job_id",
            "stage_id",
            "stage_start_timestamp",
            "stage_end_timestamp",
            "stage_duration_seconds",
        )
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
        .with_columns(
            get_readable_timing(pl.col("stage_duration_seconds").mul(1000)).alias(
                "stage_duration_struct"
            )
        )
        .with_columns(
            get_readable_timing(pl.col("task_duration_seconds").mul(1000)).alias(
                "task_duration_struct"
            )
        )
        .with_columns(
            pl.concat_str(
                [
                    pl.col("stage_duration_struct")
                    .struct.field("readable_value")
                    .round(2),
                    pl.lit(" "),
                    pl.col("stage_duration_struct").struct.field("readable_unit"),
                ]
            ).alias("stage_duration")
        )
        .with_columns(
            pl.concat_str(
                [
                    pl.col("task_duration_struct")
                    .struct.field("readable_value")
                    .round(2),
                    pl.lit(" "),
                    pl.col("task_duration_struct").struct.field("readable_unit"),
                ]
            ).alias("task_duration")
        )
        .with_columns(
            [
                get_readable_size(pl.col("bytes_read")).alias("input_struct"),
                get_readable_size(pl.col("bytes_written")).alias("output_struct"),
                get_readable_size(pl.col("shuffle_bytes_read")).alias(
                    "shuffle_read_struct"
                ),
                get_readable_size(pl.col("shuffle_bytes_written")).alias(
                    "shuffle_write_struct"
                ),
            ]
        )
        .with_columns(
            pl.concat_str(
                [
                    pl.col("input_struct").struct.field("readable_value").round(2),
                    pl.lit(" "),
                    pl.col("input_struct").struct.field("readable_unit"),
                ]
            ).alias("input")
        )
        .with_columns(
            pl.concat_str(
                [
                    pl.col("output_struct").struct.field("readable_value").round(2),
                    pl.lit(" "),
                    pl.col("output_struct").struct.field("readable_unit"),
                ]
            ).alias("output")
        )
        .with_columns(
            pl.concat_str(
                [
                    pl.col("shuffle_read_struct")
                    .struct.field("readable_value")
                    .round(2),
                    pl.lit(" "),
                    pl.col("shuffle_read_struct").struct.field("readable_unit"),
                ]
            ).alias("shuffle_read")
        )
        .with_columns(
            pl.concat_str(
                [
                    pl.col("shuffle_write_struct")
                    .struct.field("readable_value")
                    .round(2),
                    pl.lit(" "),
                    pl.col("shuffle_write_struct").struct.field("readable_unit"),
                ]
            ).alias("shuffle_write")
        )
        .filter(pl.col("query_id").is_not_null())
        .rename({"stage_start_timestamp": "submitted", "query_function": "query_func"})
        .with_columns(pl.col("submitted").str.replace("T", " ").alias("submitted"))
        .sort("query_id", "job_id", "stage_id")
        .to_pandas()
    )

    core_cols = [
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
    ]
    hidden_cols = [
        # hidden cols
        "task_duration_seconds",
        "bytes_read",
        "bytes_written",
        "shuffle_bytes_read",
        "shuffle_bytes_written",
    ]

    numeric_cols = ["query_id", "job_id", "stage_id", "tasks"]

    col_mapping = {}
    for col in core_cols:
        if col in numeric_cols:
            col_mapping[col] = {**int_style, "id": col, "name": col.replace("_", " ")}
        else:
            col_mapping[col] = {"name": col.replace("_", " "), "id": col}

    small_cols = numeric_cols + ["query_func"]
    width_mapping = {col: 150 if col not in small_cols else 80 for col in core_cols}
    width_adjustment = [
        {
            "if": {"column_id": i},
            "minWidth": width_mapping[i],
            "maxWidth": width_mapping[i],
        }
        for i in width_mapping
    ]
    metrics_style["style_cell_conditional"].extend(width_adjustment)
    core_df = df[core_cols + hidden_cols]
    for col in core_df.columns:
        if col not in hidden_cols:
            tbl_cols.append(
                {**col_mapping[col], "id": col, "name": col.replace("_", " ")}
            )

    core_records = core_df.to_dict("records")

    tbl = dash_table.DataTable(
        data=core_records,
        id="summary-table",
        columns=tbl_cols,
        sort_by=[],
        sort_action="custom",
        **metrics_style,
    )

    return tbl, {}


@callback(
    Output("summary-table", "data"),
    [
        Input("summary-table", "data"),
        Input("summary-table", "sort_by"),
    ],
)
def update_summary_table(data: list, sort_by: list):
    if len(sort_by) == 0:
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
        print(col)
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
                html.Div(
                    id="metrics-table",
                    style={"visibility": "hidden"},
                ),
            ],
            style={"transition": "opacity 200ms ease-in", "minHeight": "100vh"},
            is_in=False,
        ),
    ]
