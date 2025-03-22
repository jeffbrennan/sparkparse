import json
from typing import Any, Dict, List

import dash_bootstrap_components as dbc
import dash_cytoscape as cyto
import plotly.express as px
import polars as pl
from dash import Input, Output, State, callback, callback_context, dcc, html
from plotly.graph_objs import Figure

from sparkparse.common import create_header, timeit
from sparkparse.models import NodeType
from sparkparse.parse import get_parsed_metrics
from sparkparse.styling import get_site_colors


def get_node_color(
    node_value: float | None, min_value: float, max_value: float, dark_mode: bool
) -> str:
    """Generate a color between gray and red based on duration."""
    if node_value is None:
        node_value = min_value
    if min_value == max_value:
        normalized = 0
    else:
        normalized = (node_value - min_value) / (max_value - min_value)

    bg_color, _ = get_site_colors(dark_mode, contrast=False)
    base_rgb = bg_color.removeprefix("rgb(").removesuffix(")")
    base_r, base_g, base_b = [int(x) for x in base_rgb.split(",")]

    accent_r, accent_g, accent_b = 247, 111, 83
    r_adj = normalized * abs(base_r - accent_r)
    g_adj = normalized * abs(base_g - accent_g)
    b_adj = normalized * abs(base_b - accent_b)

    if dark_mode:
        r = int(base_r + r_adj)
        g = int(base_g + g_adj)
        b = int(base_b + b_adj)
    else:
        r = int(base_r - r_adj)
        g = int(base_g - g_adj)
        b = int(base_b - b_adj)

    return f"rgb({r},{g},{b})"


def get_codegen_elements(
    df_data: list[dict[str, Any]], dark_mode: bool
) -> None | list[dict[str, Any]]:
    df = pl.DataFrame(df_data, strict=False)

    codegen_base = df.filter(pl.col("node_id").ge(100_000))
    if codegen_base.shape[0] == 0:
        return None

    codegen_details = (
        codegen_base.select(
            pl.col("node_id_adj").alias("whole_stage_codegen_id"),
            "accumulator_totals",
        )
        .explode("accumulator_totals")
        .unnest("accumulator_totals")
        .select("whole_stage_codegen_id", "value", "readable_value", "readable_unit")
        .join(
            df.filter(pl.col("node_id").lt(100_000)).select(
                "whole_stage_codegen_id",
                pl.col("node_id").cast(pl.Int64).alias("inner_node_id"),
                pl.col("node_name").alias("inner_node_name"),
            ),
            on="whole_stage_codegen_id",
            how="inner",
        )
        .sort("whole_stage_codegen_id", "inner_node_id")
        .drop("inner_node_id")
        .group_by("whole_stage_codegen_id", "value", "readable_value", "readable_unit")
        .agg(pl.col("inner_node_name").alias("inner_node_name"))
        .to_dicts()
    )

    codegen_durations = [i["value"] for i in codegen_details if i["value"] is not None]
    min_codegen_duration = min(codegen_durations)
    max_codegen_duration = max(codegen_durations)

    codegen_elements = []
    for row in codegen_details:
        tooltip = row["inner_node_name"]
        codegen_label = f"cgen\n#{row['whole_stage_codegen_id']}\n"
        if row["readable_value"] is None:
            duration_str = "duration: 0s"
        else:
            duration_str = f"duration: {row['readable_value']} {row['readable_unit']}"
        tooltip_str = (
            codegen_label.replace("\n", " ").replace("cgen", "WholeStageCodegen")
            + "\n"
            + duration_str
            + "\n\n"
            + "\n".join(tooltip)
        )

        container_color = get_node_color(
            row["value"], min_codegen_duration, max_codegen_duration, dark_mode
        )
        codegen_elements.append(
            {
                "data": {
                    "id": f"codegen_{row['whole_stage_codegen_id']}",
                    "label": codegen_label,
                    "tooltip": tooltip_str,
                    "color": container_color,
                },
                "classes": "codegen-container",
            }
        )

    return codegen_elements


@callback(
    Output("dag-graph", "elements"),
    [
        Input("metrics-df", "data"),
        Input("color-mode-switch", "value"),
    ],
)
@timeit
def create_elements(df_data: List[Dict[str, Any]], dark_mode: bool) -> List[Dict]:
    def format_accumulators(hover_info: str) -> str:
        hover_info += "\n"
        prev_metric_type = []
        for metric in row["accumulator_totals"]:
            if metric["metric_type"] not in prev_metric_type:
                hover_info += (
                    create_header(
                        header_length,
                        metric["metric_type"].title(),
                        center=True,
                        spacer="-",
                    )
                    + "\n"
                )

                prev_metric_type.append(metric["metric_type"])
            hover_info += f"{metric['metric_name']}: {metric['readable_value']:,} {metric['readable_unit']}\n"

        return hover_info

    def format_details(hover_info: str) -> str:
        dict_to_display = json.loads(row["details"])["detail"]
        if dict_to_display is not None:
            hover_info += (
                create_header(header_length, "Details", center=True, spacer="-") + "\n"
            )
            hover_info += json.dumps(dict_to_display, indent=1)
            hover_info += "\n" + create_header(
                header_length, "", center=False, spacer="-"
            )

        return hover_info

    node_map = {i["node_id"]: i for i in df_data}
    elements = []
    codegen_elements = get_codegen_elements(df_data, dark_mode)
    if codegen_elements is not None:
        elements.extend(codegen_elements)

    durations = [row["node_duration_minutes"] for row in df_data]
    min_duration = min(durations)
    max_duration = max(durations)

    # skip nodes not displayed in spark ui and connect the previous node to the next node
    nodes_to_exclude = [
        NodeType.BroadcastQueryStage,
        NodeType.ShuffleQueryStage,
        NodeType.ReusedExchange,
        NodeType.TableCacheQueryStage,
        NodeType.InMemoryRelation,
    ]

    header_length = 30
    codegen_id_adjustment = 100_000
    # add nodes
    for row in df_data:
        # skip wholestagecodegen nodes
        if row["node_id"] >= codegen_id_adjustment:
            continue

        # skip nodes not displayed in spark ui
        if row["node_type"] in nodes_to_exclude:
            continue

        hover_info = row["node_name"] + "\n"
        if row["child_nodes"] is not None:
            hover_info += f"Child Nodes: {row['child_nodes']}\n"

        if row["accumulator_totals"] is not None:
            hover_info = format_accumulators(hover_info)

        if row["details"] is not None:
            hover_info = format_details(hover_info)

        node_color = get_node_color(
            row["node_duration_minutes"], min_duration, max_duration, dark_mode
        )
        node_id_formatted = f"query_{row['query_id']}_{row['node_id']}"
        node_data = {
            "id": node_id_formatted,
            "label": f"{row['node_name']}",
            "tooltip": hover_info,
            "color": node_color,
        }

        # add nodes to parent codegen container
        if row["whole_stage_codegen_id"] is not None:
            node_data["parent"] = f"codegen_{row['whole_stage_codegen_id']}"
        elements.append({"data": node_data})

    # add edges
    for row in df_data:
        if not row["child_nodes"]:
            continue
        if row["node_type"] in nodes_to_exclude:
            continue
        if row["node_id"] >= codegen_id_adjustment:
            continue

        print(row["node_name"])
        source_formatted = f"query_{row['query_id']}_{row['node_id']}"
        children = [int(i) for i in row["child_nodes"].split(", ")]
        for child in children:
            target_formatted = get_node_target(row, node_map, child, nodes_to_exclude)
            elements.append(
                {
                    "data": {
                        "target": target_formatted,
                        "source": source_formatted,
                    }
                }
            )
    return elements


def get_node_target(
    row: dict,
    node_map: dict,
    child: int,
    nodes_to_exclude: list[NodeType],
    search_index: int = 0,
):
    if search_index > 100:
        raise ValueError("Max depth reached")
    child_node = node_map[child]
    match_found = child_node["node_type"] not in nodes_to_exclude
    if match_found:
        return f"query_{row['query_id']}_{child}"
    children = [int(i) for i in child_node["child_nodes"].split(", ") if i != ""]
    for child in children:
        return get_node_target(row, node_map, child, nodes_to_exclude, search_index + 1)


@callback(
    Output("dag-graph", "stylesheet"),
    Input("color-mode-switch", "value"),
)
def update_stylesheet(dark_mode: bool) -> List[Dict[str, Any]]:
    bg_color, text_color = get_site_colors(dark_mode, contrast=True)

    return [
        {
            "selector": "node",
            "style": {
                "label": "data(label)",
                "text-wrap": "wrap",
                "text-valign": "top",
                "text-halign": "center",
                "background-color": "data(color)",
                "border-color": bg_color,
                "border-width": "1px",
                "font-size": "10px",
                "color": text_color,
                "text-background-color": bg_color,
                "text-background-opacity": 1,
                "text-background-shape": "round-rectangle",
                "text-background-padding": "4px",
            },
        },
        {
            "selector": ".codegen-container",
            "style": {
                "background-color": "data(color)",
                "border-width": "2px",
                "border-color": bg_color,
                "shape": "round-rectangle",
                "padding": "5px",
                "text-valign": "top",
                "text-halign": "right",
                "font-size": "12px",
                "text-background-color": "rgb(255,255,255)",
                "text-background-opacity": 0,
                "color": bg_color,
            },
        },
        {
            "selector": "edge",
            "style": {
                "curve-style": "bezier",
                "target-arrow-shape": "triangle",
                "target-arrow-color": bg_color,
                "line-color": bg_color,
            },
        },
    ]


@callback(
    Output("dag-graph", "style"),
    Input("color-mode-switch", "value"),
)
def update_cyto_border_color(dark_mode: bool) -> dict:
    _, border_color = get_site_colors(dark_mode, contrast=False)
    return {
        "width": "100%",
        "height": "100vh",
        "borderRadius": "15px",
        "border": f"2px solid {border_color}",
        "overflow": "hidden",
    }


@callback(
    [
        Output("query-id-dropdown", "value"),
        Output("query-id-dropdown", "options"),
    ],
    Input("log-name", "data"),
)
def initialize_dropdown(log_name: str):
    df = get_parsed_metrics(
        log_file=log_name, out_dir=None, out_format=None
    ).dag.filter(pl.col("node_type").is_not_null())

    query_records = (
        df.select("query_id", "query_header").unique().sort("query_id").to_dicts()
    )

    options = [
        {"label": f"Query {i['query_header']}", "value": i["query_id"]}
        for i in query_records
    ]
    return query_records[0]["query_id"], options


@callback(
    [
        Output("metrics-df", "data"),
        Output("dag-title", "children"),
    ],
    [
        Input("log-name", "data"),
        Input("query-id-dropdown", "value"),
    ],
)
@timeit
def get_records(log_name: str, query_id: int):
    df = get_parsed_metrics(
        log_file=log_name, out_dir=None, out_format=None
    ).dag.filter(pl.col("node_type").is_not_null())

    filtered_df = df.filter(pl.col("query_id").eq(query_id))
    records = filtered_df.to_pandas().to_dict("records")

    title = filtered_df.select(
        "query_start_timestamp",
        pl.col("query_duration_seconds").mul(1 / 60).alias("query_duration_minutes"),
    ).to_dicts()[0]
    dag_title = [
        html.Div(
            f"{log_name}",
            style={"fontWeight": "bold", "fontSize": "24px"},
        ),
        f"{title['query_start_timestamp']} - {title['query_duration_minutes']:.2f} minutes",
    ]

    return records, dag_title


def layout(log_name: str, **kwargs) -> html.Div:
    return html.Div(
        [
            dcc.Store("metrics-df"),
            dcc.Store("log-name", data=log_name),
            dbc.Row(
                [
                    dbc.Col(
                        width=8,
                        children=[html.Div(id="dag-title")],
                    ),
                    dbc.Col(
                        width=1,
                        children=[
                            dbc.Button(
                                "clear",
                                id="clear-tooltip",
                            )
                        ],
                    ),
                    dbc.Col(
                        width=3,
                        children=[
                            dcc.Dropdown(
                                id="query-id-dropdown",
                                clearable=False,
                                style={"marginTop": "15px", "width": "100%"},
                            )
                        ],
                    ),
                ]
            ),
            html.Br(),
            cyto.Cytoscape(
                id="dag-graph",
                layout={
                    "name": "dagre",
                    "rankDir": "TD",
                    "ranker": "tight-tree",
                    "spacingFactor": 0.95,
                    "animate": True,
                    "animationDuration": 200,
                    "fit": False,
                },
                zoom=1.5,
                pan={"x": 800, "y": 50},
                style={"width": "100%", "height": "100%", "zIndex": 999},
                userZoomingEnabled=True,
                userPanningEnabled=True,
                zoomingEnabled=True,
                className="dash-cytoscape",
            ),
            html.Div(id="detail-tooltip"),
            html.Div(id="box-tooltip"),
        ],
    )


@timeit
def get_node_box(
    df_data: list[dict[str, Any]], node_id: int, dark_mode: bool
) -> Figure | None:
    df = (
        pl.DataFrame(df_data, strict=False)
        .filter(pl.col("node_id").eq(node_id))
        .filter(pl.col("n_accumulators").gt(1))
        .select("node_id", "node_name", "accumulators")
        .explode("accumulators")
        .unnest("accumulators")
        .with_columns(
            pl.when(pl.col("readable_unit").ne(""))
            .then(
                pl.concat_str("metric_name", pl.lit(" ["), "readable_unit", pl.lit("]"))
            )
            .otherwise(pl.col("metric_name"))
            .alias("metric_name_viz")
        )
        .with_columns(pl.col("readable_value").round(1).alias("readable_value"))
        .with_columns(pl.mean("readable_value").over("metric_name_viz").alias("mean"))
        .with_columns(
            pl.median("readable_value").over("metric_name_viz").alias("median")
        )
        .with_columns(pl.col("task_id").len().over("metric_name_viz").alias("n"))
        .select(
            "metric_name_viz",
            "node_name",
            "task_id",
            "readable_value",
            "readable_unit",
            "n",
            "mean",
            "median",
        )
    )
    n_metrics = df.select("metric_name_viz").unique().shape[0]
    if n_metrics == 0:
        return None
    bg_color, font_color = get_site_colors(dark_mode, False)

    if n_metrics == 1:
        subplot_height = 150
        facet_row_spacing = 0.6
    else:
        facet_row_spacing = (1 / (n_metrics - 1)) * 0.6
        if n_metrics < 3:
            subplot_height = 140
        else:
            subplot_height = 110

    df_pandas = df.to_pandas()
    node_name = df_pandas["node_name"].iloc[0]
    fig = px.box(
        df_pandas,
        x="readable_value",
        title=f"<b>{node_name} Accumulators</b>",
        facet_col="metric_name_viz",
        facet_col_wrap=1,
        orientation="h",
        facet_row_spacing=facet_row_spacing,
        height=subplot_height * n_metrics,
        width=400,
        custom_data=["task_id", "readable_unit"],
    )

    fig.update_traces(
        hoveron="points",
        hovertemplate=(
            "Task #%{customdata[0]}<br>%{x} %{customdata[1]}<br><extra></extra>"
        ),
        selector=dict(type="box"),
    )

    fig.update_layout(
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        showlegend=False,
        boxmode="overlay",
        title_font=dict(color=font_color, size=14),
        margin=dict(l=0, r=0, t=100, b=0),
    )

    fig.update_traces(marker=dict(color=font_color, line=dict(color=bg_color, width=1)))

    for annotation in fig.layout.annotations:  # type: ignore
        annotation.font.color = font_color
        metric_name = annotation.text.split("=")[-1].strip()
        metrics = (
            df.filter(pl.col("metric_name_viz") == metric_name)
            .select(["n", "mean", "median"])
            .unique()
            .to_dicts()[0]
        )
        n_str = f"{metrics['n']:,}"
        mean_str = format(metrics["mean"], ",.2f").rstrip("0").rstrip(".")
        median_str = format(metrics["median"], ",.2f").rstrip("0").rstrip(".")

        annotation.text = (
            f"<b>{metric_name}</b><br>n={n_str} | avg={mean_str} | med={median_str}"
        )

    fig.for_each_yaxis(
        lambda y: y.update(
            matches=None,
            title="",
            showline=True,
            showgrid=False,
            zeroline=False,
            linewidth=2,
            linecolor=font_color,
            color=font_color,
            mirror=True,
            tickfont_size=10,
        )
    )
    fig.for_each_xaxis(
        lambda x: x.update(
            matches=None,
            title="",
            showline=True,
            showgrid=False,
            zeroline=False,
            linewidth=2,
            linecolor=font_color,
            color=font_color,
            mirror=True,
            showticklabels=True,
            tickfont_size=10,
        )
    )

    return fig


@callback(
    [
        Output("detail-tooltip", "children"),
        Output("detail-tooltip", "style"),
        Output("box-tooltip", "children"),
        Output("box-tooltip", "style"),
    ],
    [
        Input("dag-graph", "mouseoverNodeData"),
        Input("metrics-df", "data"),
        Input("color-mode-switch", "value"),
        Input("clear-tooltip", "n_clicks"),
    ],
    [State("box-tooltip", "children")],
)
def update_tooltip(
    mouseover_data: dict,
    df_data: list,
    dark_mode: bool,
    _: int,
    current_graph: dcc.Graph,
):
    trigger = (
        callback_context.triggered[0]["prop_id"].split(".")[0]
        if callback_context.triggered
        else ""
    )
    if trigger == "clear-tooltip":
        return "", {"display": "none"}, "", {"display": "none"}

    bg_color, text_color = get_site_colors(dark_mode, contrast=True)
    if not mouseover_data:
        return "", {"display": "none"}, "", {"display": "none"}

    detail_tooltip = html.Pre(mouseover_data["tooltip"])
    detail_style = {
        "display": "block",
        "position": "fixed",
        "backgroundColor": bg_color,
        "color": text_color,
        "padding": "10px",
        "border": f"1px solid {text_color}",
        "borderRadius": "5px",
        "zIndex": 100,
        "left": "6%",
        "top": "15vh",
        "fontSize": "14px",
    }

    node_label = mouseover_data["label"]
    if "cgen" in node_label:
        return detail_tooltip, detail_style, current_graph, {}

    node_id = int(mouseover_data["label"].split("]")[0].removeprefix("["))
    fig = get_node_box(df_data, node_id, dark_mode)
    if fig is None:
        return detail_tooltip, detail_style, current_graph, {}

    graph = dcc.Graph(
        figure=fig,
        config={"displayModeBar": False},
        style={
            "position": "fixed",
            "top": "13vh",
            "right": "6%",
            "display": "block",
        },
    )
    return detail_tooltip, detail_style, graph, {}
