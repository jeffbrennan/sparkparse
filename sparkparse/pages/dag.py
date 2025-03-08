from typing import Any, Dict, List

import dash
import dash_cytoscape as cyto
import polars as pl
from dash import Input, Output, callback, dcc, html
import dash_bootstrap_components as dbc

from sparkparse.styling import get_site_colors


def get_node_color(
    node_value: float, min_value: float, max_value: float, dark_mode: bool
) -> str:
    """Generate a color between gray and red based on duration."""
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


@callback(
    Output("dag-graph", "elements"),
    [
        Input("metrics-df", "data"),
        Input("color-mode-switch", "value"),
    ],
)
def create_elements(df_data: List[Dict[str, Any]], dark_mode: bool) -> List[Dict]:
    elements = []
    codegen_groups = {
        row["whole_stage_codegen_id"]
        for row in df_data
        if row["whole_stage_codegen_id"] is not None
    }
    for group_id in codegen_groups:
        codegen_label = f"codegen\n#{group_id}"
        tooltip = [
            i["node_name"] for i in df_data if i["whole_stage_codegen_id"] == group_id
        ]
        tooltip_str = codegen_label.replace("\n", " ") + "\n\n" + "\n".join(tooltip)
        elements.append(
            {
                "data": {
                    "id": f"codegen_{group_id}",
                    "label": codegen_label,
                    "tooltip": tooltip_str,
                },
                "classes": "codegen-container",
            }
        )

    durations = [row["task_duration_seconds"] for row in df_data]
    min_duration = min(durations)
    max_duration = max(durations)

    for row in df_data:
        hover_info = (
            f"{row['node_name']}\n"
            "\nDuration\n"
            f"Task Time: {row['task_duration_seconds']:.2f}s\n"
            f"GC Time: {row['jvm_gc_time_seconds']:.2f}s\n"
            "\nInput\n"
            f"Records Read: {row['records_read']:,}\n"
            f"Bytes Read: {row['bytes_read']:,}\n"
            "\nOutput\n"
            f"Records Written: {row['records_written']:,}\n"
            f"Bytes Written: {row['bytes_written']:,}\n"
            f"Result Size Bytes: {row['result_size_bytes']:,}\n"
            "\nSpill\n"
            f"Disk Spill Bytes: {row['disk_bytes_spilled']:,}\n"
            f"Memory Spill Bytes: {row['memory_bytes_spilled']:,}\n"
            "\nChild Nodes\n",
            row["child_nodes"],
        )

        node_color = get_node_color(
            row["task_duration_seconds"], min_duration, max_duration, dark_mode
        )

        node_id_formatted = f"query_{row['query_id']}_{row['task_id']}"
        node_data = {
            "id": node_id_formatted,
            "label": f"{row['node_name']}",
            "tooltip": hover_info,
            "color": node_color,
        }

        if row["whole_stage_codegen_id"] is not None:
            node_data["parent"] = f"codegen_{row['whole_stage_codegen_id']}"

        # skip nodes not displayed in spark ui and connect the previous node to the next node
        nodes_to_exclude = ["BroadcastQueryStage", "ShuffleQueryStage"]
        if row["node_type"] in nodes_to_exclude:
            if not row["child_nodes"]:
                continue
            next_child = row["child_nodes"].split(",")[0]
            elements[-1]["data"]["target"] = f"query_{row['query_id']}_{next_child}"
            continue

        elements.append({"data": node_data})
        if not row["child_nodes"]:
            continue

        children = row["child_nodes"].split(",")
        for child in children:
            target_formatted = f"query_{row['query_id']}_{child}"
            source_formatted = f"query_{row['query_id']}_{row['task_id']}"

            elements.append(
                {"data": {"target": target_formatted, "source": source_formatted}}
            )

    return elements


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
                "background-color": text_color,
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
    Input("metrics-df", "id"),  # This triggers when the component is mounted
)
def initialize_dropdown(_):
    df: pl.DataFrame = dash.get_app().df
    query_ids = sorted(df.select("query_id").unique().get_column("query_id").to_list())
    options = [{"label": f"Query {i}", "value": i} for i in query_ids]
    return query_ids[0], options


@callback(
    [
        Output("metrics-df", "data"),
        Output("dag-title", "children"),
    ],
    Input("query-id-dropdown", "value"),
)
def get_records(query_id: int) -> tuple[list[dict[str, Any]], list[str]]:
    df: pl.DataFrame = dash.get_app().df
    filtered_df = df.filter(pl.col("query_id") == query_id)
    records = filtered_df.to_pandas().to_dict("records")

    title = filtered_df.select(
        "parsed_log_name",
        "query_start_timestamp",
        pl.col("query_duration_seconds").mul(1 / 60).alias("query_duration_minutes"),
    ).to_dicts()[0]
    dag_title = [
        html.Div(
            f"{title['parsed_log_name']}",
            style={"fontWeight": "bold", "fontSize": "24px"},
        ),
        f"{title['query_start_timestamp']} - {title['query_duration_minutes']:.2f} minutes",
    ]

    return records, dag_title


def layout() -> html.Div:
    if not hasattr(dash.get_app(), "df"):
        return html.Div("No data loaded")

    # add a dropdown to select the query_id
    return html.Div(
        [
            dcc.Store("metrics-df"),
            dbc.Row(
                [
                    dbc.Col(
                        width=1,
                        children=[
                            dcc.Dropdown(
                                id="query-id-dropdown",
                                clearable=False,
                                style={"marginTop": "15px"},
                            )
                        ],
                    ),
                    dbc.Col(
                        width=11,
                        children=[html.Div(id="dag-title")],
                    ),
                ]
            ),
            html.Br(),
            cyto.Cytoscape(
                id="dag-graph",
                layout={
                    "name": "dagre",
                    "rankDir": "TB",
                    "ranker": "longest-path",
                    "spacingFactor": 1,
                },
                style={"width": "100%", "height": "100vh", "zIndex": 999},
            ),
            html.Div(id="tooltip"),
        ]
    )


@callback(
    [
        Output("tooltip", "children"),
        Output("tooltip", "style"),
    ],
    [
        Input("dag-graph", "mouseoverNodeData"),
        Input("color-mode-switch", "value"),
    ],
)
def update_tooltip(mouseover_data, dark_mode: bool):
    bg_color, text_color = get_site_colors(dark_mode, contrast=True)
    if not mouseover_data:
        return "", {"display": "none"}
    return (
        html.Pre(mouseover_data["tooltip"]),
        {
            "display": "block",
            "position": "fixed",
            "backgroundColor": bg_color,
            "color": text_color,
            "padding": "10px",
            "border": f"1px solid {text_color}",
            "borderRadius": "5px",
            "zIndex": 100,
            "left": "7%",
            "top": "14%",
            "fontSize": "16px",
        },
    )
