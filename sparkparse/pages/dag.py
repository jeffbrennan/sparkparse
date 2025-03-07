from typing import Any, Dict, List

import dash
import dash_cytoscape as cyto
import polars as pl
from dash import Input, Output, callback, dcc, html

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
    Input("metrics-df", "data"),
    Input("color-mode-switch", "value"),
)
def create_elements(df_data: List[Dict[str, Any]], dark_mode: bool) -> List[Dict]:
    elements = []
    codegen_groups = {
        row["whole_stage_codegen_id"]
        for row in df_data
        if row["whole_stage_codegen_id"] is not None
    }
    for group_id in codegen_groups:
        codegen_label = f"WholeStageCodegen ({group_id})"
        tooltip = [
            i["node_name"] for i in df_data if i["whole_stage_codegen_id"] == group_id
        ]
        tooltip_str = codegen_label + "\n\n" + "\n".join(tooltip)
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
        n_value = (
            row["records_read"] if row["records_read"] > 0 else row["records_written"]
        )

        node_data = {
            "id": str(row["task_id"]),
            "label": f"{row['node_name']}\nn={n_value:,}",
            "tooltip": hover_info,
            "color": node_color,
        }

        if row["whole_stage_codegen_id"] is not None:
            node_data["parent"] = f"codegen_{row['whole_stage_codegen_id']}"

        nodes_to_exclude = ["BroadcastQueryStage"]
        if row["node_type"] in nodes_to_exclude:
            elements[-1]["data"]["target"] = row["child_nodes"]
            continue

        elements.append({"data": node_data})
        if row["child_nodes"] and row["child_nodes"] != "None":
            children = row["child_nodes"].strip("[]").split(",")
            for child in children:
                child = child.strip()
                if child:
                    elements.append(
                        {"data": {"target": child, "source": str(row["task_id"])}}
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
                "padding": "50px",
                "text-valign": "top",
                "text-halign": "left",
                "text-margin-x": "160px",
                "text-margin-y": "20px",
                "font-size": "14px",
                "text-background-color": text_color,
                "text-background-opacity": 1,
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


def layout() -> html.Div:
    if not hasattr(dash.get_app(), "df"):
        return html.Div("No data loaded")

    df: pl.DataFrame = dash.get_app().df
    records = df.to_pandas().to_dict("records")

    return html.Div(
        [
            dcc.Store("metrics-df", data=records),
            cyto.Cytoscape(
                id="dag-graph",
                layout={
                    "name": "dagre",
                    "rankDir": "TB",
                    "ranker": "longest-path",
                    "spacingFactor": 1.2,
                },
                style={"width": "100%", "height": "100vh"},
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
            "zIndex": 9999,
            "left": "7%",
            "top": "10%",
            "fontSize": "16px",
        },
    )
