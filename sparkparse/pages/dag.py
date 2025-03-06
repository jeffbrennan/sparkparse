from typing import Any, Dict, List

import dash
import dash_cytoscape as cyto
import polars as pl
from dash import Input, Output, callback, dcc, html

from sparkparse.styling import get_site_colors


@callback(
    Output("dag-graph", "elements"),
    Input("metrics-df", "data"),
)
def create_elements(df_data: List[Dict[str, Any]]) -> List[Dict]:
    elements = []

    for row in df_data:
        hover_info = (
            f"{row['node_type']} (#{row['task_id']})\n"
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
            f"Memory Spill Bytes: {row['memory_bytes_spilled']:,}"
        )

        elements.append(
            {
                "data": {
                    "id": str(row["task_id"]),
                    "label": f"{row['node_type']} ({row['task_id']})\nn={row['records_read']:,}",
                    "tooltip": hover_info,
                }
            }
        )

        if row["child_nodes"] and row["child_nodes"] != "None":
            children = row["child_nodes"].strip("[]").split(",")
            for child in children:
                child = child.strip()
                if child:
                    elements.append(
                        {"data": {"source": child, "target": str(row["task_id"])}}
                    )

    return elements


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
                },
                style={"width": "100%", "height": "100vh"},
                stylesheet=[
                    {
                        "selector": "node",
                        "style": {
                            "label": "data(label)",
                            "text-wrap": "wrap",
                            "text-valign": "center",
                            "text-halign": "center",
                            "background-color": "#6FB1FC",
                            "font-size": "12px",
                        },
                    },
                    {
                        "selector": "edge",
                        "style": {
                            "curve-style": "bezier",
                            "target-arrow-shape": "triangle",
                            "line-color": "#ccc",
                            "target-arrow-color": "#ccc",
                        },
                    },
                ],
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
            "fontSize": "12px",
        },
    )
