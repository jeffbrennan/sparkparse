from typing import Any, Dict, List

import dash
import dash_cytoscape as cyto
import polars as pl
from dash import Input, Output, callback, dcc, html


@callback(
    Output("dag-graph", "elements"),
    Input("metrics-df", "data"),
)
def create_elements(df_data: List[Dict[str, Any]]) -> List[Dict]:
    elements = []

    # Create nodes
    for row in df_data:
        elements.append(
            {
                "data": {
                    "id": str(row["task_id"]),
                    "label": f"{row['node_type']} ({row['task_id']})\nn={row['records_read']:,}",
                }
            }
        )

        # Create edges from child nodes
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
                            "content": "data(label)",
                            "text-wrap": "wrap",
                            "text-valign": "center",
                            "text-halign": "center",
                            "background-color": "#6FB1FC",
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
        ]
    )
