from dash import html
import dash_cytoscape as cyto


def layout() -> html.Div:
    return html.Div(
        [
            cyto.Cytoscape(
                id="cytoscape-two-nodes",
                layout={"name": "preset"},
                style={"width": "100vw", "height": "100vh"},
                elements=[
                    {
                        "data": {"id": "one", "label": "Node 1"},
                        "position": {"x": 75, "y": 75},
                    },
                    {
                        "data": {"id": "two", "label": "Node 2"},
                        "position": {"x": 200, "y": 200},
                    },
                    {"data": {"source": "one", "target": "two"}},
                ],
            )
        ]
    )
