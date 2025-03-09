import datetime
from pathlib import Path

import pandas as pd
from dash import Input, Output, callback, dcc, html
from pydantic import BaseModel

import dash_ag_grid as dag


@callback(
    Output("available-logs", "data"),
    Input("available-logs", "id"),
)
def get_available_logs(_, base_dir="data", log_dir="logs/raw"):
    base_path = Path(__file__).parents[2] / base_dir / log_dir
    log_files = list(base_path.glob("*"))
    log_files = [log.as_posix() for log in log_files if log.name != ".DS_Store"]
    return sorted(log_files)


class RawLogDetails(BaseModel):
    name: str
    modified: str
    size_mb: float


@callback(
    [
        Output("log-table-container", "children"),
        Output("log-table-container", "style"),
    ],
    [
        Input("available-logs", "data"),
        Input("color-mode-switch", "value"),
    ],
)
def get_log_table(available_logs: list[Path], dark_mode: bool):
    log_items = []
    for log_str in available_logs:
        log = Path(log_str)
        log_items.append(
            RawLogDetails(
                name=log.stem,
                modified=datetime.datetime.fromtimestamp(log.stat().st_mtime).strftime(
                    "%Y-%m-%d %H:%M:%S"
                ),
                size_mb=round(log.stat().st_size / 1024 / 1024, 2),
            )
        )
    log_df = pd.DataFrame([log.model_dump() for log in log_items])
    log_records = (
        log_df.assign(
            days_old=lambda x: (
                (pd.Timestamp.now() - pd.to_datetime(x["modified"])).dt.total_seconds()
                / 60
                / 60
                / 24
            ).round(2)
        )
        .assign(name=lambda x: x["name"].apply(lambda y: f"[{y}](/{y}/summary)"))
        .sort_values(by=["modified"], ascending=False)
        .drop_duplicates()
        .to_dict(orient="records")
    )

    theme = "ag-theme-alpine-dark" if dark_mode else "ag-theme-alpine"

    grid = dag.AgGrid(
        id="log-table",
        rowData=log_records,
        columnDefs=[
            {"field": "name", "headerName": "Log Name"},
            {"field": "modified", "headerName": "Modified Date"},
            {"field": "days_old", "headerName": "Days Old"},
            {"field": "size_mb", "headerName": "Size (MB)"},
        ],
        defaultColDef={
            "filter": True,
            "cellRenderer": "markdown",
            "sortable": True,
            "resizable": True,
        },
        columnSize="sizeToFit",
        className=theme,
        style={"height": "100vh", "width": "80%", "marginLeft": "10%"},
    )

    return grid, {}


@callback(
    Output("selected-log", "data"),
    Input("log-table", "cellClicked"),
)
def update_selected_log(cell: dict | None):
    if cell is None:
        return None
    selected_log = cell["value"].split("]")[0].removeprefix("[")
    return selected_log


def layout():
    return html.Div(
        [
            dcc.Store(id="available-logs"),
            html.Div(
                id="log-table-container",
                style={"visibility": "hidden"},
            ),
        ]
    )
