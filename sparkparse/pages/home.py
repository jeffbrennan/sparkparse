import datetime
import json
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


class LogDuration(BaseModel):
    start_time: datetime.datetime
    end_time: datetime.datetime
    duration_seconds: float
    duration_formatted: str


class RawLogDetails(BaseModel):
    name: str
    modified: str
    size_mb: float
    start_time: datetime.datetime
    end_time: datetime.datetime
    duration_seconds: float
    duration_formatted: str


def get_log_duration(log: Path) -> LogDuration:
    with log.open("r") as f:
        lines = f.readlines()

    # iterate over first few lines until first timestamp found
    for line in lines:
        if "Timestamp" not in line:
            continue

        entry = json.loads(line)
        start_timestamp = datetime.datetime.fromtimestamp(entry["Timestamp"] / 1000)
        break

    # iterate backwards until last timestamp found
    for line in reversed(lines):
        if "Timestamp" not in line:
            continue

        entry = json.loads(line)
        end_timestamp = datetime.datetime.fromtimestamp(entry["Timestamp"] / 1000)
        break

    if not start_timestamp or not end_timestamp:
        raise ValueError("Could not find start and end timestamps in log")

    duration_seconds = (end_timestamp - start_timestamp).total_seconds()

    if duration_seconds < 60:
        duration_formatted = f"{duration_seconds:.0f} sec"
    elif duration_seconds < 3600:
        duration_formatted = f"{duration_seconds / 60:.2f} min"
    elif duration_seconds < 86400:
        duration_formatted = f"{duration_seconds / 3600:.2f} hr"
    else:
        duration_formatted = f"{duration_seconds / 86400:.2f} day"

    return LogDuration(
        start_time=start_timestamp,
        end_time=end_timestamp,
        duration_seconds=duration_seconds,
        duration_formatted=duration_formatted,
    )


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
    theme = "ag-theme-alpine-dark" if dark_mode else "ag-theme-alpine"

    for log_str in available_logs:
        log = Path(log_str)
        duration = get_log_duration(log)
        log_items.append(
            RawLogDetails(
                name=log.stem,
                modified=datetime.datetime.fromtimestamp(log.stat().st_mtime).strftime(
                    "%Y-%m-%d %H:%M:%S"
                ),
                size_mb=round(log.stat().st_size / 1024 / 1024, 2),
                start_time=duration.start_time,
                end_time=duration.end_time,
                duration_seconds=duration.duration_seconds,
                duration_formatted=duration.duration_formatted,
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
        .assign(start_time=lambda x: x["start_time"].dt.strftime("%Y-%m-%d %H:%M:%S"))
        .assign(end_time=lambda x: x["end_time"].dt.strftime("%Y-%m-%d %H:%M:%S"))
        .sort_values(by=["modified"], ascending=False)
        .drop_duplicates()
        .drop(columns=["modified"])
        .to_dict(orient="records")
    )

    grid = dag.AgGrid(
        id="log-table",
        rowData=log_records,
        columnDefs=[
            {"field": "name", "headerName": "Log Name"},
            {"field": "start_time", "headerName": "Started"},
            {"field": "end_time", "headerName": "Completed"},
            {"field": "days_old", "headerName": "Days Old"},
            {"field": "duration_formatted", "headerName": "Duration"},
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
        style={"height": "100vh", "width": "85%", "marginLeft": "7.5%"},
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
