import datetime
import json
from concurrent.futures import ThreadPoolExecutor
from functools import lru_cache
from pathlib import Path

import dash_ag_grid as dag
import pandas as pd
from dash import Input, Output, callback, dcc, get_app, html
from pydantic import BaseModel

from sparkparse.common import resolve_dir, timeit
from sparkparse.eventlog import discover_sources, iter_lines
from sparkparse.models import EventLogSource
from sparkparse.parse import source_has_queries


@callback(
    Output("available-logs", "data"),
    Input("available-logs", "id"),
)
@timeit
@lru_cache
def get_available_logs(_) -> list[str]:
    log_path = resolve_dir(get_app().server.config["LOG_DIR"])
    # discover_sources collapses rolled segments into one logical log and skips
    # marker files, so the table lists applications rather than files.
    sources = discover_sources(log_path)

    with ThreadPoolExecutor(max_workers=8) as executor:
        results = list(executor.map(source_has_queries, sources))

    return sorted(source.name for source, has in zip(sources, results) if has)


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


def get_log_duration(source: EventLogSource) -> LogDuration:
    start_timestamp: datetime.datetime | None = None
    end_timestamp: datetime.datetime | None = None

    # One streaming pass: keep the first timestamp seen and overwrite the last.
    # Reading the whole log into a list costs memory proportional to log size.
    for _, _, line in iter_lines(source):
        if "Timestamp" not in line:
            continue
        try:
            entry = json.loads(line)
        except json.JSONDecodeError:
            continue
        timestamp = entry.get("Timestamp")
        if timestamp is None:
            continue
        end_timestamp = datetime.datetime.fromtimestamp(timestamp / 1000)
        if start_timestamp is None:
            start_timestamp = end_timestamp

    if start_timestamp is None or end_timestamp is None:
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

    log_dir = resolve_dir(get_app().server.config["LOG_DIR"])
    sources = {source.name: source for source in discover_sources(log_dir)}

    for log_str in available_logs:
        source = sources.get(str(log_str))
        if source is None:
            continue
        duration = get_log_duration(source)
        size_bytes = sum(
            Path(uri).stat().st_size for uri in source.uris if Path(uri).exists()
        )
        modified = source.modified or 0
        log_items.append(
            RawLogDetails(
                name=source.name,
                modified=datetime.datetime.fromtimestamp(modified).strftime(
                    "%Y-%m-%d %H:%M:%S"
                ),
                size_mb=round(size_bytes / 1024 / 1024, 2),
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
