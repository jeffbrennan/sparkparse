import datetime
import json
from concurrent.futures import ThreadPoolExecutor
from functools import lru_cache
from pathlib import Path

import dash_ag_grid as dag
import pandas as pd
from dash import Input, Output, callback, dcc, get_app, html
from pydantic import BaseModel

from sparkparse.common import timeit
from sparkparse.eventlog import iter_lines
from sparkparse.models import CaptureMetadata, EventLogSource


@callback(
    Output("available-logs", "data"),
    Input("available-logs", "id"),
)
@timeit
@lru_cache
def get_available_logs(_) -> list[str]:
    # The dataset discovers sources once and caches parsed frames server-side.
    return get_app().server.config["DATASET"].list_logs()


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


def format_duration(duration_seconds: float) -> str:
    if duration_seconds < 60:
        return f"{duration_seconds:.0f} sec"
    if duration_seconds < 3600:
        return f"{duration_seconds / 60:.2f} min"
    if duration_seconds < 86400:
        return f"{duration_seconds / 3600:.2f} hr"
    return f"{duration_seconds / 86400:.2f} day"


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
    return LogDuration(
        start_time=start_timestamp,
        end_time=end_timestamp,
        duration_seconds=duration_seconds,
        duration_formatted=format_duration(duration_seconds),
    )


def _artifact_details(metadata: CaptureMetadata) -> RawLogDetails:
    start = metadata.capture_start
    end = metadata.capture_end or metadata.capture_start
    duration_seconds = (end - start).total_seconds()
    return RawLogDetails(
        name=metadata.workload_label or "capture",
        modified=start.strftime("%Y-%m-%d %H:%M:%S"),
        size_mb=0.0,
        start_time=start,
        end_time=end,
        duration_seconds=duration_seconds,
        duration_formatted=format_duration(duration_seconds),
    )


def collect_log_details(log_names: list[str]) -> list[RawLogDetails]:
    dataset = get_app().server.config["DATASET"]
    if dataset.kind != "event_log":
        metadata = dataset.metadata(dataset.label)
        return [_artifact_details(metadata)] if metadata is not None else []

    sources = {
        source.name: source
        for source in (dataset.event_log_source(name) for name in log_names)
        if source is not None
    }

    def build(source: EventLogSource) -> RawLogDetails:
        duration = get_log_duration(source)
        size_bytes = sum(
            Path(uri).stat().st_size for uri in source.uris if Path(uri).exists()
        )
        modified = source.modified or 0
        return RawLogDetails(
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

    with ThreadPoolExecutor(max_workers=8) as executor:
        return list(executor.map(build, sources.values()))


def _log_records(log_items: list[RawLogDetails]) -> list[dict]:
    if not log_items:
        return []
    log_df = pd.DataFrame([log.model_dump() for log in log_items])
    return (
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
def get_log_table(available_logs: list[str], dark_mode: bool):
    theme = "ag-theme-alpine-dark" if dark_mode else "ag-theme-alpine"
    log_records = _log_records(collect_log_details(available_logs or []))

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
        # Bounded pagination keeps the browser payload small for large histories.
        dashGridOptions={"pagination": True, "paginationPageSize": 25},
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
