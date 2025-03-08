import datetime
from pathlib import Path

import pandas as pd
from dash import Input, Output, callback, dash_table, dcc, html
from pydantic import BaseModel

from sparkparse.styling import get_dt_style


@callback(
    Output("available-logs", "data"),
    Input("available-logs", "id"),
)
def get_available_logs(_, base_dir="data", log_dir="logs/raw"):
    base_path = Path(__file__).parents[2] / base_dir / log_dir
    log_files = list(base_path.glob("*"))
    log_files = [log.as_posix() for log in log_files if log.name != ".DS_Store"]
    print("obtained", len(log_files), "log files")
    return sorted(log_files)


class RawLogDetails(BaseModel):
    name: str
    modified: str
    size_mb: float


@callback(
    [
        Output("log-table", "children"),
        Output("log-table", "style"),
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

    log_records = (
        pd.DataFrame([log.model_dump() for log in log_items])
        .assign(
            days_old=lambda x: (
                (pd.Timestamp.now() - pd.to_datetime(x["modified"])).dt.total_seconds()
                / 60
                / 60
                / 24
            ).round(2)
        )
        .assign(
            name=lambda x: x["name"].apply(
                lambda y: f"[{y}](http://127.0.0.1:8050/{y}/summary)"
            )
        )
        .sort_values(by=["modified"], ascending=False)
        .to_dict(orient="records")
    )

    log_table_style = get_dt_style(dark_mode)
    log_table_style["style_cell"].update({"padding": "20px"})
    log_table_style["style_table"].update({"width": "50%", "margin": "auto"})
    log_table_style["css"] = [
        dict(
            selector=".dash-cell-value a",
            rule="display: block; text-align: center !important;",
        ),
    ]

    tbl = dash_table.DataTable(
        log_records,
        columns=[
            {"name": "Name", "id": "name", "presentation": "markdown"},
            {"name": "Modified", "id": "modified"},
            {"name": "Days Old", "id": "days_old"},
            {"name": "Size (MB)", "id": "size_mb"},
        ],
        **log_table_style,
    )
    return tbl, {}


def layout():
    return html.Div(
        [
            dcc.Store(id="available-logs"),
            html.Div(
                id="log-table",
                style={"visibility": "hidden"},
            ),
        ]
    )
