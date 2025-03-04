from dataclasses import asdict, dataclass
from enum import StrEnum, auto
from typing import Any


class SitePalette(StrEnum):
    PAGE_BACKGROUND_COLOR_LIGHT = "rgb(242, 240, 227)"
    BRAND_TEXT_COLOR_LIGHT = "rgb(33, 33, 33)"
    PAGE_BACKGROUND_COLOR_DARK = "rgb(33, 33, 33)"
    BRAND_TEXT_COLOR_DARK = "rgb(242, 240, 227)"


class ScreenWidth(StrEnum):
    xs = auto()
    sm = auto()
    md = auto()
    lg = auto()
    xl = auto()


def get_site_colors(dark_mode: bool, contrast: bool) -> tuple[str, str]:
    colors = {
        "dark": {
            "background": SitePalette.PAGE_BACKGROUND_COLOR_DARK,
            "text": SitePalette.BRAND_TEXT_COLOR_DARK,
        },
        "light": {
            "background": SitePalette.PAGE_BACKGROUND_COLOR_LIGHT,
            "text": SitePalette.BRAND_TEXT_COLOR_LIGHT,
        },
    }

    # invert the selector if contrast is True
    selector = not dark_mode if contrast else dark_mode
    selector_str = "dark" if selector else "light"

    return colors[selector_str]["background"], colors[selector_str]["text"]


@dataclass
class DTStyle:
    sort_action: str
    sort_mode: str
    page_action: str
    virtualization: bool
    column_selectable: str
    row_selectable: bool
    row_deletable: bool
    fixed_rows: dict[str, bool]
    filter_options: dict[str, str]
    style_header: dict[str, str]
    filter_action: str
    style_filter: dict[str, str]
    style_cell: dict[str, Any]
    style_cell_conditional: list[dict[str, Any]]
    style_data_conditional: list[dict[str, Any]]
    style_data: dict[str, str]
    style_table: dict[str, Any]
    css: list[dict[str, Any]]


def get_dt_style(dark_mode: bool = False) -> dict:
    background, color = get_site_colors(dark_mode, contrast=False)
    dt_style = asdict(
        DTStyle(
            sort_action="native",
            sort_mode="multi",
            column_selectable="single",
            row_selectable=False,
            row_deletable=False,
            page_action="none",
            virtualization=False,
            fixed_rows={"headers": True},
            filter_action="native",
            filter_options={"case": "insensitive", "placeholder_text": ""},
            style_header={},
            style_filter={},
            style_cell={
                "textAlign": "center",
                "minWidth": 75,
                "maxWidth": 150,
                "whiteSpace": "normal",
                "wordBreak": "break-word",
            },
            style_cell_conditional=[],
            style_data_conditional=[],
            style_data={
                "color": color,
                "backgroundColor": background,
            },
            css=[],
            style_table={
                "height": "100vh",
                "maxHeight": "100vh",
                "overflowY": "scroll",
                "overflowX": "scroll",
            },
        )
    )
    return dt_style
