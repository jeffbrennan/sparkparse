import dash
import dash_bootstrap_components as dbc
import polars as pl
from dash import Input, Output, State, callback, dcc, html

from sparkparse.pages import dag, home
from sparkparse.styling import SitePalette, get_site_colors


@callback(
    [
        Output("navbar-brand", "style"),
        Output("dag-link", "style"),
    ],
    [
        Input("current-url", "pathname"),
        Input("color-mode-switch", "value"),
    ],
)
def update_link_color(pathname: str, dark_mode: bool):
    _, color = get_site_colors(dark_mode, contrast=False)
    highlighted_background_color, highlighted_text_color = get_site_colors(
        dark_mode, contrast=True
    )

    pages = ["", "dag"]
    output_styles = [{"color": color} for _ in range(len(pages))]

    current_page = pathname.removeprefix("/").split("-")[0]

    output_styles[pages.index(current_page)] = {
        "color": highlighted_text_color,
        "backgroundColor": highlighted_background_color,
        "borderRadius": "20px",
    }

    return output_styles


@callback(
    [
        Output("color-mode-switch", "children"),
        Output("color-mode-switch", "value"),
    ],
    Input("color-mode-switch", "n_clicks"),
    State("color-mode-switch", "children"),
)
def toggle_color_mode(n_clicks, _):
    is_dark = n_clicks % 2 == 1
    if is_dark:
        return html.I(
            className="fas fa-sun",
            style={"color": SitePalette.PAGE_BACKGROUND_COLOR_LIGHT},
        ), True

    return html.I(
        className="fas fa-moon",
        style={"color": SitePalette.PAGE_BACKGROUND_COLOR_DARK},
    ), False


@callback(
    [
        Output("sparkparse-page", "className"),
        Output("navbar", "className"),
    ],
    Input("color-mode-switch", "value"),
)
def toggle_page_color(dark_mode: bool):
    class_name = "dark-mode" if dark_mode else "light-mode"
    return class_name, class_name


def layout():
    navbar = dbc.Navbar(
        dbc.Container(
            [
                dbc.NavbarBrand(
                    "sparkparse", href="/", class_name="navbar-brand", id="navbar-brand"
                ),
                dbc.Collapse(
                    dbc.Nav(
                        [
                            dcc.Location("current-url", refresh=False),
                            dbc.NavItem(
                                dbc.NavLink(
                                    id="dag-link",
                                    children="dag",
                                    href="dag",
                                    # class_name="downloads-link",
                                )
                            ),
                        ],
                        className="ml-auto",
                        navbar=True,
                    ),
                    id="navbar-collapse",
                    navbar=True,
                ),
                dbc.NavItem(
                    dbc.Button(
                        id="color-mode-switch",
                        n_clicks=0,
                        children=html.I(
                            className="fas fa-moon",
                            style={
                                "color": SitePalette.PAGE_BACKGROUND_COLOR_DARK,
                            },
                        ),
                        color="link",
                    )
                ),
            ],
            fluid=True,
            id="navbar-container",
        ),
        id="navbar",
    )

    return dbc.Container(
        id="sparkparse-page",
        children=[
            navbar,
            html.Br(),
            dash.page_container,
        ],
        fluid=True,
    )


def init_dashboard(df: pl.DataFrame) -> dash.Dash:
    app = dash.Dash(
        __name__,
        use_pages=True,
        external_stylesheets=[dbc.themes.BOOTSTRAP, dbc.icons.FONT_AWESOME],
        suppress_callback_exceptions=True,
    )

    if df is not None:
        app.df = df  # type: ignore

    app.index_string = f"""
    <!DOCTYPE html>
    <html>
        <head>
            {{%metas%}}
            <title>{{%title%}}</title>
            {{%favicon%}}
            {{%css%}}
            <style>
                body, #navbar {{
                    background-color: {SitePalette.PAGE_BACKGROUND_COLOR_LIGHT} !important;
                    margin: 0;
                }}
                
            </style>
        </head>
        <body>
            {{%app_entry%}}
            <footer>
                {{%config%}}
                {{%scripts%}}
                {{%renderer%}}
            </footer>
        </body>
    </html>
    """

    app.layout = layout()
    dash.register_page(home.__name__, name="summary", path="/", layout=home.layout)
    dash.register_page(dag.__name__, name="dag", layout=dag.layout)
    return app


def run_app(app):
    app.run(host="127.0.0.1", debug=True)
