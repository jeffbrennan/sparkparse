import pandas as pd
from plotly.graph_objs import Figure

from sparkparse.styling import get_site_colors


def style_fig(
    fig: Figure, dark_mode: bool, min_x: pd.Timestamp, max_x: pd.Timestamp
) -> Figure:
    bg_color, font_color = get_site_colors(dark_mode, contrast=False)

    tick_font_size = 16
    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        showlegend=False,
        title={
            "x": 0.04,
            "xanchor": "left",
            "font": {"size": 24, "color": font_color},
        },
    )

    fig.for_each_yaxis(
        lambda y: y.update(
            title="",
            showline=True,
            showgrid=False,
            zeroline=False,
            linewidth=2,
            linecolor=font_color,
            color=font_color,
            mirror=True,
            showticklabels=False,
        )
    )
    fig.for_each_xaxis(
        lambda x: x.update(
            title="",
            showline=True,
            showgrid=False,
            zeroline=False,
            linewidth=2,
            linecolor=font_color,
            color=font_color,
            mirror=True,
            showticklabels=True,
            tickfont_size=tick_font_size,
        )
    )
    min_x_adj = min_x - (0.1 * (max_x - min_x))
    max_x_adj = max_x + (0.2 * (max_x - min_x))
    fig.update_xaxes(range=[min_x_adj, max_x_adj])

    fig.update_yaxes(
        matches=None, showticklabels=False, showgrid=False, fixedrange=True
    )

    return fig
