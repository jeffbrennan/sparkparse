from plotly.graph_objs import Figure

from sparkparse.styling import get_site_colors


def style_fig(fig: Figure, dark_mode: bool) -> Figure:
    bg_color, font_color = get_site_colors(dark_mode, contrast=False)

    legend_font_size = 16
    tick_font_size = 16
    fig.update_layout(
        legend=dict(
            title=None,
            itemsizing="constant",
            font=dict(size=legend_font_size, color=font_color),
        )
    )

    fig.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
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
    fig.update_yaxes(matches=None, showticklabels=False, showgrid=False, fixedrange=True)



    fig.update_layout(
        title={
            "x": 0.04,
            "xanchor": "left",
            "font": {"size": 24, "color": font_color},
        },
    )

    return fig
