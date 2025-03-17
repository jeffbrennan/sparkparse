from plotly.graph_objs import Figure

from sparkparse.styling import get_site_colors


def style_fig(fig: Figure, dark_mode: bool, x_min: int, x_max: int) -> Figure:
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
    font_color_transparent = f"rgba{font_color[3:-1]},0.1)"
    fig.update_traces(
        marker=dict(size=8, line=dict(width=1, color=font_color_transparent))
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
            tickfont_size=tick_font_size,
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
    fig.update_yaxes(matches=None, showticklabels=True, showgrid=False, fixedrange=True)

    x_padding = (x_max - x_min) * 0.01
    fig.update_xaxes(
        showgrid=False,
        fixedrange=True,
        range=[
            x_min - x_padding,
            x_max + x_padding,
        ],
    )

    fig.update_layout(
        title={
            "x": 0.04,
            "xanchor": "left",
            "font": {"size": 24, "color": font_color},
        },
    )

    return fig
