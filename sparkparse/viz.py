from __future__ import annotations

import json
import uuid
from typing import TYPE_CHECKING, Any

import pandas as pd
from plotly.graph_objs import Figure

from sparkparse.styling import get_site_colors

if TYPE_CHECKING:
    from sparkparse.models import ParsedLogDataFrames


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

    fig.update_traces(
        {
            "marker": {"color": font_color, "line": {"color": bg_color, "width": 1}},
            "textfont": {"color": font_color, "size": 14},
        },
    )
    fig.update_traces(textposition="outside", selector=dict(type="bar"))

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
    max_x_adj = max_x + (0.25 * (max_x - min_x))
    fig.update_xaxes(range=[min_x_adj, max_x_adj])

    fig.update_yaxes(
        matches=None, showticklabels=False, showgrid=False, fixedrange=True
    )

    return fig


def get_node_color(
    node_value: float | None, min_value: float, max_value: float, dark_mode: bool
) -> str:
    """Return an rgb color interpolated between background and accent red by metric value."""
    if node_value is None:
        normalized = 0
    else:
        if min_value == max_value:
            normalized = 0.5
        else:
            normalized = (node_value - min_value) / (max_value - min_value)

    bg_color, _ = get_site_colors(dark_mode, contrast=False)
    base_rgb = bg_color.removeprefix("rgb(").removesuffix(")")
    base_r, base_g, base_b = [int(x) for x in base_rgb.split(",")]

    accent_r, accent_g, accent_b = 247, 111, 83
    r_adj = normalized * abs(base_r - accent_r)
    g_adj = normalized * abs(base_g - accent_g)
    b_adj = normalized * abs(base_b - accent_b)

    if dark_mode:
        r = int(base_r + r_adj)
        g = int(base_g + g_adj)
        b = int(base_b + b_adj)
    else:
        r = int(base_r - r_adj)
        g = int(base_g - g_adj)
        b = int(base_b - b_adj)

    return f"rgb({r},{g},{b})"


def plot_dag(
    dfs: ParsedLogDataFrames,
    query_id: int | None = None,
    metric: str = "node_duration_minutes",
    dark_mode: bool = False,
) -> str:
    """Return an HTML string rendering the physical plan DAG with hotspot coloring.

    In Databricks or Jupyter: ``displayHTML(plot_dag(dfs))``
    Save to file: ``open("dag.html", "w").write(plot_dag(dfs))``

    Args:
        dfs: ParsedLogDataFrames from get_parsed_metrics().
        query_id: Query to visualize; defaults to the first query in dfs.dag.
        metric: Column or accumulator name used for node heat coloring.
                ``"node_duration_minutes"`` (default) colors by execution time.
                Any ``metric_name`` value from ``accumulator_totals`` is accepted.
        dark_mode: Use the dark color scheme.
    """
    import polars as pl

    from sparkparse.models import NodeType

    dag = dfs.dag.filter(pl.col("node_type").is_not_null())

    if query_id is None:
        query_id = dag.select("query_id").unique().sort("query_id").item(0, 0)

    df_data: list[dict[str, Any]] = (
        dag.filter(pl.col("query_id") == query_id).to_pandas().to_dict("records")
    )
    node_map = {row["node_id"]: row for row in df_data}

    # Build metric value map for heat coloring
    hotspot_map: dict[int, float | None] = {}
    if metric == "node_duration_minutes":
        hotspot_map = {row["node_id"]: row["node_duration_minutes"] for row in df_data}
    else:
        for row in df_data:
            if row["accumulator_totals"] is None:
                continue
            matches = [i for i in row["accumulator_totals"] if i["metric_name"] == metric]
            if matches:
                hotspot_map[row["node_id"]] = matches[0]["value"]

    valid_values = [v for v in hotspot_map.values() if v is not None]
    min_val = float(min(valid_values)) if valid_values else 0.0
    max_val = float(max(valid_values)) if valid_values else 1.0

    nodes_to_exclude = {
        NodeType.BroadcastQueryStage,
        NodeType.ShuffleQueryStage,
        NodeType.ReusedExchange,
        NodeType.TableCacheQueryStage,
        NodeType.InMemoryRelation,
    }
    codegen_threshold = 100_000

    elements: list[dict[str, Any]] = []

    # Codegen container elements
    codegen_rows = [row for row in df_data if row["node_id"] >= codegen_threshold]
    if codegen_rows:
        codegen_durations = [
            row["node_duration_minutes"]
            for row in codegen_rows
            if row["node_duration_minutes"] is not None
        ]
        min_c = float(min(codegen_durations)) if codegen_durations else 0.0
        max_c = float(max(codegen_durations)) if codegen_durations else 1.0
        for row in codegen_rows:
            cg_id = row["node_id_adj"]
            container_value = (
                row["node_duration_minutes"] if metric == "node_duration_minutes" else None
            )
            color = get_node_color(container_value, min_c, max_c, dark_mode)
            elements.append(
                {
                    "data": {
                        "id": f"codegen_{cg_id}",
                        "label": f"cgen\n#{cg_id}\n",
                        "color": color,
                    },
                    "classes": "codegen-container",
                }
            )

    # Node elements
    for row in df_data:
        if row["node_id"] >= codegen_threshold:
            continue
        if row["node_type"] in nodes_to_exclude:
            continue

        node_color = get_node_color(
            hotspot_map.get(row["node_id"]),
            min_val,
            max_val,
            dark_mode,
        )
        node_id_str = f"query_{query_id}_{row['node_id']}"
        node_data: dict[str, Any] = {
            "id": node_id_str,
            "label": row["node_name"],
            "color": node_color,
        }
        wsc_id = row.get("whole_stage_codegen_id")
        if wsc_id is not None and not (isinstance(wsc_id, float) and pd.isna(wsc_id)):
            node_data["parent"] = f"codegen_{wsc_id}"
        elements.append({"data": node_data})

    # Edge elements
    for row in df_data:
        if not row["child_nodes"]:
            continue
        if row["node_type"] in nodes_to_exclude:
            continue
        if row["node_id"] >= codegen_threshold:
            continue

        source = f"query_{query_id}_{row['node_id']}"
        children = [int(i) for i in str(row["child_nodes"]).split(", ") if i]
        for child_id in children:
            target = _resolve_edge_target(row, node_map, child_id, nodes_to_exclude)
            if target:
                elements.append({"data": {"source": source, "target": target}})

    bg_color, text_color = get_site_colors(dark_mode, contrast=False)

    stylesheet = [
        {
            "selector": "node",
            "style": {
                "label": "data(label)",
                "text-wrap": "wrap",
                "text-valign": "top",
                "text-halign": "center",
                "background-color": "data(color)",
                "border-color": bg_color,
                "border-width": "1px",
                "font-size": "10px",
                "color": text_color,
                "text-background-color": bg_color,
                "text-background-opacity": 1,
                "text-background-shape": "round-rectangle",
                "text-background-padding": "4px",
            },
        },
        {
            "selector": ".codegen-container",
            "style": {
                "background-color": "data(color)",
                "border-width": "2px",
                "border-color": bg_color,
                "shape": "round-rectangle",
                "padding": "5px",
                "text-valign": "top",
                "text-halign": "right",
                "font-size": "12px",
                "text-background-color": "rgb(255,255,255)",
                "text-background-opacity": 0,
                "color": bg_color,
            },
        },
        {
            "selector": "edge",
            "style": {
                "curve-style": "bezier",
                "target-arrow-shape": "triangle",
                "target-arrow-color": bg_color,
                "line-color": bg_color,
            },
        },
    ]

    container_id = f"cy-{uuid.uuid4().hex[:8]}"
    elements_json = json.dumps(elements)
    stylesheet_json = json.dumps(stylesheet)

    return f"""<div id="{container_id}" style="width:100%;height:650px;background:{bg_color};border:2px solid {text_color};border-radius:10px;overflow:hidden"></div>
<script src="https://unpkg.com/cytoscape@3.30.2/dist/cytoscape.min.js"></script>
<script src="https://unpkg.com/dagre@0.8.5/dist/dagre.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/cytoscape-dagre@2.5.0/cytoscape-dagre.js"></script>
<script>
(function() {{
  var cy = cytoscape({{
    container: document.getElementById('{container_id}'),
    elements: {elements_json},
    layout: {{name:'dagre',rankDir:'TD',ranker:'tight-tree',spacingFactor:0.95,animate:true,animationDuration:200}},
    style: {stylesheet_json},
    zoom: 1.2,
    pan: {{x: 400, y: 50}},
    userZoomingEnabled: true,
    userPanningEnabled: true,
  }});
}})();
</script>"""


def _resolve_edge_target(
    row: dict[str, Any],
    node_map: dict[int, dict[str, Any]],
    child: int,
    nodes_to_exclude: set[Any],
    depth: int = 0,
) -> str | None:
    if depth > 100:
        return None
    child_node = node_map.get(child)
    if child_node is None:
        return None
    if child_node["node_type"] not in nodes_to_exclude:
        return f"query_{row['query_id']}_{child}"
    if not child_node["child_nodes"]:
        return None
    grandchildren = [int(i) for i in str(child_node["child_nodes"]).split(", ") if i]
    for gc in grandchildren:
        result = _resolve_edge_target(row, node_map, gc, nodes_to_exclude, depth + 1)
        if result:
            return result
    return None
