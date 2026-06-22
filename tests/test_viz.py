from __future__ import annotations

from sparkparse.viz import get_node_color


def test_get_node_color_with_none():
    color = get_node_color(None, 0.0, 10.0, dark_mode=False)
    assert color.startswith("rgb(")


def test_get_node_color_with_nan():
    color = get_node_color(float("nan"), 0.0, 10.0, dark_mode=False)
    assert color.startswith("rgb(")
    assert color == get_node_color(None, 0.0, 10.0, dark_mode=False)


def test_get_node_color_with_value():
    min_color = get_node_color(0.0, 0.0, 10.0, dark_mode=False)
    max_color = get_node_color(10.0, 0.0, 10.0, dark_mode=False)
    assert min_color != max_color
    assert all(c.startswith("rgb(") for c in (min_color, max_color))
