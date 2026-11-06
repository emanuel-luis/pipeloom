from __future__ import annotations

from pipeloom.docs.layout import NODE_H, NODE_W, LayoutNode, layout_graph

NODES = [LayoutNode(i, (i,)) for i in "abcdefg"]
EDGES = [("a", "b"), ("a", "c"), ("b", "d"), ("c", "d"), ("a", "d"), ("e", "d"), ("d", "f")]


def test_layers_are_longest_path() -> None:
    layout = layout_graph(NODES, EDGES)
    layer = {n.id: n.layer for n in layout.nodes}
    assert layer == {"a": 0, "e": 0, "g": 0, "b": 1, "c": 1, "d": 2, "f": 3}


def test_same_input_same_output_whatever_the_input_order() -> None:
    first = layout_graph(NODES, EDGES)
    assert layout_graph(NODES, EDGES) == first
    assert layout_graph(NODES[::-1], EDGES[::-1]) == first


def test_nodes_never_overlap() -> None:
    layout = layout_graph(NODES, EDGES)
    boxes = [(n.x, n.y) for n in layout.nodes]
    assert len(set(boxes)) == len(boxes)
    for i, (x1, y1) in enumerate(boxes):
        for x2, y2 in boxes[i + 1 :]:
            assert abs(x1 - x2) >= NODE_W or abs(y1 - y2) >= NODE_H
    assert all(n.x + NODE_W <= layout.width and n.y + NODE_H <= layout.height for n in layout.nodes)


def test_every_edge_is_drawn_and_unknown_ones_are_dropped() -> None:
    layout = layout_graph(NODES, [*EDGES, ("a", "zzz")])
    assert sorted((e.source, e.target) for e in layout.edges) == sorted(EDGES)
    assert all(e.path.startswith("M") and " C" in e.path for e in layout.edges)


def test_barycenter_untangles_a_crossing() -> None:
    nodes = [LayoutNode(i, (i,)) for i in ("p1", "p2", "c1", "c2")]
    layout = layout_graph(nodes, [("p1", "c2"), ("p2", "c1")])
    y = {n.id: n.y for n in layout.nodes}
    assert (y["p1"] < y["p2"]) == (y["c2"] < y["c1"])


def test_empty_graph() -> None:
    layout = layout_graph([], [])
    assert layout.nodes == () and layout.edges == ()
