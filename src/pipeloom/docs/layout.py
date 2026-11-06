"""A layered, left-to-right drawing of a DAG, computed in pure Python (no I/O, no dependencies).

Layer = longest path from the roots. Order inside a layer starts from the nodes' sort keys and is
refined by a fixed number of alternating barycenter sweeps with stable tie-breaks, so the same input
always gives the same coordinates. Long edges get no dummy nodes: they are drawn as one curve.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Sequence
from dataclasses import dataclass

NODE_W = 184
NODE_H = 34
GAP_X = 72
GAP_Y = 14
PAD = 16
SWEEPS = 4


@dataclass(frozen=True)
class LayoutNode:
    id: str
    sort_key: tuple[str, ...]


@dataclass(frozen=True)
class Placed:
    id: str
    layer: int
    x: int
    y: int


@dataclass(frozen=True)
class PlacedEdge:
    source: str
    target: str
    path: str
    """An SVG cubic Bezier from the source's right middle to the target's left middle."""


@dataclass(frozen=True)
class Layout:
    nodes: tuple[Placed, ...]
    edges: tuple[PlacedEdge, ...]
    width: int
    height: int


def _layers(ids: Sequence[str], parents: dict[str, list[str]]) -> dict[str, int]:
    """Longest path from the roots. A cycle (never in a valid plan) leaves its nodes in layer 0."""
    depth: dict[str, int] = {}
    visiting: set[str] = set()

    def visit(n: str) -> int:
        if n in depth:
            return depth[n]
        if n in visiting:
            return 0
        visiting.add(n)
        depth[n] = 1 + max((visit(p) for p in parents[n]), default=-1)
        visiting.discard(n)
        return depth[n]

    for n in ids:
        visit(n)
    return depth


def layout_graph(nodes: Sequence[LayoutNode], edges: Sequence[tuple[str, str]]) -> Layout:
    """Place `nodes` and route `edges` (parent, child). Edges naming an unknown node are dropped."""
    ordered = sorted(nodes, key=lambda n: (n.sort_key, n.id))
    ids = [n.id for n in ordered]
    known = set(ids)
    kept = sorted({e for e in edges if e[0] in known and e[1] in known})
    parents: dict[str, list[str]] = defaultdict(list)
    children: dict[str, list[str]] = defaultdict(list)
    for p, c in kept:
        parents[c].append(p)
        children[p].append(c)
    for n in ids:
        parents.setdefault(n, [])
        children.setdefault(n, [])
    depth = _layers(ids, parents)

    columns: list[list[str]] = [[] for _ in range(max(depth.values(), default=-1) + 1)]
    for n in ids:
        columns[depth[n]].append(n)

    rank = {n: i for col in columns for i, n in enumerate(col)}

    def sweep(col: list[str], neighbours: dict[str, list[str]]) -> None:
        def bary(n: str) -> float:
            near = neighbours[n]
            return sum(rank[m] for m in near) / len(near) if near else float(rank[n])

        col.sort(key=lambda n: (bary(n), rank[n]))
        for i, n in enumerate(col):
            rank[n] = i

    for i in range(SWEEPS):
        if i % 2 == 0:
            for col in columns[1:]:
                sweep(col, parents)
        else:
            for col in reversed(columns[:-1]):
                sweep(col, children)

    heights = [len(c) * NODE_H + max(len(c) - 1, 0) * GAP_Y for c in columns]
    tallest = max(heights, default=0)
    placed: dict[str, Placed] = {}
    for layer, col in enumerate(columns):
        top = PAD + (tallest - heights[layer]) // 2
        for i, n in enumerate(col):
            placed[n] = Placed(n, layer, PAD + layer * (NODE_W + GAP_X), top + i * (NODE_H + GAP_Y))

    drawn: list[PlacedEdge] = []
    for p, c in kept:
        a, b = placed[p], placed[c]
        x1, y1, x2, y2 = a.x + NODE_W, a.y + NODE_H // 2, b.x, b.y + NODE_H // 2
        mid = (x1 + x2) // 2
        drawn.append(PlacedEdge(p, c, f"M{x1} {y1} C{mid} {y1} {mid} {y2} {x2} {y2}"))
    width = 2 * PAD + len(columns) * NODE_W + max(len(columns) - 1, 0) * GAP_X
    return Layout(
        nodes=tuple(placed[n] for n in ids),
        edges=tuple(drawn),
        width=width,
        height=2 * PAD + tallest,
    )
