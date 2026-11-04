from __future__ import annotations

import itertools
import random
from typing import Literal

from hypothesis import given, settings
from hypothesis import strategies as st

from pipeloom.graph.dag import Graph, find_cycles

Dag = tuple[int, set[tuple[int, int]]]


def make_graph(n: int, edges: set[tuple[int, int]]) -> Graph:
    """Nodes n00.., an edge (i, j) means i is a parent of j."""
    ids = [f"n{i:02d}" for i in range(n)]
    parents = {x: tuple(sorted(ids[i] for i, j in edges if ids[j] == x)) for x in ids}
    children = {x: tuple(sorted(ids[j] for i, j in edges if ids[i] == x)) for x in ids}
    kinds: dict[str, Literal["model", "source"]] = {x: "model" for x in ids}
    return Graph(parents, children, kinds)


@st.composite
def dags(draw: st.DrawFn) -> Dag:
    n = draw(st.integers(1, 30))
    pairs = [(i, j) for i in range(n) for j in range(i + 1, n)]
    edges = draw(st.sets(st.sampled_from(pairs))) if pairs else set()
    return n, edges


@settings(deadline=None)
@given(dags(), st.randoms())
def test_topo_order_respects_edges_and_ignores_input_order(dag: Dag, rnd: random.Random) -> None:
    n, edges = dag
    g = make_graph(n, edges)
    order = g.topo_order()
    pos = {x: i for i, x in enumerate(order)}
    assert sorted(order) == sorted(g.kinds)
    for x, ps in g.parents.items():
        assert all(pos[p] < pos[x] for p in ps)
    shuffled = Graph(
        {k: tuple(rnd.sample(v, len(v))) for k, v in rnd.sample(list(g.parents.items()), n)},
        {k: tuple(rnd.sample(v, len(v))) for k, v in rnd.sample(list(g.children.items()), n)},
        dict(rnd.sample(list(g.kinds.items()), n)),
    )
    assert shuffled.topo_order() == order


@settings(deadline=None)
@given(dags(), st.integers(1, 5))
def test_depth_limited_walk_is_a_subset(dag: Dag, depth: int) -> None:
    g = make_graph(*dag)
    for x in g.kinds:
        assert g.ancestors(x, depth) <= g.ancestors(x)
        assert g.descendants(x, depth) <= g.descendants(x)
        assert g.ancestors(x, 1) == frozenset(g.parents[x])
        assert x not in g.ancestors(x) | g.descendants(x)


@settings(deadline=None)
@given(dags(), st.data())
def test_injected_back_edge_yields_a_real_cycle(dag: Dag, data: st.DataObject) -> None:
    n, edges = dag
    assert find_cycles(make_graph(n, edges).parents) == []
    pairs = [(i, j) for i in range(n) for j in range(i + 1, n)]
    if not pairs:
        return
    # The forward edge i -> j plus the back edge j -> i is always a cycle.
    i, j = data.draw(st.sampled_from(pairs))
    g = make_graph(n, edges | {(i, j), (j, i)})
    cycles = find_cycles(g.parents)
    assert cycles
    for path in cycles:
        assert len(path) >= 2 and path[0] == min(path)
        ring = [*path, path[0]]
        assert all(a in g.parents[b] for a, b in itertools.pairwise(ring))
    assert any(f"n{i:02d}" in c and f"n{j:02d}" in c for c in cycles)


def test_self_loops_are_not_cycles() -> None:
    assert find_cycles({"a": ("a",)}) == []
