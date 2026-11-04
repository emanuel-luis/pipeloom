from __future__ import annotations

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from pipeloom.diagnostics import Report
from pipeloom.graph.dag import Graph
from pipeloom.graph.selectors import NodeInfo, Selection, SelectorError, parse, select_nodes
from tests.graph.test_dag import Dag, dags, make_graph

MATS = ("table", "view", "ephemeral")
TAGS = ("a", "b", "c")


def view(g: Graph, seed: list[int]) -> dict[str, NodeInfo]:
    out: dict[str, NodeInfo] = {}
    for k, uid in enumerate(sorted(g.kinds)):
        s = seed[k % len(seed)]
        tags = tuple(t for bit, t in enumerate(TAGS) if s >> bit & 1)
        folder = "x" if s % 2 else "y"
        out[uid] = NodeInfo(uid, uid, f"models/{folder}/{uid}.yml", MATS[s % 3], tags)
    return out


def run(
    g: Graph, nodes: dict[str, NodeInfo], select: tuple[str, ...], exclude: tuple[str, ...] = ()
) -> frozenset[str]:
    return select_nodes(Selection(select, exclude), g, nodes, Report())


DRAWN = st.tuples(dags(), st.lists(st.integers(0, 15), min_size=1, max_size=5))


@settings(deadline=None)
@given(DRAWN, st.data())
def test_graph_operators_and_set_algebra(drawn: tuple[Dag, list[int]], data: st.DataObject) -> None:
    (n, edges), seed = drawn
    g = make_graph(n, edges)
    nodes = view(g, seed)
    ids = sorted(nodes)
    x, y = data.draw(st.sampled_from(ids)), data.draw(st.sampled_from(ids))
    assert run(g, nodes, (f"+{x}",)) == g.ancestors(x) | {x}
    assert run(g, nodes, (f"{x}+",)) == g.descendants(x) | {x}
    assert run(g, nodes, (f"{x} {y}",)) == run(g, nodes, (f"{y} {x}",))
    assert run(g, nodes, (x, y)) == run(g, nodes, (f"{x} {y}",))
    assert run(g, nodes, (f"{x},{y}",)) == run(g, nodes, (x,)) & run(g, nodes, (y,))
    assert run(g, nodes, (f"1+{x}",)) <= run(g, nodes, (f"+{x}",))


@settings(deadline=None)
@given(DRAWN, st.sampled_from(TAGS), st.sampled_from(MATS))
def test_exclude_is_set_difference(drawn: tuple[Dag, list[int]], tag: str, mat: str) -> None:
    (n, edges), seed = drawn
    g = make_graph(n, edges)
    nodes = view(g, seed)
    chosen = run(g, nodes, (f"tag:{tag}",))
    dropped = run(g, nodes, (f"materialized:{mat}",))
    assert run(g, nodes, (f"tag:{tag}",), (f"materialized:{mat}",)) == chosen - dropped
    assert run(g, nodes, (), (f"materialized:{mat}",)) == frozenset(nodes) - dropped


def small() -> tuple[Graph, dict[str, NodeInfo]]:
    g = make_graph(3, {(0, 1), (1, 2)})
    nodes = {
        "n00": NodeInfo("n00", "n00", "models/mart/a.yml", "table", ("t",)),
        "n01": NodeInfo("n01", "n01", "models/marts/b.yml", "view", ()),
        "n02": NodeInfo("n02", "n02", "models/marts/c.yml", "table", ("t",)),
    }
    return g, nodes


def test_methods_and_path_segments() -> None:
    g, nodes = small()
    assert run(g, nodes, ("path:models/mart",)) == {"n00"}
    assert run(g, nodes, ("path:models/mart/",)) == {"n00"}
    assert run(g, nodes, ("path:models\\marts",)) == {"n01", "n02"}
    assert run(g, nodes, ("path:models/mart*",)) == {"n00", "n01", "n02"}
    assert run(g, nodes, ("path:models/mart/a.yml",)) == {"n00"}
    assert run(g, nodes, ("tag:t,materialized:table",)) == {"n00", "n02"}
    assert run(g, nodes, ("n0[01]",)) == {"n00", "n01"}
    assert run(g, nodes, ("N00",)) == set()  # case-sensitive
    assert run(g, nodes, ("n00+1",)) == {"n00", "n01"}
    assert run(g, nodes, ("2+n02",)) == {"n00", "n01", "n02"}


def test_nothing_matched_warns() -> None:
    g, nodes = small()
    report = Report()
    assert select_nodes(Selection(("tag:zzz",)), g, nodes, report) == set()
    assert [d.code for d in report.sorted()] == ["PLG303"]


@pytest.mark.parametrize("expr", ["", "  ", "a,", ",a", "a:", "0+a", "a++", "a+b+", "x:y:z", "+"])
def test_malformed(expr: str) -> None:
    with pytest.raises(SelectorError) as err:
        parse(expr)
    assert err.value.diagnostic.code == "PLG305"


def test_unknown_method() -> None:
    with pytest.raises(SelectorError) as err:
        parse("tagg:x")
    assert err.value.diagnostic.code == "PLG304"
    assert err.value.diagnostic.hint == "did you mean 'tag'?"


def test_parse_shape() -> None:
    ((a, b),) = parse("2+tag:x*+3,name:y")
    assert (a.method, a.value, a.up, a.up_depth, a.down, a.down_depth) == (
        "tag",
        "x*",
        True,
        2,
        True,
        3,
    )
    assert (b.method, b.up, b.down) == ("name", False, False)
