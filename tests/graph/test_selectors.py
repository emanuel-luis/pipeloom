from __future__ import annotations

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from pipeloom.diagnostics import Report
from pipeloom.graph.dag import Graph
from pipeloom.graph.selectors import (
    NodeInfo,
    Selection,
    SelectorError,
    StateRef,
    parse,
    select_nodes,
)
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


def stated(
    g: Graph,
    nodes: dict[str, NodeInfo],
    select: str,
    ref: StateRef | None,
    report: Report | None = None,
) -> frozenset[str]:
    return select_nodes(Selection((select,), state=ref), g, nodes, report or Report())


def chain() -> tuple[Graph, dict[str, NodeInfo]]:
    """n00 table, n01 ephemeral, n02 table; n03 table reads n02."""
    g = make_graph(4, {(0, 1), (1, 2), (2, 3)})
    mats = ("table", "ephemeral", "table", "table")
    nodes = {
        f"n0{k}": NodeInfo(f"n0{k}", f"n0{k}", f"m/{k}.yml", mats[k], (), f"sum{k}", ())
        for k in range(4)
    }
    return g, nodes


def same_state(nodes: dict[str, NodeInfo]) -> dict[str, tuple[str, tuple[tuple[str, str], ...]]]:
    return {uid: (n.checksum or "", n.inputs) for uid, n in nodes.items()}


def test_state_modified_and_new() -> None:
    g, nodes = chain()
    ref = same_state(nodes)
    assert stated(g, nodes, "state:modified", StateRef(ref)) == set()
    ref["n00"] = ("other", ())
    del ref["n03"]
    assert stated(g, nodes, "state:modified", StateRef(ref)) == {"n00", "n03"}
    assert stated(g, nodes, "state:new", StateRef(ref)) == {"n03"}
    assert stated(g, nodes, "state:modified+", StateRef(ref)) == {"n00", "n01", "n02", "n03"}
    assert stated(g, nodes, "n02,state:new", StateRef(ref)) == set()


def test_state_modified_sees_rewired_inputs() -> None:
    g, nodes = chain()
    ref = same_state(nodes)
    ref["n02"] = ("sum2", (("x", "n00"),))
    assert stated(g, nodes, "state:modified", StateRef(ref)) == {"n02"}


def test_state_modified_ephemeral_marks_the_models_that_inline_it() -> None:
    g, nodes = chain()
    ref = same_state(nodes)
    ref["n01"] = ("other", ())
    assert stated(g, nodes, "state:modified", StateRef(ref)) == {"n01", "n02"}  # not n03


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("fail", {"n00", "n02"}),
        ("skip", {"n03"}),
        ("cancelled", {"n04"}),
        ("succeeded", {"n05"}),
    ],
)
def test_result_values(value: str, expected: set[str]) -> None:
    g = make_graph(7, set())
    nodes = {
        uid: NodeInfo(uid, uid, f"m/{uid}.yml", "ephemeral" if uid == "n06" else "table", ())
        for uid in sorted(g.kinds)
    }
    results = {
        "n00": "failed",
        "n02": "timed_out",
        "n03": "skipped",
        "n04": "cancelled",
        "n05": "succeeded",
        "n06": "failed",  # ephemeral models never match
    }
    got = stated(g, nodes, f"result:{value}", StateRef({}, results))
    assert got == expected


def test_state_atoms_need_a_reference() -> None:
    g, nodes = chain()
    with pytest.raises(SelectorError) as err:
        stated(g, nodes, "state:modified", None)
    assert err.value.diagnostic.code == "PLG307"
    with pytest.raises(SelectorError) as err:
        stated(g, nodes, "result:fail", StateRef(same_state(nodes)))
    assert err.value.diagnostic.code == "PLG308"


@pytest.mark.parametrize("expr", ["state:changed", "state:*", "result:failed", "result:"])
def test_state_values_are_checked(expr: str) -> None:
    with pytest.raises(SelectorError) as err:
        parse(expr)
    assert err.value.diagnostic.code == "PLG305"


def test_state_ref_keeps_a_selection_hashable() -> None:
    ref = StateRef({})
    assert hash(Selection(("a",), state=ref)) == hash(Selection(("a",), state=ref))


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
