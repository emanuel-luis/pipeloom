"""Selectors: choose models by name, tag, path or materialization, with graph operators.

Grammar: terms are separated by spaces (union); atoms inside a term by commas (intersection).
An atom is `[N]+` (ancestors, optional depth), a `method:` (default `name`), a glob value, and
`+[N]` (descendants, optional depth). The result is the union of the select terms (all models
when there are none) minus the union of the exclude terms, and only holds models.
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from fnmatch import fnmatchcase
from typing import TYPE_CHECKING, Literal

from pipeloom.config.resolve import ResolvedProject
from pipeloom.diagnostics import Diagnostic, Report, suggest
from pipeloom.graph.dag import Graph

if TYPE_CHECKING:
    from pipeloom.plan.model import Plan

METHODS = ("name", "tag", "path", "materialized", "unit", "state", "result")

STATE_VALUES = ("modified", "new")
RESULT_STATUSES: Mapping[str, frozenset[str]] = {
    "fail": frozenset({"failed", "timed_out"}),
    "skip": frozenset({"skipped"}),
    "cancelled": frozenset({"cancelled"}),
    "succeeded": frozenset({"succeeded"}),
}
"""`result:` values and the run_results row statuses each one matches."""

_ATOM = re.compile(
    r"^(?:(?P<up>\d*)\+)?(?:(?P<method>[A-Za-z_]\w*):)?(?P<value>[^+:\s,]+)(?:\+(?P<down>\d*))?$"
)


class SelectorError(Exception):
    """A selector that cannot be parsed. Carries the PLG304 or PLG305 diagnostic."""

    def __init__(self, diagnostic: Diagnostic) -> None:
        super().__init__(diagnostic.message)
        self.diagnostic = diagnostic


@dataclass(frozen=True)
class Atom:
    text: str
    method: str
    value: str
    up: bool = False
    up_depth: int | None = None
    down: bool = False
    down_depth: int | None = None


@dataclass(frozen=True, eq=False)
class StateRef:
    """What `state:` and `result:` compare against: the reference manifest and run results.

    `nodes` maps a unique id to `(checksum, sorted (alias, input unique id) pairs)`. `results`
    maps a persisted model's unique id to its run_results row status; None when the reference
    has no run_results.json. Compared by identity so a Selection stays hashable.
    """

    nodes: Mapping[str, tuple[str, tuple[tuple[str, str], ...]]]
    results: Mapping[str, str] | None = None


@dataclass(frozen=True)
class Selection:
    select: tuple[str, ...] = ()
    exclude: tuple[str, ...] = ()
    state: StateRef | None = None


@dataclass(frozen=True)
class NodeInfo:
    """What selectors can see of a model."""

    unique_id: str
    name: str
    path: str
    materialized: str
    tags: tuple[str, ...]
    checksum: str | None = None
    inputs: tuple[tuple[str, str], ...] = ()
    """Sorted (alias, input unique id) pairs."""


def _malformed(expr: str, why: str) -> SelectorError:
    d = Report().add(
        "PLG305",
        f"malformed selector '{expr}': {why}",
        hint="use [N]+[method:]value+[N], joined with commas (and) or spaces (or)",
        context={"selector": expr},
    )
    return SelectorError(d)


def _depth(text: str | None, atom: str) -> int | None:
    if not text:
        return None
    n = int(text)
    if n < 1:
        raise _malformed(atom, "depth must be at least 1")
    return n


def parse(expr: str) -> list[list[Atom]]:
    """Parse an expression into terms of atoms. Raises SelectorError."""
    if not expr.strip():
        raise _malformed(expr, "empty selector")
    terms: list[list[Atom]] = []
    for token in expr.split():
        atoms: list[Atom] = []
        for text in token.split(","):
            m = _ATOM.match(text)
            if m is None:
                raise _malformed(text or token, "cannot read this atom")
            method = m["method"] or "name"
            if method not in METHODS:
                d = Report().add(
                    "PLG304",
                    f"unknown selector method '{method}' in '{text}'",
                    hint=suggest(method, METHODS) or f"methods: {', '.join(METHODS)}",
                    context={"selector": text, "method": method},
                )
                raise SelectorError(d)
            if method == "state" and m["value"] not in STATE_VALUES:
                raise _malformed(text, f"state: takes {' or '.join(STATE_VALUES)}")
            if method == "result" and m["value"] not in RESULT_STATUSES:
                raise _malformed(text, f"result: takes {', '.join(RESULT_STATUSES)}")
            atoms.append(
                Atom(
                    text,
                    method,
                    m["value"],
                    up=m["up"] is not None,
                    up_depth=_depth(m["up"], text),
                    down=m["down"] is not None,
                    down_depth=_depth(m["down"], text),
                )
            )
        terms.append(atoms)
    return terms


def _path_matches(pattern: str, file: str) -> bool:
    """The file is the path or under it, compared one segment at a time."""
    pat = [s for s in pattern.replace("\\", "/").strip("/").split("/") if s and s != "."]
    segs = file.split("/")
    return len(pat) <= len(segs) and all(fnmatchcase(s, p) for s, p in zip(segs, pat, strict=False))


def _matches(atom: Atom, node: NodeInfo) -> bool:
    if atom.method == "name":
        return fnmatchcase(node.name, atom.value)
    if atom.method == "tag":
        return any(fnmatchcase(t, atom.value) for t in node.tags)
    if atom.method == "materialized":
        return fnmatchcase(node.materialized, atom.value)
    if atom.method == "unit":  # a unit is named after its persisted model
        return node.materialized != "ephemeral" and fnmatchcase(node.name, atom.value)
    return _path_matches(atom.value, node.path)


def _usage_error(code: str, atom: Atom, hint: str) -> SelectorError:
    message = {
        "PLG307": f"selector '{atom.text}' needs a reference state",
        "PLG308": f"selector '{atom.text}' needs run results in the reference state",
    }[code]
    return SelectorError(Report().add(code, message, hint=hint, context={"selector": atom.text}))


def _differs(node: NodeInfo, ref: tuple[str, tuple[tuple[str, str], ...]] | None) -> bool:
    return ref is None or node.checksum is None or (node.checksum, node.inputs) != ref


def _modified(graph: Graph, nodes: Mapping[str, NodeInfo], ref: StateRef) -> set[str]:
    """Models that are new or differ from the reference, plus the persisted models that inline a
    modified ephemeral one (children are walked through ephemeral models only)."""
    own = {uid for uid, n in nodes.items() if _differs(n, ref.nodes.get(uid))}
    out = set(own)
    for uid in own:
        stack = [uid] if nodes[uid].materialized == "ephemeral" else []
        while stack:
            for child in graph.children.get(stack.pop(), ()):
                if child in nodes and child not in out:
                    out.add(child)
                    if nodes[child].materialized == "ephemeral":
                        stack.append(child)
    return out


def _state_base(
    atom: Atom, graph: Graph, nodes: Mapping[str, NodeInfo], state: StateRef | None
) -> set[str]:
    if state is None:
        raise _usage_error("PLG307", atom, "pass --state DIR, the folder of a reference manifest")
    if atom.method == "state":
        if atom.value == "new":
            return {uid for uid in nodes if uid not in state.nodes}
        return _modified(graph, nodes, state)
    if state.results is None:
        raise _usage_error("PLG308", atom, "the --state folder needs a run_results.json")
    wanted = RESULT_STATUSES[atom.value]
    return {
        uid
        for uid, n in nodes.items()
        if n.materialized != "ephemeral" and state.results.get(uid) in wanted
    }


def _eval_atom(
    atom: Atom,
    graph: Graph,
    nodes: Mapping[str, NodeInfo],
    report: Report,
    state: StateRef | None,
) -> frozenset[str]:
    if atom.method in ("state", "result"):
        base = _state_base(atom, graph, nodes, state)
    else:
        base = {uid for uid, n in nodes.items() if _matches(atom, n)}
    if not base and atom.method == "unit":
        known = sorted(n.name for n in nodes.values() if n.materialized != "ephemeral")
        raise SelectorError(
            Report().add(
                "PLG306",
                f"selector '{atom.text}' names no unit",
                hint=suggest(atom.value, known) or "units are the persisted models",
                context={"selector": atom.text},
            )
        )
    if not base:
        report.add(
            "PLG303",
            f"selector '{atom.text}' matched nothing",
            context={"selector": atom.text},
        )
    out = set(base)
    for uid in base:
        if atom.up:
            out |= graph.ancestors(uid, atom.up_depth)
        if atom.down:
            out |= graph.descendants(uid, atom.down_depth)
    return frozenset(out & nodes.keys())


def _eval(
    exprs: Iterable[str],
    graph: Graph,
    nodes: Mapping[str, NodeInfo],
    report: Report,
    state: StateRef | None,
) -> frozenset[str]:
    result: set[str] = set()
    for expr in exprs:
        for term in parse(expr):
            sets = [_eval_atom(a, graph, nodes, report, state) for a in term]
            result |= frozenset.intersection(*sets)
    return frozenset(result)


def select_nodes(
    sel: Selection, graph: Graph, nodes: Mapping[str, NodeInfo], report: Report
) -> frozenset[str]:
    """Evaluate a selection over model nodes. Raises SelectorError; PLG303 goes to `report`."""
    state = sel.state
    chosen = _eval(sel.select, graph, nodes, report, state) if sel.select else frozenset(nodes)
    return chosen - _eval(sel.exclude, graph, nodes, report, state)


def is_unit_selection(sel: Selection) -> bool:
    """True when every select atom is a plain `unit:` atom: the selection of a unit invocation."""
    if not sel.select:
        return False
    try:
        terms = [term for expr in sel.select for term in parse(expr)]
    except SelectorError:
        return False
    atoms = [a for term in terms for a in term]
    return all(a.method == "unit" and not a.up and not a.down for a in atoms)


def node_infos(
    project: ResolvedProject, checksums: Mapping[str, str] | None = None
) -> dict[str, NodeInfo]:
    """Selector views of the models; `checksums` (unique id to node checksum) feeds `state:`."""
    sums = checksums or {}
    return {
        uid: NodeInfo(
            uid,
            m.name,
            m.file,
            m.config.materialized,
            tuple(m.config.tags),
            sums.get(uid),
            tuple(sorted((alias, b.unique_id) for alias, b in m.inputs.items())),
        )
        for uid, m in project.models.items()
    }


def apply(
    sel: Selection,
    graph: Graph,
    project: ResolvedProject,
    report: Report,
    checksums: Mapping[str, str] | None = None,
) -> frozenset[str]:
    """Unique ids of the selected models."""
    return select_nodes(sel, graph, node_infos(project, checksums), report)


def plan_node_infos(plan: Plan) -> dict[str, NodeInfo]:
    return {
        uid: NodeInfo(
            uid,
            n.name,
            n.file,
            n.materialized,
            n.tags,
            n.checksum,
            tuple(sorted(n.inputs.items())),
        )
        for uid, n in plan.nodes.items()
    }


def apply_to_plan(sel: Selection, plan: Plan, report: Report) -> frozenset[str]:
    """Unique ids of the models of a manifest's plan that `sel` selects."""
    kinds: dict[str, Literal["model", "source"]] = {uid: "model" for uid in plan.nodes}
    kinds.update(dict.fromkeys(plan.sources, "source"))
    graph = Graph(plan.parent_map, plan.child_map, kinds)
    return select_nodes(sel, graph, plan_node_infos(plan), report)
