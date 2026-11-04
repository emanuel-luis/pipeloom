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

METHODS = ("name", "tag", "path", "materialized", "unit")

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


@dataclass(frozen=True)
class Selection:
    select: tuple[str, ...] = ()
    exclude: tuple[str, ...] = ()


@dataclass(frozen=True)
class NodeInfo:
    """What selectors can see of a model."""

    unique_id: str
    name: str
    path: str
    materialized: str
    tags: tuple[str, ...]


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


def _eval_atom(
    atom: Atom, graph: Graph, nodes: Mapping[str, NodeInfo], report: Report
) -> frozenset[str]:
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
    exprs: Iterable[str], graph: Graph, nodes: Mapping[str, NodeInfo], report: Report
) -> frozenset[str]:
    result: set[str] = set()
    for expr in exprs:
        for term in parse(expr):
            sets = [_eval_atom(a, graph, nodes, report) for a in term]
            result |= frozenset.intersection(*sets)
    return frozenset(result)


def select_nodes(
    sel: Selection, graph: Graph, nodes: Mapping[str, NodeInfo], report: Report
) -> frozenset[str]:
    """Evaluate a selection over model nodes. Raises SelectorError; PLG303 goes to `report`."""
    chosen = _eval(sel.select, graph, nodes, report) if sel.select else frozenset(nodes)
    return chosen - _eval(sel.exclude, graph, nodes, report)


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


def node_infos(project: ResolvedProject) -> dict[str, NodeInfo]:
    return {
        uid: NodeInfo(uid, m.name, m.file, m.config.materialized, tuple(m.config.tags))
        for uid, m in project.models.items()
    }


def apply(sel: Selection, graph: Graph, project: ResolvedProject, report: Report) -> frozenset[str]:
    """Unique ids of the selected models."""
    return select_nodes(sel, graph, node_infos(project), report)


def plan_node_infos(plan: Plan) -> dict[str, NodeInfo]:
    return {
        uid: NodeInfo(uid, n.name, n.file, n.materialized, n.tags) for uid, n in plan.nodes.items()
    }


def apply_to_plan(sel: Selection, plan: Plan, report: Report) -> frozenset[str]:
    """Unique ids of the models of a manifest's plan that `sel` selects."""
    kinds: dict[str, Literal["model", "source"]] = {uid: "model" for uid in plan.nodes}
    kinds.update(dict.fromkeys(plan.sources, "source"))
    graph = Graph(plan.parent_map, plan.child_map, kinds)
    return select_nodes(sel, graph, plan_node_infos(plan), report)
