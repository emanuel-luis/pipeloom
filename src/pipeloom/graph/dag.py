"""The dependency graph: build it from a resolved project, order it, walk it, find cycles."""

from __future__ import annotations

import heapq
from collections import deque
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Literal

from pipeloom.config.resolve import ResolvedProject
from pipeloom.diagnostics import Report


@dataclass(frozen=True)
class Graph:
    """Edges run from a parent (an input) to its child (the model that reads it)."""

    parents: Mapping[str, tuple[str, ...]]
    children: Mapping[str, tuple[str, ...]]
    kinds: Mapping[str, Literal["model", "source"]]

    def topo_order(self) -> tuple[str, ...]:
        """Parents before children; ties broken by unique id (Kahn with a min-heap)."""
        waiting = {n: len(self.parents.get(n, ())) for n in self.kinds}
        heap = [n for n, d in waiting.items() if d == 0]
        heapq.heapify(heap)
        order: list[str] = []
        while heap:
            node = heapq.heappop(heap)
            order.append(node)
            for child in self.children.get(node, ()):
                waiting[child] -= 1
                if waiting[child] == 0:
                    heapq.heappush(heap, child)
        if len(order) != len(self.kinds):
            raise ValueError("graph has a cycle")
        return tuple(order)

    def ancestors(self, node: str, depth: int | None = None) -> frozenset[str]:
        return _walk(self.parents, node, depth)

    def descendants(self, node: str, depth: int | None = None) -> frozenset[str]:
        return _walk(self.children, node, depth)


def _walk(edges: Mapping[str, tuple[str, ...]], start: str, depth: int | None) -> frozenset[str]:
    """Nodes reachable in at most `depth` steps (unlimited when None), excluding `start`."""
    seen = {start}
    frontier = [start]
    level = 0
    while frontier and (depth is None or level < depth):
        step: list[str] = []
        for node in frontier:
            for nxt in edges.get(node, ()):
                if nxt not in seen:
                    seen.add(nxt)
                    step.append(nxt)
        frontier = step
        level += 1
    seen.discard(start)
    return frozenset(seen)


def _sccs(nodes: list[str], succ: Mapping[str, list[str]]) -> list[list[str]]:
    """Tarjan's strongly connected components, iterative so deep graphs cannot overflow."""
    index: dict[str, int] = {}
    low: dict[str, int] = {}
    on_stack: set[str] = set()
    stack: list[str] = []
    out: list[list[str]] = []
    for root in nodes:
        if root in index:
            continue
        index[root] = low[root] = len(index)
        stack.append(root)
        on_stack.add(root)
        work = [(root, iter(succ.get(root, ())))]
        while work:
            node, it = work[-1]
            for nxt in it:
                if nxt not in index:
                    index[nxt] = low[nxt] = len(index)
                    stack.append(nxt)
                    on_stack.add(nxt)
                    work.append((nxt, iter(succ.get(nxt, ()))))
                    break
                if nxt in on_stack:
                    low[node] = min(low[node], index[nxt])
            else:
                work.pop()
                if work:
                    parent = work[-1][0]
                    low[parent] = min(low[parent], low[node])
                if low[node] == index[node]:
                    comp: list[str] = []
                    while True:
                        member = stack.pop()
                        on_stack.discard(member)
                        comp.append(member)
                        if member == node:
                            break
                    out.append(comp)
    return out


def find_cycles(parents: Mapping[str, tuple[str, ...]]) -> list[tuple[str, ...]]:
    """One shortest cycle per strongly connected component, ignoring self-references.

    Each path starts at the smallest node of its component and follows parent to child edges:
    every node is a parent of the next, and the last is a parent of the first.
    """
    nodes = sorted(set(parents) | {p for ps in parents.values() for p in ps})
    succ: dict[str, list[str]] = {n: [] for n in nodes}
    for child, ps in parents.items():
        for p in ps:
            if p != child:
                succ[p].append(child)
    for targets in succ.values():
        targets.sort()
    cycles: list[tuple[str, ...]] = []
    for comp in _sccs(nodes, succ):
        if len(comp) < 2:
            continue
        members = set(comp)
        start = min(comp)
        prev: dict[str, str | None] = {start: None}
        queue = deque([start])
        while queue:
            u = queue.popleft()
            if u != start and start in succ[u]:
                path = [u]
                while (back := prev[path[-1]]) is not None:
                    path.append(back)
                cycles.append(tuple(reversed(path)))
                break
            for v in succ[u]:
                if v in members and v not in prev:
                    prev[v] = u
                    queue.append(v)
    return sorted(cycles)


@dataclass(frozen=True)
class ProjectGraph:
    """A resolved project that passed L2, with its graph."""

    project: ResolvedProject
    graph: Graph


def build_graph(project: ResolvedProject, report: Report) -> Graph | None:
    """Build the graph of models and sources. Reports PLG302 and PLG301; None on error.

    Expects L2 to have passed: every input names a known model or source.
    """
    kinds: dict[str, Literal["model", "source"]] = {u: "source" for u in project.sources}
    kinds.update({u: "model" for u in project.models})
    errors = False
    parents: dict[str, tuple[str, ...]] = {u: () for u in kinds}
    for uid, m in project.models.items():
        found: set[str] = set()
        for alias, b in m.inputs.items():
            if b.unique_id == uid:
                errors = True
                report.add(
                    "PLG302",
                    f"model '{m.name}' references itself through input '{alias}'",
                    location=project.locate(m.file, f"{m.pointer}/inputs/{alias}/ref"),
                    hint="a model cannot read its own output",
                    context={"model": m.name, "input": alias},
                )
            elif b.unique_id in kinds:
                found.add(b.unique_id)
        parents[uid] = tuple(sorted(found))
    children: dict[str, list[str]] = {u: [] for u in kinds}
    for child, ps in parents.items():
        for p in ps:
            children[p].append(child)
    for path in find_cycles(parents):
        errors = True
        names = [u.split(".", 2)[2] for u in path]
        report.add(
            "PLG301",
            "cycle in the graph: " + " -> ".join([*names, names[0]]),
            location=project.locate_model(project.models[path[0]]),
            hint="remove one of the refs to break the cycle",
            context={"cycle": list(path)},
        )
    if errors:
        return None
    return Graph(parents, {u: tuple(sorted(c)) for u, c in children.items()}, kinds)
