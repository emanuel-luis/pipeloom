"""Execution units: one per persisted model, with the ephemeral models it inlines."""

from __future__ import annotations

from pipeloom.config.resolve import ResolvedProject
from pipeloom.graph.dag import Graph, inlined_ancestors
from pipeloom.plan.model import ExecutionUnit


def build_units(project: ResolvedProject, graph: Graph) -> dict[str, ExecutionUnit]:
    """Every non-ephemeral model is a unit named after it, over the whole graph.

    Members are the inlined ephemeral ancestors in topological order, then the model. Unit
    parents come from the members' non-ephemeral model parents. Selection and boundary do not
    change units.
    """
    order = {uid: i for i, uid in enumerate(graph.topo_order())}
    ephemeral = frozenset(
        u for u, m in project.models.items() if m.config.materialized == "ephemeral"
    )
    units: dict[str, ExecutionUnit] = {}
    for uid, model in project.models.items():
        if uid in ephemeral:
            continue
        members = [*sorted(inlined_ancestors(graph, uid, ephemeral), key=order.__getitem__), uid]
        parents = {
            project.models[p].name
            for member in members
            for p in graph.parents[member]
            if p in project.models and p not in ephemeral
        }
        units[model.name] = ExecutionUnit(
            unit_id=model.name, nodes=tuple(members), parents=tuple(sorted(parents))
        )
    return dict(sorted(units.items()))
