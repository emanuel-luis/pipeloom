"""`run --defer`: which parents a run reads from the reference instead of this target.

Defer swaps the read spec at run time. The plan is untouched, so its hash and manifest stay the
same with and without the flag.
"""

from __future__ import annotations

from pipeloom.diagnostics import Diagnostic, Report
from pipeloom.engine.types import TargetRef
from pipeloom.plan.model import Plan


def _catalog_kind(plan: Plan) -> str | None:
    info = plan.metadata.plugins.get("catalogs")
    return None if info is None else info.kind


def defer_refusal(plan: Plan, reference: Plan, *, unit_mode: bool) -> Diagnostic | None:
    """PLX111 when this run cannot defer: a unit invocation, the task boundary (each unit runs in
    a child process that would not know the reference), or another catalog kind."""
    why: str | None = None
    if unit_mode:
        why = "this is a unit invocation, which runs units of a run that already started"
    elif plan.metadata.boundary == "task":
        why = "the boundary is task, so each unit would rerun without the reference"
    else:
        here, there = _catalog_kind(plan), _catalog_kind(reference)
        if there is None or here != there:
            why = (
                f"the reference was compiled for catalog '{there or 'unknown'}', "
                f"this target uses '{here}'"
            )
    if why is None:
        return None
    return Report().add(
        "PLX111",
        f"--defer refused: {why}",
        hint="use --boundary process, run without -s unit:, and a reference of the same catalog kind",
    )


def deferred_relations(
    plan: Plan, units: list[str], reference: Plan
) -> tuple[dict[str, TargetRef], list[Diagnostic]]:
    """The reference relation of every persisted model that is not built by this run.

    `units` are the run's units. A model with no usable relation in the reference (an intermediate
    model, a model missing from the reference, or one that is ephemeral there) is left out, and
    PLX112 warns when a unit of the run reads it. No existence check: the relation is trusted, as
    dbt does with --favor-state. Ephemeral parents are inlined, never deferred.
    """
    running = {plan.units[u].nodes[-1] for u in units}
    persisted = {u.nodes[-1] for u in plan.units.values()}
    read = {
        parent
        for u in units
        for uid in plan.units[u].nodes
        for parent in plan.nodes[uid].inputs.values()
        if parent in persisted and parent not in running
    }
    deferred: dict[str, TargetRef] = {}
    found = Report()
    for uid in sorted(persisted - running):
        theirs = reference.nodes.get(uid)
        name = plan.nodes[uid].name
        why: str | None = None
        if plan.nodes[uid].materialized == "intermediate":
            why = "it is an intermediate model, kept under the artifacts root of its own run"
        elif theirs is None:
            why = "the reference has no such model"
        elif theirs.relation is None or theirs.materialized == "intermediate":
            why = "the reference has no relation for it"
        if why is None:
            assert theirs is not None and theirs.relation is not None
            deferred[uid] = theirs.relation
        elif uid in read:
            found.add(
                "PLX112",
                f"model '{name}' is read from this target, not from the reference: {why}",
                hint=f"select '{name}' to build it here, or build it in the reference first",
                context={"model": name},
            )
    return deferred, list(found.sorted())
