"""From a plan to the tasks of one pipeline's job: units, compute, groups and task keys."""

from __future__ import annotations

import posixpath
from collections import defaultdict
from dataclasses import dataclass

from pipeloom.diagnostics import Report
from pipeloom.graph.dag import find_cycles
from pipeloom.graph.selectors import Selection, SelectorError, apply_to_plan
from pipeloom.plan.model import Plan
from pipeloom_databricks import keys
from pipeloom_databricks.options import DatabricksDeployOptions


@dataclass(frozen=True)
class TaskUnit:
    """One Databricks task: a unit, or a group of units."""

    key: str
    units: tuple[str, ...]
    parents: tuple[str, ...]
    """Keys of the tasks this one waits for, sorted."""
    compute: str
    """Key of an entry in the deploy block's `compute`."""


def _select(plan: Plan, select: list[str], exclude: list[str]) -> frozenset[str]:
    # A throwaway report: "matched nothing" is reported by the caller in its own terms.
    return apply_to_plan(Selection(tuple(select), tuple(exclude)), plan, Report())


def pipeline_units(
    plan: Plan, opts: DatabricksDeployOptions, pipeline: str, report: Report
) -> tuple[str, ...]:
    """The units of a pipeline, sorted. Empty, with PLD004, for an unknown pipeline, a bad
    selector or a selection that holds no unit."""
    options = opts.pipelines.get(pipeline)
    if options is None:
        report.add(
            "PLD004",
            f"pipeline '{pipeline}' is not in the deploy block",
            hint=f"pipelines: {', '.join(sorted(opts.pipelines))}",
            context={"pipeline": pipeline},
        )
        return ()
    try:
        chosen = _select(plan, options.select, options.exclude)
    except SelectorError as err:
        report.add("PLD004", f"pipeline '{pipeline}': {err}", context={"pipeline": pipeline})
        return ()
    units = tuple(sorted(uid for uid, u in plan.units.items() if u.nodes[-1] in chosen))
    if not units:
        report.add(
            "PLD004",
            f"pipeline '{pipeline}' selects no unit",
            context={"pipeline": pipeline},
        )
    return units


def _rule_compute(plan: Plan, opts: DatabricksDeployOptions, report: Report) -> dict[str, str]:
    """Persisted node id -> compute key, for the nodes a rule claims (the first rule wins)."""
    claimed: dict[str, str] = {}
    for i, rule in enumerate(opts.compute_rules):
        try:
            chosen = _select(plan, rule.select, rule.exclude)
        except SelectorError as err:
            report.add("PLD004", f"compute rule {i}: {err}", context={"rule": i})
            continue
        if not chosen:
            report.add(
                "PLD005",
                f"compute rule {i} ({', '.join(rule.select)}) matches no model",
                context={"rule": i},
            )
        for node in sorted(chosen):
            claimed.setdefault(node, rule.compute)
    return claimed


def _group_of(plan: Plan, opts: DatabricksDeployOptions, unit: str) -> str:
    if opts.task_grouping == "none":
        return unit
    node = plan.nodes[plan.units[unit].nodes[-1]]
    if opts.task_grouping == "by_tag":
        return f"tag_{node.tags[0] if node.tags else 'untagged'}"
    return f"dir_{posixpath.dirname(node.file) or 'root'}"


def build_tasks(
    plan: Plan, opts: DatabricksDeployOptions, pipeline: str, report: Report
) -> tuple[TaskUnit, ...]:
    """The tasks of a pipeline's job, sorted by key, finalize not included.

    Reports PLD001 (too many tasks), PLD003 (a cycle among groups), PLD004 (a group on two
    computes) and PLD005. Returns what it could build; the caller checks `report`.
    """
    units = pipeline_units(plan, opts, pipeline, report)
    claimed = _rule_compute(plan, opts, report)
    members: dict[str, list[str]] = defaultdict(list)
    for unit in units:
        members[_group_of(plan, opts, unit)].append(unit)
    group_of = {u: g for g, us in members.items() for u in us}

    compute: dict[str, str] = {}
    for group, us in sorted(members.items()):
        used = sorted({claimed.get(plan.units[u].nodes[-1], opts.default_compute) for u in us})
        if len(used) > 1:
            report.add(
                "PLD004",
                f"task group '{group}' spans more than one compute: {', '.join(used)}",
                hint="change task_grouping or the compute rules so a group has one compute",
                context={"group": group, "compute": [c for c in used]},
            )
        compute[group] = used[0]

    parents: dict[str, tuple[str, ...]] = {
        g: tuple(
            sorted({group_of[p] for u in us for p in plan.units[u].parents if p in group_of} - {g})
        )
        for g, us in members.items()
    }
    assigned = keys.assign_keys(sorted(members))
    for cycle in find_cycles(parents):
        path = [assigned[g] for g in cycle]
        report.add(
            "PLD003",
            f"task groups form a cycle: {' -> '.join((*path, path[0]))}",
            hint="group by another key, or set task_grouping to none",
            context={"groups": [g for g in path]},
        )

    if len(members) + 1 > keys.MAX_TASKS:
        report.add(
            "PLD001",
            f"pipeline '{pipeline}' needs {len(members) + 1} tasks with its finalize task, "
            f"the limit is {keys.MAX_TASKS}",
            hint="set task_grouping in the deploy block, or split the project into pipelines",
            context={"pipeline": pipeline, "tasks": len(members) + 1},
        )

    tasks = [
        TaskUnit(
            assigned[g],
            tuple(sorted(us)),
            tuple(sorted(assigned[p] for p in parents[g])),
            compute[g],
        )
        for g, us in members.items()
    ]
    return tuple(sorted(tasks, key=lambda t: t.key))


def task_key_map(plan: Plan, opts: DatabricksDeployOptions, pipeline: str) -> dict[str, str]:
    """Unit id -> the key of the task that runs it."""
    return {u: t.key for t in build_tasks(plan, opts, pipeline, Report()) for u in t.units}
