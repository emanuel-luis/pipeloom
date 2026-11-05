"""From a loaded plan to the tasks of a DAG. Pure: no Airflow, no disk, no network."""

from __future__ import annotations

import posixpath
from dataclasses import dataclass
from typing import Final, Literal

from pipeloom.diagnostics import Report
from pipeloom.graph.selectors import Selection, SelectorError, apply_to_plan
from pipeloom.plan.model import Plan
from pipeloom_airflow import naming
from pipeloom_airflow.config import ExecutionConfig, ExecutionMode, RenderConfig
from pipeloom_airflow.load import LoadedPlan

DEFAULT_CANCEL_GRACE: Final = 30.0
"""The manifest does not carry `cancel_grace`, so the core default applies."""


@dataclass(frozen=True)
class UnitTask:
    task_id: str
    unit_id: str
    """The unit id, or the Databricks task key in DATABRICKS mode."""
    databricks_key: str | None
    units: tuple[str, ...]
    """Every unit the task runs: one, or the members of a Databricks task."""
    models: tuple[str, ...]
    upstream: tuple[str, ...]
    """Task ids, sorted."""
    group: tuple[str, ...]
    retries: int
    retry_delay_s: float
    retry_on: Literal["transient", "any"]
    timeout_s: float | None
    max_attempts: int
    assets: tuple[str, ...]


@dataclass(frozen=True)
class TaskPlan:
    project: str
    target: str
    plan_hash: str
    select: tuple[str, ...]
    exclude: tuple[str, ...]
    tasks: tuple[UnitTask, ...]
    leaves: tuple[str, ...]
    cancel_grace_s: float


def _groups(plan: Plan, units: list[str], how: str) -> dict[str, tuple[str, ...]]:
    """Unit -> visual group path. The folder is relative to the folder every model shares (the
    model path), `root` for a model directly in it."""
    if how == "none":
        return {u: () for u in units}
    nodes = {u: plan.nodes[plan.units[u].nodes[-1]] for u in units}
    if how == "tag":
        return {u: (n.tags[0] if n.tags else "untagged",) for u, n in nodes.items()}
    dirs = {
        u: posixpath.dirname(n.file).split("/") if posixpath.dirname(n.file) else []
        for u, n in nodes.items()
    }
    # ponytail: the plan does not record model_paths, so the shared leading folder stands in.
    common = 0
    while dirs and all(len(d) > common for d in dirs.values()):
        if len({d[common] for d in dirs.values()}) != 1:
            break
        common += 1
    return {u: tuple(d[common:]) or ("root",) for u, d in dirs.items()}


def _selected_units(plan: Plan, render: RenderConfig, report: Report) -> list[str]:
    try:
        chosen = apply_to_plan(Selection(render.select, render.exclude), plan, report)
    except SelectorError as err:
        report.extend([err.diagnostic])
        return []
    return sorted(u for u, unit in plan.units.items() if unit.nodes[-1] in chosen)


def _databricks_groups(
    plan: Plan, execution: ExecutionConfig, units: set[str], report: Report
) -> list[tuple[str, tuple[str, ...], tuple[str, ...]]]:
    """(key, member units, parent keys) of the Databricks tasks that hold a rendered unit."""
    from pipeloom_databricks.grouping import build_tasks
    from pipeloom_databricks.render import deploy_options

    opts = deploy_options(plan, report)
    if opts is None:
        return []
    out = []
    for t in build_tasks(plan, opts, execution.databricks_pipeline, report):
        members = tuple(u for u in t.units if u in units)
        if members:
            out.append((t.key, members, t.parents))
    kept = {k for k, _, _ in out}
    return [(k, m, tuple(p for p in ps if p in kept)) for k, m, ps in out]


def render_plan(
    loaded: LoadedPlan,
    target: str,
    render: RenderConfig,
    execution: ExecutionConfig,
    report: Report,
) -> TaskPlan:
    """The tasks of the DAG. Reports PLA003 (nothing rendered) and PLA007 (boundary)."""
    plan = loaded.plan
    units = _selected_units(plan, render, report)
    if plan.metadata.boundary != "task":
        report.add(
            "PLA007",
            f"the target's boundary is '{plan.metadata.boundary}', not 'task'",
            hint="each unit runs in its own process; set boundary: task so PLC503 checks that "
            "the state store is shared",
        )
    databricks = execution.mode is ExecutionMode.DATABRICKS
    if databricks:
        parts = _databricks_groups(plan, execution, set(units), report)
        raw = [(k, m, p) for k, m, p in parts]
    else:
        wanted = set(units)
        raw = [(u, (u,), tuple(p for p in plan.units[u].parents if p in wanted)) for u in units]
    if not raw:
        report.add(
            "PLA003",
            "the selection renders no unit",
            hint="check select and exclude",
            context={"select": list(render.select), "exclude": list(render.exclude)},
        )
        return TaskPlan(
            plan.metadata.project,
            target,
            loaded.plan_hash,
            render.select,
            render.exclude,
            (),
            (),
            DEFAULT_CANCEL_GRACE,
        )
    ids = naming.task_ids([r[0] for r in raw], reserved={naming.FINALIZE_ID, naming.LAUNCH_ID})
    groups = _groups(plan, sorted({u for _, m, _ in raw for u in m}), render.group_by)
    tasks = [
        _task(plan, render, ids, groups, key, members, parents, databricks, target)
        for key, members, parents in raw
    ]
    tasks.sort(key=lambda t: t.task_id)
    has_child = {p for t in tasks for p in t.upstream}
    return TaskPlan(
        plan.metadata.project,
        target,
        loaded.plan_hash,
        render.select,
        render.exclude,
        tuple(tasks),
        tuple(t.task_id for t in tasks if t.task_id not in has_child),
        DEFAULT_CANCEL_GRACE,
    )


def _task(
    plan: Plan,
    render: RenderConfig,
    ids: dict[str, str],
    groups: dict[str, tuple[str, ...]],
    key: str,
    members: tuple[str, ...],
    parents: tuple[str, ...],
    databricks: bool,
    target: str,
) -> UnitTask:
    nodes = [plan.nodes[plan.units[u].nodes[-1]] for u in members]
    configs = [n.config for n in nodes]
    upstream = tuple(sorted(ids[p] for p in parents))
    if databricks:
        timeouts = [c.timeout for c in configs]
        timeout = float(sum(t for t in timeouts if t)) if all(timeouts) else None
        retries, delay = 0, 0.0
        max_attempts = max(c.retries for c in configs) + 1
    else:
        c = configs[0]
        retries, delay = c.retries, float(c.retry_delay)
        timeout = float(c.timeout) if c.timeout else None
        max_attempts = retries + 1
    assets: tuple[str, ...] = ()
    if render.emit_assets:
        assets = tuple(
            naming.asset_uri(
                render.asset_uri,
                project=plan.metadata.project,
                target=target,
                model=n.name,
                relation=n.relation.relation_name,
                unique_id=n.unique_id,
            )
            for n in nodes
            if n.relation is not None
        )
    return UnitTask(
        task_id=ids[key],
        unit_id=key,
        databricks_key=key if databricks else None,
        units=members,
        models=tuple(n.name for n in nodes),
        upstream=upstream,
        group=groups[members[0]],
        retries=retries,
        retry_delay_s=delay,
        retry_on=configs[0].retry_on,
        timeout_s=timeout,
        max_attempts=max_attempts,
        assets=assets,
    )
