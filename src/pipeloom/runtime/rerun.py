"""Retry a finished run: the options of the run that redoes what it did not finish."""

from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path
from typing import TYPE_CHECKING, Any

from pipeloom.artifacts.layout import RunPaths
from pipeloom.artifacts.reference import MANIFEST_FILE, RESULTS_FILE, load_reference
from pipeloom.diagnostics import Diagnostic, Report
from pipeloom.graph.selectors import RESULT_STATUSES, Selection
from pipeloom.plan.manifest import read_manifest
from pipeloom.plan.model import Plan
from pipeloom.runtime.run import DEFAULT_ARTIFACTS_ROOT, RunOptions, artifacts_root

if TYPE_CHECKING:
    from pipeloom.api import Project

RETRIED = ("fail", "skip", "cancelled")
"""The `result:` values a retry selects: what the parent did not finish."""


def _refuse(parent_run_id: str, why: str, hint: str) -> list[Diagnostic]:
    return [Report().add("PLX113", f"run {parent_run_id} cannot be retried: {why}", hint=hint)]


def parent_dir(project: Project, parent_run_id: str) -> Path:
    """The run folder of the parent under the artifacts root of the bound target, or under the
    default root when the project does not bind (it may need the parent's vars to load)."""
    bound = project.bind().value
    if bound is None:
        root = project.options.project_dir.resolve() / DEFAULT_ARTIFACTS_ROOT
    else:
        root = artifacts_root(bound, bound.loaded.project_dir.resolve())
    return RunPaths(root, parent_run_id).run_dir


def parent_manifest(project: Project, parent_run_id: str) -> Plan | None:
    folder = parent_dir(project, parent_run_id)
    if not (folder / MANIFEST_FILE).is_file():
        return None
    return read_manifest(folder / MANIFEST_FILE, Report())


def inherited_vars(project: Project, parent_run_id: str) -> dict[str, Any]:
    """The parent manifest's vars with the project's own explicit vars on top."""
    plan = parent_manifest(project, parent_run_id)
    return {**(plan.metadata.vars if plan else {}), **project.options.vars}


def _intermediate_ancestors(plan: Plan, selected: set[str]) -> list[str]:
    """Names of the `intermediate` models the selected units read, directly or through other
    intermediates. They live under the run that built them, so the retry builds them again."""
    unit_of = {u.nodes[-1]: u for u in plan.units.values()}
    todo = [uid for uid in sorted(selected) if uid in unit_of]
    seen: set[str] = set()
    names: list[str] = []
    while todo:
        for node in unit_of[todo.pop()].nodes:
            for parent in plan.nodes[node].inputs.values():
                p = plan.nodes.get(parent)  # a source is not a node
                if p is None or p.materialized != "intermediate" or parent in seen:
                    continue
                seen.add(parent)
                todo.append(parent)
                if parent not in selected:
                    names.append(p.name)
    return sorted(names)


def retry_options(
    project: Project,
    parent_run_id: str,
    base: RunOptions,
    full_refresh: bool | None = None,
) -> tuple[RunOptions | None, list[Diagnostic]]:
    """The options that retry `parent_run_id`: its failed, skipped and cancelled units, and the
    intermediate models they read.

    Returns `(None, [])` when the parent has nothing to retry and `(None, diagnostics)` when it
    cannot be retried (PLX113, or what binding the project found). `full_refresh` None inherits the
    parent's value; `base` supplies the rest and loses its selection, state and defer. With an
    inherited defer, models the parent built are never deferred (`not_deferred`).
    """
    if base.run_id == parent_run_id:
        raise ValueError("run_id must differ from the parent run id")
    bound = project.bind()
    if bound.value is None:
        hint = "retry reads the vars of the parent run's manifest; if that is missing, pass them with --vars"
        return None, [
            replace(d, hint=f"{d.hint}; {hint}" if d.hint else hint) if d.code == "PLS106" else d
            for d in bound.diagnostics
        ]
    folder = parent_dir(project, parent_run_id)
    config = bound.value.loaded.config
    project_dir = bound.value.loaded.project_dir
    if not (folder / RESULTS_FILE).is_file():
        return None, _refuse(
            parent_run_id,
            f"it has no {RESULTS_FILE}",
            "a run that never finished, is still running or never existed has nothing to retry",
        )
    found = Report()
    reference = load_reference(folder, config.name, found, project_dir)
    if reference is None or reference.results is None:
        why = found.sorted()[0].message if found.has_errors else "no results"
        return None, _refuse(parent_run_id, why, "pick a finished run of this project")
    doc: dict[str, Any] = json.loads((folder / RESULTS_FILE).read_bytes().decode("utf-8"))
    parent_target = doc["run"]["target"]
    if parent_target != bound.value.target:
        return None, _refuse(
            parent_run_id,
            f"it ran target '{parent_target}', not '{bound.value.target}'",
            f"retry with --target {parent_target}",
        )
    values = [v for v in RETRIED if set(reference.results.values()) & RESULT_STATUSES[v]]
    if not values:
        return None, []
    atoms = [f"result:{v}" for v in values]
    selected = {
        uid
        for uid, status in reference.results.items()
        if any(status in RESULT_STATUSES[v] for v in values)
    }
    plan = project.compile().value  # the whole project; a broken one reports when it runs
    if plan is not None:
        atoms += _intermediate_ancestors(plan, selected)
    args: dict[str, Any] = doc.get("args", {})
    state = Path(args["state"]) if args.get("state") else None
    defer = bool(args.get("defer")) and state is not None
    return (
        replace(
            base,
            selection=Selection(tuple(atoms), state=reference.selector_state()),
            full_refresh=(
                base.full_refresh or bool(args.get("full_refresh", False))
                if full_refresh is None
                else full_refresh
            ),
            state=state,
            defer=defer,
            not_deferred=frozenset(
                uid for uid, status in reference.results.items() if status == "succeeded"
            )
            if defer
            else frozenset(),
            parent_run_id=parent_run_id,
        ),
        [],
    )
