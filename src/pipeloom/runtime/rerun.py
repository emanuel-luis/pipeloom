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
from pipeloom.runtime.run import RunOptions, artifacts_root

if TYPE_CHECKING:
    from pipeloom.api import Project

RETRIED = ("fail", "skip", "cancelled")
"""The `result:` values a retry selects: what the parent did not finish."""


def _refuse(parent_run_id: str, why: str, hint: str) -> list[Diagnostic]:
    return [Report().add("PLX113", f"run {parent_run_id} cannot be retried: {why}", hint=hint)]


def parent_dir(project: Project, parent_run_id: str) -> Path | None:
    """The run folder of the parent under the bound target's artifacts root."""
    bound = project.bind().value
    if bound is None:
        return None
    return RunPaths(
        artifacts_root(bound, bound.loaded.project_dir.resolve()), parent_run_id
    ).run_dir


def inherited_vars(project: Project, parent_run_id: str) -> dict[str, Any]:
    """The parent manifest's vars with the project's own explicit vars on top."""
    folder = parent_dir(project, parent_run_id)
    plan = None if folder is None else read_manifest(folder / MANIFEST_FILE, Report())
    return {**(plan.metadata.vars if plan else {}), **project.options.vars}


def retry_options(
    project: Project,
    parent_run_id: str,
    base: RunOptions,
    full_refresh: bool | None = None,
) -> tuple[RunOptions | None, list[Diagnostic]]:
    """The options that retry `parent_run_id`: its failed, skipped and cancelled units.

    Returns `(None, [])` when the parent has nothing to retry and `(None, diagnostics)` when it
    cannot be retried (PLX113, or what binding the project found). `full_refresh` None inherits the
    parent's value; `base` supplies the rest and loses its selection, state and defer.
    """
    if base.run_id == parent_run_id:
        raise ValueError("run_id must differ from the parent run id")
    bound = project.bind()
    folder = parent_dir(project, parent_run_id)
    if bound.value is None or folder is None:
        return None, list(bound.diagnostics)
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
    present = set(reference.results.values())
    atoms = tuple(f"result:{v}" for v in RETRIED if present & RESULT_STATUSES[v])
    if not atoms:
        return None, []
    args: dict[str, Any] = doc.get("args", {})
    state = Path(args["state"]) if args.get("state") else None
    return (
        replace(
            base,
            selection=Selection(atoms, state=reference.selector_state()),
            full_refresh=(
                base.full_refresh or bool(args.get("full_refresh", False))
                if full_refresh is None
                else full_refresh
            ),
            state=state,
            defer=bool(args.get("defer")) and state is not None,
            parent_run_id=parent_run_id,
        ),
        [],
    )
