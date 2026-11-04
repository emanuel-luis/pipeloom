"""Resolve a project and apply --select and --exclude, for commands that list models."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, NoReturn

from pipeloom.api import Project, ProjectOptions
from pipeloom.cli._events import Invocation
from pipeloom.config.bind import BoundProject
from pipeloom.diagnostics import Diagnostic, Report
from pipeloom.graph.dag import ProjectGraph
from pipeloom.graph.selectors import Selection, SelectorError, apply


@dataclass(frozen=True)
class Selected:
    resolved: ProjectGraph
    ids: frozenset[str]
    """Unique ids of the selected models."""
    diagnostics: tuple[Diagnostic, ...]
    """Everything found so far, including selector warnings."""
    bound: BoundProject | None


def _fail(
    inv: Invocation, diagnostics: tuple[Diagnostic, ...], code: int, bound: BoundProject | None
) -> NoReturn:
    inv.finish(diagnostics, code, bound=bound)
    raise SystemExit(code)  # finish exits already; this keeps the type checker honest


def resolve_selection(
    inv: Invocation,
    project_dir: Path,
    vars_: dict[str, Any],
    target: str | None,
    plugins: tuple[str, ...],
    select: tuple[str, ...],
    exclude: tuple[str, ...],
) -> Selected:
    """Resolve the project and select models. Exits 1 on errors and 2 on a bad selector."""
    opts = ProjectOptions(
        project_dir=project_dir, target=target, vars=vars_, plugins=plugins, env=os.environ
    )
    project = Project(opts, invocation_id=inv.invocation_id)
    outcome = project.resolve()
    bound = project.bind().value
    if outcome.value is None:
        _fail(inv, outcome.diagnostics, 1, bound)
    report = Report()
    report.extend(outcome.diagnostics)
    try:
        ids = apply(Selection(select, exclude), outcome.value.graph, outcome.value.project, report)
    except SelectorError as err:
        report.extend([err.diagnostic])
        _fail(inv, report.sorted(), 2, bound)
    return Selected(outcome.value, ids, report.sorted(), bound)
