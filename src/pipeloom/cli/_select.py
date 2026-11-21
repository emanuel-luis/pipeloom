"""Resolve a project and apply --select and --exclude, for commands that list models."""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, NoReturn

from pipeloom.api import Project, ProjectOptions
from pipeloom.artifacts.reference import load_reference
from pipeloom.cli._events import Invocation
from pipeloom.config.bind import BoundProject
from pipeloom.diagnostics import Diagnostic, Report
from pipeloom.graph.dag import ProjectGraph
from pipeloom.graph.selectors import (
    NodeInfo,
    Selection,
    SelectorError,
    StateRef,
    node_infos,
    select_nodes,
)
from pipeloom.plan.compile import model_checksums, split_checksums


@dataclass(frozen=True)
class Selected:
    resolved: ProjectGraph
    ids: frozenset[str]
    """Unique ids of the selected models."""
    diagnostics: tuple[Diagnostic, ...]
    """Everything found so far, including selector warnings."""
    bound: BoundProject | None
    state: StateRef | None = None
    nodes: Mapping[str, NodeInfo] = field(default_factory=dict)
    """Selector views of every model, with checksums when `state` is set."""


def _fail(
    inv: Invocation, diagnostics: tuple[Diagnostic, ...], code: int, bound: BoundProject | None
) -> NoReturn:
    inv.finish(diagnostics, code, bound=bound)
    raise SystemExit(code)  # finish exits already; this keeps the type checker honest


def reference_state(
    inv: Invocation,
    project_dir: Path,
    state: Path | None,
    project_name: str,
    bound: BoundProject | None,
) -> StateRef | None:
    """The selector input of `--state`; None without the flag. Exits 1 on PLX110."""
    if state is None:
        return None
    report = Report()
    ref = load_reference(project_dir / state, project_name, report, project_dir)
    if ref is None:
        _fail(inv, report.sorted(), 1, bound)
    return ref.selector_state()


def resolve_selection(
    inv: Invocation,
    project_dir: Path,
    vars_: dict[str, Any],
    target: str | None,
    plugins: tuple[str, ...],
    select: tuple[str, ...],
    exclude: tuple[str, ...],
    state: Path | None = None,
) -> Selected:
    """Resolve the project and select models. Exits 1 on errors and PLX110, 2 on a bad selector."""
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
    ref = reference_state(inv, project_dir, state, outcome.value.project.name, bound)
    sums = None if ref is None else model_checksums(outcome.value)
    parts = None if ref is None else split_checksums(outcome.value)
    nodes = node_infos(outcome.value.project, sums, parts)
    try:
        ids = select_nodes(Selection(select, exclude, ref), outcome.value.graph, nodes, report)
    except SelectorError as err:
        report.extend([err.diagnostic])
        _fail(inv, report.sorted(), 2, bound)
    return Selected(outcome.value, ids, report.sorted(), bound, ref, nodes)
