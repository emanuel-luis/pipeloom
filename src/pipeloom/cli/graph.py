"""The graph command: print the subgraph of the selected models."""

from __future__ import annotations

import json
import os
from dataclasses import replace
from pathlib import Path
from typing import Any

import click

from pipeloom.api import Project, ProjectOptions
from pipeloom.artifacts.unit_graph import unit_graph_dict
from pipeloom.cli._events import Invocation
from pipeloom.cli._options import (
    log_format_option,
    manifest_vars,
    plugin_option,
    project_options,
    select_options,
    state_option,
    target_option,
)
from pipeloom.cli._select import Selected, reference_state, resolve_selection
from pipeloom.cli.compile import USAGE
from pipeloom.diagnostics import Report
from pipeloom.graph.selectors import Selection
from pipeloom.plan.manifest import read_manifest
from pipeloom.runtime.run import RunOptions, _plan_from_manifest


def _render(sel: Selected, output: str) -> str:
    project, graph = sel.resolved.project, sel.resolved.graph
    order = [u for u in graph.topo_order() if u in sel.ids]
    names = {u: project.models[u].name for u in order}
    edges = sorted(
        ((p, u) for u in order for p in graph.parents[u] if p in sel.ids),
        key=lambda e: (names[e[0]], names[e[1]]),
    )
    if output == "json":
        data = {
            "nodes": [
                {
                    "unique_id": u,
                    "name": names[u],
                    "materialized": project.models[u].config.materialized,
                    "tags": list(project.models[u].config.tags),
                    "path": project.models[u].file,
                }
                for u in order
            ],
            "edges": [{"from": p, "to": c} for p, c in edges],
        }
        return json.dumps(data, indent=2, ensure_ascii=True)
    if output == "dot":
        lines = ["digraph pipeloom {", "  rankdir=LR;"]
        lines += [f'  "{names[u]}";' for u in order]
        lines += [f'  "{names[p]}" -> "{names[c]}";' for p, c in edges]
        return "\n".join([*lines, "}"])
    if output == "mermaid":
        index = {u: f"n{i}" for i, u in enumerate(order)}
        lines = ["graph LR"]
        lines += [f'  {index[u]}["{names[u]}"]' for u in order]
        lines += [f"  {index[p]} --> {index[c]}" for p, c in edges]
        return "\n".join(lines)
    parents = {u: sorted(names[p] for p in graph.parents[u] if p in sel.ids) for u in order}
    return "\n".join(
        names[u] + (" <- " + ", ".join(parents[u]) if parents[u] else "") for u in order
    )


def _units(
    inv: Invocation,
    project_dir: Path,
    vars_: dict[str, Any],
    target: str | None,
    plugins: tuple[str, ...],
    selection: Selection,
    state: Path | None,
    manifest: Path | None,
) -> None:
    """Print the unit graph of the compiled plan, or of the manifest's. Exits 2 on a bad selector,
    1 on any other error."""
    given = read_manifest(manifest, Report()) if manifest is not None else None
    opts = ProjectOptions(
        project_dir=project_dir,
        target=target,
        vars=manifest_vars(given, vars_),
        plugins=plugins,
        env=os.environ,
    )
    project = Project(opts, invocation_id=inv.invocation_id)
    bound = project.bind().value
    if bound is not None:
        ref = reference_state(inv, project_dir, state, bound.loaded.config.name, bound)
        selection = replace(selection, state=ref)
    if manifest is None:
        outcome = project.compile(selection)
        plan, diags = outcome.value, outcome.diagnostics
    else:
        report = Report()
        run_opts = RunOptions(selection=selection, manifest=manifest)
        plan, _ = _plan_from_manifest(project, run_opts, report)
        diags = report.sorted()
    if plan is None:
        inv.finish(diags, 2 if any(d.code in USAGE for d in diags) else 1, bound=bound)
        return
    click.echo(json.dumps(unit_graph_dict(plan, plugins), indent=2, ensure_ascii=True))
    inv.finish(diags, 0, bound=bound)


@click.command()
@project_options
@target_option
@plugin_option
@select_options
@state_option
@log_format_option
@click.option(
    "--output",
    type=click.Choice(["text", "dot", "mermaid", "json"]),
    default="text",
    show_default=True,
    help="Format of the graph: text (name <- parents), Graphviz dot, Mermaid or JSON. "
    "--units needs json.",
)
@click.option(
    "--units",
    is_flag=True,
    help="Print the execution units of the plan instead of the models, with the command line "
    "that runs each and the one that finalizes the run (schema pipeloom.unit_graph/1). "
    "Needs --output json.",
)
@click.option(
    "--manifest",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    default=None,
    help="With --units: read the plan from this manifest instead of compiling the project, so "
    "plan_hash is the manifest's.",
)
def graph(
    project_dir: Path,
    vars_: dict[str, Any],
    target: str | None,
    plugins: tuple[str, ...],
    select: tuple[str, ...],
    exclude: tuple[str, ...],
    state: Path | None,
    log_format: str,
    output: str,
    units: bool,
    manifest: Path | None,
) -> None:
    """Print the graph of the selected models, in dependency order.

    With --units --output json, print the selected execution units instead, for an orchestrator
    that runs them one by one.
    """
    if units and output != "json":
        raise click.UsageError("--units needs --output json")
    if manifest is not None and not units:
        raise click.UsageError("--manifest needs --units")
    inv = Invocation("graph", log_format)
    if units:
        chosen = Selection(select, exclude)
        _units(inv, project_dir, vars_, target, plugins, chosen, state, manifest)
        return
    sel = resolve_selection(inv, project_dir, vars_, target, plugins, select, exclude, state)
    text = _render(sel, output)
    if text:
        click.echo(text)
    inv.finish(sel.diagnostics, 0, bound=sel.bound)
