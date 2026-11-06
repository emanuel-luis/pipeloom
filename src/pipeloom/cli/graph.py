"""The graph command: print the subgraph of the selected models."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import click

from pipeloom.cli._events import Invocation
from pipeloom.cli._options import (
    log_format_option,
    plugin_option,
    project_options,
    select_options,
    state_option,
    target_option,
)
from pipeloom.cli._select import Selected, resolve_selection


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
    help="Format of the graph: text (name <- parents), Graphviz dot, Mermaid or JSON.",
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
) -> None:
    """Print the graph of the selected models, in dependency order."""
    inv = Invocation("graph", log_format)
    sel = resolve_selection(inv, project_dir, vars_, target, plugins, select, exclude, state)
    text = _render(sel, output)
    if text:
        click.echo(text)
    inv.finish(sel.diagnostics, 0, bound=sel.bound)
