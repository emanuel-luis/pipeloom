"""The plugins command group."""

from __future__ import annotations

import json
import os
from typing import Any

import click

from pipeloom.cli._options import plugin_option
from pipeloom.cli._output import print_diagnostics
from pipeloom.diagnostics import Report
from pipeloom.plugins import GROUPS, PluginRegistry


@click.group()
def plugins() -> None:
    """Inspect installed plugins."""


@plugins.command("list")
@click.option("--group", type=click.Choice(list(GROUPS)), default=None, help="Only this group.")
@click.option(
    "--output",
    "output",
    type=click.Choice(["text", "json"]),
    default="text",
    show_default=True,
)
@click.option("--check", is_flag=True, help="Import each plugin and report its API and status.")
@plugin_option
def list_(group: Any, output: str, check: bool, plugins: tuple[str, ...]) -> None:
    """List plugins by group and kind. Nothing is imported unless --check is given."""
    report = Report()
    registry = PluginRegistry.discover(cli_plugins=plugins, env=os.environ, report=report)
    dupes = registry.duplicates()
    for (grp, kind), found in dupes.items():
        if group is None or grp == group:
            where = ", ".join(f"{e.distribution} ({e.value})" for e in found)
            report.add("PLU002", f"{grp} kind '{kind}' is provided more than once: {where}")
    rows: list[dict[str, str]] = []
    for e in registry.entries(group):
        row = {
            "group": e.group,
            "kind": e.kind,
            "source": e.source,
            "distribution": e.distribution,
            "version": e.version,
            "value": e.value,
        }
        if check:
            one = Report()
            spec = registry.check(e, one)
            report.extend(one.sorted())
            if spec is None:
                row["api"], row["status"] = "-", one.sorted()[0].code
            else:
                row["api"] = f"{spec.api_version[0]}.{spec.api_version[1]}"
                row["status"] = "PLU002" if (e.group, e.kind) in dupes else "ok"
        rows.append(row)
    if output == "json":
        click.echo(json.dumps(rows, indent=2, ensure_ascii=True))
    else:
        columns = list(rows[0]) if rows else []
        header = [c.upper() for c in columns]
        widths = [
            max(len(h), *(len(r[c]) for r in rows)) for h, c in zip(header, columns, strict=True)
        ]
        for line in [header, *([r[c] for c in columns] for r in rows)]:
            click.echo("  ".join(v.ljust(w) for v, w in zip(line, widths, strict=True)).rstrip())
    errors, _ = print_diagnostics(report.sorted())
    if errors:
        raise SystemExit(1)
