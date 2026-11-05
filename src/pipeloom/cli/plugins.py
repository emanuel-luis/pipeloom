"""The plugins command group."""

from __future__ import annotations

import json
import os
from typing import Any

import click

from pipeloom.cli._events import Invocation
from pipeloom.cli._ext import command_entries, load_command
from pipeloom.cli._options import log_format_option, plugin_option
from pipeloom.diagnostics import Report, Severity
from pipeloom.plugins import GROUPS, PluginEntry, PluginRegistry


def _command_loads(entry: PluginEntry, report: Report) -> bool:
    """Import a command entry; a failure is PLU003."""
    cmd = load_command(entry)
    if cmd.help is not None and cmd.help.startswith("Unavailable: "):
        report.add("PLU003", cmd.help.removeprefix("Unavailable: "))
        return False
    return True


@click.group()
def plugins() -> None:
    """Inspect installed plugins."""


@plugins.command("list")
@click.option(
    "--group", type=click.Choice([*GROUPS, "commands"]), default=None, help="Only this group."
)
@click.option(
    "--output",
    "output",
    type=click.Choice(["text", "json"]),
    default="text",
    show_default=True,
)
@click.option("--check", is_flag=True, help="Import each plugin and report its API and status.")
@plugin_option
@log_format_option
def list_(group: Any, output: str, check: bool, plugins: tuple[str, ...], log_format: str) -> None:
    """List plugins by group and kind. Nothing is imported unless --check is given."""
    inv = Invocation("plugins.list", log_format)
    report = Report()
    registry = PluginRegistry.discover(cli_plugins=plugins, env=os.environ, report=report)
    dupes = registry.duplicates()
    for (grp, kind), found in dupes.items():
        if group is None or grp == group:
            where = ", ".join(f"{e.distribution} ({e.value})" for e in found)
            report.add("PLU002", f"{grp} kind '{kind}' is provided more than once: {where}")
    rows: list[dict[str, str]] = []
    listed = list(registry.entries(group if group != "commands" else None))
    if group == "commands":
        listed = []
    if group in (None, "commands"):
        listed += command_entries(os.environ)
    for e in listed:
        row = {
            "group": e.group,
            "kind": e.kind,
            "source": e.source,
            "distribution": e.distribution,
            "version": e.version,
            "value": e.value,
        }
        if check and e.group == "commands":
            row["api"] = "-"
            row["status"] = "ok" if _command_loads(e, report) else "PLU003"
        elif check:
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
    diagnostics = report.sorted()
    errors = any(d.severity is Severity.ERROR for d in diagnostics)
    inv.finish(diagnostics, 1 if errors else 0)
