"""CLI commands that plugins add through the `pipeloom.commands` entry-point group.

Discovery reads entry-point metadata only. A command module is imported when its command is
invoked, never to list it, so `pipeloom --help` stays cheap and cannot break.
"""

from __future__ import annotations

import importlib.metadata
from collections.abc import Mapping
from typing import Final

import click

from pipeloom.plugins import PluginEntry, _import_ref, autoload_enabled

COMMANDS_GROUP: Final = "pipeloom.commands"


def command_entries(env: Mapping[str, str]) -> list[PluginEntry]:
    """The command entry points, sorted by command name then distribution. Nothing is imported.
    Empty when plugin autoload is disabled."""
    if not autoload_enabled(env):
        return []
    found: list[PluginEntry] = []
    for ep in importlib.metadata.entry_points(group=COMMANDS_GROUP):
        dist = getattr(ep, "dist", None)
        found.append(
            PluginEntry(
                "commands",
                ep.name,
                ep.value,
                getattr(dist, "name", None) or "unknown",
                getattr(dist, "version", None) or "",
                "entry_point",
            )
        )
    return sorted(found, key=lambda e: (e.kind, e.distribution, e.value))


def load_command(entry: PluginEntry) -> click.Command:
    """Import the command. A failure gives a stub that prints the error and exits 1."""
    try:
        obj = _import_ref(entry.value)
        if not isinstance(obj, click.Command):
            raise TypeError("not a click command")
    except Exception as exc:  # noqa: BLE001 - any import-time failure of plugin code
        return _stub(entry, f"{type(exc).__name__}: {exc}")
    return obj


def _stub(entry: PluginEntry, why: str) -> click.Command:
    message = f"cannot load command '{entry.kind}' from {entry.value} ({entry.distribution}): {why}"

    def fail() -> None:
        click.echo(message, err=True)
        raise SystemExit(1)

    return click.Command(
        entry.kind,
        callback=fail,
        add_help_option=False,
        context_settings={"ignore_unknown_options": True, "allow_extra_args": True},
        help=f"Unavailable: {message}",
    )


_META: Final = "pipeloom.command_entries"


class PipeloomGroup(click.Group):
    """The CLI group: Ctrl+C becomes exit code 130, and plugin commands join the built-ins."""

    def invoke(self, ctx: click.Context) -> object:
        try:
            return super().invoke(ctx)
        except KeyboardInterrupt:
            click.echo("Interrupted.", err=True)
            ctx.exit(130)

    def _plugin_commands(self, ctx: click.Context) -> dict[str, PluginEntry]:
        """Entry points that name a free command; built-ins win, then the first distribution."""
        cached: dict[str, PluginEntry] | None = ctx.meta.get(_META)
        if cached is None:
            import os

            cached = {}
            for e in command_entries(os.environ):
                if e.kind in self.commands:
                    click.echo(
                        f"warning: command '{e.kind}' from {e.distribution} is ignored: "
                        "a built-in command has that name",
                        err=True,
                    )
                elif e.kind in cached:
                    click.echo(
                        f"warning: command '{e.kind}' from {e.distribution} is ignored: "
                        f"{cached[e.kind].distribution} provides it first",
                        err=True,
                    )
                else:
                    cached[e.kind] = e
            ctx.meta[_META] = cached
        return cached

    def list_commands(self, ctx: click.Context) -> list[str]:
        return sorted({*super().list_commands(ctx), *self._plugin_commands(ctx)})

    def get_command(self, ctx: click.Context, cmd_name: str) -> click.Command | None:
        plugin = self._plugin_commands(ctx)  # also warns about names a built-in shadows
        built_in = super().get_command(ctx, cmd_name)
        if built_in is not None:
            return built_in
        entry = plugin.get(cmd_name)
        return None if entry is None else load_command(entry)

    def format_commands(self, ctx: click.Context, formatter: click.HelpFormatter) -> None:
        """Like click's, but plugin commands are listed without importing them."""
        rows: list[tuple[str, str]] = []
        for name, cmd in self.commands.items():
            if not cmd.hidden:
                rows.append((name, cmd.get_short_help_str(45)))
        for name, e in self._plugin_commands(ctx).items():
            rows.append((name, f"Added by {e.distribution}."))
        if rows:
            with formatter.section("Commands"):
                formatter.write_dl(sorted(rows))
