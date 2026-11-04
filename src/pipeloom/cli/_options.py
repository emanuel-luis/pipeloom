"""Shared CLI plumbing: the group class and options common to commands."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any, TypeVar

import click

from pipeloom.config.yaml import parse_yaml
from pipeloom.diagnostics import Report

F = TypeVar("F", bound=Callable[..., Any])


class PipeloomGroup(click.Group):
    """A click group that turns Ctrl+C into exit code 130."""

    def invoke(self, ctx: click.Context) -> Any:
        try:
            return super().invoke(ctx)
        except KeyboardInterrupt:
            click.echo("Interrupted.", err=True)
            ctx.exit(130)


def _parse_vars(ctx: click.Context, param: click.Parameter, value: str | None) -> dict[str, Any]:
    if value is None:
        return {}
    report = Report()
    doc = parse_yaml(value.encode("utf-8"), "--vars", report)
    if doc is None:
        first = report.sorted()[0]
        raise click.BadParameter(first.message, ctx=ctx, param=param)
    if not isinstance(doc.data, dict):
        raise click.BadParameter("must be a YAML or JSON mapping", ctx=ctx, param=param)
    return dict(doc.data)


def plugin_option(func: F) -> F:
    """Add a repeatable --plugin MODULE:ATTR."""
    return click.option(
        "--plugin",
        "plugins",
        multiple=True,
        metavar="MODULE:ATTR",
        help="A PluginSpec to load besides the installed plugins. Repeatable.",
    )(func)


def target_option(func: F) -> F:
    """Add --target."""
    return click.option(
        "--target",
        envvar="PIPELOOM_TARGET",
        default=None,
        help="Target to bind; default_target or the only target when omitted.",
    )(func)


def project_options(func: F) -> F:
    """Add --project-dir and --vars."""
    func = click.option(
        "--vars",
        "vars_",
        callback=_parse_vars,
        default=None,
        help="Variables as a YAML or JSON mapping; they override the vars block.",
    )(func)
    func = click.option(
        "--project-dir",
        envvar="PIPELOOM_PROJECT_DIR",
        type=click.Path(exists=True, file_okay=False, path_type=Path),
        default=".",
        show_default=True,
        help="Directory that holds pipeloom.yml.",
    )(func)
    return func


def select_options(func: F) -> F:
    """Add repeatable -s/--select and --exclude (selector expressions)."""
    func = click.option(
        "--exclude",
        multiple=True,
        metavar="SELECTOR",
        help="Remove the models this selector matches from the selection. Repeatable.",
    )(func)
    func = click.option(
        "-s",
        "--select",
        multiple=True,
        metavar="SELECTOR",
        help="Models to include: name, tag:x, path:dir, materialized:x, +model, model+, "
        "N+model, model+N. Space unions, comma intersects. Repeatable, all are unioned.",
    )(func)
    return func


def log_format_option(func: F) -> F:
    """Add --log-format."""
    return click.option(
        "--log-format",
        "log_format",
        envvar="PIPELOOM_LOG_FORMAT",
        type=click.Choice(["text", "json"]),
        default="text",
        show_default=True,
        help="Diagnostics and events on stderr: readable text or one JSON object per line.",
    )(func)
