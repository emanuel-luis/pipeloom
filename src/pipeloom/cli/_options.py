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
