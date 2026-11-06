"""Shared CLI plumbing: the group class and options common to commands."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any, TypeVar

import click

from pipeloom.config.yaml import parse_yaml
from pipeloom.diagnostics import Report
from pipeloom.plan.model import Plan
from pipeloom.platform import check_orchestrator_id

F = TypeVar("F", bound=Callable[..., Any])


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


def _orchestrator_ids(
    ctx: click.Context, param: click.Parameter, value: tuple[str, ...]
) -> dict[str, str]:
    out: dict[str, str] = {}
    for item in value:
        key, sep, text = item.partition("=")
        if not sep:
            raise click.BadParameter(f"'{item}' is not KEY=VALUE", ctx=ctx, param=param)
        try:
            check_orchestrator_id(key, text)
        except ValueError as err:
            raise click.BadParameter(str(err), ctx=ctx, param=param) from err
        out[key] = text  # a later duplicate wins
    return out


def orchestrator_id_option(func: F) -> F:
    """Add a repeatable --orchestrator-id KEY=VALUE; the parameter is `orchestrator_ids`."""
    return click.option(
        "--orchestrator-id",
        "orchestrator_ids",
        multiple=True,
        callback=_orchestrator_ids,
        metavar="KEY=VALUE",
        help="An id the orchestrator gave this run, such as job_run_id=42. It goes into the "
        "attempt files, the run results and the context of every event. Repeatable; a later "
        "duplicate wins. Key: [a-z][a-z0-9_]{0,63}; value: 1 to 256 printable characters.",
    )(func)


def orchestrator_context(ids: Mapping[str, str]) -> dict[str, str]:
    """The ids as event `context` entries: `orchestrator_id.<key>`."""
    return {f"orchestrator_id.{k}": v for k, v in ids.items()}


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


def manifest_vars(plan: Plan | None, vars_: Mapping[str, Any]) -> dict[str, Any]:
    """The manifest's vars as the base and the explicit --vars on top; --vars alone without a plan."""
    return {**(plan.metadata.vars if plan else {}), **vars_}


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


def state_option(func: F) -> F:
    """Add --state DIR: the reference state for state: and result: selectors and for --defer."""
    return click.option(
        "--state",
        "state",
        type=click.Path(file_okay=False, path_type=Path),
        default=None,
        metavar="DIR",
        help="Folder of a reference manifest.json (and optionally run_results.json), for the "
        "state: and result: selectors. Relative to the project dir unless absolute. "
        "`compile --stable` or the artifacts root (target) both work.",
    )(func)


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
