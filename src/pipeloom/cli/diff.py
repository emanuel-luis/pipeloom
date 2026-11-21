"""The diff command: compare one model's relation in a reference with this target (ADR-0053)."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import click

from pipeloom.api import Project, ProjectOptions
from pipeloom.artifacts.table_diff import render_markdown, table_diff_dict
from pipeloom.cli._events import Invocation
from pipeloom.cli._options import log_format_option, plugin_option, project_options, target_option


@click.command()
@click.argument("model")
@project_options
@target_option
@plugin_option
@click.option(
    "--state",
    "state",
    type=click.Path(file_okay=False, path_type=Path),
    required=True,
    metavar="DIR",
    help="Folder of the reference manifest.json, whose relation of MODEL is the source side. "
    "Relative to the project dir unless absolute.",
)
@click.option(
    "--key",
    "key",
    multiple=True,
    metavar="COLUMN",
    help="Key column; repeatable. Default: the model's write.unique_key.",
)
@click.option(
    "--samples",
    is_flag=True,
    help="Add up to 10 rows per kind of difference. Columns tagged sensitive are masked.",
)
@click.option(
    "--output",
    type=click.Choice(["markdown", "json"]),
    default="markdown",
    show_default=True,
    help="Markdown for a pull request comment, or the pipeloom.table_diff/1 document.",
)
@log_format_option
def diff(
    model: str,
    project_dir: Path,
    vars_: dict[str, Any],
    target: str | None,
    plugins: tuple[str, ...],
    state: Path,
    key: tuple[str, ...],
    samples: bool,
    output: str,
    log_format: str,
) -> None:
    """Compare MODEL's relation in the --state reference with its relation on this target.

    Rows are matched by key and compared column by column, nulls equal to nulls. Without
    --samples the output holds counts and the schema diff, never a cell value. Exit codes: 0 no
    differences, 1 differences or a failure, 2 usage error.
    """
    opts = ProjectOptions(
        project_dir=project_dir, target=target, vars=vars_, plugins=plugins, env=os.environ
    )
    inv = Invocation("diff", log_format)
    project = Project(opts, invocation_id=inv.invocation_id)
    outcome = project.diff(model, state, key=key, samples=samples)
    if outcome.diff is not None:
        if output == "json":
            click.echo(json.dumps(table_diff_dict(outcome.diff), indent=2, ensure_ascii=False))
        else:
            click.echo(render_markdown(outcome.diff), nl=False)
    inv.finish(outcome.diagnostics, outcome.exit_code, bound=project.bind().value)
