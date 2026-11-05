"""The `pipeloom databricks` command group, added through the `pipeloom.commands` entry point."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import click

from pipeloom.api import Project, ProjectOptions
from pipeloom.cli._events import Invocation
from pipeloom.cli._options import log_format_option, plugin_option, project_options, target_option
from pipeloom.diagnostics import Report
from pipeloom.engine.types import StateStoreCapabilities
from pipeloom.runtime.run import DEFAULT_CANCEL_GRACE
from pipeloom_databricks.bundle import Runtime, find_bundle_root, relative_to_root, render_bundle


@click.group()
def databricks() -> None:
    """Databricks jobs for a pipeloom project."""


@databricks.command()
@project_options
@target_option
@plugin_option
@log_format_option
@click.option(
    "--out",
    type=click.Path(file_okay=False, path_type=Path),
    required=True,
    help="Folder for the generated files; it must lie inside the bundle.",
)
@click.option(
    "--pipeline",
    "pipelines",
    multiple=True,
    metavar="NAME",
    help="A pipeline of the deploy block. Repeatable. Default: all of them.",
)
@click.option(
    "--bundle-root",
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    default=None,
    help="The bundle's root folder. Default: the nearest folder above --out with databricks.yml.",
)
@click.option(
    "--check",
    is_flag=True,
    help="Write nothing; exit 1 and list the files that differ from what would be written.",
)
def bundle(
    project_dir: Path,
    vars_: dict[str, Any],
    target: str | None,
    plugins: tuple[str, ...],
    log_format: str,
    out: Path,
    pipelines: tuple[str, ...],
    bundle_root: Path | None,
    check: bool,
) -> None:
    """Write one job file and one manifest per pipeline into a bundle.

    The job is <out>/pipeloom_<project>_<pipeline>_<target>.job.yml and refers to the project and
    the manifest through ${workspace.file_path}. Exit codes: 0 ok, 1 a diagnostic or a changed
    file with --check, 2 bad usage.
    """
    root = find_bundle_root(out, bundle_root)
    if root is None:
        raise click.UsageError("no databricks.yml above --out; pass --bundle-root")
    project_rel = relative_to_root(project_dir, root)
    out_rel = relative_to_root(out, root)
    if project_rel is None or out_rel is None:
        raise click.UsageError(f"the project dir and --out must lie inside the bundle {root}")

    inv = Invocation("databricks bundle", log_format)
    project = Project(
        ProjectOptions(
            project_dir=project_dir, target=target, vars=vars_, plugins=plugins, env=os.environ
        ),
        invocation_id=inv.invocation_id,
    )
    compiled = project.compile()
    bound = project.bind().value
    if compiled.value is None or bound is None:
        inv.finish(compiled.diagnostics, 1, bound=bound)
        return
    store = bound.plugins.get("state_stores")
    caps = None if store is None else store.spec.capabilities
    grace = bound.runtime.cancel_grace
    runtime = Runtime(
        artifacts_root=bound.runtime.artifacts.root,
        state_shared=isinstance(caps, StateStoreCapabilities) and caps.shared,
        cancel_grace=int(DEFAULT_CANCEL_GRACE if grace is None else grace),
    )
    report = Report()
    report.extend(compiled.diagnostics)
    files = render_bundle(
        compiled.value,
        runtime,
        pipelines,
        out=out,
        project_rel=project_rel,
        out_rel=out_rel,
        report=report,
    )
    diagnostics = report.sorted()
    if files is None:
        inv.finish(diagnostics, 1, bound=bound)
        return
    changed = [f for f in files if not f.path.is_file() or f.path.read_bytes() != f.content]
    if check:
        for f in changed:
            click.echo(f.path.as_posix())
        inv.finish(diagnostics, 1 if changed else 0, bound=bound)
        return
    for f in files:
        f.path.parent.mkdir(parents=True, exist_ok=True)
        f.path.write_bytes(f.content)
        click.echo(f.path.as_posix())
    inv.finish(diagnostics, 0, bound=bound)
