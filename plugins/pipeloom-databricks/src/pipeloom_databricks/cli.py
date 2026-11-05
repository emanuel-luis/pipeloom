"""The `pipeloom databricks` command group, added through the `pipeloom.commands` entry point."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Final

import click

from pipeloom.api import Project, ProjectOptions
from pipeloom.cli._events import Invocation
from pipeloom.cli._options import log_format_option, plugin_option, project_options, target_option
from pipeloom.config.bind import BoundProject
from pipeloom.diagnostics import Report
from pipeloom.engine.types import StateStoreCapabilities
from pipeloom.plan.model import Plan
from pipeloom.runtime.run import DEFAULT_CANCEL_GRACE
from pipeloom_databricks.bundle import Runtime, find_bundle_root, relative_to_root, render_bundle
from pipeloom_databricks.client import ConnectionNotConfigured, connect
from pipeloom_databricks.deploy import deploy as deploy_pipeline
from pipeloom_databricks.deploy import prepare
from pipeloom_databricks.jobs_model import to_json
from pipeloom_databricks.render import deploy_options

CONNECTION_KEY: Final = "pipeloom_databricks.connection"


def _load(
    inv: Invocation,
    project_dir: Path,
    target: str | None,
    vars_: dict[str, Any],
    plugins: tuple[str, ...],
) -> tuple[Plan, BoundProject, Runtime, Report]:
    """Compile and bind the project; ends the command with exit 1 when that fails."""
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
        raise SystemExit(1)
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
    return compiled.value, bound, runtime, report


@click.group()
@click.option(
    "--profile",
    metavar="NAME",
    default=None,
    help="The Databricks profile to use. Without it, DATABRICKS_CONFIG_PROFILE, then "
    "DATABRICKS_HOST, then the runtime; pipeloom never picks a profile by itself.",
)
@click.option("--client-factory", hidden=True, default=None, help="MODULE:ATTR of a test client.")
@click.pass_context
def databricks(ctx: click.Context, profile: str | None, client_factory: str | None) -> None:
    """Databricks jobs for a pipeloom project."""
    ctx.meta[CONNECTION_KEY] = (profile, client_factory)


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
    loaded = _load(inv, project_dir, target, vars_, plugins)
    plan, bound, runtime, report = loaded
    files = render_bundle(
        plan,
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


@databricks.command()
@project_options
@target_option
@plugin_option
@log_format_option
@click.option(
    "--pipeline",
    "pipelines",
    multiple=True,
    metavar="NAME",
    help="A pipeline of the deploy block. Repeatable. Default: all of them.",
)
@click.option(
    "--dry-run",
    is_flag=True,
    help="Print the job settings as JSON and connect to nothing.",
)
@click.pass_context
def deploy(
    ctx: click.Context,
    /,
    project_dir: Path,
    vars_: dict[str, Any],
    target: str | None,
    plugins: tuple[str, ...],
    log_format: str,
    pipelines: tuple[str, ...],
    dry_run: bool,
) -> None:
    """Upload the project to the workspace and create or update one job per pipeline.

    Prints one JSON line per pipeline: {pipeline, job_id, action, root}. With --dry-run it prints
    {pipeline, root, settings} instead and creates no client. Exit codes: 0 ok, 1 a diagnostic.
    """
    inv = Invocation("databricks deploy", log_format)
    plan, bound, runtime, report = _load(inv, project_dir, target, vars_, plugins)
    opts = deploy_options(plan, report)
    names = pipelines or (sorted(opts.pipelines) if opts else ())
    for name in names:
        if opts is not None and name not in opts.pipelines:
            report.add("PLD004", f"pipeline '{name}' is not in the deploy block")
    if opts is None or report.has_errors:
        inv.finish(report.sorted(), 1, bound=bound)
        return
    if dry_run:
        for name in names:
            prepared = prepare(project_dir, plan, name, runtime, report, user=lambda: "<user>")
            if prepared is not None:
                doc: dict[str, Any] = {
                    "pipeline": name,
                    "root": prepared.root,
                    "settings": to_json(prepared.job.settings),
                }
                click.echo(json.dumps(doc, ensure_ascii=True))
    else:
        profile, factory = ctx.meta.get(CONNECTION_KEY, (None, None))
        try:
            client = connect(profile=profile, env=os.environ, factory=factory)
        except ConnectionNotConfigured as err:
            report.add("PLD010", str(err))
            inv.finish(report.sorted(), 1, bound=bound)
            return
        for name in names:
            result = deploy_pipeline(client, project_dir, plan, name, runtime, report=report)
            if result is not None:
                doc = {
                    "pipeline": name,
                    "job_id": result.job_id,
                    "action": result.action,
                    "root": result.root,
                }
                click.echo(json.dumps(doc, ensure_ascii=True))
    inv.finish(report.sorted(), 1 if report.has_errors else 0, bound=bound)
