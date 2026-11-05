"""The `pipeloom databricks` command group, added through the `pipeloom.commands` entry point."""

from __future__ import annotations

import json
import os
import re
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Final

import click

from pipeloom.api import Project, ProjectOptions
from pipeloom.cli._events import Invocation
from pipeloom.cli._options import (
    log_format_option,
    plugin_option,
    project_options,
    select_options,
    target_option,
)
from pipeloom.config.bind import BoundProject
from pipeloom.diagnostics import Report
from pipeloom.engine.types import StateStoreCapabilities
from pipeloom.graph.selectors import Selection
from pipeloom.plan.model import Plan
from pipeloom.runtime.run import DEFAULT_CANCEL_GRACE
from pipeloom_databricks.bundle import Runtime, find_bundle_root, relative_to_root, render_bundle
from pipeloom_databricks.client import (
    ConnectionNotConfigured,
    DatabricksClient,
    connect,
)
from pipeloom_databricks.deploy import api_errors, find_job, prepare
from pipeloom_databricks.deploy import deploy as deploy_pipeline
from pipeloom_databricks.jobs_model import to_json
from pipeloom_databricks.options import DatabricksDeployOptions
from pipeloom_databricks.render import deploy_options, job_name
from pipeloom_databricks.repair import repair as repair_run
from pipeloom_databricks.run import (
    SelectionError,
    WaitTimeout,
    effective_run_id,
    failure_lines,
    find_run,
    run_job,
    submit_ephemeral,
    wait_for_run,
)

CONNECTION_KEY: Final = "pipeloom_databricks.connection"
SLEEP_KEY: Final = "pipeloom_databricks.sleep"
DEFAULT_TIMEOUT: Final = 24 * 3600
_RUN_ID = re.compile(r"^[A-Za-z0-9._-]{1,64}$")


@dataclass
class Hooks:
    """What a test hands to the command group as `obj`: a stand-in for `time.sleep`."""

    sleep: Callable[[float], None] = time.sleep


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
    ctx.meta[SLEEP_KEY] = ctx.obj.sleep if isinstance(ctx.obj, Hooks) else time.sleep


def _connect(
    ctx: click.Context, inv: Invocation, report: Report, bound: BoundProject | None
) -> DatabricksClient:
    """The client the group options select; ends the command with PLD010 (exit 1) when none."""
    profile, factory = ctx.meta.get(CONNECTION_KEY, (None, None))
    try:
        return connect(profile=profile, env=os.environ, factory=factory)
    except ConnectionNotConfigured as err:
        report.add("PLD010", str(err))
        inv.finish(report.sorted(), 1, bound=bound)
        raise SystemExit(1) from None


def _pipeline(name: str | None, opts: DatabricksDeployOptions) -> str:
    """The pipeline to act on: `name`, or the only one of the deploy block. Exit 2 otherwise."""
    if name is None:
        if len(opts.pipelines) != 1:
            raise click.UsageError(
                f"--pipeline is required: the deploy block has {', '.join(sorted(opts.pipelines))}"
            )
        return next(iter(opts.pipelines))
    if name not in opts.pipelines:
        raise click.UsageError(
            f"pipeline '{name}' is not in the deploy block ({', '.join(sorted(opts.pipelines))})"
        )
    return name


def _run_id(ctx: click.Context, param: click.Parameter, value: str | None) -> str | None:
    if value is not None and not _RUN_ID.match(value):
        raise click.BadParameter(
            "use 1 to 64 letters, digits, dots, underscores or hyphens", ctx=ctx, param=param
        )
    return value


def _echo_started(job_id: int | None, job_run_id: int, run_id: str | None, url: str | None) -> None:
    doc = {"job_id": job_id, "job_run_id": job_run_id, "run_id": run_id, "run_page_url": url}
    click.echo(json.dumps(doc, ensure_ascii=True))


def _follow(
    ctx: click.Context,
    client: DatabricksClient,
    job_run_id: int,
    run_id: str | None,
    runtime: Runtime,
    report: Report,
    *,
    repair_id: int | None = None,
    timeout: int = DEFAULT_TIMEOUT,
) -> int:
    """Wait for the run, say what happened to its tasks and units; the exit code of the command."""
    say: Callable[[str], None] = lambda line: click.echo(line, err=True)
    code = 1
    with api_errors(report):
        try:
            run = wait_for_run(
                client,
                job_run_id,
                sleep=ctx.meta[SLEEP_KEY],
                say=say,
                repair_id=repair_id,
                timeout=timeout,
            )
        except WaitTimeout as err:
            say(f"{err}; the run keeps going")
            return 1
        if run.result_state == "SUCCESS":
            code = 0
        else:
            say(f"run {job_run_id} ended: {run.life_cycle_state}/{run.result_state}")
            for line in failure_lines(
                client, runtime.artifacts_root, run_id or effective_run_id(run)
            ):
                say(line)
    return code


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
        client = _connect(ctx, inv, report, bound)
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


_PIPELINE_OPTION = click.option(
    "--pipeline",
    metavar="NAME",
    default=None,
    help="A pipeline of the deploy block. Default: the only one.",
)


def _wait_options(func: Callable[..., Any]) -> Callable[..., Any]:
    """--wait/--no-wait and --timeout, shared by run and repair."""
    func = click.option(
        "--timeout",
        type=click.IntRange(min=1),
        default=DEFAULT_TIMEOUT,
        show_default=True,
        metavar="SECONDS",
        help="With --wait, stop waiting after this long and exit 1. The run keeps going.",
    )(func)
    return click.option(
        "--wait/--no-wait",
        default=True,
        show_default=True,
        help="Wait for the run to end (polling every 15 seconds) and exit 1 unless it succeeded.",
    )(func)


@databricks.command()
@project_options
@target_option
@plugin_option
@log_format_option
@select_options
@_PIPELINE_OPTION
@click.option(
    "--run-id",
    callback=_run_id,
    default=None,
    metavar="RID",
    help="Name the run (1 to 64 letters, digits, dots, underscores or hyphens) and make the "
    "start idempotent. Default: dbx-<job run id>.",
)
@click.option("--full-refresh", is_flag=True, help="Rebuild append tables from scratch.")
@click.option(
    "--ensure",
    is_flag=True,
    help="Deploy the job first when it is missing or out of date, instead of stopping.",
)
@click.option(
    "--ephemeral",
    is_flag=True,
    help="Run once without a job: upload the project and submit the run. Serverless only.",
)
@_wait_options
@click.pass_context
def run(
    ctx: click.Context,
    /,
    project_dir: Path,
    vars_: dict[str, Any],
    target: str | None,
    plugins: tuple[str, ...],
    log_format: str,
    select: tuple[str, ...],
    exclude: tuple[str, ...],
    pipeline: str | None,
    run_id: str | None,
    full_refresh: bool,
    ensure: bool,
    ephemeral: bool,
    wait: bool,
    timeout: int,
) -> None:
    """Run the deployed job of a pipeline, or run it once without a job (--ephemeral).

    Prints {job_id, job_run_id, run_id, run_page_url} as one JSON line. --select and --exclude
    pick units: only their tasks (and finalize) run. Exit codes: 0 ok, 1 a diagnostic or a run
    that did not succeed, 2 bad usage.
    """
    inv = Invocation("databricks run", log_format)
    plan, bound, runtime, report = _load(inv, project_dir, target, vars_, plugins)
    opts = deploy_options(plan, report)
    if opts is None:
        inv.finish(report.sorted(), 1, bound=bound)
        return
    name = _pipeline(pipeline, opts)
    if ephemeral and ensure:
        raise click.UsageError("--ensure deploys a job; --ephemeral runs without one")
    client = _connect(ctx, inv, report, bound)
    selection = Selection(select, exclude)
    try:
        if ephemeral:
            started = submit_ephemeral(
                client,
                project_dir,
                plan,
                name,
                runtime,
                selection=selection,
                full_refresh=full_refresh,
                report=report,
                run_id=run_id,
            )
        else:
            started = run_job(
                client,
                project_dir,
                plan,
                name,
                runtime,
                selection=selection,
                run_id=run_id,
                full_refresh=full_refresh,
                ensure=ensure,
                report=report,
            )
    except SelectionError as err:
        raise click.UsageError(str(err)) from err
    code = 1
    if started is not None:
        shown = started.run_id or f"dbx-{started.job_run_id}"
        _echo_started(started.job_id, started.job_run_id, shown, started.run_page_url)
        code = 0
        if wait:
            code = _follow(
                ctx, client, started.job_run_id, started.run_id, runtime, report, timeout=timeout
            )
    inv.finish(report.sorted(), 1 if report.has_errors else code, bound=bound)


@databricks.command()
@project_options
@target_option
@plugin_option
@log_format_option
@_PIPELINE_OPTION
@click.option("--run-id", callback=_run_id, default=None, metavar="RID", help="The run to repair.")
@click.option(
    "--job-run-id",
    type=click.IntRange(min=1),
    default=None,
    metavar="N",
    help="The job run to repair.",
)
@_wait_options
@click.pass_context
def repair(
    ctx: click.Context,
    /,
    project_dir: Path,
    vars_: dict[str, Any],
    target: str | None,
    plugins: tuple[str, ...],
    log_format: str,
    pipeline: str | None,
    run_id: str | None,
    job_run_id: int | None,
    wait: bool,
    timeout: int,
) -> None:
    """Rerun the failed, cancelled and skipped tasks of a finished run, and finalize again.

    Name the run with --run-id (the pipeloom run id) or --job-run-id (the Databricks run id),
    exactly one. Prints {job_run_id, repair_id} as one JSON line. Exit codes: 0 ok (also when
    there is nothing to repair), 1 a diagnostic or a repair that did not succeed, 2 bad usage.
    """
    if (run_id is None) == (job_run_id is None):
        raise click.UsageError("use exactly one of --run-id and --job-run-id")
    inv = Invocation("databricks repair", log_format)
    plan, bound, runtime, report = _load(inv, project_dir, target, vars_, plugins)
    opts = deploy_options(plan, report)
    if opts is None:
        inv.finish(report.sorted(), 1, bound=bound)
        return
    name = _pipeline(pipeline, opts)
    client = _connect(ctx, inv, report, bound)
    target_run = job_run_id
    if run_id is not None:
        with api_errors(report):
            meta = plan.metadata
            job = find_job(
                client, f"{meta.project}.{name}.{meta.target}", name=job_name(plan, name)
            )
            found = None if job is None else find_run(client, job.job_id, run_id)
            if found is None:
                report.add(
                    "PLD007",
                    f"no run '{run_id}' of pipeline '{name}'"
                    if job is not None
                    else f"no job is deployed for pipeline '{name}'",
                    hint="check --run-id, --pipeline and --target",
                )
            else:
                target_run = found.run_id
    code = 1
    if target_run is not None and not report.has_errors:
        repair_id = repair_run(client, target_run, report)
        if repair_id is not None:
            click.echo(
                json.dumps({"job_run_id": target_run, "repair_id": repair_id}, ensure_ascii=True)
            )
            code = 0
            if wait:
                code = _follow(
                    ctx,
                    client,
                    target_run,
                    run_id,
                    runtime,
                    report,
                    repair_id=repair_id,
                    timeout=timeout,
                )
        elif not report.has_errors:
            click.echo(f"nothing to repair in run {target_run}", err=True)
            code = 0
    inv.finish(report.sorted(), 1 if report.has_errors else code, bound=bound)
