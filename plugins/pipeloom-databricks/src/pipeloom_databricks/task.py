"""The wrapper that every task of a rendered job runs (`python_wheel_task`, ADR-0035).

It resolves what Databricks substituted into the parameters, calls the pipeloom CLI in this
process (so a Spark session that already exists stays the active one), leaves a task value
for the job and exits with the core's exit code. Parameters are `--opt=value`, so an empty
value stays one argument.

Importing this module pulls in click and the pipeloom core, and nothing else.
"""

from __future__ import annotations

import json
import os
import re
import tempfile
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Literal, NoReturn

import click

from pipeloom.diagnostics import Diagnostic, Report, format_diagnostic
from pipeloom.plan.manifest import read_manifest
from pipeloom.plan.model import Plan
from pipeloom_databricks.client import DatabricksClient, connect
from pipeloom_databricks.finalize_states import unit_states, unit_states_json
from pipeloom_databricks.grouping import build_tasks, pipeline_units, task_key_map
from pipeloom_databricks.options import DatabricksDeployOptions
from pipeloom_databricks.render import deploy_options
from pipeloom_databricks.task_values import (
    TASK_VALUE_KEY,
    TaskValues,
    runtime_task_values,
    summary,
)

_RUN_ID = re.compile(r"^[A-Za-z0-9._-]{1,64}$")
_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


class _Context:
    """What `main` hands to the commands: a stand-in for the platform's task values."""

    def __init__(
        self,
        values: TaskValues | None,
        client_factory: Callable[[], DatabricksClient] | None = None,
    ) -> None:
        self.values = values
        self.client_factory = client_factory


def _show(diagnostics: Sequence[Diagnostic]) -> None:
    """Diagnostics on stderr: text, or one JSON object per line like `--log-format json`."""
    as_json = os.environ.get("PIPELOOM_LOG_FORMAT") == "json"
    for d in diagnostics:
        if as_json:
            click.echo(json.dumps(d.to_json(), ensure_ascii=True), err=True)
        else:
            click.echo(format_diagnostic(d), err=True)


def _stop(report: Report) -> NoReturn:
    """Show what the report found and exit 1."""
    _show(report.sorted())
    raise SystemExit(1)


def check_resolved(values: Mapping[str, str]) -> None:
    """Exit 1 with PLD013 for every parameter that still holds a `{{...}}` reference."""
    report = Report()
    for name, value in sorted(values.items()):
        if "{{" in value:
            report.add(
                "PLD013",
                f"--{name} was not resolved by Databricks: {value}",
                hint="the job or its runtime did not substitute the reference; "
                "check the job parameters and the runtime version",
                context={"parameter": name},
            )
    if report.has_errors:
        _stop(report)


def resolve_run_id(param: str, job_run_id: str | None) -> str:
    """The run id: the `pipeloom_run_id` job parameter, or `dbx-<job run id>` when it is empty."""
    if param == "":
        if not job_run_id:
            raise click.UsageError("--run-id is empty and --job-run-id is missing")
        param = f"dbx-{job_run_id}"
    if not _RUN_ID.match(param):
        raise click.BadParameter(
            "use 1 to 64 letters, digits, dots, underscores or hyphens", param_hint="--run-id"
        )
    return param


def started_at(start_ms: str) -> str:
    """The job start, `{{job.start_time.timestamp_ms}}`, as a UTC ISO 8601 timestamp."""
    try:
        ms = int(start_ms)
    except ValueError:
        raise click.BadParameter(
            f"'{start_ms}' is not an integer", param_hint="--start-ms"
        ) from None
    return (_EPOCH + timedelta(milliseconds=ms)).isoformat(timespec="milliseconds")


def _full_refresh(value: str) -> bool:
    if value.lower() in ("", "false"):
        return False
    if value.lower() == "true":
        return True
    raise click.BadParameter(f"'{value}' is not true or false", param_hint="--full-refresh")


def core_argv(
    command: Literal["run", "finalize"],
    *,
    project_dir: str,
    target: str,
    manifest: str,
    plan_hash: str,
    run_id: str,
    run_started_at: str,
    full_refresh: bool = False,
    select: Sequence[str] = (),
    exclude: Sequence[str] = (),
    ids: Mapping[str, str] | None = None,
    unit_states: str | None = None,
) -> list[str]:
    """The arguments of the core command `pipeloom run` or `pipeloom finalize`."""
    argv = [
        command,
        f"--project-dir={project_dir}",
        f"--target={target}",
        f"--manifest={manifest}",
        f"--expect-plan-hash={plan_hash}",
        f"--run-id={run_id}",
        f"--run-started-at={run_started_at}",
    ]
    if command == "run":
        argv.append(f"--full-refresh={'true' if full_refresh else 'false'}")
    argv += [f"--select={s}" for s in select]
    argv += [f"--exclude={s}" for s in exclude]
    argv += [f"--orchestrator-id=databricks_{k}={v}" for k, v in (ids or {}).items() if v]
    if unit_states is not None:
        argv.append(f"--unit-states={unit_states}")
    return argv


def _code(raw: object) -> int:
    if raw is None:
        return 0
    if isinstance(raw, int):
        return raw
    click.echo(str(raw), err=True)
    return 1


def _call_core(argv: Sequence[str]) -> int:
    """Run the pipeloom CLI in this process; its exit code, never an exception."""
    from pipeloom.cli import main as core

    try:
        result = core.main(args=list(argv), prog_name="pipeloom", standalone_mode=False)
    except SystemExit as stop:
        return _code(stop.code)
    except click.ClickException as err:
        err.show()
        return err.exit_code
    except click.Abort:
        click.echo("Aborted!", err=True)
        return 1
    return result if isinstance(result, int) else 0


def _deploy(manifest: str, pipeline: str) -> tuple[Plan, DatabricksDeployOptions]:
    """The plan of the shipped manifest and the pipeline's options, or exit 1."""
    report = Report()
    plan = read_manifest(Path(manifest), report)
    opts = None if plan is None else deploy_options(plan, report)
    if plan is None or opts is None:
        _stop(report)
    if pipeline not in opts.pipelines:
        report.add("PLD004", f"pipeline '{pipeline}' is not in the deploy block")
        _stop(report)
    return plan, opts


def _members(manifest: str, pipeline: str, group: str) -> tuple[str, ...]:
    """The units of a group task, recomputed from the manifest's deploy options."""
    plan, opts = _deploy(manifest, pipeline)
    report = Report()
    tasks = build_tasks(plan, opts, pipeline, report)
    if report.has_errors:
        _stop(report)
    for task in tasks:
        if task.key == group:
            return task.units
    click.echo(f"error: pipeline '{pipeline}' has no task group '{group}'", err=True)
    raise SystemExit(1)


def _attempts(opts: Mapping[str, str], run_id: str) -> dict[str, dict[str, Any]]:
    """The newest attempt file of every unit of the run, read from the artifacts root."""
    from pipeloom.api import Project, ProjectOptions
    from pipeloom.artifacts.finalize import latest_attempts
    from pipeloom.artifacts.layout import RunPaths
    from pipeloom.runtime.run import artifacts_root

    bound = (
        Project(
            ProjectOptions(
                project_dir=Path(opts["project_dir"]), target=opts["target"], env=os.environ
            )
        )
        .bind()
        .value
    )
    if bound is None:
        raise RuntimeError("the project could not be bound")
    root = artifacts_root(bound, bound.loaded.project_dir.resolve())
    return latest_attempts(RunPaths(root, run_id))


def _default_client() -> DatabricksClient:
    return connect(profile=None, env=os.environ)


def _run_start(ctx: click.Context, given: Mapping[str, str]) -> str:
    """The start of the job run from the Jobs API, else `--start-ms`.

    `{{job.start_time.timestamp_ms}}` is the start of the latest repair, not of the run, so a
    repaired task would pass a start the run did not record (PLX108). Best effort: on any client
    error one warning goes to stderr and `--start-ms` is used. A one-time run has no job run id
    and keeps `--start-ms`, which the renderer writes literally.
    """
    fallback = started_at(given["start_ms"])
    job_run_id = given["job_run_id"]
    if not job_run_id:
        return fallback
    factory = ctx.obj.client_factory if isinstance(ctx.obj, _Context) else None
    try:
        start_ms = (factory or _default_client)().get_run(int(job_run_id)).start_time_ms
    except Exception as exc:  # noqa: BLE001 - the flag is a usable fallback
        click.echo(
            f"warning: run start not read, using --start-ms: {type(exc).__name__}: {exc}",
            err=True,
        )
        return fallback
    return fallback if start_ms is None else started_at(str(start_ms))


def _states_file(
    ctx: click.Context,
    opts: Mapping[str, str],
    run_id: str,
    key_map: Mapping[str, str],
    folder: Path,
) -> str | None:
    """Write the unit states the Jobs API reports for the run; the path, or None for no states.

    A unit whose task died wrote no attempt file, and only the Jobs API knows how its task ended.
    Best effort: on any client error one warning goes to stderr and finalize runs without states,
    so those units count as not run. A one-time run has no job run id to ask about.
    """
    job_run_id = opts["job_run_id"]
    if not job_run_id:
        return None
    factory = ctx.obj.client_factory if isinstance(ctx.obj, _Context) else None
    try:
        client = (factory or _default_client)()
        run = client.get_run(int(job_run_id))
        states = unit_states(key_map, run, set(_attempts(opts, run_id)))
    except Exception as exc:  # noqa: BLE001 - task states are best effort
        click.echo(
            f"warning: task states not read, units without a result count as not run: "
            f"{type(exc).__name__}: {exc}",
            err=True,
        )
        return None
    if not states:
        return None
    path = folder / "unit_states.json"
    path.write_bytes(unit_states_json(states))
    return path.as_posix()


def _publish(
    values: TaskValues | None,
    opts: Mapping[str, str],
    run_id: str,
    members: Sequence[str],
    code: int,
) -> None:
    """Leave the summary of the members as the task value. Best effort: never changes the exit."""
    if values is None:
        return
    try:
        attempts = _attempts(opts, run_id)
        rows = [attempts[u] for u in members if u in attempts]
        status = {0: "succeeded", 130: "cancelled"}.get(code, "failed")
        values.set(TASK_VALUE_KEY, summary(run_id, status, rows))
    except Exception as exc:  # noqa: BLE001 - task values are best effort
        click.echo(f"warning: task value not set: {type(exc).__name__}: {exc}", err=True)


def _common(func: Callable[..., Any]) -> Callable[..., Any]:
    """The options both commands share, in the order the renderer writes them."""
    for flag, required in (
        ("start-ms", True),
        ("job-run-id", False),
        ("job-id", False),
        ("full-refresh", False),
        ("run-id", False),
        ("plan-hash", True),
        ("pipeline", True),
        ("target", True),
        ("manifest", True),
        ("project-dir", True),
    ):
        func = click.option(
            f"--{flag}",
            required=required,
            default=None if required else "",
            help="Set by the rendered job.",
        )(func)
    return func


@click.group(help="Run pipeloom units inside a Databricks job. Used by rendered jobs.")
def cli() -> None:
    pass


@cli.command("unit")
@_common
@click.option("--task-run-id", default="", help="Set by the rendered job.")
@click.option("--task-key", default="", help="Set by the rendered job.")
@click.option("--group", default=None, help="The task group to run (needs task_grouping).")
@click.option("--unit", "unit_id", default=None, help="The unit to run.")
@click.pass_context
def unit_command(
    ctx: click.Context,
    /,
    unit_id: str | None,
    group: str | None,
    task_key: str,
    task_run_id: str,
    **given: str,
) -> None:
    """Run one unit, or every unit of a group, and leave a task value."""
    if (unit_id is None) == (group is None):
        raise click.UsageError("use exactly one of --unit and --group")
    named = {k.replace("_", "-"): v for k, v in given.items()}
    named.update({"task-key": task_key, "task-run-id": task_run_id})
    named.update({"unit": unit_id or "", "group": group or ""})
    check_resolved(named)
    run_id = resolve_run_id(given["run_id"], given["job_run_id"])
    start = _run_start(ctx, given)
    refresh = _full_refresh(given["full_refresh"])
    members = (
        (unit_id,)
        if unit_id is not None
        else _members(given["manifest"], given["pipeline"], group or "")
    )
    argv = core_argv(
        "run",
        project_dir=given["project_dir"],
        target=given["target"],
        manifest=given["manifest"],
        plan_hash=given["plan_hash"],
        run_id=run_id,
        run_started_at=start,
        full_refresh=refresh,
        select=[f"unit:{m}" for m in members],
        ids={
            "job_id": given["job_id"],
            "job_run_id": given["job_run_id"],
            "task_key": task_key,
            "task_run_id": task_run_id,
        },
    )
    code = _call_core(argv)
    values = ctx.obj.values if isinstance(ctx.obj, _Context) else runtime_task_values()
    _publish(values, given, run_id, members, code)
    raise SystemExit(code)


@cli.command("finalize")
@_common
@click.pass_context
def finalize_command(ctx: click.Context, /, **given: str) -> None:
    """Close the run: merge the unit results of the pipeline into run_results.json.

    A unit that wrote no attempt file takes the state of its task from the Jobs API: a task that
    failed, timed out or was cancelled fails or cancels the run.
    """
    check_resolved({k.replace("_", "-"): v for k, v in given.items()})
    run_id = resolve_run_id(given["run_id"], given["job_run_id"])
    start = _run_start(ctx, given)
    plan, opts = _deploy(given["manifest"], given["pipeline"])
    report = Report()
    units = pipeline_units(plan, opts, given["pipeline"], report)
    if report.has_errors:
        _stop(report)
    key_map = task_key_map(plan, opts, given["pipeline"])
    with tempfile.TemporaryDirectory(prefix="pipeloom-states-") as folder:
        states = _states_file(ctx, given, run_id, key_map, Path(folder))
        argv = core_argv(
            "finalize",
            project_dir=given["project_dir"],
            target=given["target"],
            manifest=given["manifest"],
            plan_hash=given["plan_hash"],
            run_id=run_id,
            run_started_at=start,
            # The shipped manifest has an empty selection, so the scope is spelled out unit by unit.
            select=[f"unit:{u}" for u in units],
            ids={"job_id": given["job_id"], "job_run_id": given["job_run_id"]},
            unit_states=states,
        )
        code = _call_core(argv)
    raise SystemExit(code)


def main(
    argv: Sequence[str] | None = None,
    *,
    task_values: TaskValues | None = None,
    client_factory: Callable[[], DatabricksClient] | None = None,
) -> None:
    """The entry point. `argv` defaults to `sys.argv[1:]`.

    Returns on success and raises `SystemExit` with a non-zero code on failure. Serverless runs
    the wheel entry point under IPython, which reports even `SystemExit(0)` as a failed task.

    `task_values` replaces the platform's task values and `client_factory` the connection that
    finalize reads task states through (a test passes a recorder and a fake client).
    """
    given = task_values is not None or client_factory is not None
    try:
        cli.main(
            args=None if argv is None else list(argv),
            prog_name="pipeloom_databricks_task",
            obj=_Context(task_values, client_factory) if given else None,
        )
    except SystemExit as stop:
        if stop.code not in (None, 0):
            raise
