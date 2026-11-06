"""The runs commands: list past runs and show one, from the state store or the artifacts root."""

from __future__ import annotations

import json
import os
from collections.abc import Callable, Mapping, Sequence
from datetime import datetime
from pathlib import Path
from typing import Any, TypeVar

import click

from pipeloom.api import Project, ProjectOptions
from pipeloom.artifacts.results import RESULTS_SCHEMA
from pipeloom.cli._events import Invocation
from pipeloom.cli._options import (
    log_format_option,
    plugin_option,
    project_options,
    target_option,
)
from pipeloom.config.bind import BoundProject
from pipeloom.diagnostics import Diagnostic, Report
from pipeloom.runtime.run import artifacts_root
from pipeloom.secrets.resolve import SecretError, secret_diagnostic
from pipeloom.state import codec
from pipeloom.state.types import (
    ListRunsUnsupported,
    RunRecord,
    UnitRecord,
    UnknownRun,
    list_runs,
    page_runs,
)

F = TypeVar("F", bound=Callable[..., Any])


def _common(fn: F) -> F:
    for option in (
        click.option(
            "--output",
            type=click.Choice(["text", "json"]),
            default="text",
            show_default=True,
            help="Tab-separated lines, or JSON.",
        ),
        click.option(
            "--artifacts",
            is_flag=True,
            help="Read runs/*/run_results.json under the artifacts root instead of the state store.",
        ),
        log_format_option,
        plugin_option,
        target_option,
        project_options,
    ):
        fn = option(fn)
    return fn


def _not_found(run_id: str) -> Diagnostic:
    return Report().add(
        "PLX118",
        f"run {run_id} not found",
        hint="check --target, or list the runs with pipeloom runs ls",
    )


def _artifact_runs(
    bound: BoundProject, project_dir: Path
) -> dict[str, tuple[RunRecord, list[dict[str, Any]]]]:
    """The finished runs of the bound project and target under the artifacts root, with their
    result rows. Folders without a readable run_results.json (running, crashed) are skipped."""
    project, target = bound.loaded.config.name, bound.target
    out: dict[str, tuple[RunRecord, list[dict[str, Any]]]] = {}
    for path in sorted((artifacts_root(bound, project_dir) / "runs").glob("*/run_results.json")):
        try:
            doc = json.loads(path.read_bytes().decode("utf-8"))
            if doc.get("schema") != RESULTS_SCHEMA:
                continue
            run = doc["run"]
            record = RunRecord(
                run_id=run["run_id"],
                project=run["project"],
                target=run["target"],
                plan_hash=run["plan_hash"],
                status=run["status"],
                started_at=datetime.fromisoformat(run["started_at"]),
                ended_at=codec.dt_load(run["ended_at"]),
                parent_run_id=run.get("parent_run_id"),
                invocation_ids=tuple(run.get("invocation_ids") or ()),
                counts=dict(run["counts"]),
            )
            rows = list(doc["results"])
        except (OSError, ValueError, KeyError, TypeError, AttributeError):
            continue
        if record.project == project and record.target == target:
            out[record.run_id] = (record, rows)
    return out


def _unit(run_id: str, row: Mapping[str, Any]) -> UnitRecord:
    return UnitRecord(
        run_id=run_id,
        unit_id=str(row["unit_id"]),
        attempt=int(row.get("attempt") or 0),
        status=str(row["status"]),
        invocation_id=str(row.get("invocation_id") or ""),
        started_at=codec.dt_load(row.get("started_at")),
        ended_at=codec.dt_load(row.get("ended_at")),
        data=dict(row),
    )


def _line(r: RunRecord) -> str:
    took = "-" if r.ended_at is None else f"{(r.ended_at - r.started_at).total_seconds():.1f}s"
    counts = " ".join(f"{k}={v}" for k, v in sorted(r.counts.items()) if v)
    return "\t".join((r.run_id, r.status, r.started_at.isoformat(), took, counts))


class _Context:
    """The invocation, the bound project and, for store reads, the state store."""

    def __init__(
        self,
        command: str,
        project_dir: Path,
        vars_: dict[str, Any],
        target: str | None,
        plugins: tuple[str, ...],
        log_format: str,
    ) -> None:
        self.inv = Invocation(command, log_format)
        self.project_dir = project_dir
        opts = ProjectOptions(
            project_dir=project_dir,
            target=target,
            vars=vars_,
            plugins=plugins,
            boundary=None,
            env=os.environ,
        )
        self.project = Project(opts, invocation_id=self.inv.invocation_id)
        outcome = self.project.bind()
        self.diagnostics: Sequence[Diagnostic] = outcome.diagnostics
        if outcome.value is None:
            self.inv.finish(self.diagnostics, 1)
        assert outcome.value is not None
        self.bound: BoundProject = outcome.value

    @property
    def names(self) -> tuple[str, str]:
        return self.bound.loaded.config.name, self.bound.target

    def fail(self, diagnostic: Diagnostic) -> None:
        self.inv.finish([*self.diagnostics, diagnostic], 1, bound=self.bound)

    def done(self) -> None:
        self.inv.finish(self.diagnostics, 0, bound=self.bound)

    def store(self) -> Any:
        try:
            return self.bound.plugins["state_stores"].create(self.project.secrets)
        except SecretError as err:
            self.fail(secret_diagnostic(err))


@click.group()
def runs() -> None:
    """List past runs and show one run (read only)."""


@runs.command("ls")
@_common
@click.option(
    "--limit",
    type=click.IntRange(min=1),
    default=20,
    show_default=True,
    help="Runs to list, newest first.",
)
@click.option(
    "--before",
    metavar="RUN_ID",
    default=None,
    help="List the runs older than this one: the next page after the last run shown.",
)
def ls_runs(
    project_dir: Path,
    vars_: dict[str, Any],
    target: str | None,
    plugins: tuple[str, ...],
    log_format: str,
    artifacts: bool,
    output: str,
    limit: int,
    before: str | None,
) -> None:
    """List the last runs of the target, newest first. Exit 1 for PLX117 and PLX118."""
    ctx = _Context("runs ls", project_dir, vars_, target, plugins, log_format)
    project, target_name = ctx.names
    try:
        if artifacts:
            found = [r for r, _ in _artifact_runs(ctx.bound, project_dir).values()]
            records = page_runs(found, project, target_name, limit, before)
        else:
            store = ctx.store()
            try:
                records = list_runs(store, project, target_name, limit, before)
            finally:
                store.close()
    except UnknownRun as err:
        ctx.fail(_not_found(err.run_id))
        return
    except ListRunsUnsupported as err:
        ctx.fail(Report().add("PLX117", str(err), hint="use --artifacts, or upgrade the plugin"))
        return
    if output == "json":
        click.echo(json.dumps([codec.run_to_json(r) for r in records], indent=2))
    else:
        for r in records:
            click.echo(_line(r))
    ctx.done()


@runs.command("show")
@_common
@click.argument("run_id")
def show(
    project_dir: Path,
    vars_: dict[str, Any],
    target: str | None,
    plugins: tuple[str, ...],
    log_format: str,
    artifacts: bool,
    output: str,
    run_id: str,
) -> None:
    """Show one run and its unit attempts. Exit 1 for PLX118."""
    ctx = _Context("runs show", project_dir, vars_, target, plugins, log_format)
    project, target_name = ctx.names
    record: RunRecord | None
    units: tuple[UnitRecord, ...] = ()
    if artifacts:
        hit = _artifact_runs(ctx.bound, project_dir).get(run_id)
        record = None if hit is None else hit[0]
        if hit is not None:
            units = tuple(_unit(run_id, row) for row in hit[1])
    else:
        store = ctx.store()
        try:
            record = store.get_run(run_id)
            if record is not None:
                units = tuple(store.list_units(run_id))
        finally:
            store.close()
    if record is None or (record.project, record.target) != (project, target_name):
        ctx.fail(_not_found(run_id))
        return
    if output == "json":
        doc = {
            "run": codec.run_to_json(record),
            "units": [codec.unit_to_json(u) for u in units],
        }
        click.echo(json.dumps(doc, indent=2))
    else:
        click.echo(f"run {record.run_id} {record.status}")
        click.echo(f"started_at {record.started_at.isoformat()}")
        click.echo(f"ended_at {codec.dt_text(record.ended_at) or '-'}")
        click.echo(f"parent_run_id {record.parent_run_id or '-'}")
        for u in units:
            click.echo(f"{u.unit_id}\t{u.attempt}\t{u.status}")
    ctx.done()
