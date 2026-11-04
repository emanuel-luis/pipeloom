"""Run a project: the sequence behind `Project.run` (thread executor only in this version)."""

from __future__ import annotations

import hashlib
import os
import platform
import secrets
from collections import Counter
from collections.abc import Mapping
from contextlib import nullcontext
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal

import pipeloom
from pipeloom.artifacts.finalize import finalize
from pipeloom.artifacts.layout import RunPaths
from pipeloom.artifacts.results import RunHeader, exit_code_of, git_sha, result_row
from pipeloom.config.bind import BoundProject
from pipeloom.diagnostics import Diagnostic, JsonValue, Report, Severity
from pipeloom.engine.protocols import Catalog, Engine
from pipeloom.events.bus import EventBus
from pipeloom.events.model import ArtifactWritten, RunCompiled, RunCompleted, RunStarted
from pipeloom.graph.selectors import Selection
from pipeloom.plan.manifest import write_manifest
from pipeloom.plan.model import Plan
from pipeloom.runtime.context import RuntimeContext
from pipeloom.runtime.errors import ErrorInfo, to_diagnostic
from pipeloom.runtime.executors.thread import ThreadExecutor
from pipeloom.runtime.loader import CallableLoader
from pipeloom.runtime.memo import EphemeralMemo
from pipeloom.runtime.runner import ModelRunner
from pipeloom.runtime.scheduler import Scheduler, ScheduleResult, UnitGraph
from pipeloom.runtime.states import UnitState
from pipeloom.state.types import RunRecord, RunStatus, StateStore

if TYPE_CHECKING:
    from pipeloom.api import Project

DEFAULT_ARTIFACTS_ROOT = "target"
MAX_DEFAULT_WORKERS = 4


@dataclass(frozen=True)
class RunOptions:
    selection: Selection = field(default_factory=Selection)
    manifest: Path | None = None
    expect_plan_hash: str | None = None
    run_id: str | None = None
    """Reusing a run id continues that run: tokens replay and the results merge."""
    full_refresh: bool = False
    fail_fast: bool = False
    lock: bool = True
    workers: int | None = None
    boundary: Literal["process", "task"] | None = None
    warn_error: bool = False


@dataclass(frozen=True)
class RunOutcome:
    status: RunStatus
    exit_code: int
    run_results: Path | None
    diagnostics: tuple[Diagnostic, ...]
    """What validation found, and one diagnostic per failed unit."""


def new_run_id(now: datetime) -> str:
    return f"{now.astimezone(UTC):%Y%m%dT%H%M%SZ}-{secrets.token_hex(3)}"


def _iso(value: datetime) -> str:
    return value.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _rel(path: Path, base: Path) -> str:
    try:
        return path.relative_to(base).as_posix()
    except ValueError:
        return path.as_posix()


def _workers(opts: RunOptions, bound: BoundProject) -> int:
    if opts.workers:
        return opts.workers
    if "workers" in bound.runtime.pointers:
        return bound.runtime.workers
    return min(MAX_DEFAULT_WORKERS, os.cpu_count() or 1)


def _artifacts_root(bound: BoundProject, project_dir: Path) -> Path:
    root = Path(bound.runtime.artifacts.root or DEFAULT_ARTIFACTS_ROOT)
    return root if root.is_absolute() else project_dir / root


def _missing(bound: BoundProject, what: str) -> Diagnostic:
    report = Report()
    return report.add(
        "PLS105",
        f"target '{bound.target}' has no {what} to run with",
        location=bound.loaded.project_file.doc.locate(f"/targets/{bound.target}"),
        hint=f"add `{what}: {{kind: ...}}` to the target or to the top level",
    )


def _consumers(plan: Plan, units: list[str]) -> dict[str, int]:
    count: Counter[str] = Counter()
    for u in units:
        count.update(plan.units[u].nodes[:-1])
    return dict(count)


def _verdict(
    result: ScheduleResult | None, units: list[str]
) -> tuple[Mapping[str, tuple[str, str | None]], list[str], list[str]]:
    """Skipped units with their reason, cancelled units, and units that never finished."""
    if result is None:
        return {}, [], list(units)
    skipped = {u: (reason, up) for u, (reason, up) in result.skipped.items()}
    cancelled = [u for u, s in result.states.items() if s is UnitState.CANCELLED]
    unfinished = [
        u
        for u, s in result.states.items()
        if s in (UnitState.RUNNING, UnitState.PENDING, UnitState.QUEUED)
    ]
    return skipped, cancelled, unfinished


def execute_run(project: Project, opts: RunOptions, bus: EventBus) -> RunOutcome:
    """Validate, plan and run the selected units; write the artifacts; report the outcome."""
    if opts.manifest is not None or opts.expect_plan_hash is not None:
        raise NotImplementedError("--manifest and --expect-plan-hash arrive with the run command")
    compiled = project.compile(opts.selection, opts.boundary)
    diags = list(compiled.diagnostics)
    plan = compiled.value
    bound = project.bind().value
    warned = any(d.severity is Severity.WARNING for d in diags)
    if plan is None or bound is None or (opts.warn_error and warned):
        return RunOutcome("failed", 1, None, tuple(diags))
    if plan.metadata.boundary == "task":
        raise NotImplementedError("boundary 'task' needs the process executor, not built yet")

    chosen = set(plan.selection.selected)
    units = [u for u in plan.units if plan.units[u].nodes[-1] in chosen]
    if not units:  # nothing selected, or only ephemeral models, which never run alone
        return RunOutcome("succeeded", 0, None, tuple(diags))
    engine_plugin, catalog_plugin = bound.plugins.get("engines"), bound.plugins.get("catalogs")
    for plugin, what in ((engine_plugin, "engine"), (catalog_plugin, "catalog")):
        if plugin is None:
            return RunOutcome("failed", 1, None, (*diags, _missing(bound, what)))
    assert engine_plugin is not None and catalog_plugin is not None

    clock = project.clock
    t0 = clock.monotonic()
    started = clock.now()
    run_id = opts.run_id or new_run_id(started)
    bus.set_run_id(run_id)
    # Resolved once: sys.path, tracebacks and artifact paths all use absolute forms.
    project_dir = bound.loaded.project_dir.resolve()
    paths = RunPaths(_artifacts_root(bound, project_dir), run_id)
    workers = _workers(opts, bound)
    identity = project.identity
    rt = RuntimeContext(
        run_id=run_id,
        invocation_id=project.invocation_id,
        project=plan.metadata.project,
        target=plan.metadata.target,
        project_dir=project_dir,
        clock=clock,
        identity=identity,
        vars=plan.metadata.vars,
        full_refresh=opts.full_refresh,
        orchestrator=dict(identity.orchestrator_ids),
    )
    engine: Engine = engine_plugin.create()
    catalog: Catalog = catalog_plugin.create()
    state: StateStore = bound.plugins["state_stores"].create()
    scope = f"{rt.project}/{rt.target}"
    write_manifest(plan, paths.manifest)
    floor = {u: paths.next_attempt(u) for u in units}
    """The attempt number each unit gets first in this invocation."""
    schedule: ScheduleResult | None = None
    internal: BaseException | None = None
    try:
        with state.lock(scope, run_id) if opts.lock else nullcontext():
            prior = state.get_run(run_id)
            run_started = prior.started_at if prior else started
            invocation_ids = tuple(
                dict.fromkeys((*(prior.invocation_ids if prior else ()), rt.invocation_id))
            )
            record = RunRecord(
                run_id=run_id,
                project=rt.project,
                target=rt.target,
                plan_hash=plan.metadata.plan_hash,
                status="running",
                started_at=run_started,
                invocation_ids=invocation_ids,
            )
            state.record_run(record)
            bus.emit(
                RunStarted(
                    project=rt.project,
                    target=rt.target,
                    plan_hash=plan.metadata.plan_hash,
                    selected=len(units),
                    workers=workers,
                    boundary="process",
                    executor="thread",
                    full_refresh=opts.full_refresh,
                    fail_fast=opts.fail_fast,
                    engine=engine.name,
                    engine_version=engine.version,
                )
            )
            persisted = [plan.nodes[u.nodes[-1]] for u in plan.units.values()]
            bus.emit(
                RunCompiled(
                    units=len(plan.units),
                    by_materialization=dict(
                        sorted(Counter(n.materialized for n in persisted).items())
                    ),
                    ephemeral_inlined=sum(
                        1 for n in plan.nodes.values() if n.materialized == "ephemeral"
                    ),
                    selected=len(units),
                    plan_hash=plan.metadata.plan_hash,
                    executor="thread",
                )
            )
            with (
                engine.session(rt, catalog=catalog) as session,
                CallableLoader(project_dir, plan.metadata.python_paths) as loader,
            ):
                memo = EphemeralMemo(_consumers(plan, units), persist=session.persist)
                runner = ModelRunner(
                    plan=plan,
                    engine=engine,
                    session=session,
                    catalog=catalog,
                    state=state,
                    bus=bus,
                    paths=paths,
                    rt=rt,
                    loader=loader,
                    memo=memo,
                    executor_name="thread",
                )
                executor = ThreadExecutor(workers, runner.run_unit)
                scheduler = Scheduler(
                    UnitGraph.from_plan(plan, units),
                    executor,
                    workers=workers,
                    fail_fast=opts.fail_fast,
                    bus=bus,
                    clock=clock,
                )
                try:
                    schedule = scheduler.run()
                except BaseException as exc:  # noqa: BLE001 - finalize first, then re-raise
                    internal = exc
                    schedule = scheduler.result
                finally:
                    executor.shutdown(wait=True)
            return _finish(
                project,
                opts,
                bus,
                plan,
                bound,
                paths,
                rt,
                state,
                schedule,
                internal,
                units,
                floor,
                workers,
                started,
                run_started,
                invocation_ids,
                t0,
                diags,
            )
    finally:
        state.close()


def _finish(
    project: Project,
    opts: RunOptions,
    bus: EventBus,
    plan: Plan,
    bound: BoundProject,
    paths: RunPaths,
    rt: RuntimeContext,
    state: StateStore,
    schedule: ScheduleResult | None,
    internal: BaseException | None,
    units: list[str],
    floor: dict[str, int],
    workers: int,
    started: datetime,
    run_started: datetime,
    invocation_ids: tuple[str, ...],
    t0: float,
    diags: list[Diagnostic],
) -> RunOutcome:
    clock = project.clock
    skipped, cancelled, unfinished = _verdict(schedule, units)
    # A unit the scheduler still saw as running may have finished while the executor shut down.
    unfinished = [u for u in unfinished if paths.next_attempt(u) == floor.get(u)]
    ended = clock.now()
    duration_ms = max(0, int((clock.monotonic() - t0) * 1000))
    args: dict[str, Any] = {
        "select": list(opts.selection.select),
        "exclude": list(opts.selection.exclude),
        "manifest": None,
        "run_id": rt.run_id,
        "full_refresh": opts.full_refresh,
        "fail_fast": opts.fail_fast,
        "workers": workers,
        "boundary": "process",
        "lock": opts.lock,
    }
    header = RunHeader(
        run_id=rt.run_id,
        invocation_id=rt.invocation_id,
        invocation_ids=invocation_ids,
        project=rt.project,
        target=rt.target,
        plan_hash=plan.metadata.plan_hash,
        pipeloom_version=pipeloom.__version__,
        plugins={k: v.model_dump(mode="json") for k, v in plan.metadata.plugins.items()},
        args=args,
        selector={"select": list(opts.selection.select), "exclude": list(opts.selection.exclude)},
        full_refresh=opts.full_refresh,
        generated_at=_iso(ended),
        started_at=_iso(run_started),
        ended_at=_iso(ended),
        duration_ms=duration_ms,
        identity={
            "user": rt.identity.user,
            "host": rt.identity.host,
            "orchestrator": rt.identity.orchestrator,
            "orchestrator_ids": dict(rt.identity.orchestrator_ids),
        },
        python_version=platform.python_version(),
        git_sha=git_sha(rt.project_dir),
    )
    unit_nodes = {u: plan.units[u].nodes[-1] for u in plan.units}
    crashed: dict[str, dict[str, Any]] = {}
    if schedule is not None:
        for unit_id, comp in schedule.completions.items():
            exc = comp.internal_error
            if exc is not None and comp.result is None:
                err = ErrorInfo(
                    code="PLX101",
                    category="permanent",
                    message=f"internal error: {type(exc).__name__}: {exc}",
                    hint="this is probably a pipeloom bug; report it",
                    exception_type=type(exc).__name__,
                )
                crashed[unit_id] = result_row(
                    unique_id=unit_nodes[unit_id],
                    unit_id=unit_id,
                    status="failed",
                    error=err.model_dump(mode="json"),
                )
    done = finalize(
        paths,
        header,
        unit_nodes=unit_nodes,
        skipped=skipped,
        cancelled=cancelled,
        unfinished=unfinished,
        crashed=crashed,
    )
    run = done.run_results["run"]
    status: RunStatus = run["status"]
    state.record_run(
        RunRecord(
            run_id=rt.run_id,
            project=rt.project,
            target=rt.target,
            plan_hash=plan.metadata.plan_hash,
            status=status,
            started_at=run_started,
            ended_at=ended,
            invocation_ids=invocation_ids,
            counts=run["counts"],
        )
    )
    written: list[str] = []
    for path in (paths.manifest, *done.written):
        content = path.read_bytes()
        rel = _rel(path, rt.project_dir)
        written.append(rel)
        bus.emit(
            ArtifactWritten(
                path=rel,
                kind="run_results" if path.name == "run_results.json" else "manifest",
                bytes=len(content),
                sha256=hashlib.sha256(content).hexdigest(),
            )
        )
    failures: list[dict[str, JsonValue]] = []
    unit_diags: list[Diagnostic] = []
    if schedule is not None:
        for unit_id in sorted(schedule.completions):
            result = schedule.completions[unit_id].result
            if result is not None and result.error is not None:
                d = to_diagnostic(result.error, unit_id=unit_id)
                unit_diags.append(d)
                failures.append({"unit_id": unit_id, **d.to_json()})
    bus.emit(
        RunCompleted(
            status=run["status"],
            exit_code=exit_code_of(run["status"]),
            duration_ms=duration_ms,
            counts=run["counts"],
            rows_written=run["totals"]["rows_written"],
            bytes_written=run["totals"]["bytes_written"],
            failures=failures,
            artifacts=written,
        )
    )
    if internal is not None:
        raise internal
    return RunOutcome(
        status,
        exit_code_of(run["status"]),
        paths.run_results,
        (*diags, *unit_diags),
    )


__all__ = ["RunOptions", "RunOutcome", "execute_run", "new_run_id"]
