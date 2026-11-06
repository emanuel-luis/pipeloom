"""Run a project: the sequence behind `Project.run`."""

from __future__ import annotations

import hashlib
import json
import os
import platform
import secrets
from collections import Counter
from collections.abc import Iterable, Mapping
from contextlib import AbstractContextManager, ExitStack, nullcontext
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal

import pipeloom
from pipeloom.artifacts.finalize import finalize
from pipeloom.artifacts.layout import RunPaths
from pipeloom.artifacts.reference import Reference, load_reference
from pipeloom.artifacts.results import RunHeader, RunStatusName, exit_code_of, git_sha, result_row
from pipeloom.config.bind import BoundProject
from pipeloom.diagnostics import Diagnostic, JsonValue, Location, Report, Severity
from pipeloom.engine.protocols import Catalog, Engine, EngineSession
from pipeloom.engine.types import TargetRef
from pipeloom.events.bus import EventBus
from pipeloom.events.model import (
    ArtifactWritten,
    DiagnosticEmitted,
    RunCompiled,
    RunCompleted,
    RunStarted,
)
from pipeloom.graph.selectors import Selection, SelectorError, apply_to_plan, is_unit_selection
from pipeloom.plan.defer import defer_refusal, deferred_relations
from pipeloom.plan.manifest import read_manifest, write_manifest
from pipeloom.plan.model import Plan, PlanSelection
from pipeloom.platform import check_orchestrator_id
from pipeloom.runtime.context import RuntimeContext
from pipeloom.runtime.control import RunControl
from pipeloom.runtime.errors import ErrorInfo, to_diagnostic
from pipeloom.runtime.executors import Executor
from pipeloom.runtime.executors.process import ChildCommand, ProcessExecutor
from pipeloom.runtime.executors.thread import ThreadExecutor
from pipeloom.runtime.loader import CallableLoader
from pipeloom.runtime.memo import EphemeralMemo
from pipeloom.runtime.retry import UnitPolicy
from pipeloom.runtime.runner import ModelRunner
from pipeloom.runtime.scheduler import Scheduler, ScheduleResult, UnitGraph
from pipeloom.runtime.states import UnitState
from pipeloom.secrets.resolve import SecretError, secret_diagnostic
from pipeloom.state.types import LockHeld, RunRecord, RunStatus, StateStore

if TYPE_CHECKING:
    from pipeloom.api import Project

DEFAULT_ARTIFACTS_ROOT = "target"
DEFAULT_CANCEL_GRACE = 30.0
MAX_DEFAULT_WORKERS = 4
USAGE_CODES = frozenset({"PLG304", "PLG305", "PLG307", "PLG308", "PLX108", "PLX111"})


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
    cancel_grace: float | None = None
    """Seconds a stopped or timed-out unit gets to finish before it is killed; default 30."""
    control: RunControl | None = None
    run_started_at: datetime | None = None
    """Timezone-aware start time of the run, for example the start of the orchestrator job.
    Needs `run_id`. A reused run id keeps the start it recorded; a different value is PLX108."""
    orchestrator_ids: Mapping[str, str] = field(default_factory=dict)
    """Ids the orchestrator gave the run (job, run and task ids): they go into every attempt file,
    the run results and the `context` of every event. They win over the identity provider's."""
    state: Path | None = None
    """Folder of the reference manifest.json (and run_results.json), relative to the project dir
    unless absolute. It fills `selection.state` when that is unset, and feeds `defer`."""
    defer: bool = False
    """Read the parents this run does not select from the reference's relations. Needs `state`."""
    not_deferred: frozenset[str] = frozenset()
    """Unique ids of models `defer` never reads from the reference: a retry sets the models its
    parent run built, so the retry reads what that run wrote here."""
    parent_run_id: str | None = None
    """The run this one retries (`Project.retry`). A reused run id keeps the parent it recorded."""

    def __post_init__(self) -> None:
        if self.defer and self.state is None:
            raise ValueError("defer needs state")
        for key, value in self.orchestrator_ids.items():
            check_orchestrator_id(key, value)
        if self.run_started_at is not None:
            if self.run_started_at.tzinfo is None:
                raise ValueError("run_started_at must be timezone-aware")
            if self.run_id is None:
                raise ValueError("run_started_at needs run_id")


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


def _grace(opts: RunOptions, bound: BoundProject) -> float:
    if opts.cancel_grace is not None:
        return opts.cancel_grace
    configured = bound.runtime.cancel_grace
    return DEFAULT_CANCEL_GRACE if configured is None else float(configured)


def artifacts_root(bound: BoundProject, project_dir: Path) -> Path:
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


def _policies(plan: Plan, units: list[str], *, unit_mode: bool) -> dict[str, UnitPolicy]:
    """Retries belong to whoever schedules the unit: a unit invocation never retries."""
    out: dict[str, UnitPolicy] = {}
    for u in units:
        c = plan.nodes[plan.units[u].nodes[-1]].config
        out[u] = UnitPolicy(
            retries=0 if unit_mode else c.retries,
            retry_delay=float(c.retry_delay),
            retry_on=c.retry_on,
            timeout=None if c.timeout is None else float(c.timeout),
        )
    return out


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
        if s in (UnitState.RUNNING, UnitState.PENDING, UnitState.QUEUED, UnitState.RETRYING)
    ]
    return skipped, cancelled, unfinished


def resolve_run_started_at(
    flag: datetime | None, prior: RunRecord | None, now: datetime, *, unit_mode: bool
) -> tuple[datetime | None, Diagnostic | None]:
    """The start time every invocation of a run shares: the recorded start, else the flag,
    else now. A unit invocation without either has none."""
    flag = None if flag is None else flag.astimezone(UTC)
    if prior is not None:
        recorded = prior.started_at.astimezone(UTC)
        if flag is not None and flag != recorded:
            return None, Report().add(
                "PLX108",
                f"--run-started-at {_iso(flag)} differs from the start recorded for run "
                f"{prior.run_id}: {_iso(recorded)}",
                hint="omit the flag to continue the run, or use a new --run-id",
                context={"given": _iso(flag), "recorded": _iso(recorded)},
            )
        return recorded, None
    if flag is not None:
        return flag, None
    return (None if unit_mode else now.astimezone(UTC)), None


def _lock_held(held: LockHeld) -> Diagnostic:
    info = held.info
    return Report().add(
        "PLX105",
        f"run lock '{info.scope}' is held by {info.owner} for run {info.run_id}, "
        f"until {_iso(info.expires_at)}",
        hint="wait for that run to finish or for its lease to expire; --no-lock skips the lock",
        context={"owner": info.owner, "run_id": info.run_id, "expires_at": _iso(info.expires_at)},
    )


def _plan_from_manifest(
    project: Project, opts: RunOptions, report: Report
) -> tuple[Plan | None, BoundProject | None]:
    """Load the project for L0 and L1 and binding only, then trust the manifest for the rest."""
    assert opts.manifest is not None
    bound_outcome = project.bind()
    report.extend(bound_outcome.diagnostics)
    bound = bound_outcome.value
    if bound is None:
        return None, None
    here = _rel(opts.manifest.resolve(), bound.loaded.project_dir.resolve())
    found = Report()
    plan = read_manifest(opts.manifest, found)
    report.extend(replace(d, location=Location(here)) for d in found.sorted())
    if plan is None:
        return None, bound
    meta = plan.metadata
    if (meta.project, meta.target) != (bound.loaded.config.name, bound.target):
        report.add(
            "PLX107",
            f"manifest is for project '{meta.project}', target '{meta.target}', "
            f"not '{bound.loaded.config.name}', '{bound.target}'",
            location=Location(here),
            hint="compile again for this project and target",
        )
        return None, bound
    sel = opts.selection
    recorded = plan.selection
    # A stable manifest records no selection at all: select every model, as a compile without
    # selectors does. A selector that matched nothing at compile time keeps `select` non-empty.
    blank = not (recorded.select or recorded.exclude or recorded.selected)
    if sel.select or sel.exclude or blank:
        try:
            chosen = apply_to_plan(sel, plan, report)
        except SelectorError as err:
            report.extend([err.diagnostic])
            return None, bound
        plan = plan.model_copy(
            update={
                "selection": PlanSelection(
                    select=sel.select, exclude=sel.exclude, selected=tuple(sorted(chosen))
                )
            }
        )
    return plan, bound


def _early(bus: EventBus, diags: Iterable[Diagnostic]) -> RunOutcome:
    """A run that stopped before any unit started: report what was found."""
    found = tuple(diags)
    for d in found:
        bus.emit(DiagnosticEmitted.from_diagnostic(d))
    usage = any(d.code in USAGE_CODES for d in found)
    return RunOutcome("failed", 2 if usage else 1, None, found)


def execute_run(project: Project, opts: RunOptions, bus: EventBus) -> RunOutcome:
    """Validate, plan and run the selected units; write the artifacts; report the outcome.

    A selection made only of `unit:` atoms is a unit invocation: it runs those units on the thread
    executor and takes no lock, emits no `run.*` events and writes no results; the invocation
    that started it (process executor) owns those.
    """
    unit_mode = is_unit_selection(opts.selection)
    report = Report()
    reference: Reference | None = None
    if opts.state is not None:
        seed = project.bind().value  # a project that does not bind reports its errors below
        if seed is not None:
            ref_report = Report()
            reference = load_reference(
                seed.loaded.project_dir / opts.state,
                seed.loaded.config.name,
                ref_report,
                seed.loaded.project_dir,
            )
            if reference is None:
                return _early(bus, ref_report.sorted())
            if opts.selection.state is None:
                opts = replace(
                    opts, selection=replace(opts.selection, state=reference.selector_state())
                )
    if opts.manifest is not None:
        plan, bound = _plan_from_manifest(project, opts, report)
        diags = list(report.sorted())
    else:
        compiled = project.compile(opts.selection, opts.boundary)
        plan, bound, diags = compiled.value, project.bind().value, list(compiled.diagnostics)
    deferred: dict[str, TargetRef] = {}
    if opts.defer and plan is not None and bound is not None and reference is not None:
        refusal = defer_refusal(plan, reference.plan, unit_mode=unit_mode)
        if refusal is not None:
            return _early(bus, [*diags, refusal])
        picked = set(plan.selection.selected)
        run_units = [u for u in plan.units if plan.units[u].nodes[-1] in picked]
        if run_units:
            deferred, warnings = deferred_relations(
                plan, run_units, reference.plan, opts.not_deferred
            )
            diags.extend(warnings)
    warned = any(d.severity is Severity.WARNING for d in diags)
    if plan is None or bound is None or (opts.warn_error and warned):
        return _early(bus, diags)
    expected = opts.expect_plan_hash
    if expected is not None and plan.metadata.plan_hash != expected.lower():
        found = Report().add(
            "PLX103",
            f"plan hash {plan.metadata.plan_hash[:12]} differs from the expected {expected[:12]}",
            hint="the project or the manifest changed since the hash was taken; compile again",
            context={"expected": expected.lower(), "actual": plan.metadata.plan_hash},
        )
        return _early(bus, [*diags, found])
    for d in diags:
        bus.emit(DiagnosticEmitted.from_diagnostic(d))

    chosen = set(plan.selection.selected)
    units = [u for u in plan.units if plan.units[u].nodes[-1] in chosen]
    if not units:  # nothing selected, or only ephemeral models, which never run alone
        return RunOutcome("succeeded", 0, None, tuple(diags))
    engine_plugin, catalog_plugin = bound.plugins.get("engines"), bound.plugins.get("catalogs")
    for plugin, what in ((engine_plugin, "engine"), (catalog_plugin, "catalog")):
        if plugin is None:
            return _early(bus, [*diags, _missing(bound, what)])
    assert engine_plugin is not None and catalog_plugin is not None

    clock = project.clock
    t0 = clock.monotonic()
    started = clock.now()
    run_id = opts.run_id or new_run_id(started)
    bus.set_run_id(run_id)
    # Resolved once: sys.path, tracebacks and artifact paths all use absolute forms.
    project_dir = bound.loaded.project_dir.resolve()
    paths = RunPaths(artifacts_root(bound, project_dir), run_id)
    workers = _workers(opts, bound)
    grace = _grace(opts, bound)
    policies = _policies(plan, units, unit_mode=unit_mode)
    task = plan.metadata.boundary == "task" and not unit_mode
    executor_name = "process" if plan.metadata.boundary == "task" else "thread"
    identity = project.identity
    identity = replace(
        identity, orchestrator_ids={**identity.orchestrator_ids, **opts.orchestrator_ids}
    )
    secrets = project.secrets  # references resolve here and nowhere earlier
    try:
        engine: Engine = engine_plugin.create(secrets)
        catalog: Catalog = catalog_plugin.create(secrets)
        state: StateStore = bound.plugins["state_stores"].create(secrets)
    except SecretError as err:
        return _early(bus, [*diags, secret_diagnostic(err)])
    scope = f"{plan.metadata.project}/{plan.metadata.target}"
    if not unit_mode:
        write_manifest(plan, paths.manifest)
    floor = {u: paths.next_attempt(u) for u in units}
    """The attempt number each unit gets first in this invocation."""
    schedule: ScheduleResult | None = None
    internal: BaseException | None = None
    abandoned = 0
    try:
        with ExitStack() as stack:
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
                artifacts_root=paths.root.as_posix(),
            )
            # The engine session opens before any state-store call: a store that lives in the
            # engine (a table) reuses the engine's own session. It opens without the start time,
            # which engines read from `WriteConfig.run_started_at` instead.
            session: EngineSession | None = None
            if not task:
                session = stack.enter_context(engine.session(rt, catalog=catalog))
            prior = state.get_run(run_id)
            run_started_at, conflict = resolve_run_started_at(
                opts.run_started_at, prior, started, unit_mode=unit_mode
            )
            if conflict is not None:
                return _early(bus, [*diags, conflict])
            rt = replace(rt, run_started_at=run_started_at)
            if opts.lock and not unit_mode:
                try:
                    stack.enter_context(state.lock(scope, run_id))
                except LockHeld as held:
                    return _early(bus, [*diags, _lock_held(held)])
            prior = None if unit_mode else prior
            opts = replace(
                opts, parent_run_id=opts.parent_run_id or (prior.parent_run_id if prior else None)
            )
            run_started = run_started_at or started
            invocation_ids = tuple(
                dict.fromkeys((*(prior.invocation_ids if prior else ()), rt.invocation_id))
            )
            if not unit_mode:
                state.record_run(
                    RunRecord(
                        run_id=run_id,
                        project=rt.project,
                        target=rt.target,
                        plan_hash=plan.metadata.plan_hash,
                        status="running",
                        started_at=run_started,
                        parent_run_id=opts.parent_run_id,
                        invocation_ids=invocation_ids,
                    )
                )
                bus.emit(
                    RunStarted(
                        project=rt.project,
                        target=rt.target,
                        plan_hash=plan.metadata.plan_hash,
                        selected=len(units),
                        workers=workers,
                        boundary=plan.metadata.boundary,
                        executor=executor_name,
                        full_refresh=opts.full_refresh,
                        fail_fast=opts.fail_fast,
                        engine=engine.name,
                        engine_version=engine.version,
                        parent_run_id=opts.parent_run_id,
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
                        executor=executor_name,
                    )
                )
            executor: Executor
            loader_cm: AbstractContextManager[Any]
            if task:
                env = {**os.environ, **(project.options.env or {})}
                executor = ProcessExecutor(
                    workers=workers,
                    command=ChildCommand(
                        project_dir=project_dir,
                        target=rt.target,
                        manifest=paths.manifest,
                        run_id=run_id,
                        plan_hash=plan.metadata.plan_hash,
                        full_refresh=opts.full_refresh,
                        vars=json.dumps(dict(project.options.vars))
                        if project.options.vars
                        else None,
                        plugins=project.options.plugins,
                        cancel_grace=grace,
                        orchestrator_ids=opts.orchestrator_ids,
                    ),
                    paths=paths,
                    bus=bus,
                    env=env,
                    unit_nodes={u: plan.units[u].nodes[-1] for u in plan.units},
                    cancel_grace=grace,
                )
                loader_cm = nullcontext()
            else:
                loader_cm = CallableLoader(project_dir, plan.metadata.python_paths)
            with loader_cm as loader:
                if not task:
                    assert session is not None and loader is not None
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
                        executor_name=executor_name,
                        policies=policies,
                        redactor=project.redactor,
                        deferred=deferred,
                    )
                    executor = ThreadExecutor(
                        runner.run_unit,
                        session=session,
                        attempt_of=runner.attempt_number,
                        on_interrupt=runner.record_interrupt,
                    )
                scheduler = Scheduler(
                    UnitGraph.from_plan(plan, units),
                    executor,
                    workers=workers,
                    fail_fast=opts.fail_fast,
                    bus=bus,
                    clock=clock,
                    policies=policies,
                    control=opts.control,
                    cancel_grace=grace,
                    run_id=run_id,
                    announce=not unit_mode,
                )
                try:
                    schedule = scheduler.run()
                except BaseException as exc:  # noqa: BLE001 - finalize first, then re-raise
                    internal = exc
                    schedule = scheduler.result
                finally:
                    abandoned = getattr(executor, "abandoned_threads", 0)
                    executor.shutdown(wait=True)  # joins are bounded
            if unit_mode:
                return _unit_outcome(schedule, internal, diags)
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
                abandoned,
            )
    finally:
        state.close()


def _unit_outcome(
    schedule: ScheduleResult | None, internal: BaseException | None, diags: list[Diagnostic]
) -> RunOutcome:
    """The outcome of a unit invocation: no artifacts, only a status and the failures."""
    if internal is not None:
        raise internal
    found = list(diags)
    states = schedule.states.values() if schedule else ()
    if schedule is not None:
        for unit_id in sorted(schedule.completions):
            result = schedule.completions[unit_id].result
            if result is not None and result.error is not None:
                found.append(to_diagnostic(result.error, unit_id=unit_id))
    status: RunStatusName = "succeeded"
    if UnitState.FAILED in states or UnitState.TIMED_OUT in states:
        status = "failed"
    elif UnitState.CANCELLED in states:
        status = "cancelled"
    return RunOutcome(status, exit_code_of(status), None, tuple(found))


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
    abandoned: int,
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
        "manifest": None
        if opts.manifest is None
        else _rel(opts.manifest.resolve(), rt.project_dir),
        "run_id": rt.run_id,
        "full_refresh": opts.full_refresh,
        "fail_fast": opts.fail_fast,
        "workers": workers,
        "boundary": plan.metadata.boundary,
        "lock": opts.lock,
        "state": None
        if opts.state is None
        else _rel((rt.project_dir / opts.state).resolve(), rt.project_dir),
        "defer": opts.defer,
    }
    header = RunHeader(
        run_id=rt.run_id,
        parent_run_id=opts.parent_run_id,
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
    timed_out: dict[str, tuple[int, dict[str, Any]]] = {}
    if schedule is not None:
        for unit_id, comp in schedule.completions.items():
            if schedule.states[unit_id] is UnitState.TIMED_OUT and comp.result is not None:
                err = comp.result.error
                timed_out[unit_id] = (
                    comp.attempt,
                    {} if err is None else err.model_dump(mode="json"),
                )
    crashed: dict[str, dict[str, Any]] = {}
    if schedule is not None:
        for unit_id, comp in schedule.completions.items():
            exc = comp.internal_error
            if exc is not None and comp.result is None:
                err = ErrorInfo(
                    code="PLX101",
                    category="permanent",
                    message=project.redactor.mask(f"internal error: {type(exc).__name__}: {exc}"),
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
        timed_out=timed_out,
        intermediate_ttl=bound.runtime.artifacts.intermediate_ttl,
        redactor=project.redactor,
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
            parent_run_id=opts.parent_run_id,
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
                failures.append(
                    {
                        "unit_id": unit_id,
                        **d.to_json(),
                        "trace_excerpt": list[JsonValue](result.error.trace_excerpt),
                    }
                )
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
            abandoned_threads=abandoned,
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


__all__ = ["RunOptions", "RunOutcome", "execute_run", "new_run_id", "resolve_run_started_at"]
