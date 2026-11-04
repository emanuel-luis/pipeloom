"""Run one execution unit: bind inputs, run the body, write the result, record the attempt."""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import Awaitable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Literal, NoReturn

from pipeloom.artifacts.io import write_json_atomic
from pipeloom.artifacts.layout import RunPaths
from pipeloom.artifacts.results import result_row, schema_hash
from pipeloom.diagnostics import JsonValue, Location
from pipeloom.engine import portable
from pipeloom.engine.protocols import Catalog, Engine, EngineSession
from pipeloom.engine.types import ReadSpec, Relation, TargetRef, WriteConfig, WriteResult
from pipeloom.events.bus import EventBus
from pipeloom.events.model import (
    ModelInputRead,
    ModelLog,
    ModelWritten,
    UnitFailed,
    UnitStarted,
    UnitSucceeded,
)
from pipeloom.plan.model import Plan, PlanNode, PlanSource, PythonBody, SqlBody
from pipeloom.runtime.context import ModelContext, RuntimeContext
from pipeloom.runtime.errors import ErrorInfo, Phase, build_error, full_traceback
from pipeloom.runtime.executors import UnitCompletion
from pipeloom.runtime.loader import CallableLoader
from pipeloom.runtime.memo import EphemeralMemo
from pipeloom.state.types import StateStore, UnitRecord

DEFAULT_SCHEMA = "main"


@dataclass(frozen=True)
class ModelResult:
    """What a unit reports to the scheduler: its results row and, on failure, why."""

    unit_id: str
    status: Literal["succeeded", "failed"]
    attempt: int
    row: Mapping[str, Any]
    error: ErrorInfo | None = None


class _Fail(Exception):
    """A phase of a unit failed with `cause`; it becomes the unit's `ErrorInfo`."""

    def __init__(
        self, code: str, phase: Phase, cause: BaseException, message: str, hint: str | None
    ) -> None:
        super().__init__(message)
        self.code, self.phase, self.cause, self.message, self.hint = (
            code,
            phase,
            cause,
            message,
            hint,
        )


def _iso(value: datetime) -> str:
    return value.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _target_ref(dotted: str) -> TargetRef:
    parts = dotted.split(".")
    if len(parts) == 1:
        return TargetRef(None, DEFAULT_SCHEMA, parts[0])
    if len(parts) == 2:
        return TargetRef(None, parts[0], parts[1])
    return TargetRef(parts[0], parts[1], ".".join(parts[2:]))


async def _await(awaitable: Awaitable[Any]) -> Any:
    return await awaitable


class ModelRunner:
    """Runs units for the thread executor: `run_unit` is its `run_unit` callable.

    One runner, one engine session and one state store serve every thread of an invocation.
    """

    def __init__(
        self,
        *,
        plan: Plan,
        engine: Engine,
        session: EngineSession,
        catalog: Catalog,
        state: StateStore,
        bus: EventBus,
        paths: RunPaths,
        rt: RuntimeContext,
        loader: CallableLoader,
        memo: EphemeralMemo | None,
        executor_name: str,
    ) -> None:
        self._plan = plan
        self._engine = engine
        self._session = session
        self._catalog = catalog
        self._state = state
        self._bus = bus
        self._paths = paths
        self._rt = rt
        self._loader = loader
        self._memo = memo
        self._executor = executor_name

    # -- the unit ---------------------------------------------------------------------------

    def run_unit(self, unit_id: str, queued_at: float) -> UnitCompletion:
        rt = self._rt
        unit = self._plan.units[unit_id]
        node = self._plan.nodes[unit.nodes[-1]]
        started = rt.clock.now()
        t0 = rt.clock.monotonic()
        attempt = self._paths.next_attempt(unit_id)
        wait_ms = max(0, int((t0 - queued_at) * 1000))
        self._bus.emit(
            UnitStarted(
                materialization=node.materialized, executor=self._executor, queue_wait_ms=wait_ms
            ),
            unit_id=unit_id,
            model=node.name,
            attempt=attempt,
        )
        strategy = self._strategy(node)
        row = result_row(
            unique_id=node.unique_id,
            unit_id=unit_id,
            ephemeral_inlined=[self._plan.nodes[u].name for u in unit.nodes[:-1]],
            materialization=node.materialized,
            strategy=strategy,
            relation_name=None if node.relation is None else node.relation.relation_name,
            attempt=attempt,
            started_at=_iso(started),
            queue_wait_ms=wait_ms,
            full_refresh=rt.full_refresh,
            engine=self._engine.name,
            engine_version=self._engine.version,
            executor=self._executor,
            idempotency_token=None if strategy is None else f"{rt.run_id}:{node.name}",
        )
        error: ErrorInfo | None = None
        trace: str | None = None
        try:
            self._execute(unit_id, node, strategy, attempt, started, row)
        except _Fail as fail:
            error = build_error(
                fail.code,
                fail.cause,
                phase=fail.phase,
                message=fail.message,
                hint=fail.hint,
                project_dir=rt.project_dir,
                fallback=Location(node.file, node.line),
            )
            trace = full_traceback(fail.cause, rt.project_dir)
        except Exception as exc:  # noqa: BLE001 - a bug in the runner still ends the unit cleanly
            error = self._unexpected(exc, node)
            trace = full_traceback(exc, rt.project_dir)
        finally:
            if self._memo is not None:
                self._memo.release(unit_id)
        ended = rt.clock.now()
        duration_ms = max(0, int((rt.clock.monotonic() - t0) * 1000))
        status: Literal["succeeded", "failed"] = "failed" if error else "succeeded"
        row.update(
            status=status,
            ended_at=_iso(ended),
            duration_ms=duration_ms,
            error=None if error is None else error.model_dump(mode="json"),
        )
        try:
            write_json_atomic(self._paths.attempt(unit_id, attempt), row)
            self._state.record_unit(
                UnitRecord(
                    rt.run_id, unit_id, attempt, status, rt.invocation_id, started, ended, dict(row)
                )
            )
        except Exception as exc:
            self._bus.emit(
                UnitFailed(
                    error=self._unexpected(exc, node),
                    duration_ms=duration_ms,
                    traceback=full_traceback(exc, rt.project_dir),
                ),
                unit_id=unit_id,
                model=node.name,
                attempt=attempt,
            )
            raise
        if error is None:
            self._bus.emit(
                UnitSucceeded(
                    materialization=node.materialized,
                    strategy=strategy,
                    relation=row["relation_name"],
                    rows_written=row["rows_written"],
                    rows_inserted=row["rows_inserted"],
                    bytes_written=row["bytes_written"],
                    duration_ms=duration_ms,
                ),
                unit_id=unit_id,
                model=node.name,
                attempt=attempt,
            )
        else:
            self._bus.emit(
                UnitFailed(error=error, duration_ms=duration_ms, traceback=trace),
                unit_id=unit_id,
                model=node.name,
                attempt=attempt,
            )
        return UnitCompletion(unit_id, status, ModelResult(unit_id, status, attempt, row, error))

    def _unexpected(self, exc: Exception, node: PlanNode) -> ErrorInfo:
        return build_error(
            "PLX101",
            exc,
            phase="body",
            message=f"unexpected error while running model '{node.name}'",
            hint="this is probably a pipeloom bug; report it with the traceback",
            project_dir=self._rt.project_dir,
            fallback=Location(node.file, node.line),
        )

    def _strategy(self, node: PlanNode) -> str | None:
        """Read from the plan: it stores the effective strategy. Only tables use one."""
        if node.materialized != "table":
            return None
        strategy = node.config.write.strategy or "overwrite"
        # A full refresh rebuilds the table instead of adding to it.
        return "overwrite" if self._rt.full_refresh and strategy == "append" else strategy

    def _execute(
        self,
        unit_id: str,
        node: PlanNode,
        strategy: str | None,
        attempt: int,
        started: datetime,
        row: dict[str, Any],
    ) -> None:
        target = node.relation
        if node.materialized == "view":
            assert target is not None and isinstance(node.body, SqlBody)
            result = self._create_view(node, target, node.body)
        elif node.materialized == "table" and strategy is not None:
            assert target is not None
            rows_read: dict[str, int | None] = {}
            local: dict[str, Relation] = {}
            rel = self._compute(node, unit_id, attempt, started, local, rows_read)
            row["rows_read"] = rows_read
            result = self._write(node, target, strategy, rel, row)
        else:  # L5 rejects the rest (PLC509) before a run starts
            raise ValueError(f"cannot run materialization '{node.materialized}'")
        row.update(
            rows_written=result.rows_written,
            rows_inserted=result.rows_inserted,
            bytes_written=result.bytes_written,
            target_version=result.target_version,
        )

    # -- reading and the body -----------------------------------------------------------------

    def _compute(
        self,
        node: PlanNode,
        unit_id: str,
        attempt: int,
        started: datetime,
        local: dict[str, Relation],
        rows_read: dict[str, int | None] | None,
    ) -> Relation:
        """Read the node's inputs and run its body."""
        inputs: dict[str, Relation] = {}
        for alias, uid in node.inputs.items():
            inputs[alias] = self._input(alias, uid, node, unit_id, attempt, started, local)
            if rows_read is not None:
                rows_read[alias] = (
                    None if inputs[alias].lazy else self._session.count(inputs[alias])
                )
        ctx = self._context(node, unit_id, attempt, started)
        failed = f"model '{node.name}' raised"
        try:
            if isinstance(node.body, SqlBody):
                return self._session.sql(node.body.sql, inputs)
            out = self._call(node.body, node, ctx, inputs)
        except Exception as exc:  # noqa: BLE001 - the body is user code
            self._fail("PLX101", "body", exc, failed)
        try:
            return self._session.relation(portable.from_frame_api(out))
        except TypeError as exc:
            self._fail(
                "PLX101",
                "body",
                exc,
                f"model '{node.name}' returned a value its engine cannot use",
                f"return the native frame of engine '{self._engine.name}'",
            )

    def _fail(
        self, code: str, phase: Phase, exc: Exception, message: str, hint: str | None = None
    ) -> NoReturn:
        raise _Fail(code, phase, exc, message, hint) from exc

    def _call(
        self, body: PythonBody, node: PlanNode, ctx: ModelContext, inputs: Mapping[str, Relation]
    ) -> Any:
        fn = self._loader.load(body.callable)
        api = node.config.frame_api
        out = fn(ctx, **{a: portable.to_frame_api(r.native, api) for a, r in inputs.items()})
        return asyncio.run(_await(out)) if inspect.isawaitable(out) else out

    def _context(
        self, node: PlanNode, unit_id: str, attempt: int, started: datetime
    ) -> ModelContext:
        rt = self._rt

        def emit_log(message: str, level: str, fields: Mapping[str, JsonValue]) -> None:
            level_name: Literal["debug", "info", "warning"] = (
                "debug" if level == "debug" else "warning" if level == "warning" else "info"
            )
            self._bus.emit(
                ModelLog(message=message, level=level_name, fields=dict(fields)),
                unit_id=unit_id,
                model=node.name,
                attempt=attempt,
            )

        return ModelContext(
            run_id=rt.run_id,
            invocation_id=rt.invocation_id,
            unit_id=unit_id,
            unique_id=node.unique_id,
            model=node.name,
            attempt=attempt,
            target=node.relation,
            materialized=node.materialized,
            vars=rt.vars,
            full_refresh=rt.full_refresh,
            engine=self._session,
            frame_api=node.config.frame_api,
            started_at=started,
            emit_log=emit_log,
        )

    def _input(
        self,
        alias: str,
        uid: str,
        node: PlanNode,
        unit_id: str,
        attempt: int,
        started: datetime,
        local: dict[str, Relation],
    ) -> Relation:
        plan = self._plan
        if uid in plan.sources:
            source = plan.sources[uid]
            return self._read(
                self._source_spec(source), alias, "source", source.name, node, unit_id, attempt
            )
        upstream = plan.nodes[uid]
        if upstream.materialized != "ephemeral":
            spec = ReadSpec("table", table=upstream.relation)
            return self._read(spec, alias, "model", upstream.name, node, unit_id, attempt)
        if uid not in local:

            def compute() -> Relation:
                return self._compute(upstream, unit_id, attempt, started, local, None)

            local[uid] = (
                compute() if self._memo is None else self._memo.get(uid, compute, unit_id=unit_id)
            )
        return local[uid]

    def _read(
        self,
        spec: ReadSpec,
        alias: str,
        kind: Literal["source", "model"],
        name: str,
        node: PlanNode,
        unit_id: str,
        attempt: int,
    ) -> Relation:
        t0 = self._rt.clock.monotonic()
        try:
            rel = self._session.reader(spec.format).read(spec)
            rows = None if rel.lazy else self._session.count(rel)
        except Exception as exc:  # noqa: BLE001 - engines raise their own errors
            self._fail(
                "PLM204",
                "read",
                exc,
                f"cannot read input '{alias}' of model '{node.name}'",
                f"check that {kind} '{name}' exists and can be read",
            )
        self._bus.emit(
            ModelInputRead(
                alias=alias,
                kind=kind,
                name=name,
                rows=rows,
                duration_ms=max(0, int((self._rt.clock.monotonic() - t0) * 1000)),
            ),
            unit_id=unit_id,
            model=node.name,
            attempt=attempt,
        )
        return rel

    def _source_spec(self, source: PlanSource) -> ReadSpec:
        options: dict[str, JsonValue] = dict(source.options)
        if source.table is not None:
            return ReadSpec("table", table=_target_ref(source.table), options=options)
        location = source.location or ""
        if "://" not in location and not location.startswith("/") and ":" not in location[:2]:
            location = (self._rt.project_dir / location).as_posix()
        return ReadSpec(source.format or "", location=location, options=options)

    # -- writing ------------------------------------------------------------------------------

    def _write(
        self, node: PlanNode, target: TargetRef, strategy: str, rel: Relation, row: dict[str, Any]
    ) -> WriteResult:
        write = node.config.write
        token = f"{self._rt.run_id}:{node.name}"
        t0 = self._rt.clock.monotonic()
        try:
            columns = self._session.schema(rel)
            row["schema_hash"] = schema_hash((c.name, c.dtype, c.nullable) for c in columns)
            self._catalog.ensure_namespace(target.namespace)
            cfg = WriteConfig(
                strategy, unique_key=tuple(write.unique_key or ()), options=dict(write.options)
            )
            result = self._session.materializer(strategy).materialize(rel, target, cfg, token)
        except Exception as exc:  # noqa: BLE001 - engines raise their own errors
            self._fail(
                "PLM201",
                "write",
                exc,
                f"cannot write '{target.relation_name}' for model '{node.name}'",
                "check that the target can be written, then run again",
            )
        self._written(node, target, strategy, result, t0)
        return result

    def _create_view(self, node: PlanNode, target: TargetRef, body: SqlBody) -> WriteResult:
        inputs: dict[str, ReadSpec] = {}
        for alias, uid in node.inputs.items():
            if uid in self._plan.sources:
                inputs[alias] = self._source_spec(self._plan.sources[uid])
            else:
                inputs[alias] = ReadSpec("table", table=self._plan.nodes[uid].relation)
        t0 = self._rt.clock.monotonic()
        try:
            self._catalog.ensure_namespace(target.namespace)
            result = self._session.create_view(target, body.sql, inputs)
        except Exception as exc:  # noqa: BLE001 - engines raise their own errors
            self._fail(
                "PLM201",
                "write",
                exc,
                f"cannot create view '{target.relation_name}' for model '{node.name}'",
                "check that the target can be written, then run again",
            )
        self._written(node, target, None, result, t0)
        return result

    def _written(
        self,
        node: PlanNode,
        target: TargetRef,
        strategy: str | None,
        result: WriteResult,
        t0: float,
    ) -> None:
        self._bus.emit(
            ModelWritten(
                relation=target.relation_name,
                materialization=node.materialized,
                strategy=strategy,
                rows_written=result.rows_written,
                rows_inserted=result.rows_inserted,
                bytes_written=result.bytes_written,
                duration_ms=max(0, int((self._rt.clock.monotonic() - t0) * 1000)),
            ),
            unit_id=node.name,
            model=node.name,
        )
