"""Run one execution unit: bind inputs, run the body, write the result, record the attempt."""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Literal, NoReturn

from pipeloom.artifacts.io import write_json_atomic
from pipeloom.artifacts.layout import RunPaths
from pipeloom.artifacts.results import result_row, schema_hash
from pipeloom.diagnostics import JsonValue, Location
from pipeloom.engine import portable
from pipeloom.engine.errors import WriteKeyError
from pipeloom.engine.protocols import Catalog, Engine, EngineSession
from pipeloom.engine.types import (
    ROW_CHECKS,
    DeleteSpec,
    ExpectationSpec,
    QuarantineMeta,
    ReadSpec,
    Relation,
    TargetRef,
    WriteConfig,
    WriteResult,
)
from pipeloom.events.bus import EventBus
from pipeloom.events.model import (
    ModelInputRead,
    ModelLog,
    ModelQualityEvaluated,
    ModelSchemaChanged,
    ModelWritten,
    UnitFailed,
    UnitStarted,
    UnitSucceeded,
)
from pipeloom.plan.model import Plan, PlanNode, PlanSource, PythonBody, SqlBody
from pipeloom.quality import (
    QualityReport,
    build_specs,
    check_columns,
    judge,
    quarantine_target,
    quarantine_token,
)
from pipeloom.runtime.context import ModelContext, RuntimeContext
from pipeloom.runtime.errors import ErrorInfo, Phase, build_error, full_traceback
from pipeloom.runtime.executors import UnitCompletion
from pipeloom.runtime.loader import CallableLoader
from pipeloom.runtime.memo import EphemeralMemo
from pipeloom.runtime.schema_policy import SchemaRejected, changes_json, resolve_columns
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
        keeps = strategy in ("append", "merge", "replace_partitions")
        return "overwrite" if self._rt.full_refresh and keeps else strategy

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
            rel, before_write, finish = self._quality(node, target, unit_id, rel, row)
            try:
                result = self._write(node, target, strategy, rel, row, before_write)
            finally:
                if finish is not None:
                    finish()  # the event reports what was really quarantined
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

    # -- quality ------------------------------------------------------------------------------

    def _quality(
        self, node: PlanNode, target: TargetRef, unit_id: str, rel: Relation, row: dict[str, Any]
    ) -> tuple[Relation, Callable[[], None] | None, Callable[[], None] | None]:
        """Steps 5, 6 and 8: count the output, drop rows, judge. Returns the rows to write and
        the quarantine write to run just before the table write, if rows were dropped, and a
        `finish` that emits the quality event once (after the quarantine commit, or on failure)."""
        specs = build_specs(node.config)
        if not specs:
            return rel, None, None
        session, cfg = self._session, node.config.quality
        cached = rel.lazy
        if cached:  # a lazy plan would run once per count
            rel = session.persist(rel)
        problems = check_columns(specs, session.schema(rel))
        if problems:
            self._fail(
                "PLQ302",
                "write",
                ValueError("; ".join(problems)),
                f"model '{node.name}': {problems[0]}",
                "check the column names in expectations against the model output",
            )
        rows = [s for s in specs if s.kind in ROW_CHECKS]
        aggs = [s for s in specs if s.kind not in ROW_CHECKS]
        quarantine = (
            QuarantineMeta(self._rt.run_id, self._rt.clock.now()) if cfg.quarantine else None
        )
        pending: Relation | None = None
        kept = rel
        try:
            if any(s.action == "drop" for s in rows):
                row_counts = session.evaluate(rel, rows)
                if row_counts.dropped:
                    dropped = session.apply_drops(rel, rows, quarantine=quarantine)
                    kept, pending = dropped.kept, dropped.quarantined
                agg_counts = session.evaluate(kept, aggs) if aggs else None
            else:  # nothing can drop: one call counts everything
                row_counts = session.evaluate(rel, list(specs))
                agg_counts = row_counts
        except Exception as exc:  # noqa: BLE001 - engines raise their own errors
            self._quality_error(node, exc, rel, specs)
        report = judge(
            specs,
            row_counts,
            agg_counts,
            now=self._rt.clock.now(),
            max_drop_ratio=cfg.max_drop_ratio,
            cached=cached,
        )
        row.update(
            expectations=[r.to_json() for r in report.results],
            rows_dropped=report.rows_dropped,
            rows_quarantined=0,  # set once the quarantine write has committed
            warnings=[
                *row.get("warnings", []),
                *(f"expectation '{r.name}' ({r.kind}) failed" for r in report.warnings),
            ],
        )
        done = False

        def finish() -> None:
            nonlocal done
            if not done:
                done = True
                self._quality_event(node, unit_id, report, row["rows_quarantined"])

        if report.failures:
            finish()
            self._fail(
                "PLQ301",
                "write",
                ValueError(", ".join(report.failures)),
                f"model '{node.name}' failed expectation(s): {', '.join(report.failures)}",
                "fix the data, or relax on_violation or quality.max_drop_ratio",
            )
        if pending is None:
            finish()
            return kept, None, None
        quarantined = pending

        def write_quarantine() -> None:
            self._write_quarantine(node, target, quarantined)
            row["rows_quarantined"] = report.rows_dropped

        return kept, write_quarantine, finish

    def _quality_error(
        self, node: PlanNode, exc: Exception, rel: Relation, specs: tuple[ExpectationSpec, ...]
    ) -> NoReturn:
        """An engine failure while counting: PLQ302 when one expr is the cause, else PLX101."""
        for spec in specs:
            if spec.kind != "expr":
                continue
            try:
                self._session.evaluate(rel, [spec])
            except Exception:  # noqa: BLE001 - probing which expression the engine rejects
                self._fail(
                    "PLQ302",
                    "write",
                    exc,
                    f"expectation '{spec.name}' of model '{node.name}' cannot be evaluated",
                    "write the expr in the dialect of the engine",
                )
        self._fail(
            "PLX101",
            "write",
            exc,
            f"cannot evaluate expectations of model '{node.name}'",
            "this is probably a pipeloom or engine bug; report it with the traceback",
        )

    def _quality_event(
        self, node: PlanNode, unit_id: str, report: QualityReport, quarantined: int
    ) -> None:
        self._bus.emit(
            ModelQualityEvaluated(
                total_rows=report.total_rows,
                results=[r.to_json() for r in report.results],
                rows_dropped=report.rows_dropped,
                rows_quarantined=quarantined,
                drop_ratio=report.drop_ratio,
                cached=report.cached,
                failures=list(report.failures),
            ),
            unit_id=unit_id,
            model=node.name,
        )

    def _write_quarantine(self, node: PlanNode, target: TargetRef, rows: Relation) -> None:
        """Append the dropped rows to `<model>__quarantine` under its own token. Its schema
        policy is append_new_columns, so a changed model output never blocks the quarantine."""
        qt = quarantine_target(target)
        policy = "append_new_columns"
        try:
            columns, diff, action = resolve_columns(
                policy,
                self._catalog.table_schema(qt),
                self._session.schema(rows),
                strategy="append",
                full_refresh=False,
            )
            if not diff.empty and action != "none":
                self._schema_changed(node, qt, policy, action, diff.to_json())
            token = quarantine_token(f"{self._rt.run_id}:{node.name}")
            self._catalog.ensure_namespace(qt.namespace)
            self._session.materializer("append").materialize(
                self._session.align(rows, columns),
                qt,
                WriteConfig("append", columns=columns),
                token,
            )
        except SchemaRejected as exc:
            self._schema_changed(node, qt, policy, "rejected", exc.diff.to_json())
            self._fail(
                "PLM202",
                "write",
                exc,
                f"schema change rejected for quarantine table '{qt.relation_name}' "
                f"of model '{node.name}'",
                "a quarantine column changed type incompatibly; rename or drop the old "
                "quarantine table, or keep the model output types stable",
            )
        except Exception as exc:  # noqa: BLE001 - engines raise their own errors
            self._fail(
                "PLM201",
                "write",
                exc,
                f"cannot write quarantine table '{qt.relation_name}' for model '{node.name}'",
                "check that the target can be written, then run again",
            )

    # -- writing ------------------------------------------------------------------------------

    def _write(
        self,
        node: PlanNode,
        target: TargetRef,
        strategy: str,
        rel: Relation,
        row: dict[str, Any],
        before_write: Callable[[], None] | None = None,
    ) -> WriteResult:
        write = node.config.write
        token = f"{self._rt.run_id}:{node.name}"
        t0 = self._rt.clock.monotonic()
        try:
            batch = self._session.schema(rel)
            self._catalog.ensure_namespace(target.namespace)
            policy = write.on_schema_change
            columns, diff, action = resolve_columns(
                policy,
                self._catalog.table_schema(target),
                batch,
                strategy=strategy,
                full_refresh=self._rt.full_refresh,
                keys=(*(write.unique_key or ()), *node.config.partition_by),
            )
            if not diff.empty and action != "none":
                row["schema_changes"] = changes_json(policy, diff, action)
                self._schema_changed(node, target, policy, action, diff.to_json())
            row["schema_hash"] = schema_hash((c.name, c.dtype, c.nullable) for c in columns)
            same = [(c.name, c.dtype) for c in batch] == [(c.name, c.dtype) for c in columns]
            keep = same and action in ("none", "replaced")
            if not keep:
                rel = self._session.align(rel, columns)
            deletes = (
                None
                if write.deletes is None
                else DeleteSpec(write.deletes.column, write.deletes.value, write.deletes.mode)
            )
            cfg = WriteConfig(
                strategy,
                unique_key=tuple(write.unique_key or ()),
                partition_by=tuple(node.config.partition_by),
                update_condition=write.update_condition,
                deletes=deletes,
                columns=None if keep else columns,
                options=dict(write.options),
            )
            if before_write is not None:
                before_write()  # step 8: quarantine rows, once the schema policy accepted
            result = self._session.materializer(strategy).materialize(rel, target, cfg, token)
        except _Fail:
            raise
        except SchemaRejected as exc:
            row["schema_changes"] = changes_json(exc.policy, exc.diff, "rejected")
            self._schema_changed(node, target, exc.policy, "rejected", exc.diff.to_json())
            self._fail(
                "PLM202",
                "write",
                exc,
                f"schema change rejected for model '{node.name}' (on_schema_change: {exc.policy})",
                "change the model, or set write.on_schema_change to ignore, append_new_columns "
                "or sync_all_columns",
            )
        except WriteKeyError as exc:
            self._fail(
                "PLM206",
                "write",
                exc,
                f"invalid write key for model '{node.name}'",
                "check write.unique_key and partition_by, and that the batch has no duplicate key",
            )
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

    def _schema_changed(
        self,
        node: PlanNode,
        target: TargetRef,
        policy: str,
        action: Literal["applied", "ignored", "rejected", "replaced"],
        changes: list[dict[str, JsonValue]],
    ) -> None:
        self._bus.emit(
            ModelSchemaChanged(
                relation=target.relation_name, policy=policy, action=action, changes=changes
            ),
            unit_id=node.name,
            model=node.name,
        )

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
