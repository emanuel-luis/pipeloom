"""The Spark engine: DataFrames and Spark SQL on a local, Connect or already active session.

Pyspark is imported inside functions, so this module loads without it. One session object serves
every worker thread of an invocation; the only per-thread state is the job tag of a unit.
"""

from __future__ import annotations

import os
import re
import threading
import time
from collections.abc import Iterator, Mapping, Sequence
from contextlib import AbstractContextManager, contextmanager, suppress
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any, Final

from pipeloom.engine.protocols import EngineSession, Materializer, Reader
from pipeloom.engine.types import (
    DropResult,
    ExpectationSpec,
    IntermediateRef,
    QualityCounts,
    QuarantineMeta,
    ReadSpec,
    Relation,
    Schema,
    TargetRef,
    WriteConfig,
    WriteResult,
)
from pipeloom_spark import errors, io, quality, sqltext
from pipeloom_spark.catalog import DeltaPathCatalog, SparkSessionCatalog, schema_of
from pipeloom_spark.session import SessionHandle, open_session
from pipeloom_spark.spec import CAPABILITIES, SparkEngineOptions
from pipeloom_spark.types import spark_ddl
from pipeloom_spark.write import DeltaWriter

if TYPE_CHECKING:
    from pipeloom.engine.protocols import Catalog
    from pipeloom.runtime.context import RuntimeContext

NAME: Final = "spark"
_EPOCH: Final = datetime(1970, 1, 1, tzinfo=UTC)
_INTERRUPT_EVERY: Final = 0.25
_INTERRUPT_ROUNDS: Final = 240
"""A cancel keeps interrupting for up to a minute, or until the unit's scope ends."""
_TAG_CHARS: Final = re.compile(r"[^A-Za-z0-9_.:-]")


def _rel(frame: Any) -> Relation:
    return Relation(frame, NAME, lazy=True)


def _from_micros(value: int | None) -> datetime | None:
    return None if value is None else _EPOCH + timedelta(microseconds=value)


class _Reader:
    def __init__(self, session: SparkEngineSession, fmt: str) -> None:
        self._session = session
        self._fmt = fmt

    def read(self, spec: ReadSpec) -> Relation:
        spark = self._session.spark
        if self._fmt == "table":
            if spec.table is None:
                raise ValueError("a table read needs a table")
            return _rel(self._session.catalog.read(spec.table))
        if spec.location is None:
            raise ValueError(f"reading {self._fmt} needs a location")
        fmt = "delta" if self._fmt == "intermediate" else self._fmt
        return _rel(io.read(spark, fmt, spec.location, spec.options))


class _Materializer:
    def __init__(self, writer: DeltaWriter, strategy: str) -> None:
        self._writer = writer
        self.strategy = strategy

    def materialize(
        self, rel: Relation, target: TargetRef, cfg: WriteConfig, token: str
    ) -> WriteResult:
        return self._writer.materialize(self.strategy, rel, target, cfg, token)


class SparkEngineSession:
    """Thread-safe: `spark` is shared, and a unit's tag and persisted frames are per unit."""

    def __init__(
        self,
        handle: SessionHandle,
        catalog: DeltaPathCatalog | SparkSessionCatalog,
        run_id: str,
        project: str = "pipeloom",
        *,
        txn_guard: bool = True,
    ) -> None:
        self.handle = handle
        self.spark = handle.spark
        self.native: Any = handle.spark
        self.catalog = catalog
        self._writer = DeltaWriter(self, project, txn_guard=txn_guard)
        self._run_id = run_id
        # Classic Spark keeps session tags for Connect operations only: its jobs are tagged and
        # cancelled through the SparkContext, which a Connect session does not have.
        self._classic = "connect" not in type(self.spark).__module__
        self._local = threading.local()
        self._lock = threading.Lock()
        self._active: set[str] = set()
        self._persisted: dict[str | None, list[Any]] = {}

    # -- reading ------------------------------------------------------------------------------

    def reader(self, fmt: str) -> Reader:
        if fmt not in ("table", "intermediate") and fmt not in CAPABILITIES.formats:
            raise ValueError(f"spark engine cannot read format '{fmt}'")
        return _Reader(self, fmt)

    # -- writing --------------------------------------------------------------------------------

    def materializer(self, strategy: str) -> Materializer:
        if strategy not in CAPABILITIES.strategies:
            raise ValueError(f"spark engine does not support strategy '{strategy}'")
        return _Materializer(self._writer, strategy)

    def create_view(
        self, target: TargetRef, query: str, inputs: Mapping[str, ReadSpec]
    ) -> WriteResult:
        if not isinstance(self.catalog, SparkSessionCatalog):
            raise TypeError("views need the spark_catalog catalog")
        ctes = []
        for alias, spec in inputs.items():
            if spec.format == "table" and spec.table is not None:
                physical = self.catalog.ident(spec.table)
            elif spec.format in ("delta", "parquet") and spec.location is not None:
                physical = f"{spec.format}.{sqltext.quote_ident(spec.location)}"
            else:
                raise ValueError(
                    f"input '{alias}' ({spec.format}) cannot back a view: use a table, delta or "
                    "parquet source"
                )
            ctes.append(f"{sqltext.quote_ident(alias)} AS (SELECT * FROM {physical})")
        body = query.strip().rstrip(";").rstrip()
        with_ = f"WITH {', '.join(ctes)} " if ctes else ""
        self.spark.sql(
            f"CREATE OR REPLACE VIEW {self.catalog.ident(target)} AS "
            f"{with_}SELECT * FROM (\n{body}\n)"
        )
        return WriteResult()

    def write_intermediate(self, rel: Relation, ref: IntermediateRef, token: str) -> WriteResult:
        return self._writer.intermediate(rel, ref.location, ref.model, token, ref.run_started_at)

    def applied_token(self, target: TargetRef, token: str) -> WriteResult | None:
        return self._writer.applied(self.catalog.ident(target), token)

    def evaluate(self, rel: Relation, checks: Sequence[ExpectationSpec]) -> QualityCounts:
        return quality.evaluate(rel, checks, self.schema(rel))

    def apply_drops(
        self, rel: Relation, checks: Sequence[ExpectationSpec], *, quarantine: QuarantineMeta | None
    ) -> DropResult:
        kept, dropped = quality.apply_drops(rel, checks, self.schema(rel), quarantine)
        return DropResult(_rel(kept), None if dropped is None else _rel(dropped))

    # -- relations ----------------------------------------------------------------------------

    def sql(self, query: str, inputs: Mapping[str, Relation]) -> Relation:
        template, keywords = sqltext.wrap_inputs(query, list(inputs))
        frames = {keywords[alias]: rel.native for alias, rel in inputs.items()}
        frame = self.spark.sql(template, **frames)
        frame.schema  # noqa: B018 - Connect may analyze lazily: an unknown table fails here
        return _rel(frame)

    def relation(self, native: Any) -> Relation:
        from pyspark.sql import DataFrame

        if not isinstance(native, DataFrame):
            raise TypeError(f"the spark engine takes a DataFrame, not {type(native).__name__}")
        return _rel(native)

    def persist(self, rel: Relation) -> Relation:
        from pyspark.storagelevel import StorageLevel

        frame = rel.native.persist(StorageLevel.MEMORY_AND_DISK)
        with self._lock:
            self._persisted.setdefault(getattr(self._local, "unit", None), []).append(frame)
        return _rel(frame)

    def schema(self, rel: Relation) -> Schema:
        return schema_of(rel.native)

    def count(self, rel: Relation) -> int:
        return int(rel.native.count())

    def _dtype(self, rel: Relation, column: str) -> str:
        for col in self.schema(rel):
            if col.name == column:
                return col.dtype
        raise KeyError(f"no column '{column}'")

    def filter_since(
        self, rel: Relation, column: str, lower: Any, *, inclusive: bool = True
    ) -> Relation:
        from pyspark.sql import functions as F

        col = F.col(sqltext.quote_ident(column))
        bound = F.expr(sqltext.literal(lower, self._dtype(rel, column)))
        return _rel(rel.native.where(col >= bound if inclusive else col > bound))

    def align(self, rel: Relation, schema: Schema) -> Relation:
        from pyspark.sql import functions as F

        have = set(rel.native.columns)
        exprs = []
        for col in schema:
            expr = F.col(sqltext.quote_ident(col.name)) if col.name in have else F.lit(None)
            try:
                expr = expr.cast(spark_ddl(col.dtype))
            except ValueError:  # a type Spark cannot name: keep what the batch has
                pass
            exprs.append(expr.alias(col.name))
        return _rel(rel.native.select(*exprs))

    def max_value(self, rel: Relation, column: str) -> Any:
        from pyspark.sql import functions as F

        col = F.col(sqltext.quote_ident(column))
        if self._dtype(rel, column) == "timestamp[us, UTC]":
            micros = rel.native.agg(F.max(F.expr(f"unix_micros({sqltext.quote_ident(column)})")))
            return _from_micros(micros.first()[0])
        return rel.native.agg(F.max(col)).first()[0]

    # -- units --------------------------------------------------------------------------------

    def _tag(self, unit_id: str) -> str:
        return "pipeloom:" + _TAG_CHARS.sub("_", f"{self._run_id}:{unit_id}")

    @contextmanager
    def _scope(self, unit_id: str) -> Iterator[None]:
        tag = self._tag(unit_id)
        self._add_tag(tag)
        with self._lock:
            self._active.add(tag)
        self._local.unit = unit_id
        try:
            yield
        finally:
            self._local.unit = None
            with self._lock:
                self._active.discard(tag)
            try:
                self._remove_tag(tag)
            finally:
                self._unpersist(unit_id)

    def _add_tag(self, tag: str) -> None:
        if self._classic:
            self.spark.sparkContext.addJobTag(tag)
        else:
            self.spark.addTag(tag)

    def _remove_tag(self, tag: str) -> None:
        if self._classic:
            self.spark.sparkContext.removeJobTag(tag)
        else:
            self.spark.removeTag(tag)

    def _interrupt_tag(self, tag: str) -> None:
        if self._classic:
            self.spark.sparkContext.cancelJobsWithTag(tag)
        else:
            self.spark.interruptTag(tag)

    def unit_scope(self, unit_id: str) -> AbstractContextManager[None]:
        return self._scope(unit_id)

    def _unpersist(self, key: str | None) -> None:
        with self._lock:
            frames = self._persisted.pop(key, [])
        for frame in frames:
            with suppress(Exception):  # cleanup must not hide the unit's own outcome
                frame.unpersist(blocking=False)

    def cancel(self, unit_id: str) -> None:
        """Interrupt the unit's jobs. A job that has not started yet is not seen by the first
        interrupt, so a short-lived thread repeats it until the unit's scope ends."""
        tag = self._tag(unit_id)
        with self._lock:
            if tag not in self._active:
                return
        threading.Thread(target=self._interrupt, args=(tag,), daemon=True).start()

    def _interrupt(self, tag: str) -> None:
        for _ in range(_INTERRUPT_ROUNDS):
            with self._lock:
                if tag not in self._active:
                    return
            with suppress(Exception):  # the session may be gone: the unit ended meanwhile
                self._interrupt_tag(tag)
            time.sleep(_INTERRUPT_EVERY)

    def is_transient(self, exc: BaseException) -> bool | None:
        return errors.is_transient(exc)

    def close(self) -> None:
        for key in list(self._persisted):
            self._unpersist(key)


class SparkEngine:
    name = NAME
    capabilities = CAPABILITIES

    def __init__(self, opts: SparkEngineOptions) -> None:
        self.opts = opts
        try:
            import pyspark

            self.version = str(pyspark.__version__)
        except ImportError:
            self.version = "not installed"

    @contextmanager
    def session(self, ctx: RuntimeContext, *, catalog: Catalog) -> Iterator[EngineSession]:
        if not isinstance(catalog, (DeltaPathCatalog, SparkSessionCatalog)):
            raise TypeError(
                "the spark engine needs the delta_path or spark_catalog catalog, "
                f"not {type(catalog).__name__}"
            )
        with open_session(self.opts, os.environ) as handle, catalog.attach(handle):
            session = SparkEngineSession(
                handle, catalog, ctx.run_id, ctx.project, txn_guard=self.opts.txn_guard
            )
            try:
                yield session
            finally:
                session.close()
