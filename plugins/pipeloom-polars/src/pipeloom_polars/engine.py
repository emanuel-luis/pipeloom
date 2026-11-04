"""The polars engine: lazy frames, SQL on `pl.SQLContext`, tables on the filesystem catalog."""

from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from contextlib import AbstractContextManager, contextmanager, nullcontext
from dataclasses import replace
from datetime import UTC
from pathlib import Path
from typing import TYPE_CHECKING, Any

import polars as pl

from pipeloom.builtins import fs_write
from pipeloom.builtins.filesystem import FilesystemCatalog
from pipeloom.builtins.fs_table import FsTable, ViewDefinition
from pipeloom.engine import portable
from pipeloom.engine.protocols import Catalog, EngineSession, Materializer, Reader
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
from pipeloom.platform import SystemClock
from pipeloom_polars import io
from pipeloom_polars.spec import CAPABILITIES

if TYPE_CHECKING:
    from pipeloom.runtime.context import RuntimeContext

NAME = "polars"


def _rel(frame: pl.LazyFrame) -> Relation:
    return Relation(frame, NAME, lazy=True)


class _Reader:
    def __init__(self, session: PolarsSession, fmt: str) -> None:
        self._session = session
        self._fmt = fmt

    def read(self, spec: ReadSpec) -> Relation:
        if self._fmt == "intermediate":
            if spec.location is None:
                raise ValueError("reading an intermediate needs a location")
            return self._session.read_intermediate(spec.location)
        if self._fmt == "table":
            if spec.table is None:
                raise ValueError("a table read needs a table")
            return self._session.read_table(spec.table)
        if spec.location is None:
            raise ValueError(f"reading {self._fmt} needs a location")
        return _rel(io.scan(self._fmt, spec.location, spec.options))


class _Ops:
    """The polars side of `pipeloom.builtins.fs_write.FrameOps`."""

    def collect(self, native: Any) -> pl.DataFrame:
        return native.collect() if isinstance(native, pl.LazyFrame) else native

    def read_file(self, path: Path, fmt: str, schema: Schema) -> pl.LazyFrame:
        return io.scan_part(fmt, path, schema)

    def align(self, native: Any, schema: Schema) -> Any:
        return io.align_frame(native, schema)

    def concat(self, frames: Sequence[Any]) -> pl.LazyFrame:
        return io.concat(frames)

    def write(self, native: pl.DataFrame, fmt: str, path: Path) -> None:
        io.write(native, fmt, path)

    def condition(self, native: pl.DataFrame, condition: str) -> pl.Series:
        return native.select(pl.sql_expr(condition).alias("c")).to_series()


_OPS = _Ops()


def _sql_hook(frame: pl.DataFrame, expr: str) -> pl.Series:
    return frame.select(pl.sql_expr(expr).alias("c")).to_series()


class _Materializer:
    def __init__(self, session: PolarsSession, strategy: str) -> None:
        self.strategy = strategy
        self._session = session

    def materialize(
        self, rel: Relation, target: TargetRef, cfg: WriteConfig, token: str
    ) -> WriteResult:
        catalog = self._session.catalog
        return fs_write.materialize(
            catalog.table(target),
            _OPS,
            catalog.format,
            io.EXTENSIONS[catalog.format],
            rel.native,
            replace(cfg, strategy=self.strategy),
            token,
        )


class PolarsSession:
    """Thread-safe: it holds no mutable state, and a LazyFrame is immutable."""

    native: Any

    def __init__(self, catalog: Catalog) -> None:
        if not isinstance(catalog, FilesystemCatalog):
            raise TypeError(
                f"the polars engine needs the filesystem catalog, not {type(catalog).__name__}"
            )
        self.catalog = catalog
        self.native = pl

    # -- reading ------------------------------------------------------------------------------

    def reader(self, fmt: str) -> Reader:
        if fmt not in ("table", "intermediate") and fmt not in CAPABILITIES.formats:
            raise ValueError(f"polars engine cannot read format '{fmt}'")
        return _Reader(self, fmt)

    def read_table(self, target: TargetRef) -> Relation:
        table: FsTable = self.catalog.table(target)
        commit = table.current()
        if commit is None:
            raise FileNotFoundError(f"table '{target.relation_name}' does not exist")
        if commit.kind == "view":
            assert commit.view is not None
            return self._run_view(commit.view)
        assert commit.format is not None
        return _rel(fs_write.read_table(table, _OPS, commit))

    def read_intermediate(self, location: str) -> Relation:
        table = FsTable(Path(location), clock=SystemClock())
        return _rel(fs_write.read_table(table, _OPS))

    def _run_view(self, view: ViewDefinition) -> Relation:
        inputs = {a: self.reader(s.format).read(s) for a, s in view.inputs.items()}
        return self.sql(view.query, inputs)

    # -- writing ------------------------------------------------------------------------------

    def materializer(self, strategy: str) -> Materializer:
        if strategy not in CAPABILITIES.strategies:
            raise ValueError(f"polars engine does not support strategy '{strategy}'")
        return _Materializer(self, strategy)

    def create_view(
        self, target: TargetRef, query: str, inputs: Mapping[str, ReadSpec]
    ) -> WriteResult:
        view = ViewDefinition(query, dict(inputs))
        rel = self._run_view(view)  # bad SQL fails here
        table = self.catalog.table(target)
        current = table.current()
        if current is not None and current.kind == "view" and current.view == view:
            return current.result  # same definition: a rerun commits nothing
        commit = table.commit(
            mode="view",
            files=[],
            columns=portable.schema(rel.native),
            rows=None,
            token="view",
            result=WriteResult(),
            view=view,
        )
        return commit.result

    # -- relations ----------------------------------------------------------------------------

    def sql(self, query: str, inputs: Mapping[str, Relation]) -> Relation:
        # A fresh context per query: aliases never leak into another one.
        ctx = pl.SQLContext(frames={a: r.native for a, r in inputs.items()}, eager=False)
        frame = ctx.execute(query)
        frame.collect_schema()  # resolve the plan now: an unknown table or column fails here
        return _rel(frame)

    def relation(self, native: Any) -> Relation:
        if isinstance(native, pl.DataFrame):
            native = native.lazy()
        if not isinstance(native, pl.LazyFrame):
            raise TypeError(f"the polars engine takes a LazyFrame, not {type(native).__name__}")
        return _rel(native)

    def persist(self, rel: Relation) -> Relation:
        return _rel(rel.native.collect().lazy())

    def schema(self, rel: Relation) -> Schema:
        return portable.schema(rel.native)

    def count(self, rel: Relation) -> int:
        return portable.count(rel.native)

    def filter_since(
        self, rel: Relation, column: str, lower: Any, *, inclusive: bool = True
    ) -> Relation:
        return _rel(portable.filter_since(rel.native, column, lower, inclusive=inclusive))

    def align(self, rel: Relation, schema: Schema) -> Relation:
        return _rel(io.align_frame(rel.native, schema))

    def max_value(self, rel: Relation, column: str) -> Any:
        return portable.max_value(rel.native, column)

    def evaluate(self, rel: Relation, checks: Sequence[ExpectationSpec]) -> QualityCounts:
        return portable.evaluate(rel.native, checks, dialect_hook=_sql_hook)

    def apply_drops(
        self, rel: Relation, checks: Sequence[ExpectationSpec], *, quarantine: QuarantineMeta | None
    ) -> DropResult:
        split = portable.split_drops(rel.native, checks, dialect_hook=_sql_hook)
        kept = _rel(split.kept.lazy())
        if quarantine is None:
            return DropResult(kept, None)
        failed = pl.concat_list(
            [
                pl.when(pl.col(f)).then(pl.lit(n))
                for n, f in zip(split.names, split.flags, strict=True)
            ]
        ).list.drop_nulls()
        if self.catalog.format == "csv":  # a csv cell cannot hold a list: JSON text instead
            failed = pl.concat_str([pl.lit('["'), failed.list.join('","'), pl.lit('"]')])
        ts = quarantine.ts.astimezone(UTC)
        out = split.dropped.with_columns(
            pl.lit(quarantine.run_id, dtype=pl.String).alias("_pipeloom_run_id"),
            failed.alias("_pipeloom_failed"),
            pl.lit(ts, dtype=pl.Datetime("us", "UTC")).alias("_pipeloom_ts"),
        ).drop(split.flags)
        return DropResult(kept, _rel(out.lazy()))

    def applied_token(self, target: TargetRef, token: str) -> WriteResult | None:
        replay = self.catalog.table(target).applied(token)
        return None if replay is None else replace(replay, skipped=True)

    def write_intermediate(self, rel: Relation, ref: IntermediateRef, token: str) -> WriteResult:
        catalog = self.catalog
        return fs_write.materialize(
            FsTable(Path(ref.location), clock=SystemClock()),
            _OPS,
            catalog.format,
            io.EXTENSIONS[catalog.format],
            rel.native,
            WriteConfig("overwrite"),
            token,
        )

    def unit_scope(self, unit_id: str) -> AbstractContextManager[None]:
        return nullcontext()

    def is_transient(self, exc: BaseException) -> bool | None:
        return None

    def cancel(self, unit_id: str) -> None:
        return None


class PolarsEngine:
    name = NAME
    capabilities = CAPABILITIES

    def __init__(self) -> None:
        self.version = pl.__version__

    @contextmanager
    def session(self, ctx: RuntimeContext, *, catalog: Catalog) -> Iterator[EngineSession]:
        yield PolarsSession(catalog)
