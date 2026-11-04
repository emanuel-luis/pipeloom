"""The pandas engine: eager DataFrames, tables on the filesystem catalog. No SQL in 0.2."""

from __future__ import annotations

import ast
import json
import tokenize
from collections.abc import Iterator, Mapping, Sequence
from contextlib import AbstractContextManager, contextmanager, nullcontext
from dataclasses import replace
from datetime import UTC
from io import StringIO
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pandas as pd
import pyarrow as pa

from pipeloom.builtins import fs_write
from pipeloom.builtins.filesystem import FilesystemCatalog
from pipeloom.builtins.fs_table import FsTable
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
from pipeloom_pandas import io
from pipeloom_pandas.spec import CAPABILITIES

if TYPE_CHECKING:
    from pipeloom.runtime.context import RuntimeContext

NAME = "pandas"


def _rel(frame: pd.DataFrame) -> Relation:
    return Relation(frame, NAME, lazy=False)


class _Reader:
    def __init__(self, session: PandasSession, fmt: str) -> None:
        self._session = session
        self._fmt = fmt

    def read(self, spec: ReadSpec) -> Relation:
        if self._fmt == "table":
            if spec.table is None:
                raise ValueError("a table read needs a table")
            return self._session.read_table(spec.table)
        if spec.location is None:
            raise ValueError(f"reading {self._fmt} needs a location")
        return _rel(io.read(self._fmt, spec.location, spec.options))


_ALLOWED = (
    ast.Expression,
    ast.BoolOp,
    ast.And,
    ast.Or,
    ast.UnaryOp,
    ast.Not,
    ast.USub,
    ast.UAdd,
    ast.Invert,
    ast.BinOp,
    ast.Add,
    ast.Sub,
    ast.Mult,
    ast.Div,
    ast.FloorDiv,
    ast.Mod,
    ast.Pow,
    ast.BitAnd,
    ast.BitOr,
    ast.Compare,
    ast.Eq,
    ast.NotEq,
    ast.Lt,
    ast.LtE,
    ast.Gt,
    ast.GtE,
    ast.In,
    ast.NotIn,
    ast.Name,
    ast.Load,
    ast.Constant,
    ast.Tuple,
    ast.List,
)


def check_expr(expr: str) -> None:
    """Allow only column names, literals, comparisons, boolean and arithmetic operators.

    `DataFrame.eval` reaches Python locals and globals through `@name` and calls methods, so an
    expression from a config file could run code. Reject `@`, calls and attribute access.
    """
    for tok in tokenize.generate_tokens(StringIO(expr).readline):
        if tok.type == tokenize.OP and tok.string == "@":
            raise ValueError("'@' is not allowed in an expression")
    tree = ast.parse(expr.strip(), mode="eval")
    for node in ast.walk(tree):
        if not isinstance(node, _ALLOWED):
            raise TypeError(f"{type(node).__name__} is not allowed in an expression")


def eval_series(frame: pd.DataFrame, expr: str) -> pd.Series:
    """Evaluate a checked expression against the columns of `frame` only."""
    check_expr(expr)
    result = frame.eval(expr, engine="python", local_dict={}, global_dict={}, resolvers=())
    if not isinstance(result, pd.Series):
        raise TypeError(f"expression '{expr}' is not a column expression")
    return result


def _eval_hook(frame: pd.DataFrame, expr: str) -> pd.Series:
    return eval_series(frame, expr)


class _Ops:
    """The pandas side of `pipeloom.builtins.fs_write.FrameOps`."""

    def collect(self, native: pd.DataFrame) -> pd.DataFrame:
        return native

    def read_file(self, path: Path, fmt: str, schema: Schema) -> pd.DataFrame:
        return io.read_part(fmt, path, schema)

    def align(self, native: pd.DataFrame, schema: Schema) -> pd.DataFrame:
        return io.align_frame(native, schema)

    def concat(self, frames: Sequence[pd.DataFrame]) -> pd.DataFrame:
        return io.concat(frames)

    def write(self, native: pd.DataFrame, fmt: str, path: Path) -> None:
        io.write(native, fmt, path)

    def condition(self, native: pd.DataFrame, condition: str) -> Any:
        return eval_series(native, condition)


_OPS = _Ops()


class _Materializer:
    def __init__(self, session: PandasSession, strategy: str) -> None:
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


class PandasSession:
    """Thread-safe: it holds no mutable state. A model must not mutate its inputs in place."""

    native: Any

    def __init__(self, catalog: Catalog) -> None:
        if not isinstance(catalog, FilesystemCatalog):
            raise TypeError(
                f"the pandas engine needs the filesystem catalog, not {type(catalog).__name__}"
            )
        self.catalog = catalog
        self.native = pd

    # -- reading ------------------------------------------------------------------------------

    def reader(self, fmt: str) -> Reader:
        if fmt != "table" and fmt not in CAPABILITIES.formats:
            raise ValueError(f"pandas engine cannot read format '{fmt}'")
        return _Reader(self, fmt)

    def read_table(self, target: TargetRef) -> Relation:
        table: FsTable = self.catalog.table(target)
        commit = table.current()
        if commit is None:
            raise FileNotFoundError(f"table '{target.relation_name}' does not exist")
        if commit.kind == "view" or commit.format is None:
            raise ValueError(f"'{target.relation_name}' is a view; the pandas engine has none")
        return _rel(fs_write.read_table(table, _OPS, commit))

    # -- writing ------------------------------------------------------------------------------

    def materializer(self, strategy: str) -> Materializer:
        if strategy not in CAPABILITIES.strategies:
            raise ValueError(f"pandas engine does not support strategy '{strategy}'")
        return _Materializer(self, strategy)

    def create_view(
        self, target: TargetRef, query: str, inputs: Mapping[str, ReadSpec]
    ) -> WriteResult:
        raise NotImplementedError("the pandas engine has no views")

    # -- relations ----------------------------------------------------------------------------

    def sql(self, query: str, inputs: Mapping[str, Relation]) -> Relation:
        raise NotImplementedError("the pandas engine has no SQL in 0.2")

    def relation(self, native: Any) -> Relation:
        if not isinstance(native, pd.DataFrame):
            raise TypeError(f"the pandas engine takes a DataFrame, not {type(native).__name__}")
        return _rel(native)

    def persist(self, rel: Relation) -> Relation:
        return rel  # already in memory

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
        return portable.evaluate(rel.native, checks, dialect_hook=_eval_hook)

    def apply_drops(
        self, rel: Relation, checks: Sequence[ExpectationSpec], *, quarantine: QuarantineMeta | None
    ) -> DropResult:
        split = portable.split_drops(rel.native, checks, dialect_hook=_eval_hook)
        kept = _rel(split.kept.reset_index(drop=True))
        if quarantine is None:
            return DropResult(kept, None)
        df: pd.DataFrame = split.dropped.reset_index(drop=True)
        flags = [df[f].astype(bool).tolist() for f in split.flags]
        names = [
            [n for n, hit in zip(split.names, row, strict=True) if hit]
            for row in zip(*flags, strict=True)
        ]
        if self.catalog.format == "csv":  # a csv cell cannot hold a list: JSON text instead
            failed = pd.Series(
                [json.dumps(n, separators=(",", ":")) for n in names],
                index=df.index,
                dtype=pd.ArrowDtype(pa.string()),
            )
        else:
            failed = pd.Series(names, index=df.index, dtype=pd.ArrowDtype(pa.list_(pa.string())))
        n = len(df)
        ts = quarantine.ts.astimezone(UTC)
        df = df.drop(columns=split.flags)
        df["_pipeloom_run_id"] = pd.Series(
            [quarantine.run_id] * n, index=df.index, dtype=pd.ArrowDtype(pa.string())
        )
        df["_pipeloom_failed"] = failed
        df["_pipeloom_ts"] = pd.Series(
            [ts] * n, index=df.index, dtype=pd.ArrowDtype(pa.timestamp("us", tz="UTC"))
        )
        return DropResult(kept, _rel(df))

    def applied_token(self, target: TargetRef, token: str) -> WriteResult | None:
        replay = self.catalog.table(target).applied(token)
        return None if replay is None else replace(replay, skipped=True)

    def write_intermediate(self, rel: Relation, ref: IntermediateRef, token: str) -> WriteResult:
        raise NotImplementedError("intermediate models arrive with the next change")

    def unit_scope(self, unit_id: str) -> AbstractContextManager[None]:
        return nullcontext()

    def is_transient(self, exc: BaseException) -> bool | None:
        return None

    def cancel(self, unit_id: str) -> None:
        return None


class PandasEngine:
    name = NAME
    capabilities = CAPABILITIES

    def __init__(self) -> None:
        self.version = pd.__version__

    @contextmanager
    def session(self, ctx: RuntimeContext, *, catalog: Catalog) -> Iterator[EngineSession]:
        yield PandasSession(catalog)
