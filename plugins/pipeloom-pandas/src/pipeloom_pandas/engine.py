"""The pandas engine: eager DataFrames, tables on the filesystem catalog. No SQL in 0.2."""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any

import pandas as pd

from pipeloom.builtins.filesystem import FilesystemCatalog
from pipeloom.builtins.fs_table import FsTable
from pipeloom.engine import portable
from pipeloom.engine.protocols import Catalog, EngineSession, Materializer, Reader
from pipeloom.engine.types import ReadSpec, Relation, Schema, TargetRef, WriteConfig, WriteResult
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


class _Materializer:
    def __init__(self, session: PandasSession, strategy: str) -> None:
        self.strategy = strategy
        self._session = session

    def materialize(
        self, rel: Relation, target: TargetRef, cfg: WriteConfig, token: str
    ) -> WriteResult:
        catalog = self._session.catalog
        table = catalog.table(target)
        replay = table.applied(token)
        if replay is not None:
            return replay
        path = table.new_file(io.EXTENSIONS[catalog.format])
        try:
            df: pd.DataFrame = rel.native
            io.write(df, catalog.format, path)
            commit = table.commit(
                mode="append" if self.strategy == "append" else "overwrite",
                files=[path],
                columns=portable.schema(df),
                rows=len(df),
                token=token,
                result=WriteResult(rows_inserted=len(df), rows_written=len(df)),
            )
        except BaseException:
            table.discard([path])
            raise
        return commit.result


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
        return _rel(io.read_parts(commit.format, table.files(), commit.columns))

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

    def filter_since(self, rel: Relation, column: str, lower: Any) -> Relation:
        return _rel(portable.filter_since(rel.native, column, lower))

    def max_value(self, rel: Relation, column: str) -> Any:
        return portable.max_value(rel.native, column)

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
