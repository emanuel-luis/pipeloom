"""An in-memory engine and catalog for tests and the contract suites.

Rows are tuples of dicts. SQL runs on stdlib sqlite3, so queries use the sqlite dialect.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator, Mapping, MutableMapping, Sequence
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any

from pydantic import Field

from pipeloom.engine.protocols import Catalog, Engine, EngineSession, Materializer, Reader
from pipeloom.engine.types import (
    Column,
    EngineCapabilities,
    Namespace,
    ReadSpec,
    Relation,
    Schema,
    TargetRef,
    WriteConfig,
    WriteResult,
)
from pipeloom.plugins import API_VERSION, OptionsModel, PluginSpec

if TYPE_CHECKING:
    from pipeloom.runtime.context import RuntimeContext

Rows = tuple[dict[str, Any], ...]
Store = MutableMapping[str, Rows]

CAPABILITIES = EngineCapabilities(
    formats=frozenset({"memory"}),
    strategies=frozenset({"append", "overwrite"}),
    materializations=frozenset({"table", "view", "ephemeral"}),
    sql_models=True,
    idempotent_writes=True,
)


def _dtype(value: Any) -> str:
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "int64"
    if isinstance(value, float):
        return "float64"
    if isinstance(value, bytes):
        return "binary"
    return "string"


def infer_schema(rows: Rows) -> Schema:
    """Columns in first-seen order; the type comes from the first non-null value."""
    types: dict[str, str | None] = {}
    for row in rows:
        for key, value in row.items():
            if types.get(key) is None:
                types[key] = None if value is None else _dtype(value)
    return tuple(Column(k, t or "string") for k, t in types.items())


def _rows(rel: Relation) -> Rows:
    rows: Rows = rel.native
    return rows


class _MemoryReader:
    def __init__(self, store: Store) -> None:
        self._store = store

    def read(self, spec: ReadSpec) -> Relation:
        key = spec.location if spec.location is not None else None
        if key is None and spec.table is not None:
            key = spec.table.relation_name
        if key is None or key not in self._store:
            raise KeyError(f"no in-memory table '{key}'")
        return Relation(self._store[key], "memory")


class _MemoryMaterializer:
    def __init__(self, strategy: str, store: Store, tokens: set[tuple[str, str]]) -> None:
        self.strategy = strategy
        self._store = store
        self._tokens = tokens

    def materialize(
        self, rel: Relation, target: TargetRef, cfg: WriteConfig, token: str
    ) -> WriteResult:
        key = target.relation_name
        if (key, token) in self._tokens:
            return WriteResult(skipped=True)
        new = _rows(rel)
        if self.strategy == "append":
            self._store[key] = (*self._store.get(key, ()), *new)
        else:
            self._store[key] = new
        self._tokens.add((key, token))
        return WriteResult(rows=len(new))


class MemorySession:
    native: Any

    def __init__(self, store: Store, tokens: set[tuple[str, str]]) -> None:
        self._store = store
        self._tokens = tokens
        self.native = store

    def reader(self, fmt: str) -> Reader:
        if fmt not in ("memory", "table"):
            raise ValueError(f"memory engine cannot read format '{fmt}'")
        return _MemoryReader(self._store)

    def materializer(self, strategy: str) -> Materializer:
        if strategy not in CAPABILITIES.strategies:
            raise ValueError(f"memory engine does not support strategy '{strategy}'")
        return _MemoryMaterializer(strategy, self._store, self._tokens)

    def sql(self, query: str, inputs: Mapping[str, Relation]) -> Relation:
        conn = sqlite3.connect(":memory:")
        try:
            for alias, rel in inputs.items():
                rows = _rows(rel)
                cols = [c.name for c in infer_schema(rows)] or ["_empty"]
                quoted = ", ".join(f'"{c}"' for c in cols)
                conn.execute(f'CREATE TABLE "{alias}" ({quoted})')
                marks = ", ".join("?" for _ in cols)
                conn.executemany(
                    f'INSERT INTO "{alias}" VALUES ({marks})',
                    [tuple(r.get(c) for c in cols) for r in rows],
                )
            cur = conn.execute(query)
            names = [d[0] for d in cur.description or ()]
            out: Rows = tuple(dict(zip(names, row, strict=True)) for row in cur.fetchall())
        finally:
            conn.close()
        return Relation(out, "memory")

    def schema(self, rel: Relation) -> Schema:
        return infer_schema(_rows(rel))

    def count(self, rel: Relation) -> int:
        return len(_rows(rel))

    def filter_since(self, rel: Relation, column: str, lower: Any) -> Relation:
        """Rows whose column is at least `lower`; nulls are dropped."""
        kept = tuple(r for r in _rows(rel) if r.get(column) is not None and r[column] >= lower)
        return Relation(kept, "memory")

    def max_value(self, rel: Relation, column: str) -> Any:
        values = [r[column] for r in _rows(rel) if r.get(column) is not None]
        return max(values) if values else None

    def cancel(self, unit_id: str) -> None:
        return None


class MemoryEngine:
    name = "memory"
    version = "1"
    capabilities = CAPABILITIES

    def __init__(self, store: Store | None = None) -> None:
        self.store: Store = {} if store is None else store
        self._tokens: set[tuple[str, str]] = set()

    @contextmanager
    def session(self, ctx: RuntimeContext) -> Iterator[EngineSession]:
        yield MemorySession(self.store, self._tokens)


class MemoryCatalog:
    """Knows the tables of a store. Share the store with a MemoryEngine to see its writes."""

    def __init__(self, store: Store | None = None) -> None:
        self.store: Store = {} if store is None else store
        self.namespaces: set[Namespace] = set()

    def exists(self, target: TargetRef) -> bool:
        return target.relation_name in self.store

    def ensure_namespace(self, ns: Namespace) -> None:
        self.namespaces.add(ns)

    def table_schema(self, target: TargetRef) -> Schema | None:
        rows = self.store.get(target.relation_name)
        return None if rows is None else infer_schema(rows)


class MemoryEngineOptions(OptionsModel):
    tables: dict[str, list[dict[str, Any]]] = Field(default_factory=dict)
    """Tables to start with, by name; each is a list of rows."""


class MemoryCatalogOptions(OptionsModel):
    pass


def _engine(opts: MemoryEngineOptions) -> Engine:
    return MemoryEngine({k: tuple(v) for k, v in opts.tables.items()})


def _catalog(_opts: MemoryCatalogOptions) -> Catalog:
    return MemoryCatalog()


SPEC: PluginSpec[Engine] = PluginSpec(
    "memory",
    "memory",
    "engines",
    API_VERSION,
    _engine,
    MemoryEngineOptions,
    capabilities=CAPABILITIES,
    description="In-memory engine for tests; SQL runs on sqlite3.",
)
CATALOG_SPEC: PluginSpec[Catalog] = PluginSpec(
    "memory",
    "memory",
    "catalogs",
    API_VERSION,
    _catalog,
    MemoryCatalogOptions,
    compatible_engines=frozenset({"memory"}),
    description="In-memory catalog for tests.",
)

__all__: Sequence[str] = [
    "CAPABILITIES",
    "CATALOG_SPEC",
    "SPEC",
    "MemoryCatalog",
    "MemoryEngine",
    "MemorySession",
    "infer_schema",
]
