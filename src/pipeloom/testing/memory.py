"""An in-memory engine and catalog for tests and the contract suites.

Rows are tuples of dicts. SQL runs on stdlib sqlite3, so queries use the sqlite dialect. A
`root` directory makes tables survive the process as JSON files (values must be JSON-safe),
which lets tests share data between a parent and the child processes it starts.
"""

from __future__ import annotations

import json
import os
import sqlite3
import uuid
from collections.abc import Callable, Iterator, Mapping, MutableMapping, Sequence
from contextlib import contextmanager
from pathlib import Path
from typing import TYPE_CHECKING, Any
from urllib.parse import quote, unquote

from pydantic import Field

from pipeloom.config.types import ProjectPath
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
Tokens = dict[tuple[str, str], WriteResult]
Views = dict[str, tuple[str, Mapping[str, ReadSpec]]]

CAPABILITIES = EngineCapabilities(
    formats=frozenset({"memory"}),
    strategies=frozenset({"append", "overwrite"}),
    materializations=frozenset({"table", "view", "ephemeral"}),
    sql_models=True,
    views=True,
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


class JsonTableStore(MutableMapping[str, Rows]):
    """A table store backed by one JSON file per table under `root`."""

    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)

    def _path(self, key: str) -> Path:
        return self.root / f"{quote(key, safe='')}.json"

    def __getitem__(self, key: str) -> Rows:
        try:
            text = self._path(key).read_text(encoding="utf-8")
        except FileNotFoundError:
            raise KeyError(key) from None
        return tuple(json.loads(text))

    def __setitem__(self, key: str, rows: Rows) -> None:
        path = self._path(key)
        tmp = path.with_name(f"{path.name}.{uuid.uuid4().hex}.tmp")
        tmp.write_bytes(json.dumps(list(rows), ensure_ascii=True).encode("utf-8"))
        os.replace(tmp, path)

    def __delitem__(self, key: str) -> None:
        try:
            self._path(key).unlink()
        except FileNotFoundError:
            raise KeyError(key) from None

    def __iter__(self) -> Iterator[str]:
        return (unquote(p.stem) for p in sorted(self.root.glob("*.json")))

    def __len__(self) -> int:
        return sum(1 for _ in self.root.glob("*.json"))


class _MemoryReader:
    def __init__(
        self,
        store: Store,
        views: Views,
        run_view: Callable[[str, Mapping[str, ReadSpec]], Relation],
    ) -> None:
        self._store = store
        self._views = views
        self._run_view = run_view

    def read(self, spec: ReadSpec) -> Relation:
        key = spec.location if spec.location is not None else None
        if key is None and spec.table is not None:
            key = spec.table.relation_name
        if key is not None and key in self._views:
            query, inputs = self._views[key]
            return self._run_view(query, inputs)
        if key is None or key not in self._store:
            raise KeyError(f"no in-memory table '{key}'")
        return Relation(self._store[key], "memory")


class _MemoryMaterializer:
    def __init__(self, strategy: str, store: Store, tokens: Tokens) -> None:
        self.strategy = strategy
        self._store = store
        self._tokens = tokens

    def materialize(
        self, rel: Relation, target: TargetRef, cfg: WriteConfig, token: str
    ) -> WriteResult:
        key = target.relation_name
        applied = self._tokens.get((key, token))
        if applied is not None:
            return applied
        new = _rows(rel)
        if self.strategy == "append":
            self._store[key] = (*self._store.get(key, ()), *new)
        else:
            self._store[key] = new
        result = WriteResult(rows_inserted=len(new), rows_written=len(new))
        self._tokens[(key, token)] = result
        return result


class MemorySession:
    native: Any

    def __init__(self, store: Store, tokens: Tokens, views: Views | None = None) -> None:
        self._store = store
        self._tokens = tokens
        self._views: Views = {} if views is None else views
        self.native = store

    def reader(self, fmt: str) -> Reader:
        if fmt not in ("memory", "table"):
            raise ValueError(f"memory engine cannot read format '{fmt}'")
        return _MemoryReader(self._store, self._views, self._run_view)

    def _run_view(self, query: str, inputs: Mapping[str, ReadSpec]) -> Relation:
        return self.sql(query, {a: self.reader(r.format).read(r) for a, r in inputs.items()})

    def materializer(self, strategy: str) -> Materializer:
        if strategy not in CAPABILITIES.strategies:
            raise ValueError(f"memory engine does not support strategy '{strategy}'")
        return _MemoryMaterializer(strategy, self._store, self._tokens)

    def relation(self, native: Any) -> Relation:
        if not (isinstance(native, tuple | list) and all(isinstance(r, dict) for r in native)):
            raise TypeError(f"the memory engine takes a list of dicts, not {type(native).__name__}")
        return Relation(tuple(native), "memory")

    def persist(self, rel: Relation) -> Relation:
        return rel  # rows are already in memory

    def create_view(
        self, target: TargetRef, query: str, inputs: Mapping[str, ReadSpec]
    ) -> WriteResult:
        self._views[target.relation_name] = (query, dict(inputs))
        return WriteResult()

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
        self._tokens: Tokens = {}
        self._views: Views = {}

    @contextmanager
    def session(self, ctx: RuntimeContext, *, catalog: Catalog) -> Iterator[EngineSession]:
        yield MemorySession(self.store, self._tokens, self._views)


class MemoryCatalog:
    """Knows the tables of a store. Share the store with a MemoryEngine to see its writes.

    With `root` the tables are JSON files in that directory instead of a dict.
    """

    def __init__(self, store: Store | None = None, root: str | Path | None = None) -> None:
        if store is None:
            store = {} if root is None else JsonTableStore(root)
        self.store: Store = store
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
    root: ProjectPath | None = None
    """Keep tables as JSON files in this directory, so other processes see them."""


class MemoryCatalogOptions(OptionsModel):
    root: ProjectPath | None = None
    """The engine's `root`, to see the same tables."""


def _engine(opts: MemoryEngineOptions) -> Engine:
    store: Store = {} if opts.root is None else JsonTableStore(opts.root)
    store.update({k: tuple(v) for k, v in opts.tables.items()})
    return MemoryEngine(store)


def _catalog(opts: MemoryCatalogOptions) -> Catalog:
    return MemoryCatalog(root=opts.root)


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
    "JsonTableStore",
    "MemoryCatalog",
    "MemoryEngine",
    "MemorySession",
    "infer_schema",
]
