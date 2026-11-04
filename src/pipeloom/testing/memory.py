"""An in-memory engine and catalog for tests and the contract suites.

Rows are tuples of dicts. SQL runs on stdlib sqlite3, so queries and the `expr` and
`update_condition` dialect strings use the sqlite dialect. A `root` directory makes tables
survive the process as JSON files (values must be JSON-safe), which lets tests share data
between a parent and the child processes it starts. Quality counts, drops, merge, replace and
align come from `pipeloom.testing.reference`.
"""

from __future__ import annotations

import json
import os
import sqlite3
import threading
import time
import uuid
from collections.abc import Callable, Iterator, Mapping, MutableMapping, Sequence
from contextlib import AbstractContextManager, contextmanager
from dataclasses import replace
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING, Any
from urllib.parse import quote, unquote

from pydantic import Field

from pipeloom.config.types import ProjectPath
from pipeloom.engine.portable import rewrite_qualified
from pipeloom.engine.protocols import Catalog, Engine, EngineSession, Materializer, Reader
from pipeloom.engine.types import (
    Column,
    DropResult,
    EngineCapabilities,
    ExpectationSpec,
    Namespace,
    QualityCounts,
    QuarantineMeta,
    ReadSpec,
    Relation,
    Schema,
    TargetRef,
    WriteConfig,
    WriteResult,
)
from pipeloom.plugins import API_VERSION, OptionsModel, PluginSpec
from pipeloom.testing import reference

if TYPE_CHECKING:
    from pipeloom.runtime.context import RuntimeContext

Rows = tuple[dict[str, Any], ...]
Store = MutableMapping[str, Rows]
Tokens = dict[tuple[str, str], WriteResult]
Views = dict[str, tuple[str, Mapping[str, ReadSpec]]]
Cancels = dict[str, threading.Event]

CAPABILITIES = EngineCapabilities(
    formats=frozenset({"memory"}),
    strategies=frozenset({"append", "overwrite", "merge", "replace_partitions"}),
    materializations=frozenset({"table", "view", "ephemeral", "incremental"}),
    sql_models=True,
    views=True,
    idempotent_writes=True,
    cancel=True,
    quality=True,
)


class Cancelled(Exception):
    """Raised by `slow_relation` when its unit is cancelled."""


def _dtype(value: Any) -> str:
    """The logical type of a Python value. A Decimal is `decimal(18,s)`: the precision is a fixed
    default and the scale comes from the value."""
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, datetime):
        return "timestamp[us]" if value.tzinfo is None else "timestamp[us, UTC]"
    if isinstance(value, date):
        return "date"
    if isinstance(value, Decimal):
        exponent = value.as_tuple().exponent
        return f"decimal(18,{max(0, -exponent) if isinstance(exponent, int) else 0})"
    if isinstance(value, int):
        return "int64"
    if isinstance(value, float):
        return "float64"
    if isinstance(value, bytes):
        return "binary"
    if isinstance(value, list | tuple):
        return f"list<{_dtype(value[0]) if value else 'string'}>"
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


def _sql_value(value: Any) -> Any:
    if isinstance(value, datetime | date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, list | tuple | dict):
        return json.dumps(value, default=str)
    return value


def _sql_predicate(expr: str, *, qualified: bool = False) -> Callable[..., bool | None]:
    """A Python predicate that runs `expr` on sqlite, one row (or source and target) per call."""
    sql = rewrite_qualified(expr) if qualified else expr

    def run(*rows: Mapping[str, Any]) -> bool | None:
        values: dict[str, Any] = {}
        if qualified:
            source, target = rows
            values.update({f"__pipeloom_t_{k}": v for k, v in target.items()})
            values.update(source)
        else:
            values.update(rows[0])
        cols = ", ".join(f'? as "{k}"' for k in values) or "1 as _empty"
        conn = sqlite3.connect(":memory:")
        try:
            row = conn.execute(
                f"select ({sql}) from (select {cols})", [_sql_value(v) for v in values.values()]
            ).fetchone()
        finally:
            conn.close()
        return None if row[0] is None else bool(row[0])

    return run


def _exprs(checks: Sequence[ExpectationSpec]) -> dict[str, reference.RowPredicate]:
    return {c.name: _sql_predicate(str(c.params["expr"])) for c in checks if c.kind == "expr"}


def _json_default(value: Any) -> Any:
    """Dates and decimals are stored as text: a file-backed table reads them back as strings."""
    if isinstance(value, datetime | date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


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
        tmp.write_bytes(json.dumps(list(rows), ensure_ascii=True, default=_json_default).encode("utf-8"))
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
    def __init__(self, strategy: str, store: Store, tokens: Tokens, lock: threading.RLock) -> None:
        self.strategy = strategy
        self._store = store
        self._tokens = tokens
        self._lock = lock

    def materialize(
        self, rel: Relation, target: TargetRef, cfg: WriteConfig, token: str
    ) -> WriteResult:
        key = target.relation_name
        with self._lock:
            applied = self._tokens.get((key, token))
            if applied is not None:
                return replace(applied, skipped=True)
            new = tuple(dict(r) for r in _rows(rel))  # read it all first: a failure keeps the table
            if cfg.columns is not None:
                new = tuple(reference.align_rows(new, cfg.columns))
            current = self._store.get(key, ())
            written = len(new)
            inserted: int | None = len(new)
            updated = deleted = None
            if self.strategy == "append":
                rows = (*current, *new)
            elif self.strategy == "overwrite":
                rows = new
            elif self.strategy == "merge":
                cond = (
                    None
                    if cfg.update_condition is None
                    else _sql_predicate(cfg.update_condition, qualified=True)
                )
                merged = reference.merge_rows(current, new, cfg, condition=cond)
                rows = tuple(merged.rows)
                inserted, updated, deleted = merged.inserted, merged.updated, merged.deleted
                written = reference.merge_written(merged, cfg)
            else:
                swapped, removed = reference.replace_partitions_rows(current, new, cfg.partition_by)
                rows, deleted = tuple(swapped), removed
            if cfg.columns is not None and self.strategy != "overwrite":
                rows = tuple(reference.align_rows(rows, cfg.columns))
            self._store[key] = rows
            result = WriteResult(
                rows_written=written,
                rows_inserted=inserted,
                rows_updated=updated,
                rows_deleted=deleted,
                commit_meta=dict(cfg.commit_meta),
            )
            self._tokens[(key, token)] = result
            return result


class MemorySession:
    native: Any

    def __init__(
        self,
        store: Store,
        tokens: Tokens,
        views: Views | None = None,
        *,
        cancels: Cancels | None = None,
        lock: threading.RLock | None = None,
    ) -> None:
        self._store = store
        self._tokens = tokens
        self._views: Views = {} if views is None else views
        self._cancels: Cancels = {} if cancels is None else cancels
        self._lock = threading.RLock() if lock is None else lock
        self._local = threading.local()
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
        return _MemoryMaterializer(strategy, self._store, self._tokens, self._lock)

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

    def filter_since(
        self, rel: Relation, column: str, lower: Any, *, inclusive: bool = True
    ) -> Relation:
        """Rows whose column is at least `lower` (above it when not inclusive); nulls drop."""
        kept = tuple(
            r
            for r in _rows(rel)
            if not reference.is_null(r.get(column))
            and (r[column] >= lower if inclusive else r[column] > lower)
        )
        return Relation(kept, "memory")

    def max_value(self, rel: Relation, column: str) -> Any:
        values = [r[column] for r in _rows(rel) if not reference.is_null(r.get(column))]
        return max(values) if values else None

    def evaluate(self, rel: Relation, checks: Sequence[ExpectationSpec]) -> QualityCounts:
        return reference.evaluate_rows(_rows(rel), checks, exprs=_exprs(checks))

    def apply_drops(
        self, rel: Relation, checks: Sequence[ExpectationSpec], *, quarantine: QuarantineMeta | None
    ) -> DropResult:
        kept, dropped = reference.apply_drops_rows(
            _rows(rel), checks, quarantine, exprs=_exprs(checks)
        )
        return DropResult(
            Relation(tuple(kept), "memory"),
            None if dropped is None else Relation(tuple(dropped), "memory"),
        )

    def align(self, rel: Relation, schema: Schema) -> Relation:
        return Relation(tuple(reference.align_rows(_rows(rel), schema)), "memory")

    def applied_token(self, target: TargetRef, token: str) -> WriteResult | None:
        with self._lock:
            result = self._tokens.get((target.relation_name, token))
        return None if result is None else replace(result, skipped=True)

    @contextmanager
    def _scope(self, unit_id: str) -> Iterator[None]:
        event = threading.Event()
        with self._lock:
            self._cancels[unit_id] = event
        previous = getattr(self._local, "event", None)
        self._local.event = event
        try:
            yield
        finally:
            self._local.event = previous
            with self._lock:
                if self._cancels.get(unit_id) is event:
                    del self._cancels[unit_id]

    def unit_scope(self, unit_id: str) -> AbstractContextManager[None]:
        return self._scope(unit_id)

    def is_transient(self, exc: BaseException) -> bool | None:
        return None

    def cancel(self, unit_id: str) -> None:
        with self._lock:
            event = self._cancels.get(unit_id)
        if event is not None:
            event.set()

    def current_cancel_event(self) -> threading.Event | None:
        """The cancel flag of the calling thread's unit scope, if it is inside one."""
        event: threading.Event | None = getattr(self._local, "event", None)
        return event


def slow_relation(session: EngineSession, seconds: float) -> Relation:
    """Block up to `seconds`, polling the cancel flag of the calling thread's unit scope; raise
    `Cancelled` when `session.cancel` fires. Use it as `EngineHarness.slow_relation`."""
    assert isinstance(session, MemorySession)
    event = session.current_cancel_event()
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if event is None:
            time.sleep(0.01)
        elif event.wait(0.01):
            raise Cancelled("unit cancelled")
    return Relation(({"a": 1},), "memory")


class MemoryEngine:
    name = "memory"
    version = "1"
    capabilities = CAPABILITIES

    def __init__(self, store: Store | None = None) -> None:
        self.store: Store = {} if store is None else store
        self._tokens: Tokens = {}
        self._views: Views = {}
        self._cancels: Cancels = {}
        self._lock = threading.RLock()

    @contextmanager
    def session(self, ctx: RuntimeContext, *, catalog: Catalog) -> Iterator[EngineSession]:
        yield MemorySession(
            self.store, self._tokens, self._views, cancels=self._cancels, lock=self._lock
        )


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
    "Cancelled",
    "JsonTableStore",
    "MemoryCatalog",
    "MemoryEngine",
    "MemorySession",
    "infer_schema",
    "slow_relation",
]
