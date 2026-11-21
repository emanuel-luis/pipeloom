"""The DuckDB engine: relations on one connection per session, writes pushed down as SQL inside
one transaction each (ADR-0051)."""

from __future__ import annotations

import itertools
import json
import os
import threading
from collections.abc import Iterator, Mapping, Sequence
from contextlib import AbstractContextManager, contextmanager
from dataclasses import asdict, replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any

import duckdb

from pipeloom.engine.errors import WriteKeyError
from pipeloom.engine.portable import scd2_time
from pipeloom.engine.protocols import Catalog, EngineSession, Materializer, Reader
from pipeloom.engine.types import (
    ROW_CHECKS,
    SCD2_COLUMNS,
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
from pipeloom_duckdb import extensions
from pipeloom_duckdb.catalog import DuckDBCatalog
from pipeloom_duckdb.spec import CAPABILITIES, DuckDBEngineOptions
from pipeloom_duckdb.sqltext import (
    ident,
    is_float,
    literal,
    named_args,
    schema_of,
    string,
    to_duckdb,
    to_logical,
)

if TYPE_CHECKING:
    from pipeloom.runtime.context import RuntimeContext

NAME = "duckdb"
_TSTZ = "TIMESTAMP WITH TIME ZONE"
_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
_SYSTEM = frozenset(c.name for c in SCD2_COLUMNS)
_COMMITS = "commits"
_META_SCHEMA = "_pipeloom"
_INTERMEDIATE_DATA = "data.parquet"
_INTERMEDIATE_TOKEN = "_pipeloom_token.json"


def _q(con: Any, sql: str, params: Sequence[Any] | None = None) -> list[tuple[Any, ...]]:
    """Run one statement and fetch its whole result: DuckDB refuses the next statement on a
    connection while a result is unfinished."""
    out: list[tuple[Any, ...]] = con.execute(sql, params).fetchall()
    return out


def _rel(native: Any) -> Relation:
    return Relation(native, NAME, lazy=True)


def _plain(value: Any, duck: str) -> Any:
    if duck == _TSTZ and value is not None:
        return _EPOCH + timedelta(microseconds=value)
    return value


def _value(alias: str, column: str, duck: str) -> str:
    """A column reference where NaN reads as null."""
    ref = f"{alias}{ident(column)}"
    return f"CASE WHEN isnan({ref}) THEN NULL ELSE {ref} END" if is_float(duck) else ref


class _Reader:
    def __init__(self, session: DuckDBSession, fmt: str) -> None:
        self._session = session
        self._fmt = fmt

    def read(self, spec: ReadSpec) -> Relation:
        return _rel(self._session.query(self._session.source_sql(self._fmt, spec)))


class _Materializer:
    def __init__(self, session: DuckDBSession, strategy: str) -> None:
        self.strategy = strategy
        self._session = session

    def materialize(
        self, rel: Relation, target: TargetRef, cfg: WriteConfig, token: str
    ) -> WriteResult:
        return self._session.write(rel, target, replace(cfg, strategy=self.strategy), token)


def _dump(result: WriteResult) -> str:
    data = asdict(result)
    del data["skipped"]
    return json.dumps(data, sort_keys=True)


def _load(text: str) -> WriteResult:
    return WriteResult(**json.loads(text), skipped=True)


class DuckDBSession:
    """One connection, shared by the unit threads under a lock: DuckDB parallelizes each query
    itself, and a connection must not run two statements at once."""

    def __init__(self, con: Any, catalog: DuckDBCatalog, opts: DuckDBEngineOptions) -> None:
        self.native = con
        self.catalog = catalog
        self._opts = opts
        self._lock = threading.RLock()
        self._names = itertools.count()
        self._loaded: set[str] = set()
        self._pending: extensions.ExtensionMissingError | None = None
        self._local = threading.local()
        self._busy: str | None = None
        self._cancelled: set[str] = set()
        for name in opts.extensions:
            try:
                self.ensure_extension(name)
            except extensions.ExtensionMissingError as exc:
                self._pending = self._pending or exc  # raised on first use: never at open

    # -- the connection ---------------------------------------------------------------------

    @contextmanager
    def _use(self) -> Iterator[Any]:
        unit: str | None = getattr(self._local, "unit", None)
        with self._lock:
            if unit is not None and unit in self._cancelled:
                raise duckdb.InterruptException(f"unit '{unit}' was cancelled")
            prior, self._busy = self._busy, unit
            try:
                yield self.native
            finally:
                self._busy = prior

    def execute(self, sql: str, params: Sequence[Any] | None = None) -> list[tuple[Any, ...]]:
        with self._use() as con:
            return _q(con, sql, params)

    def scalar(self, sql: str, params: Sequence[Any] | None = None) -> Any:
        return self.execute(sql, params)[0][0]

    def rows(self, sql: str, params: Sequence[Any] | None = None) -> list[tuple[Any, ...]]:
        return self.execute(sql, params)

    def query(self, sql: str) -> Any:
        """A lazy relation; binding errors (an unknown table or column) raise here."""
        self._ready()
        with self._use() as con:
            return con.sql(sql)

    @property
    def lock(self) -> threading.RLock:
        return self._lock

    def _ready(self) -> None:
        """Data work fails while a listed extension is missing; catalog lookups do not, so the
        session still opens."""
        if self._pending is not None:
            raise self._pending

    def _name(self, prefix: str) -> str:
        return f"__pipeloom_{prefix}{next(self._names)}"

    def _view(self, rel: Relation | Any) -> str:
        """A temporary view over a relation, under a fresh name. Views live as long as the
        session: a lazy relation built on one reads it again at every execution."""
        self._ready()
        native = rel.native if isinstance(rel, Relation) else rel
        name = self._name("v")
        with self._use():
            native.to_view(name, replace=True)
        return ident(name)

    def ensure_extension(self, name: str) -> None:
        with self._lock:
            if name in self._loaded:
                return
            try:
                _q(self.native, f"LOAD {ident(name)}")
            except duckdb.Error as exc:
                if not self._opts.install_extensions:
                    diag = extensions.missing(name, self._opts.extension_directory)
                    raise extensions.ExtensionMissingError(diag) from exc
                _q(self.native, f"INSTALL {ident(name)}")
                _q(self.native, f"LOAD {ident(name)}")
            self._loaded.add(name)

    # -- reading ----------------------------------------------------------------------------

    def source_sql(self, fmt: str, spec: ReadSpec) -> str:
        """The SELECT that reads a source, a table or an intermediate."""
        if fmt == "table":
            if spec.table is None:
                raise ValueError("a table read needs a table")
            db = self.catalog.database(spec.table.catalog, create=False)
            if db is None:
                raise FileNotFoundError(f"table '{spec.table.relation_name}' does not exist")
            parts = (db, spec.table.schema, spec.table.name)
            return "SELECT * FROM " + ".".join(ident(p) for p in parts)
        if spec.location is None:
            raise ValueError(f"reading {fmt} needs a location")
        location = spec.location
        if fmt == "intermediate":
            return f"SELECT * FROM read_parquet({string(f'{location}/{_INTERMEDIATE_DATA}')})"
        if fmt not in CAPABILITIES.formats:
            raise ValueError(f"duckdb engine cannot read format '{fmt}'")
        args = named_args(spec.options)
        if fmt == "delta":
            self.ensure_extension("delta")
            return f"SELECT * FROM delta_scan({string(location)}{args})"
        if Path(location).is_dir():
            location = f"{location.rstrip('/')}/*.{fmt}"
        function = "read_parquet" if fmt == "parquet" else "read_csv"
        return f"SELECT * FROM {function}({string(location)}{args})"

    def reader(self, fmt: str) -> Reader:
        if fmt not in ("table", "intermediate") and fmt not in CAPABILITIES.formats:
            raise ValueError(f"duckdb engine cannot read format '{fmt}'")
        return _Reader(self, fmt)

    # -- relations --------------------------------------------------------------------------

    def sql(self, query: str, inputs: Mapping[str, Relation]) -> Relation:
        # Each alias is a CTE over a fresh view: aliases never leak into another query.
        ctes = ", ".join(
            f"{ident(a)} AS (SELECT * FROM {self._view(r)})" for a, r in inputs.items()
        )
        body = query.strip().rstrip(";")
        text = (f"WITH {ctes} " if ctes else "") + f"SELECT * FROM (\n{body}\n)"
        return _rel(self.query(text))

    def relation(self, native: Any) -> Relation:
        if not isinstance(native, duckdb.DuckDBPyRelation):
            raise TypeError(
                f"the duckdb engine takes a DuckDBPyRelation, not {type(native).__name__}"
            )
        return _rel(native)

    def _on(self, rel: Relation, template: str) -> Any:
        """`template` with `{r}` naming `rel`, as a relation."""
        return self.query(template.format(r=self._view(rel)))

    def _fetch(self, native: Any) -> list[dict[str, Any]]:
        with self._use():
            names, types = native.columns, [str(t) for t in native.types]
        cols = ", ".join(
            f"epoch_us({ident(c)}) AS {ident(c)}" if t == _TSTZ else ident(c)
            for c, t in zip(names, types, strict=True)
        )
        rows = self.rows(f"SELECT {cols} FROM {self._view(native)}")
        return [
            {c: _plain(v, t) for c, t, v in zip(names, types, row, strict=True)} for row in rows
        ]

    def to_rows(self, rel: Relation) -> list[dict[str, Any]]:
        return self._fetch(rel.native)

    def from_rows(self, rows: Sequence[Mapping[str, Any]], schema: Schema) -> Relation:
        cols = []
        for c in schema:
            duck = to_duckdb(c.dtype)
            if duck is None:
                raise ValueError(f"the duckdb engine cannot hold logical type {c.dtype}")
            cols.append(f"{ident(c.name)} {duck}")
        name = ident(self._name("rows"))
        with self._use() as con:
            _q(con, f"CREATE TEMP TABLE {name} ({', '.join(cols)})")
            if rows:
                marks = ", ".join("?" for _ in schema)
                con.executemany(
                    f"INSERT INTO {name} VALUES ({marks})",
                    [[r.get(c.name) for c in schema] for r in rows],
                )
        return _rel(self.query(f"SELECT * FROM {name}"))

    def persist(self, rel: Relation) -> Relation:
        name = ident(self._name("p"))
        self.execute(f"CREATE TEMP TABLE {name} AS SELECT * FROM {self._view(rel)}")
        return _rel(self.query(f"SELECT * FROM {name}"))

    def schema(self, rel: Relation) -> Schema:
        with self._use():
            return schema_of(rel.native.columns, rel.native.types)

    def _types(self, rel: Relation) -> dict[str, str]:
        with self._use():
            return {c: str(t) for c, t in zip(rel.native.columns, rel.native.types, strict=True)}

    def count(self, rel: Relation) -> int:
        return int(self.scalar(f"SELECT count(*) FROM {self._view(rel)}"))

    def max_value(self, rel: Relation, column: str) -> Any:
        rows = self._fetch(self._on(rel, f"SELECT max({ident(column)}) AS v FROM {{r}}"))
        return rows[0]["v"]

    def filter_since(
        self, rel: Relation, column: str, lower: Any, *, inclusive: bool = True
    ) -> Relation:
        op = ">=" if inclusive else ">"
        return _rel(
            self._on(rel, f"SELECT * FROM {{r}} WHERE {ident(column)} {op} {literal(lower)}")
        )

    def align(self, rel: Relation, schema: Schema) -> Relation:
        have = set(self._types(rel))
        exprs = []
        for c in schema:
            duck = to_duckdb(c.dtype)
            ref = ident(c.name) if c.name in have else "NULL"
            exprs.append(f"{ref if duck is None else f'CAST({ref} AS {duck})'} AS {ident(c.name)}")
        return _rel(self._on(rel, f"SELECT {', '.join(exprs)} FROM {{r}}"))

    def orphans(self, rel: Relation, column: str, parent: Relation, field: str) -> int:
        child = _value("c.", column, self._types(rel)[column])
        key = _value("p.", field, self._types(parent)[field])
        query = (
            f"SELECT count(*) AS n FROM {self._view(rel)} AS c WHERE {child} IS NOT NULL "
            f"AND NOT EXISTS (SELECT 1 FROM {self._view(parent)} AS p WHERE {key} = {child})"
        )
        return int(self.scalar(query))

    # -- quality ----------------------------------------------------------------------------

    def _violation(self, spec: ExpectationSpec, types: Mapping[str, str]) -> str:
        """True when a row of the NaN-free projection violates a row check; never null."""
        if spec.kind == "expr":
            return f"NOT coalesce(({spec.params['expr']}), true)"
        terms = []
        for column in spec.columns:
            ref = ident(column)
            if spec.kind == "not_null":
                terms.append(f"{ref} IS NULL")
                continue
            if spec.kind == "accepted_values":
                values = spec.params["values"]
                assert isinstance(values, list)
                bad = (
                    f"{ref} NOT IN ({', '.join(literal(v) for v in values)})" if values else "true"
                )
            elif spec.kind == "range":
                low, high = spec.params.get("min"), spec.params.get("max")
                parts = [f"{ref} < {literal(low)}"] if low is not None else []
                parts += [f"{ref} > {literal(high)}"] if high is not None else []
                bad = " OR ".join(parts) or "false"
            else:  # regex: a search, as Python's re.search
                pattern = string(str(spec.params["pattern"]))
                bad = f"NOT regexp_matches(CAST({ref} AS VARCHAR), {pattern})"
            terms.append(f"({ref} IS NOT NULL AND coalesce({bad}, false))")
        return " OR ".join(terms) or "false"

    def _marked(self, rel: Relation, specs: Sequence[ExpectationSpec]) -> tuple[str, list[str]]:
        """A view of `rel` with NaN replaced by null, the original float columns kept as
        `__pipeloom_o_<c>`, and one boolean `__pipeloom_v<i>` per row check."""
        types = self._types(rel)
        floats = [c for c, t in types.items() if is_float(t)]
        clean = ", ".join(f"{_value('', c, types[c])} AS {ident(c)}" for c in floats)
        keep = "".join(f", {ident(c)} AS {ident('__pipeloom_o_' + c)}" for c in floats)
        star = f"* REPLACE ({clean})" if floats else "*"
        flags = [f"__pipeloom_v{i}" for i in range(len(specs))]
        checks = "".join(
            f", {self._violation(s, types)} AS {ident(f)}"
            for s, f in zip(specs, flags, strict=True)
        )
        inner = f"SELECT {star}{keep} FROM {self._view(rel)}"
        return self._view(self.query(f"SELECT *{checks} FROM ({inner})")), flags

    def evaluate(self, rel: Relation, checks: Sequence[ExpectationSpec]) -> QualityCounts:
        rows = [c for c in checks if c.kind in ROW_CHECKS]
        marked, flags = self._marked(rel, rows)
        drops = [f for s, f in zip(rows, flags, strict=True) if s.action == "drop"]
        sums = "".join(f", count_if({ident(f)})" for f in flags)
        any_drop = " OR ".join(ident(f) for f in drops) or "false"
        got = self.rows(f"SELECT count(*), count_if({any_drop}){sums} FROM {marked}")[0]
        aggregates: dict[str, Any] = {}
        for spec in checks:
            if spec.kind == "row_count":
                aggregates[spec.name] = int(got[0])
            elif spec.kind == "unique":
                keys = ", ".join(ident(c) for c in spec.columns)
                present = " AND ".join(f"{ident(c)} IS NOT NULL" for c in spec.columns)
                aggregates[spec.name] = int(
                    self.scalar(
                        f"SELECT count(*) - (SELECT count(*) FROM (SELECT DISTINCT {keys} "
                        f"FROM {marked} WHERE {present})) FROM {marked} WHERE {present}"
                    )
                )
            elif spec.kind == "freshness":
                aggregates[spec.name] = self.max_value(rel, spec.columns[0])
        failed = {s.name: int(got[2 + i]) for i, s in enumerate(rows)}
        return QualityCounts(int(got[0]), failed, int(got[1]), aggregates)

    def apply_drops(
        self, rel: Relation, checks: Sequence[ExpectationSpec], *, quarantine: QuarantineMeta | None
    ) -> DropResult:
        drops = [c for c in checks if c.kind in ROW_CHECKS and c.action == "drop"]
        types = self._types(rel)
        original = ", ".join(
            f"{ident('__pipeloom_o_' + c) if is_float(t) else ident(c)} AS {ident(c)}"
            for c, t in types.items()
        )
        if not drops:
            if quarantine is None:
                return DropResult(rel, None)
            empty = _rel(self._on(rel, "SELECT * FROM {r} LIMIT 0"))
            return DropResult(rel, self._quarantine(empty, [], [], quarantine))
        marked, flags = self._marked(rel, drops)
        bad = " OR ".join(ident(f) for f in flags)
        kept = _rel(self.query(f"SELECT {original} FROM {marked} WHERE NOT ({bad})"))
        if quarantine is None:
            return DropResult(kept, None)
        dropped = _rel(
            self.query(
                f"SELECT {original}, {', '.join(ident(f) for f in flags)} FROM {marked} WHERE {bad}"
            )
        )
        return DropResult(
            kept, self._quarantine(dropped, [d.name for d in drops], flags, quarantine)
        )

    def _quarantine(
        self, rel: Relation, names: list[str], flags: list[str], meta: QuarantineMeta
    ) -> Relation:
        failed = (
            " || ".join(
                f"CASE WHEN {ident(f)} THEN [{string(n)}] ELSE CAST([] AS VARCHAR[]) END"
                for n, f in zip(names, flags, strict=True)
            )
            or "CAST([] AS VARCHAR[])"
        )
        drop = f" EXCLUDE ({', '.join(ident(f) for f in flags)})" if flags else ""
        ts = literal(meta.ts.astimezone(UTC))
        return _rel(
            self._on(
                rel,
                f"SELECT *{drop}, {string(meta.run_id)} AS _pipeloom_run_id, "
                f"{failed} AS _pipeloom_failed, {ts} AS _pipeloom_ts FROM {{r}}",
            )
        )

    # -- writing ----------------------------------------------------------------------------

    def materializer(self, strategy: str) -> Materializer:
        if strategy not in CAPABILITIES.strategies:
            raise ValueError(f"duckdb engine does not support strategy '{strategy}'")
        return _Materializer(self, strategy)

    def _commits(self, db: str) -> str:
        return f"{ident(db)}.{ident(_META_SCHEMA)}.{ident(_COMMITS)}"

    def _applied(self, db: str, target: TargetRef, token: str) -> WriteResult | None:
        exists = self.scalar(
            "SELECT count(*) FROM duckdb_tables() WHERE database_name = ? AND schema_name = ? "
            "AND table_name = ?",
            [db, _META_SCHEMA, _COMMITS],
        )
        if not exists:
            return None
        found = self.rows(
            f"SELECT result FROM {self._commits(db)} WHERE relation = ? AND token = ?",
            [f"{target.schema}.{target.name}", token],
        )
        return _load(found[0][0]) if found else None

    def applied_token(self, target: TargetRef, token: str) -> WriteResult | None:
        db = self.catalog.database(target.catalog, create=False)
        return None if db is None else self._applied(db, target, token)

    def write(self, rel: Relation, target: TargetRef, cfg: WriteConfig, token: str) -> WriteResult:
        with self._use() as con:
            db = self.catalog.database(target.catalog)
            assert db is not None
            replay = self._applied(db, target, token)
            if replay is not None:
                return replay
            # The batch is computed first, outside the transaction: a failing relation never
            # touches the table, and a model that reads its own table sees the old rows.
            stage = ident(self._name("stage"))
            _q(con, f"CREATE TEMP TABLE {stage} AS SELECT * FROM {self._view(rel)}")
            try:
                types = {r[0]: r[1] for r in _q(con, f"DESCRIBE {stage}")}
                ident_ = self.catalog.ident(target)
                exists = self.catalog.exists(target)
                write = _Write(con, ident_, stage, types, cfg, exists)
                write.check()
                con.begin()
                try:
                    result = write.run()
                    commits = self._commits(db)
                    _q(con, f"CREATE SCHEMA IF NOT EXISTS {ident(db)}.{ident(_META_SCHEMA)}")
                    _q(
                        con,
                        f"CREATE TABLE IF NOT EXISTS {commits} "
                        "(relation VARCHAR, token VARCHAR, result VARCHAR)",
                    )
                    result = replace(result, commit_meta=dict(cfg.commit_meta))
                    _q(
                        con,
                        f"INSERT INTO {commits} VALUES (?, ?, ?)",
                        [f"{target.schema}.{target.name}", token, _dump(result)],
                    )
                    con.commit()
                except BaseException:
                    con.rollback()
                    raise
            finally:
                _q(con, f"DROP TABLE IF EXISTS {stage}")
        return result

    def create_view(
        self, target: TargetRef, query: str, inputs: Mapping[str, ReadSpec]
    ) -> WriteResult:
        ctes = ", ".join(
            f"{ident(a)} AS ({self.source_sql(s.format, s)})" for a, s in inputs.items()
        )
        body = query.strip().rstrip(";")
        text = (f"WITH {ctes} " if ctes else "") + f"SELECT * FROM (\n{body}\n)"
        self.execute(f"CREATE OR REPLACE VIEW {self.catalog.ident(target)} AS {text}")
        return WriteResult()

    def write_intermediate(self, rel: Relation, ref: IntermediateRef, token: str) -> WriteResult:
        folder = Path(ref.location)
        marker = folder / _INTERMEDIATE_TOKEN
        if marker.is_file():
            stored = json.loads(marker.read_text(encoding="utf-8"))
            if stored["token"] == token:
                return _load(json.dumps(stored["result"]))
        folder.mkdir(parents=True, exist_ok=True)
        part = folder / f"{_INTERMEDIATE_DATA}.tmp"
        n = self.scalar(
            f"COPY (SELECT * FROM {self._view(rel)}) TO {string(part.as_posix())} (FORMAT parquet)"
        )
        os.replace(part, folder / _INTERMEDIATE_DATA)
        result = WriteResult(rows_written=int(n))
        marker.write_text(
            json.dumps({"token": token, "result": json.loads(_dump(result))}), encoding="utf-8"
        )
        return result

    # -- control ----------------------------------------------------------------------------

    def unit_scope(self, unit_id: str) -> AbstractContextManager[None]:
        return self._scope(unit_id)

    @contextmanager
    def _scope(self, unit_id: str) -> Iterator[None]:
        prior = getattr(self._local, "unit", None)
        self._local.unit = unit_id
        try:
            yield
        finally:
            self._local.unit = prior

    def is_transient(self, exc: BaseException) -> bool | None:
        if isinstance(exc, extensions.ExtensionMissingError):
            return False
        return None

    def cancel(self, unit_id: str) -> None:
        self._cancelled.add(unit_id)
        if self._busy == unit_id:
            self.native.interrupt()


class _Write:
    """One write strategy as SQL over the staged batch, inside the caller's transaction."""

    def __init__(
        self,
        con: Any,
        target: str,
        stage: str,
        types: Mapping[str, str],
        cfg: WriteConfig,
        exists: bool,
    ) -> None:
        self.con, self.target, self.stage, self.types, self.cfg = con, target, stage, types, cfg
        self.exists = exists

    def _has(self, columns: Sequence[str], what: str) -> None:
        for c in columns:
            if c not in self.types:
                raise WriteKeyError(f"{what} column '{c}' is missing")

    def _one(self, sql: str) -> Any:
        return _q(self.con, sql)[0][0]

    def _is_delete(self, alias: str) -> str:
        d = self.cfg.deletes
        if d is None:
            return "false"
        return f"coalesce({alias}{ident(d.column)} = {literal(d.value)}, false)"

    def _keys_match(self, a: str, b: str, keys: Sequence[str]) -> str:
        return " AND ".join(f"{a}.{ident(k)} = {b}.{ident(k)}" for k in keys)

    def _repeats(self, keys: Sequence[str]) -> bool:
        cols = ", ".join(ident(k) for k in keys)
        present = " AND ".join(f"{ident(k)} IS NOT NULL" for k in keys)
        return bool(
            _q(
                self.con,
                f"SELECT 1 FROM {self.stage} WHERE {present} GROUP BY {cols} "
                "HAVING count(*) > 1 LIMIT 1",
            )
        )

    def check(self) -> None:
        cfg = self.cfg
        if cfg.strategy == "merge":
            if not cfg.unique_key:
                raise WriteKeyError("merge needs a unique_key")
            self._has(cfg.unique_key, "unique_key")
            if self._repeats(cfg.unique_key):
                raise WriteKeyError("the batch repeats a unique_key value")
        elif cfg.strategy == "replace_partitions":
            if not cfg.partition_by:
                raise WriteKeyError("replace_partitions needs partition_by")
            self._has(cfg.partition_by, "partition_by")
        elif cfg.strategy == "scd2":
            keys = cfg.unique_key
            if not keys:
                raise WriteKeyError("scd2 needs a unique_key")
            clash = [c for c in self.types if c in _SYSTEM]
            if clash:
                raise WriteKeyError(f"the batch has scd2 system column(s) {', '.join(clash)}")
            self._has(keys, "unique_key")
            self._has(cfg.tracked_columns, "tracked_columns")
            if cfg.deletes is not None:
                if cfg.deletes.mode != "hard":
                    raise ValueError("scd2 supports hard deletes only")
                self._has((cfg.deletes.column,), "deletes")
            nulls = " OR ".join(f"{ident(k)} IS NULL" for k in keys)
            if _q(self.con, f"SELECT 1 FROM {self.stage} WHERE {nulls} LIMIT 1"):
                raise WriteKeyError("the batch has a null unique_key")
            if self._repeats(keys):
                raise WriteKeyError("the batch repeats a unique_key value")

    def run(self) -> WriteResult:
        strategy = self.cfg.strategy
        if strategy == "overwrite":
            _q(self.con, f"CREATE OR REPLACE TABLE {self.target} AS SELECT * FROM {self.stage}")
            return WriteResult(rows_written=int(self._one(f"SELECT count(*) FROM {self.stage}")))
        if strategy == "scd2":
            return self._scd2()
        if not self.exists:
            _q(self.con, f"CREATE TABLE {self.target} AS SELECT * FROM {self.stage} LIMIT 0")
        elif self.cfg.columns is not None:
            self._evolve(self.cfg.columns)
        if strategy == "append":
            n = self._one(f"INSERT INTO {self.target} BY NAME SELECT * FROM {self.stage}")
            return WriteResult(rows_written=int(n))
        if strategy == "replace_partitions":
            return self._replace_partitions()
        return self._merge()

    def _evolve(self, columns: Schema) -> None:
        """Rebuild the table with `columns`: new ones null, others cast (the core already
        checked that every cast widens)."""
        current = {r[0]: r[1] for r in _q(self.con, f"DESCRIBE {self.target}")}
        if [(n, to_logical(t)) for n, t in current.items()] == [(c.name, c.dtype) for c in columns]:
            return
        exprs = []
        for c in columns:
            duck = to_duckdb(c.dtype) or current.get(c.name, "VARCHAR")
            ref = ident(c.name) if c.name in current else "NULL"
            exprs.append(f"CAST({ref} AS {duck}) AS {ident(c.name)}")
        _q(
            self.con,
            f"CREATE OR REPLACE TABLE {self.target} AS SELECT {', '.join(exprs)} FROM {self.target}",
        )

    def _replace_partitions(self) -> WriteResult:
        parts = self.cfg.partition_by
        same = " AND ".join(
            f"s.{ident(p)} IS NOT DISTINCT FROM {self.target}.{ident(p)}" for p in parts
        )
        deleted = self._one(
            f"DELETE FROM {self.target} WHERE EXISTS (SELECT 1 FROM {self.stage} AS s WHERE {same})"
        )
        n = self._one(f"INSERT INTO {self.target} BY NAME SELECT * FROM {self.stage}")
        return WriteResult(rows_written=int(n), rows_deleted=int(deleted))

    def _merge(self) -> WriteResult:
        cfg, keys = self.cfg, self.cfg.unique_key
        on = self._keys_match("target", "source", keys)
        cond = (
            "true" if cfg.update_condition is None else f"coalesce(({cfg.update_condition}), false)"
        )
        is_del = self._is_delete("source.")
        matched = f"target.{ident(keys[0])} IS NOT NULL"
        inserted, updated, deleted = _q(
            self.con,
            f"SELECT count_if(NOT ({matched}) AND NOT {is_del}), "
            f"count_if({matched} AND {cond} AND NOT {is_del}), "
            f"count_if({matched} AND {cond} AND {is_del}) "
            f"FROM {self.stage} AS source LEFT JOIN {self.target} AS target ON {on}",
        )[0]
        cols = list(self.types)
        sets = ", ".join(f"{ident(c)} = source.{ident(c)}" for c in cols)
        names = ", ".join(ident(c) for c in cols)
        values = ", ".join(f"source.{ident(c)}" for c in cols)
        clauses = []
        soft = cfg.deletes is not None and cfg.deletes.mode == "soft"
        if cfg.deletes is not None and not soft:
            clauses.append(f"WHEN MATCHED AND {cond} AND {is_del} THEN DELETE")
        clauses.append(f"WHEN MATCHED AND {cond} THEN UPDATE SET {sets}")
        clauses.append(f"WHEN NOT MATCHED AND NOT {is_del} THEN INSERT ({names}) VALUES ({values})")
        _q(
            self.con,
            f"MERGE INTO {self.target} AS target USING {self.stage} AS source ON {on}\n"
            + "\n".join(clauses),
        )
        written = int(inserted) + int(updated) + (int(deleted) if soft else 0)
        return WriteResult(
            rows_written=written,
            rows_inserted=int(inserted),
            rows_updated=int(updated),
            rows_deleted=int(deleted),
        )

    def _scd2(self) -> WriteResult:
        cfg, keys = self.cfg, self.cfg.unique_key
        ts = literal(scd2_time(cfg))
        is_del = self._is_delete("source.")
        opened = f"{ts} AS valid_from, CAST(NULL AS {_TSTZ}) AS valid_to, true AS is_current"
        if cfg.full_refresh or not self.exists:  # a first load: every live row opens a version
            _q(
                self.con,
                f"CREATE OR REPLACE TABLE {self.target} AS SELECT source.*, {opened} "
                f"FROM {self.stage} AS source WHERE NOT {is_del}",
            )
            n = int(self._one(f"SELECT count(*) FROM {self.stage} AS source WHERE NOT {is_del}"))
            return WriteResult(rows_written=n, rows_inserted=n, rows_updated=0, rows_deleted=0)
        if cfg.columns is not None:
            self._evolve((*cfg.columns, *SCD2_COLUMNS))
        same = (
            " AND ".join(
                f"{_value('target.', c, self.types[c])} IS NOT DISTINCT FROM "
                f"{_value('source.', c, self.types[c])}"
                for c in cfg.tracked_columns
            )
            or "true"
        )
        on = self._keys_match("target", "source", keys) + " AND target.is_current"
        matched = f"target.{ident(keys[0])} IS NOT NULL"
        deleted, changed, inserted = _q(
            self.con,
            f"SELECT count_if({matched} AND {is_del}), "
            f"count_if({matched} AND NOT {is_del} AND NOT ({same})), "
            f"count_if(NOT {is_del} AND (NOT ({matched}) OR NOT ({same}))) "
            f"FROM {self.stage} AS source LEFT JOIN {self.target} AS target ON {on}",
        )[0]
        _q(
            self.con,
            f"MERGE INTO {self.target} AS target USING {self.stage} AS source ON {on}\n"
            f"WHEN MATCHED AND ({is_del} OR NOT ({same})) THEN UPDATE SET "
            f"valid_to = {ts}, is_current = false",
        )
        live = self._keys_match("c", "source", keys)
        _q(
            self.con,
            f"INSERT INTO {self.target} BY NAME SELECT source.*, {opened} "
            f"FROM {self.stage} AS source WHERE NOT {is_del} AND NOT EXISTS "
            f"(SELECT 1 FROM {self.target} AS c WHERE {live} AND c.is_current)",
        )
        closed = int(changed) + int(deleted)
        return WriteResult(
            rows_written=int(inserted) + closed,
            rows_inserted=int(inserted),
            rows_updated=int(changed),
            rows_deleted=int(deleted),
        )


class DuckDBEngine:
    name = NAME
    capabilities = CAPABILITIES

    def __init__(self, opts: DuckDBEngineOptions | None = None) -> None:
        self.version = duckdb.__version__
        self._opts = opts or DuckDBEngineOptions()

    @contextmanager
    def session(self, ctx: RuntimeContext, *, catalog: Catalog) -> Iterator[EngineSession]:
        if not isinstance(catalog, DuckDBCatalog):
            raise TypeError(
                f"the duckdb engine needs the duckdb catalog, not {type(catalog).__name__}"
            )
        opts = self._opts
        config: dict[str, Any] = {
            "TimeZone": "UTC",
            "autoinstall_known_extensions": False,
            "autoload_known_extensions": True,
            **opts.config,
        }
        if opts.extension_directory is not None:
            config["extension_directory"] = Path(opts.extension_directory).resolve().as_posix()
        if not catalog.memory:
            Path(catalog.path).parent.mkdir(parents=True, exist_ok=True)
        con = duckdb.connect(catalog.path, config=config)
        try:
            session = DuckDBSession(con, catalog, opts)
            with catalog.attach(session):
                yield session
        finally:
            con.close()
