"""Shared helpers: source writers that use a throwaway DuckDB connection, and the contract harness.

Delta reads need the DuckDB `delta` extension. Set PIPELOOM_TEST_DUCKDB_EXTENSIONS to a directory
where it is installed; without it the harness leaves `delta` out of the declared formats and the
Delta tests skip.
"""

from __future__ import annotations

import json
import os
import re
import uuid
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import replace
from pathlib import Path
from typing import Any

import duckdb
import pytest

from pipeloom.engine.protocols import Catalog, EngineSession
from pipeloom.engine.types import Relation, Schema
from pipeloom.testing import reference
from pipeloom.testing.contract.engine import EngineHarness
from pipeloom_duckdb.catalog import DuckDBCatalog
from pipeloom_duckdb.engine import DuckDBEngine, DuckDBSession
from pipeloom_duckdb.spec import DuckDBEngineOptions
from pipeloom_duckdb.sqltext import ident, string, to_duckdb

Rows = Sequence[Mapping[str, Any]]
EXTENSIONS_ENV = "PIPELOOM_TEST_DUCKDB_EXTENSIONS"


def extension_dir() -> str | None:
    """The preinstalled extension directory, when the environment names one."""
    return os.environ.get(EXTENSIONS_ENV) or None


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    if extension_dir() is None:
        skip = pytest.mark.skip(reason=f"set {EXTENSIONS_ENV} to a directory holding 'delta'")
        for item in items:
            if item.get_closest_marker("duckdb_extensions") is not None:
                item.add_marker(skip)


def _table(con: Any, rows: Rows, schema: Schema) -> None:
    cols = ", ".join(f"{ident(c.name)} {to_duckdb(c.dtype)}" for c in schema)
    con.execute(f"CREATE TABLE t ({cols})")
    if rows:
        marks = ", ".join("?" for _ in schema)
        con.executemany(f"INSERT INTO t VALUES ({marks})", [[r[c.name] for c in schema] for r in rows])


_DELTA_TYPES = {
    "int64": "long",
    "int32": "integer",
    "float64": "double",
    "string": "string",
    "boolean": "boolean",
    "date": "date",
    "timestamp[us, UTC]": "timestamp",
}


def _delta_type(logical: str) -> str:
    dec = re.fullmatch(r"decimal\((\d+),(\d+)\)", logical)
    return f"decimal({dec.group(1)},{dec.group(2)})" if dec else _DELTA_TYPES[logical]


def write_delta(con: Any, path: Path) -> None:
    """A one-commit Delta table of table `t`: a Parquet file and a log written by hand, after the
    Delta protocol (reader version 1, writer version 2)."""
    (path / "_delta_log").mkdir(parents=True)
    part = path / "part-00000.parquet"
    con.execute(f"COPY t TO {string(part.as_posix())} (FORMAT parquet)")
    columns = con.execute("DESCRIBE t").fetchall()
    logical = {
        "BIGINT": "int64",
        "INTEGER": "int32",
        "DOUBLE": "float64",
        "VARCHAR": "string",
        "BOOLEAN": "boolean",
        "DATE": "date",
        "TIMESTAMP WITH TIME ZONE": "timestamp[us, UTC]",
    }
    fields = [
        {
            "name": name,
            "type": _delta_type(logical.get(kind, kind.lower().replace(", ", ","))),
            "nullable": True,
            "metadata": {},
        }
        for name, kind, *_ in columns
    ]
    actions = [
        {"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}},
        {
            "metaData": {
                "id": str(uuid.UUID(int=1)),
                "format": {"provider": "parquet", "options": {}},
                "schemaString": json.dumps({"type": "struct", "fields": fields}),
                "partitionColumns": [],
                "configuration": {},
                "createdTime": 0,
            }
        },
        {
            "add": {
                "path": part.name,
                "partitionValues": {},
                "size": part.stat().st_size,
                "modificationTime": 0,
                "dataChange": True,
            }
        },
    ]
    log = path / "_delta_log" / "00000000000000000000.json"
    log.write_text("".join(json.dumps(a) + "\n" for a in actions), encoding="utf-8")


def write_source(fmt: str, path: Path, rows: Rows, schema: Schema) -> None:
    con = duckdb.connect()
    try:
        _table(con, rows, schema)
        if fmt == "delta":
            write_delta(con, path)
        else:
            options = "FORMAT parquet" if fmt == "parquet" else "FORMAT csv, HEADER"
            con.execute(f"COPY t TO {string(path.as_posix())} ({options})")
    finally:
        con.close()


class TrackedEngine(DuckDBEngine):
    """Remembers the open session, so the harness can build relations on its connection."""

    current: DuckDBSession | None = None

    @contextmanager
    def session(self, ctx: Any, *, catalog: Catalog) -> Iterator[EngineSession]:
        with super().session(ctx, catalog=catalog) as s:
            assert isinstance(s, DuckDBSession)
            self.current = s
            try:
                yield s
            finally:
                self.current = None

    @property
    def open(self) -> DuckDBSession:
        assert self.current is not None, "no session is open"
        return self.current


def _settings() -> dict[str, Any]:
    return dict(duckdb.sql("SELECT name, value FROM duckdb_settings()").fetchall())


def harness(tmp_path: Path) -> EngineHarness:
    directory = extension_dir()
    engine = TrackedEngine(DuckDBEngineOptions(extension_directory=directory))
    if directory is None:
        engine.capabilities = replace(
            engine.capabilities, formats=engine.capabilities.formats - {"delta"}
        )

    def failing(session: EngineSession) -> Relation:
        con = session.native
        return session.relation(
            con.sql("SELECT CAST(error('cannot produce rows ' || range) AS BIGINT) AS a FROM range(1)")
        )

    def slow(session: EngineSession, seconds: float) -> Relation:
        con = session.native
        rel = session.relation(con.sql("SELECT sum(sqrt(range)) AS s FROM range(10000000000000)"))
        session.count(rel)  # runs until interrupted
        return rel

    return EngineHarness(
        engine=engine,
        catalog=DuckDBCatalog((tmp_path / "wh" / "warehouse.duckdb").as_posix()),
        make_native=lambda rows, schema: engine.open.from_rows(rows, schema).native,
        to_rows=lambda native: engine.open.to_rows(Relation(native, "duckdb", lazy=True)),
        write_source=write_source,
        failing_relation=failing,
        settings_snapshot=_settings,
        update_condition="source.v > target.v",
        expr_cases=(
            ("id > 1", lambda r: None if r["id"] is None else r["id"] > 1),
            ("f < 5.0", lambda r: None if reference.is_null(r["f"]) else r["f"] < 5.0),
        ),
        schema_on_read=True,
        slow_relation=slow,
    )


@pytest.fixture
def duckdb_harness() -> Callable[[Path], EngineHarness]:
    return harness
