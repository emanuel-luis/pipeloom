"""The DuckDB catalog: tables and views in a database file, a model catalog in a sibling file.

It imports no DuckDB. The engine binds its session with `attach` for the length of a session.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import AbstractContextManager, contextmanager
from pathlib import Path, PurePosixPath, PureWindowsPath
from typing import TYPE_CHECKING, Any

from pipeloom.engine.types import CatalogCapabilities, Namespace, Schema, TargetRef
from pipeloom_duckdb.sqltext import ident, schema_of, string
from pipeloom_duckdb.spec import MEMORY

if TYPE_CHECKING:
    from pipeloom_duckdb.engine import DuckDBSession


def _name(value: str) -> str:
    if (
        not value
        or value in (".", "..")
        or ":" in value
        or any(ord(c) < 32 for c in value)
        or PurePosixPath(value).name != value
        or PureWindowsPath(value).name != value
    ):
        raise ValueError(f"{value!r} is not usable as a DuckDB catalog name")
    return value


class DuckDBCatalog:
    """`path` holds the tables without a catalog. A target catalog `<name>` is the database
    `<name>.duckdb` next to it, attached as `<name>` (ADR-0051)."""

    capabilities = CatalogCapabilities(views=True)

    def __init__(self, path: str) -> None:
        self.path = path if path == MEMORY else Path(path).resolve().as_posix()
        self._s: DuckDBSession | None = None
        self._main = ""

    @property
    def memory(self) -> bool:
        return self.path == MEMORY

    def attach(self, session: DuckDBSession) -> AbstractContextManager[None]:
        return self._attach(session)

    @contextmanager
    def _attach(self, session: DuckDBSession) -> Iterator[None]:
        self._s = session
        self._main = str(session.scalar("SELECT current_database()"))
        if not self.memory:  # earlier catalogs, so views across them bind
            for sibling in sorted(Path(self.path).parent.glob("*.duckdb")):
                if sibling.as_posix() != self.path:
                    self._attach_db(sibling.stem, create=False)
        try:
            yield
        finally:
            self._s = None

    @property
    def session(self) -> DuckDBSession:
        if self._s is None:
            raise RuntimeError("the DuckDB catalog is used outside an engine session")
        return self._s

    def _attached(self) -> set[str]:
        return {r[0] for r in self.session.rows("SELECT database_name FROM duckdb_databases()")}

    def _attach_db(self, name: str, *, create: bool) -> bool:
        """Attach catalog `name`; False when its file does not exist and `create` is off."""
        if name in self._attached():
            return True
        if self.memory:
            if not create:
                return False
            location = MEMORY
        else:
            file = Path(self.path).parent / f"{name}.duckdb"
            if not create and not file.exists():
                return False
            location = file.as_posix()
        self.session.execute(f"ATTACH {string(location)} AS {ident(name)}")
        return True

    def database(self, catalog: str | None, *, create: bool = True) -> str | None:
        """The attached database of a target catalog; None when it does not exist yet."""
        if catalog is None or catalog == self._main:
            return self._main
        name = _name(catalog)
        return name if self._attach_db(name, create=create) else None

    def ident(self, target: TargetRef) -> str:
        db = self.database(target.catalog)
        assert db is not None
        return ".".join(ident(p) for p in (db, target.schema, target.name))

    # -- the Catalog protocol ---------------------------------------------------------------

    def ensure_namespace(self, ns: Namespace) -> None:
        db = self.database(ns.catalog)
        assert db is not None
        self.session.execute(f"CREATE SCHEMA IF NOT EXISTS {ident(db)}.{ident(ns.schema)}")

    def exists(self, target: TargetRef) -> bool:
        db = self.database(target.catalog, create=False)
        if db is None:
            return False
        query = (
            "SELECT count(*) FROM (SELECT database_name, schema_name, table_name AS n "
            "FROM duckdb_tables() UNION ALL SELECT database_name, schema_name, view_name "
            "FROM duckdb_views()) WHERE database_name = ? AND schema_name = ? AND n = ?"
        )
        return bool(self.session.scalar(query, [db, target.schema, target.name]))

    def table_schema(self, target: TargetRef) -> Schema | None:
        if not self.exists(target):
            return None
        rel: Any = self.session.query(f"SELECT * FROM {self.ident(target)}")
        return schema_of(rel.columns, rel.types)
