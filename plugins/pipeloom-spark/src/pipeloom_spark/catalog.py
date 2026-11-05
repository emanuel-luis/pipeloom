"""Catalogs: Delta tables at a path, and tables by name through the session catalog.

Neither imports Spark. The engine binds its session with `attach` for the length of a run.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import AbstractContextManager, contextmanager
from pathlib import Path, PurePosixPath, PureWindowsPath
from typing import TYPE_CHECKING, Any

from pipeloom.engine.types import CatalogCapabilities, Column, Namespace, Schema, TargetRef
from pipeloom_spark import errors, sqltext
from pipeloom_spark.types import to_logical

if TYPE_CHECKING:
    from pipeloom_spark.session import SessionHandle


def _segment(value: str) -> str:
    if (
        not value
        or value in (".", "..")
        or ":" in value
        or any(ord(c) < 32 for c in value)
        or PurePosixPath(value).name != value
        or PureWindowsPath(value).name != value
    ):
        raise ValueError(f"{value!r} is not usable as a directory name")
    return value


def schema_of(frame: Any) -> Schema:
    return tuple(Column(f.name, to_logical(f.dataType), f.nullable) for f in frame.schema.fields)


class _SparkCatalog:
    capabilities: CatalogCapabilities

    def __init__(self) -> None:
        self._spark: Any = None

    def attach(self, handle: SessionHandle) -> AbstractContextManager[None]:
        return self._attach(handle)

    @contextmanager
    def _attach(self, handle: SessionHandle) -> Iterator[None]:
        self._spark = handle.spark
        try:
            yield
        finally:
            self._spark = None

    @property
    def spark(self) -> Any:
        if self._spark is None:
            raise RuntimeError("the catalog is used outside an engine session")
        return self._spark

    def ident(self, target: TargetRef) -> str:
        raise NotImplementedError

    def read(self, target: TargetRef) -> Any:
        raise NotImplementedError

    def ensure_namespace(self, ns: Namespace) -> None:
        raise NotImplementedError

    def exists(self, target: TargetRef) -> bool:
        raise NotImplementedError

    def save(self, writer: Any, target: TargetRef) -> None:
        """Run a configured DataFrameWriter against the target."""
        raise NotImplementedError

    def table_schema(self, target: TargetRef) -> Schema | None:
        return schema_of(self.read(target)) if self.exists(target) else None


class DeltaPathCatalog(_SparkCatalog):
    """`<root>/<schema>/<name>`; the target's catalog name is ignored. Needs no metastore and
    works across processes."""

    capabilities = CatalogCapabilities(views=False)

    def __init__(self, root: str) -> None:
        super().__init__()
        self.root = (
            root.rstrip("/") if sqltext.has_scheme(root) else Path(root).resolve().as_posix()
        )

    def path(self, target: TargetRef) -> str:
        return f"{self.root}/{_segment(target.schema)}/{_segment(target.name)}"

    def ident(self, target: TargetRef) -> str:
        return f"delta.{sqltext.quote_ident(self.path(target))}"

    def read(self, target: TargetRef) -> Any:
        return self.spark.read.format("delta").load(self.path(target))

    def save(self, writer: Any, target: TargetRef) -> None:
        writer.save(self.path(target))

    def ensure_namespace(self, ns: Namespace) -> None:
        return None

    def exists(self, target: TargetRef) -> bool:
        try:
            self.spark.sql(f"DESCRIBE DETAIL {self.ident(target)}").collect()
        except Exception as exc:
            if errors.condition(exc) in errors.MISSING:
                return False
            raise
        return True


class SparkSessionCatalog(_SparkCatalog):
    """Tables and views by name through the session catalog (`catalog.schema.name`)."""

    capabilities = CatalogCapabilities(views=True)

    def __init__(self, catalog: str | None) -> None:
        super().__init__()
        self.catalog = catalog

    def _parts(self, catalog: str | None, schema: str, name: str | None = None) -> str:
        parts = [p for p in (catalog or self.catalog, schema, name) if p]
        return ".".join(sqltext.quote_ident(p) for p in parts)

    def ident(self, target: TargetRef) -> str:
        return self._parts(target.catalog, target.schema, target.name)

    def read(self, target: TargetRef) -> Any:
        return self.spark.table(self.ident(target))

    def save(self, writer: Any, target: TargetRef) -> None:
        writer.saveAsTable(self.ident(target))

    def ensure_namespace(self, ns: Namespace) -> None:
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self._parts(ns.catalog, ns.schema)}")

    def exists(self, target: TargetRef) -> bool:
        return bool(self.spark.catalog.tableExists(self.ident(target)))
