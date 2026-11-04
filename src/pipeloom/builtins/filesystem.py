"""Filesystem catalog: tables are directories under a root, laid out `<root>/<schema>/<name>/`."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Literal

from pydantic import Field

from pipeloom.builtins.fs_table import FsTable
from pipeloom.config.types import ProjectPath
from pipeloom.engine.protocols import Catalog
from pipeloom.engine.types import Namespace, Schema, TargetRef
from pipeloom.platform import Clock, SystemClock
from pipeloom.plugins import API_VERSION, OptionsModel, PluginSpec


class FilesystemOptions(OptionsModel):
    root: ProjectPath = Field(".", validate_default=True)
    """Directory under which schemas and tables are laid out."""
    format: Literal["parquet", "csv", "ndjson"] = "parquet"
    """File format of the part files engines write."""


def _segment(value: str) -> str:
    if not value or value in (".", "..") or "/" in value or "\\" in value:
        raise ValueError(f"{value!r} is not usable as a directory name")
    return value


class FilesystemCatalog:
    """The target's catalog name, if any, is ignored: the layout has no catalog level."""

    def __init__(self, root: Path, *, format: str = "parquet", clock: Clock | None = None) -> None:
        self.root = root
        self.format = format
        self._clock = clock or SystemClock()

    def table(self, target: TargetRef) -> FsTable:
        path = self.root / _segment(target.schema) / _segment(target.name)
        return FsTable(path, clock=self._clock)

    def exists(self, target: TargetRef) -> bool:
        return self.table(target).current() is not None

    def ensure_namespace(self, ns: Namespace) -> None:
        (self.root / _segment(ns.schema)).mkdir(parents=True, exist_ok=True)

    def table_schema(self, target: TargetRef) -> Schema | None:
        commit = self.table(target).current()
        return None if commit is None else commit.columns


def _build(opts: Any) -> Catalog:
    return FilesystemCatalog(Path(opts.root), format=opts.format)


SPEC: PluginSpec[Catalog] = PluginSpec(
    "filesystem",
    "filesystem",
    "catalogs",
    API_VERSION,
    _build,
    FilesystemOptions,
    compatible_engines=frozenset({"polars", "pandas"}),
    description="Tables as part files under a directory, committed atomically.",
)
