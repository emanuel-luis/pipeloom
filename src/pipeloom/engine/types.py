"""Logical types shared by the core and engine plugins."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

from pipeloom.diagnostics import JsonValue


@dataclass(frozen=True)
class Column:
    name: str
    dtype: str
    """Logical type, for example int64, string, decimal(18,2) or timestamp[UTC]."""
    nullable: bool = True


Schema = tuple[Column, ...]


@dataclass(frozen=True)
class Relation:
    """An opaque handle to data. The core never looks inside `native`."""

    native: Any
    engine: str
    lazy: bool = False


@dataclass(frozen=True)
class EngineCapabilities:
    """What an engine can do. Static data: L5 reads it without importing the engine."""

    formats: frozenset[str] = frozenset()
    strategies: frozenset[str] = frozenset()
    materializations: frozenset[str] = frozenset()
    sql_models: bool = False
    views: bool = False
    idempotent_writes: bool = False
    cancel: bool = False
    bytes_metrics: bool = False
    portable: bool = False
    """Frames are Narwhals-compatible."""


@dataclass(frozen=True)
class StateStoreCapabilities:
    shared: bool
    """Visible to every process and task of a run."""


@dataclass(frozen=True)
class Namespace:
    catalog: str | None
    schema: str


@dataclass(frozen=True)
class TargetRef:
    """A physical relation: where a model is written."""

    catalog: str | None
    schema: str
    name: str

    @property
    def namespace(self) -> Namespace:
        return Namespace(self.catalog, self.schema)

    @property
    def relation_name(self) -> str:
        return ".".join(p for p in (self.catalog, self.schema, self.name) if p)


@dataclass(frozen=True)
class ReadSpec:
    format: str
    """A file or table format, or "table" for a catalog table."""
    location: str | None = None
    table: TargetRef | None = None
    options: Mapping[str, JsonValue] = field(default_factory=dict)


@dataclass(frozen=True)
class WriteConfig:
    strategy: str
    unique_key: tuple[str, ...] = ()
    options: Mapping[str, JsonValue] = field(default_factory=dict)


@dataclass(frozen=True)
class WriteResult:
    rows: int | None = None
    bytes: int | None = None
    skipped: bool = False
    """The idempotency token was already applied, so nothing was written."""
