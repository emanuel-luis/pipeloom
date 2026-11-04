"""Logical types shared by the core and engine plugins."""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Final, Literal

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
    quality: bool = False
    """`evaluate` and `apply_drops` are implemented; L5 rejects expectations otherwise (PLC510)."""


@dataclass(frozen=True)
class StateStoreCapabilities:
    shared: bool
    """Visible to every process and task of a run."""


@dataclass(frozen=True)
class CatalogCapabilities:
    """What a catalog can do. Static data: L5 reads it without importing the catalog."""

    views: bool = True
    """The catalog can hold views (PLC507 otherwise)."""


@dataclass(frozen=True)
class IntermediateRef:
    """Where a run keeps the persisted result of an `intermediate` model."""

    run_id: str
    model: str
    location: str
    """Absolute POSIX path or URI. `ReadSpec(format="intermediate", location=...)` reads it."""


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
class DeleteSpec:
    """Rows whose `column` equals `value` are deletes; a null column value is not a delete."""

    column: str
    value: JsonValue
    mode: Literal["hard", "soft"]


@dataclass(frozen=True)
class WriteConfig:
    strategy: str
    unique_key: tuple[str, ...] = ()
    partition_by: tuple[str, ...] = ()
    update_condition: str | None = None
    """Engine dialect. `source.<c>` and `target.<c>` name columns; a bare name means source."""
    deletes: DeleteSpec | None = None
    columns: Schema | None = None
    """The final table schema chosen by the core; None means the batch schema."""
    options: Mapping[str, JsonValue] = field(default_factory=dict)
    commit_meta: Mapping[str, JsonValue] = field(default_factory=dict)
    """Stored with the idempotency token and echoed when the token is replayed."""


@dataclass(frozen=True)
class WriteResult:
    rows_written: int | None = None
    rows_inserted: int | None = None
    rows_updated: int | None = None
    rows_deleted: int | None = None
    bytes_written: int | None = None
    target_version: int | str | None = None
    skipped: bool = False
    """The token had already been applied: nothing was written."""
    commit_meta: Mapping[str, JsonValue] = field(default_factory=dict)


@dataclass(frozen=True)
class ColumnChange:
    kind: Literal["added", "removed", "type_changed"]
    column: str
    old: str | None
    new: str | None


@dataclass(frozen=True)
class SchemaDiff:
    changes: tuple[ColumnChange, ...]

    @property
    def empty(self) -> bool:
        return not self.changes

    def to_json(self) -> list[dict[str, JsonValue]]:
        return [
            {"kind": c.kind, "column": c.column, "old": c.old, "new": c.new} for c in self.changes
        ]


CheckKind = Literal[
    "not_null", "accepted_values", "range", "regex", "expr", "unique", "row_count", "freshness"
]
ROW_CHECKS: Final = frozenset({"not_null", "accepted_values", "range", "regex", "expr"})


@dataclass(frozen=True)
class ExpectationSpec:
    name: str
    kind: CheckKind
    action: Literal["warn", "drop", "fail"]
    columns: tuple[str, ...]
    params: Mapping[str, JsonValue]
    """`values`, `min`, `max`, `pattern` or `expr`, by kind."""


@dataclass(frozen=True)
class QualityCounts:
    total_rows: int
    failed: Mapping[str, int]
    """Row checks: violating rows per expectation name."""
    dropped: int
    """Rows violating at least one drop check."""
    aggregates: Mapping[str, Any]
    """unique: duplicate rows; row_count: total; freshness: the max value as a Python value."""


@dataclass(frozen=True)
class QuarantineMeta:
    run_id: str
    ts: datetime


@dataclass(frozen=True)
class DropResult:
    kept: Relation
    quarantined: Relation | None
    """Rows dropped, plus `_pipeloom_run_id`, `_pipeloom_failed` and `_pipeloom_ts`."""


_INT = re.compile(r"(u?)int(\d+)")
_DECIMAL = re.compile(r"decimal\((\d+),(\d+)\)")
_TIMESTAMP = re.compile(r"timestamp\[(ms|us|ns)(?:, ([^\]]+))?\]")
_UNITS: Final = ("ms", "us", "ns")


def can_widen(old: str, new: str) -> bool:
    """True when every `old` value converts to `new` without loss."""
    if old == new:
        return True
    if old == "null":
        return True
    o_int, n_int = _INT.fullmatch(old), _INT.fullmatch(new)
    if o_int and n_int:
        o_unsigned, o_bits = o_int.group(1), int(o_int.group(2))
        n_unsigned, n_bits = n_int.group(1), int(n_int.group(2))
        if o_unsigned == n_unsigned:
            return n_bits > o_bits
        return not n_unsigned and n_bits > o_bits
    if o_int and new == "float64":
        return True
    if old == "float32" and new == "float64":
        return True
    o_dec, n_dec = _DECIMAL.fullmatch(old), _DECIMAL.fullmatch(new)
    if o_dec and n_dec:
        p, s = int(o_dec.group(1)), int(o_dec.group(2))
        p2, s2 = int(n_dec.group(1)), int(n_dec.group(2))
        return s2 >= s and p2 - s2 >= p - s
    o_ts, n_ts = _TIMESTAMP.fullmatch(old), _TIMESTAMP.fullmatch(new)
    if o_ts and n_ts and o_ts.group(2) == n_ts.group(2):
        return _UNITS.index(n_ts.group(1)) > _UNITS.index(o_ts.group(1))
    return False


def diff_schemas(target: Schema, batch: Schema) -> SchemaDiff:
    """Changes needed to take `target` to `batch`: by name, case-sensitive.

    Order and nullability are ignored. A batch type that widens to the target type is not a change.
    """
    batch_by = {c.name: c for c in batch}
    target_by = {c.name: c for c in target}
    changes: list[ColumnChange] = []
    for col in target:
        other = batch_by.get(col.name)
        if other is None:
            changes.append(ColumnChange("removed", col.name, col.dtype, None))
        elif other.dtype != col.dtype and not can_widen(other.dtype, col.dtype):
            changes.append(ColumnChange("type_changed", col.name, col.dtype, other.dtype))
    changes.extend(
        ColumnChange("added", col.name, None, col.dtype)
        for col in batch
        if col.name not in target_by
    )
    return SchemaDiff(tuple(changes))
