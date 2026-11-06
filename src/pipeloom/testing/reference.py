"""A pure-Python reference for what engines must compute: quality counts, drops, merge,
replace_partitions and align, over rows (`list[dict]`) and a `Schema`.

The contract suite compares every engine against these functions. They are slow and plain on
purpose. Dialect strings (`expr`, `update_condition`) are not parsed here: callers pass Python
callables that stand for them.
"""

from __future__ import annotations

import re
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any

from pipeloom.engine.errors import WriteKeyError
from pipeloom.engine.types import (
    ROW_CHECKS,
    SCD2_COLUMNS,
    ExpectationSpec,
    QualityCounts,
    QuarantineMeta,
    Schema,
    WriteConfig,
)

Row = dict[str, Any]
RowPredicate = Callable[[Mapping[str, Any]], bool | None]
"""Stands for an `expr` check: True passes, False fails, None passes."""
MergePredicate = Callable[[Mapping[str, Any], Mapping[str, Any]], bool | None]
"""Stands for `update_condition`: (source row, target row); None counts as False."""

FAILED_COLUMN = "_pipeloom_failed"
RUN_ID_COLUMN = "_pipeloom_run_id"
TS_COLUMN = "_pipeloom_ts"


def is_null(value: Any) -> bool:
    """None and NaN are both null."""
    return value is None or (isinstance(value, float) and value != value)  # noqa: PLR0124


def _row_passes(spec: ExpectationSpec, row: Mapping[str, Any], expr: RowPredicate | None) -> bool:
    if spec.kind == "expr":
        if expr is None:
            raise ValueError(f"no predicate given for expr check '{spec.name}'")
        return expr(row) is not False
    for column in spec.columns:
        value = row.get(column)
        if spec.kind == "not_null":
            ok = not is_null(value)
        elif is_null(value):
            ok = True
        elif spec.kind == "accepted_values":
            values = spec.params["values"]
            assert isinstance(values, list)
            ok = any(v == value and isinstance(v, bool) == isinstance(value, bool) for v in values)
        elif spec.kind == "range":
            low, high = spec.params.get("min"), spec.params.get("max")
            ok = (low is None or value >= low) and (high is None or value <= high)  # type: ignore[operator]
        elif spec.kind == "regex":
            ok = re.search(str(spec.params["pattern"]), str(value)) is not None
        else:  # pragma: no cover - guarded by ROW_CHECKS
            raise ValueError(f"not a row check: {spec.kind}")
        if not ok:
            return False
    return True


def _violations(
    rows: Sequence[Mapping[str, Any]],
    specs: Sequence[ExpectationSpec],
    exprs: Mapping[str, RowPredicate],
) -> list[list[ExpectationSpec]]:
    """For each row, the row checks it violates, in declaration order."""
    return [[s for s in specs if not _row_passes(s, row, exprs.get(s.name))] for row in rows]


def evaluate_rows(
    rows: Sequence[Mapping[str, Any]],
    checks: Sequence[ExpectationSpec],
    *,
    exprs: Mapping[str, RowPredicate] | None = None,
) -> QualityCounts:
    """Counts for `checks` over all of `rows`. `exprs` maps an `expr` check name to its predicate."""
    exprs = exprs or {}
    row_specs = [c for c in checks if c.kind in ROW_CHECKS]
    bad = _violations(rows, row_specs, exprs)
    failed = {s.name: sum(1 for v in bad if s in v) for s in row_specs}
    dropped = sum(1 for v in bad if any(s.action == "drop" for s in v))
    aggregates: dict[str, Any] = {}
    for spec in checks:
        if spec.kind == "row_count":
            aggregates[spec.name] = len(rows)
        elif spec.kind == "unique":
            keys = [tuple(r.get(c) for c in spec.columns) for r in rows]
            keys = [k for k in keys if not any(is_null(v) for v in k)]
            aggregates[spec.name] = len(keys) - len(set(keys))
        elif spec.kind == "freshness":
            present: list[Any] = [r.get(spec.columns[0]) for r in rows]
            present = [v for v in present if not is_null(v)]
            aggregates[spec.name] = max(present) if present else None
    return QualityCounts(len(rows), failed, dropped, aggregates)


def apply_drops_rows(
    rows: Sequence[Mapping[str, Any]],
    checks: Sequence[ExpectationSpec],
    quarantine: QuarantineMeta | None,
    *,
    exprs: Mapping[str, RowPredicate] | None = None,
) -> tuple[list[Row], list[Row] | None]:
    """(kept, quarantined). `quarantined` is None without `quarantine`, else the dropped rows with
    the three `_pipeloom_*` columns; `_pipeloom_failed` lists failing drop checks in order."""
    drops = [c for c in checks if c.kind in ROW_CHECKS and c.action == "drop"]
    bad = _violations(rows, drops, exprs or {})
    kept = [dict(r) for r, v in zip(rows, bad, strict=True) if not v]
    if quarantine is None:
        return kept, None
    out = [
        {
            **r,
            RUN_ID_COLUMN: quarantine.run_id,
            FAILED_COLUMN: [s.name for s in v],
            TS_COLUMN: quarantine.ts,
        }
        for r, v in zip(rows, bad, strict=True)
        if v
    ]
    return kept, out


def cast_value(value: Any, dtype: str) -> Any:
    """Cast one value to a logical type; null stays null."""
    if is_null(value):
        return None
    if dtype.startswith(("int", "uint")):
        return int(value)
    if dtype.startswith("float"):
        return float(value)
    if dtype == "string":
        return str(value)
    if dtype == "boolean":
        return bool(value)
    if dtype.startswith("decimal"):
        return Decimal(str(value))
    return value


def align_rows(rows: Sequence[Mapping[str, Any]], schema: Schema) -> list[Row]:
    """Columns in `schema` order; missing ones are null; values cast to the schema types."""
    return [{c.name: cast_value(r.get(c.name), c.dtype) for c in schema} for r in rows]


@dataclass(frozen=True)
class MergeResult:
    rows: list[Row]
    inserted: int
    updated: int
    deleted: int


def _check_columns(rows: Sequence[Mapping[str, Any]], columns: Sequence[str], what: str) -> None:
    for row in rows:
        for column in columns:
            if column not in row:
                raise WriteKeyError(f"{what} column '{column}' is missing")


def _union(*frames: Sequence[Mapping[str, Any]]) -> list[str]:
    seen: dict[str, None] = {}
    for frame in frames:
        for row in frame:
            seen.update(dict.fromkeys(row))
    return list(seen)


def merge_rows(
    current: Sequence[Mapping[str, Any]],
    batch: Sequence[Mapping[str, Any]],
    cfg: WriteConfig,
    *,
    condition: MergePredicate | None = None,
) -> MergeResult:
    """Merge `batch` into `current` by `cfg.unique_key`. Row order is not preserved."""
    key = cfg.unique_key
    if not key:
        raise WriteKeyError("merge needs a unique_key")
    if batch:
        _check_columns(batch, key, "unique_key")
    if current:
        _check_columns(current, key, "unique_key")
    seen: set[tuple[Any, ...]] = set()
    for row in batch:
        k = tuple(row[c] for c in key)
        if any(is_null(v) for v in k):
            continue
        if k in seen:
            raise WriteKeyError(f"the batch repeats the unique_key {k!r}")
        seen.add(k)
    by_key: dict[tuple[Any, ...], Row] = {}
    keyless: list[Row] = []  # target rows with a null key can never match
    for row in current:
        k = tuple(row.get(c) for c in key)
        if any(is_null(v) for v in k):
            keyless.append(dict(row))
        else:
            by_key[k] = dict(row)
    inserted = updated = deleted = 0
    for row in batch:
        k = tuple(row[c] for c in key)
        is_delete = cfg.deletes is not None and (
            not is_null(row.get(cfg.deletes.column))
            and row.get(cfg.deletes.column) == cfg.deletes.value
        )
        target = None if any(is_null(v) for v in k) else by_key.get(k)
        if target is None:
            if is_delete:
                continue
            by_key_key = k if not any(is_null(v) for v in k) else None
            if by_key_key is None:
                keyless.append(dict(row))
            else:
                by_key[by_key_key] = dict(row)
            inserted += 1
            continue
        if cfg.update_condition is not None:
            if condition is None:
                raise ValueError("update_condition needs a predicate")
            if condition(row, target) is not True:
                continue
        if is_delete:
            deleted += 1
            if cfg.deletes is not None and cfg.deletes.mode == "hard":
                del by_key[k]
                continue
        by_key[k] = dict(row)
        if not is_delete:
            updated += 1
    columns = _union(current, batch)
    rows = [{c: r.get(c) for c in columns} for r in [*by_key.values(), *keyless]]
    return MergeResult(rows, inserted, updated, deleted)


def merge_written(result: MergeResult, cfg: WriteConfig) -> int:
    """`rows_written` of a merge: inserts, updates and soft deletes."""
    soft = 0
    if cfg.deletes is not None and cfg.deletes.mode == "soft":
        soft = result.deleted
    return result.inserted + result.updated + soft


def replace_partitions_rows(
    current: Sequence[Mapping[str, Any]],
    batch: Sequence[Mapping[str, Any]],
    partition_by: Sequence[str],
) -> tuple[list[Row], int]:
    """Swap every partition present in `batch`; null is a partition value. Returns (rows, deleted)."""
    if not partition_by:
        raise WriteKeyError("replace_partitions needs partition_by")
    _check_columns(batch, partition_by, "partition_by")
    _check_columns(current, partition_by, "partition_by")
    replaced = {tuple(_part(r.get(c)) for c in partition_by) for r in batch}
    kept = [
        dict(r) for r in current if tuple(_part(r.get(c)) for c in partition_by) not in replaced
    ]
    columns = _union(current, batch)
    rows = [{c: r.get(c) for c in columns} for r in [*kept, *batch]]
    return rows, len(current) - len(kept)


def _part(value: Any) -> Any:
    return None if is_null(value) else value


@dataclass(frozen=True)
class Scd2Result:
    rows: list[Row]
    inserted: int
    """New current rows."""
    closed: int
    """Current rows closed by a change (`rows_updated`)."""
    deleted: int
    """Current rows closed by a delete."""


def _same(a: Any, b: Any) -> bool:
    return (is_null(a) and is_null(b)) or (not is_null(a) and not is_null(b) and a == b)


def scd2_rows(
    current: Sequence[Mapping[str, Any]],
    batch: Sequence[Mapping[str, Any]],
    cfg: WriteConfig,
    ts: datetime,
) -> Scd2Result:
    """Apply `batch` to the scd2 table `current` (business plus system columns) at time `ts`.
    `cfg.full_refresh` starts from an empty table. Row order is not preserved."""
    key, tracked = cfg.unique_key, cfg.tracked_columns
    system = [c.name for c in SCD2_COLUMNS]
    if not key:
        raise WriteKeyError("scd2 needs a unique_key")
    _check_columns(batch, (*key, *tracked), "unique_key or tracked")
    seen: set[tuple[Any, ...]] = set()
    for row in batch:
        if any(c in row for c in system):
            raise WriteKeyError("the batch has a column named like an scd2 system column")
        k = tuple(row[c] for c in key)
        if any(is_null(v) for v in k):
            raise WriteKeyError(f"the batch has a null unique_key {k!r}")
        if k in seen:
            raise WriteKeyError(f"the batch repeats the unique_key {k!r}")
        seen.add(k)
    old = [] if cfg.full_refresh else [dict(r) for r in current]
    live = {tuple(r[c] for c in key): r for r in old if r["is_current"] is True}
    out = [r for r in old if r["is_current"] is not True]
    inserted = closed = deleted = 0
    for row in batch:
        k = tuple(row[c] for c in key)
        d = cfg.deletes
        is_delete = d is not None and not is_null(row.get(d.column)) and row[d.column] == d.value
        target = live.get(k)
        changed = target is not None and any(not _same(row[c], target[c]) for c in tracked)
        if target is not None and (is_delete or changed):
            out.append({**live.pop(k), "valid_to": ts, "is_current": False})
            deleted += is_delete
            closed += not is_delete
        if not is_delete and (target is None or changed):
            out.append({**row, "valid_from": ts, "valid_to": None, "is_current": True})
            inserted += 1
    out += live.values()
    business = [c for c in _union(old, batch) if c not in system]
    rows = [{c: r.get(c) for c in [*business, *system]} for r in out]
    return Scd2Result(rows, inserted, closed, deleted)


def sort_key(row: Mapping[str, Any]) -> str:
    """A total order for comparing row sets whose order is unspecified."""
    return repr(sorted(row.items(), key=lambda kv: kv[0]))


__all__ = [
    "FAILED_COLUMN",
    "RUN_ID_COLUMN",
    "TS_COLUMN",
    "MergeResult",
    "Scd2Result",
    "align_rows",
    "apply_drops_rows",
    "cast_value",
    "evaluate_rows",
    "is_null",
    "merge_rows",
    "merge_written",
    "replace_partitions_rows",
    "scd2_rows",
    "sort_key",
]
