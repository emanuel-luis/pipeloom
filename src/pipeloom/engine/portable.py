"""Narwhals helpers engines may reuse. The only module of the core that imports narwhals."""

from __future__ import annotations

import re
from collections.abc import Callable, Collection, Iterator, Sequence
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from typing import Any, Literal

import narwhals.stable.v2 as nw

from pipeloom.diagnostics import JsonValue
from pipeloom.engine.errors import WriteKeyError
from pipeloom.engine.types import (
    ROW_CHECKS,
    Column,
    ExpectationSpec,
    QualityCounts,
    Schema,
    WriteConfig,
)

_SIMPLE: tuple[tuple[Any, str], ...] = (
    (nw.Int8, "int8"),
    (nw.Int16, "int16"),
    (nw.Int32, "int32"),
    (nw.Int64, "int64"),
    (nw.Int128, "int128"),
    (nw.UInt8, "uint8"),
    (nw.UInt16, "uint16"),
    (nw.UInt32, "uint32"),
    (nw.UInt64, "uint64"),
    (nw.UInt128, "uint128"),
    (nw.Float32, "float32"),
    (nw.Float64, "float64"),
    (nw.String, "string"),
    (nw.Categorical, "string"),
    (nw.Boolean, "boolean"),
    (nw.Date, "date"),
    (nw.Time, "time"),
    (nw.Binary, "binary"),
)


def logical_dtype(dtype: Any) -> str:
    """The logical type name of a Narwhals dtype, or "unknown"."""
    for cls, name in _SIMPLE:
        if isinstance(dtype, cls):
            return name
    if isinstance(dtype, nw.Datetime):
        unit = f"timestamp[{dtype.time_unit}"
        return f"{unit}, {dtype.time_zone}]" if dtype.time_zone else f"{unit}]"
    if isinstance(dtype, nw.Duration):
        return f"duration[{dtype.time_unit}]"
    if isinstance(dtype, nw.Decimal):
        precision, scale = getattr(dtype, "precision", None), getattr(dtype, "scale", None)
        return f"decimal({precision},{scale})" if precision is not None else "decimal"
    if isinstance(dtype, nw.List):
        return f"list<{logical_dtype(dtype.inner)}>"
    if isinstance(dtype, nw.Struct):
        return (
            "struct<" + ",".join(f"{f.name}:{logical_dtype(f.dtype)}" for f in dtype.fields) + ">"
        )
    return "unknown"


def to_frame_api(native: Any, api: Literal["native", "narwhals"]) -> Any:
    """What a Python model receives: the engine's own frame or a Narwhals one."""
    if api == "native":
        return native
    if api == "narwhals":
        return nw.from_native(native)
    raise ValueError(f"unknown frame_api {api!r}")


def from_frame_api(obj: Any) -> Any:
    """The native frame behind a model's result; a native frame passes through."""
    return nw.to_native(obj, pass_through=True)


def schema(native: Any) -> Schema:
    return tuple(
        Column(name, logical_dtype(dtype))
        for name, dtype in nw.from_native(native).collect_schema().items()
    )


def _collect(frame: Any) -> Any:
    return frame.collect() if isinstance(frame, nw.LazyFrame) else frame


def _plain(value: Any) -> Any:
    """A plain Python value, or None for a missing one (None, NaN, NaT, pandas NA)."""
    if value is None:
        return None
    try:
        if value != value:  # noqa: PLR0124 - NaN and NaT differ from themselves; pandas NA raises
            return None
    except TypeError:
        return None
    if hasattr(value, "to_pydatetime"):  # pandas Timestamp
        return value.to_pydatetime()
    if type(value).__module__ == "numpy":
        return value.item()
    return value


def count(native: Any) -> int:
    return int(_collect(nw.from_native(native).select(nw.len())).item())


def max_value(native: Any, column: str) -> Any:
    """The maximum of `column` as a Python value; None for an empty frame."""
    return _plain(_collect(nw.from_native(native).select(nw.col(column).max())).item())


def filter_since(native: Any, column: str, lower: Any, *, inclusive: bool = True) -> Any:
    """Rows with `column >= lower` (`>` when not inclusive), as a native frame of the same kind."""
    col = nw.col(column)
    keep = col >= lower if inclusive else col > lower
    return nw.to_native(nw.from_native(native).filter(keep))


_QUALIFIED = re.compile(r"'(?:[^']|'')*'|\b(source|target)\.(\w+)")


def rewrite_qualified(cond: str) -> str:
    """`source.x` becomes `x` and `target.x` becomes `__pipeloom_t_x`; quoted literals stay."""

    def sub(m: re.Match[str]) -> str:
        if m.group(1) is None:
            return m.group(0)
        return m.group(2) if m.group(1) == "source" else f"__pipeloom_t_{m.group(2)}"

    return _QUALIFIED.sub(sub, cond)


# -- partition values ---------------------------------------------------------------------------
#
# A partition key is computed natively, never from a Python round trip of the values: a timestamp
# becomes its integer epoch in nanoseconds, a decimal its text, a float its value with NaN as
# null, a date or time its text. Rows are grouped by those exact native columns. The JSON key
# stored with a part file is a normalized text of the same value (ISO text for timestamps in UTC
# with nine fractional digits, a decimal without trailing zeros, null for null and NaN), so a
# value keeps matching after a lossless widening (decimal scale, timestamp unit, int to float).
# float32 to float64 is not lossless for most values and keys of those columns do not match.

_NS = {"ns": 1, "us": 1_000, "ms": 1_000_000, "s": 1_000_000_000}
_EPOCH = datetime(1970, 1, 1)


@dataclass(frozen=True)
class _PartCol:
    name: str
    kind: Literal["ts", "dec", "float", "plain", "str"]
    scale: int = 1
    tz: bool = False


def _part_col(name: str, dtype: Any) -> _PartCol:
    if isinstance(dtype, nw.Datetime):
        return _PartCol(name, "ts", _NS[dtype.time_unit], dtype.time_zone is not None)
    if isinstance(dtype, nw.Decimal):
        return _PartCol(name, "dec")
    if isinstance(dtype, nw.Float32 | nw.Float64):
        return _PartCol(name, "float")
    if isinstance(dtype, nw.Boolean | nw.String) or logical_dtype(dtype).startswith(
        ("int", "uint")
    ):
        return _PartCol(name, "plain")
    return _PartCol(name, "str")


def _key_expr(col: _PartCol) -> Any:
    ref = nw.col(col.name)
    if col.kind == "ts":
        return ref.cast(nw.Int64)
    if col.kind in ("dec", "str"):
        return ref.cast(nw.String)
    if col.kind == "float":
        return ref.fill_nan(None)
    return ref


def _format_key(col: _PartCol, raw: Any) -> JsonValue:
    value = _plain(raw)
    if value is None:
        return None
    if col.kind == "ts":
        seconds, nanos = divmod(int(value) * col.scale, 1_000_000_000)
        text = (_EPOCH + timedelta(seconds=seconds)).isoformat(timespec="seconds")
        return f"{text}.{nanos:09d}" + ("+00:00" if col.tz else "")
    if col.kind == "dec":
        return format(Decimal(str(value)).normalize(), "f")
    return value  # type: ignore[no-any-return]


def decode_partition_value(dtype: str, raw: JsonValue) -> Any:
    """Read back a stored partition value, given the column's logical type. A timestamp keeps
    microseconds only (Python datetimes have no nanoseconds)."""
    if raw is None:
        return None
    if dtype.startswith("timestamp"):
        return datetime.fromisoformat(str(raw))
    if dtype == "date":
        return date.fromisoformat(str(raw))
    if dtype == "time":
        return time.fromisoformat(str(raw))
    if dtype.startswith("decimal"):
        return Decimal(str(raw))
    return raw


def _eager(native: Any) -> Any:
    return _collect(nw.from_native(native))


def empty_like(native: Any) -> Any:
    """The frame's columns and types with no rows."""
    return nw.to_native(_eager(native).head(0))


def check_columns(native: Any, columns: Sequence[str], what: str) -> None:
    """Raise `WriteKeyError` (PLM206) when a frame lacks one of `columns`."""
    have = set(_eager(native).columns)
    for column in columns:
        if column not in have:
            raise WriteKeyError(f"{what} column '{column}' is missing")


def _null_expr(col: _PartCol) -> Any:
    """Expression: the partition value is null. Read from the column itself for a timestamp,
    whose integer cast turns a pandas NaT into a fake number instead of null."""
    ref = nw.col(col.name)
    return (ref.fill_nan(None) if col.kind == "float" else ref).is_null()


def _keyed(
    frame: Any, partition_by: Sequence[str]
) -> tuple[Any, list[_PartCol], list[tuple[Any, ...]]]:
    """The frame plus a native key column (`__pk<i>`) and a null flag (`__pn<i>`) per partition
    column, the column specs and the distinct raw tuples (the n keys, then the n flags), in
    order of first appearance. The key of a null row is meaningless; the flag decides."""
    schema = frame.schema
    specs = [_part_col(c, schema[c]) for c in partition_by]
    keys = [f"__pk{i}" for i in range(len(specs))]
    flags = [f"__pn{i}" for i in range(len(specs))]
    keyed = frame.with_columns(
        *(_key_expr(s).alias(n) for s, n in zip(specs, keys, strict=True)),
        *(_null_expr(s).alias(n) for s, n in zip(specs, flags, strict=True)),
    )
    raws = [tuple(r) for r in keyed.select(*keys, *flags).unique(maintain_order=True).iter_rows()]
    return keyed, specs, raws


def _match(raw: tuple[Any, ...]) -> Any:
    """Expression: the row's key columns equal `raw` exactly (null equals null)."""
    n = len(raw) // 2
    hit: Any = nw.lit(True)
    for i in range(n):
        flag = nw.col(f"__pn{i}")
        if bool(_plain(raw[n + i])):
            hit = hit & flag
        else:
            hit = hit & ~flag & (nw.col(f"__pk{i}") == _plain(raw[i])).fill_null(False)
    return hit


def _json_key(specs: Sequence[_PartCol], raw: tuple[Any, ...]) -> tuple[JsonValue, ...]:
    n = len(specs)
    return tuple(
        None if bool(_plain(raw[n + i])) else _format_key(s, raw[i]) for i, s in enumerate(specs)
    )


def partition_keys(native: Any, partition_by: Sequence[str]) -> list[tuple[JsonValue, ...]]:
    """The distinct partition keys of a frame (null is a value), without splitting it."""
    _, specs, raws = _keyed(_eager(native), partition_by)
    return list(dict.fromkeys(_json_key(specs, r) for r in raws))


def partition_groups(
    native: Any, partition_by: Sequence[str]
) -> Iterator[tuple[tuple[JsonValue, ...], Any]]:
    """Yield (JSON key, native frame) for each distinct partition, one at a time so the caller
    can write and release it. Raises when the parts do not add up to the frame.
    ponytail: one equality filter per partition (O(partitions x rows)); Narwhals has no
    single-pass split, so a group-by index split is the upgrade when that hurts."""
    frame = _eager(native)
    cols = list(frame.columns)
    keyed, specs, raws = _keyed(frame, partition_by)
    seen = 0
    for raw in raws:
        part = keyed.filter(_match(raw)).select(*cols)
        seen += part.shape[0]
        yield _json_key(specs, raw), nw.to_native(part)
    if seen != frame.shape[0]:
        raise RuntimeError(f"partitioning lost rows: {seen} of {frame.shape[0]}")


def drop_partitions(
    native: Any, wanted: Collection[tuple[JsonValue, ...]], partition_by: Sequence[str]
) -> tuple[Any, int]:
    """`native` without the rows whose partition key is in `wanted`, and how many rows that
    removed."""
    frame = _eager(native)
    keyed, specs, raws = _keyed(frame, partition_by)
    hit: Any = nw.lit(False)
    found = False
    for raw in raws:
        if _json_key(specs, raw) in wanted:
            hit = hit | _match(raw)
            found = True
    if not found:
        return nw.to_native(frame), 0
    cols = list(frame.columns)
    kept = keyed.filter(~hit).select(*cols)
    return nw.to_native(kept), int(frame.shape[0] - kept.shape[0])


def replace_partition_frames(
    current: Any, batch: Any, partition_by: Sequence[str]
) -> tuple[Any, int]:
    """Swap every partition present in `batch`; both frames share their columns. Returns the new
    frame and the number of rows removed from `current`."""
    if not partition_by:
        raise WriteKeyError("replace_partitions needs partition_by")
    check_columns(batch, partition_by, "partition_by")
    check_columns(current, partition_by, "partition_by")
    kept, deleted = drop_partitions(current, set(partition_keys(batch, partition_by)), partition_by)
    cols = list(_eager(current).columns)
    out = nw.concat([nw.from_native(kept), _eager(batch).select(*cols)], how="vertical")
    return nw.to_native(out), deleted


# -- merge --------------------------------------------------------------------------------------


@dataclass(frozen=True)
class MergeOutcome:
    frame: Any
    inserted: int
    updated: int
    deleted: int


_T = "__pipeloom_t_"
_MATCH = "__pipeloom_matched"
_NEW = "__pipeloom_new"
_COND = "__pipeloom_cond"
_DEL = "__pipeloom_del"


def _null_any(columns: Sequence[str]) -> Any:
    expr = nw.col(columns[0]).is_null()
    for c in columns[1:]:
        expr = expr | nw.col(c).is_null()
    return expr


def merge_frames(
    current: Any,
    batch: Any,
    cfg: WriteConfig,
    *,
    condition_hook: Callable[[Any, str], Any] | None,
) -> MergeOutcome:
    """Merge `batch` into `current` by `cfg.unique_key`; both share their columns. Row order is
    not preserved. `condition_hook(frame, condition)` evaluates the engine-dialect
    `update_condition` (already rewritten, see `rewrite_qualified`) on a native frame holding the
    batch columns and the target ones as `__pipeloom_t_<c>`, and returns a boolean native series.
    ponytail: assumes the target has no duplicate keys (merge keeps it that way)."""
    key = list(cfg.unique_key)
    if not key:
        raise WriteKeyError("merge needs a unique_key")
    check_columns(batch, key, "unique_key")
    check_columns(current, key, "unique_key")
    cur, bat = _eager(current), _eager(batch)
    cols = list(cur.columns)
    if set(bat.columns) != set(cols):
        raise ValueError("merge needs a batch with the columns of the target (align first)")
    bat = bat.select(*cols)
    nokey = _null_any(key)
    bat_keyed, bat_nokey = bat.filter(~nokey), bat.filter(nokey)
    cur_keyed, cur_nokey = cur.filter(~nokey), cur.filter(nokey)
    if bat_keyed.shape[0] and bool(bat_keyed.select(*key).is_duplicated().any()):
        raise WriteKeyError("the batch repeats a unique_key value")

    deletes = cfg.deletes
    soft = deletes is not None and deletes.mode == "soft"

    def flag_deletes(frame: Any) -> Any:
        if deletes is None:
            return frame.with_columns(nw.lit(False).alias(_DEL))
        hit = (nw.col(deletes.column) == deletes.value).fill_null(False)
        return frame.with_columns(hit.alias(_DEL))

    right = cur_keyed.with_columns(nw.lit(True).alias(_MATCH))
    right = right.rename({c: f"{_T}{c}" for c in cols})
    right = right.with_columns(*(nw.col(f"{_T}{k}").alias(f"{_T}k_{k}") for k in key))
    joined = bat_keyed.join(right, left_on=key, right_on=[f"{_T}k_{k}" for k in key], how="left")
    joined = flag_deletes(joined).with_columns(nw.col(_MATCH).is_null().alias(_NEW))
    if cfg.update_condition is None:
        joined = joined.with_columns(nw.lit(True).alias(_COND))
    else:
        if condition_hook is None:
            raise ValueError("update_condition needs a condition hook")
        mask = condition_hook(nw.to_native(joined), rewrite_qualified(cfg.update_condition))
        series = nw.from_native(mask, series_only=True).alias(_COND)
        joined = joined.with_columns(series).with_columns(nw.col(_COND).fill_null(False))
    is_new, is_del, cond = nw.col(_NEW), nw.col(_DEL), nw.col(_COND)
    inserts = joined.filter(is_new & ~is_del)
    changes = joined.filter(~is_new & cond)
    updates, deleted = changes.filter(~is_del), changes.filter(is_del)
    rewritten = changes if soft else updates
    nokey_inserts = flag_deletes(bat_nokey).filter(~is_del)
    survivors = (
        cur_keyed.join(changes.select(*key), on=key, how="anti") if changes.shape[0] else cur_keyed
    )
    parts = [cur_nokey, survivors, inserts, rewritten, nokey_inserts]
    frame = nw.concat([p.select(*cols) for p in parts], how="vertical")
    return MergeOutcome(
        nw.to_native(frame),
        int(inserts.shape[0] + nokey_inserts.shape[0]),
        int(updates.shape[0]),
        int(deleted.shape[0]),
    )


# -- quality ------------------------------------------------------------------------------------
#
# Row checks become one non-null boolean column per check (`__pipeloom_v<i>`, true = the row
# violates it), built with explicit null guards: a null input passes (except not_null) and NaN is
# null. Counts then come from one `select`. Only `expr` checks use the engine dialect, through the
# `dialect_hook(frame, expression)` the engine passes: it evaluates the string on a native frame
# and returns a boolean native series (true passes, false fails, null passes).

_V = "__pipeloom_v"

DialectHook = Callable[[Any, str], Any]


def violation_name(i: int) -> str:
    return f"{_V}{i}"


def _row_specs(checks: Sequence[ExpectationSpec]) -> list[ExpectationSpec]:
    return [c for c in checks if c.kind in ROW_CHECKS]


def _is_float(dtype: Any) -> bool:
    return isinstance(dtype, nw.Float32 | nw.Float64)


def _violated(spec: ExpectationSpec, schema: Any) -> Any:
    """Expression: the row violates the (non-expr) row check."""
    out: Any = None
    for column in spec.columns:
        ref = nw.col(column)
        if _is_float(schema[column]):
            ref = ref.fill_nan(None)
        if spec.kind == "not_null":
            bad = ref.is_null()
        else:
            if spec.kind == "accepted_values":
                values = spec.params["values"]
                assert isinstance(values, list)
                bad = ~ref.is_in(values)
            elif spec.kind == "range":
                low, high = spec.params.get("min"), spec.params.get("max")
                bad = nw.lit(False)
                if low is not None:
                    bad = bad | (ref < low)
                if high is not None:
                    bad = bad | (ref > high)
            else:  # regex
                bad = ~ref.cast(nw.String).str.contains(str(spec.params["pattern"]))
            bad = ~ref.is_null() & bad
        bad = bad.fill_null(False)
        out = bad if out is None else out | bad
    return out


def violation_columns(
    native: Any, specs: Sequence[ExpectationSpec], *, dialect_hook: DialectHook
) -> Any:
    """`native` as an eager frame plus `__pipeloom_v<i>`, a non-null boolean per row check in
    `specs` (other kinds are skipped; `i` counts the row checks only)."""
    frame = _eager(native)
    schema = frame.schema
    rows = _row_specs(specs)
    if any(s.kind == "expr" for s in rows):
        floats = [n for n, t in schema.items() if _is_float(t)]
        clean = frame.with_columns(*(nw.col(n).fill_nan(None) for n in floats))
        raw = {
            violation_name(i): nw.from_native(
                dialect_hook(nw.to_native(clean), str(s.params["expr"])), series_only=True
            )
            .cast(nw.Boolean)
            .alias(violation_name(i))
            for i, s in enumerate(rows)
            if s.kind == "expr"
        }
        frame = frame.with_columns(*raw.values())
        frame = frame.with_columns(*(~nw.col(n) for n in raw))
        frame = frame.with_columns(*(nw.col(n).fill_null(False) for n in raw))
    cols = [
        _violated(s, schema).alias(violation_name(i))
        for i, s in enumerate(rows)
        if s.kind != "expr"
    ]
    if cols:
        frame = frame.with_columns(*cols)
    return nw.to_native(frame)


def count_violations(native: Any, specs: Sequence[ExpectationSpec]) -> QualityCounts:
    """Counts from a frame that `violation_columns` made for the same `specs`, in one select.
    `aggregates` is empty."""
    frame = _eager(native)
    rows = _row_specs(specs)
    drops = [violation_name(i) for i, s in enumerate(rows) if s.action == "drop"]
    exprs = [nw.len().alias("__n")]
    exprs += [
        nw.col(violation_name(i)).cast(nw.Int64).sum().alias(f"__f{i}") for i in range(len(rows))
    ]
    if drops:
        any_drop = nw.any_horizontal(*(nw.col(n) for n in drops), ignore_nulls=False)
        exprs.append(any_drop.cast(nw.Int64).sum().alias("__d"))
    row = frame.select(*exprs).row(0)
    failed = {s.name: int(row[1 + i] or 0) for i, s in enumerate(rows)}
    dropped = int(row[-1] or 0) if drops else 0
    return QualityCounts(int(row[0]), failed, dropped, {})


def aggregate_values(native: Any, specs: Sequence[ExpectationSpec]) -> dict[str, Any]:
    """unique: duplicate rows (rows with a null key part are left out); row_count: the rows;
    freshness: the max of the column as a Python value."""
    frame = _eager(native)
    out: dict[str, Any] = {}
    for spec in specs:
        if spec.kind == "row_count":
            out[spec.name] = int(frame.shape[0])
        elif spec.kind == "unique":
            keys = list(spec.columns)
            floats = [k for k in keys if _is_float(frame.schema[k])]
            keyed = frame.select(*keys).with_columns(*(nw.col(k).fill_nan(None) for k in floats))
            keyed = keyed.drop_nulls()
            out[spec.name] = int(keyed.shape[0] - keyed.unique().shape[0])
        elif spec.kind == "freshness":
            out[spec.name] = max_value(nw.to_native(frame), spec.columns[0])
    return out


def evaluate(
    native: Any, checks: Sequence[ExpectationSpec], *, dialect_hook: DialectHook
) -> QualityCounts:
    """The full `EngineSession.evaluate` for portable engines."""
    rows = _row_specs(checks)
    marked = violation_columns(native, rows, dialect_hook=dialect_hook) if rows else native
    counts = count_violations(marked, rows) if rows else QualityCounts(count(native), {}, 0, {})
    return QualityCounts(
        counts.total_rows, counts.failed, counts.dropped, aggregate_values(native, checks)
    )


@dataclass(frozen=True)
class DropSplit:
    kept: Any
    """Native frame: the original columns of the rows no drop check caught."""
    dropped: Any
    """Native frame: the other rows, plus one boolean column per drop check (`flags`)."""
    names: list[str]
    flags: list[str]
    """Column names of the drop checks, parallel to `names` (declaration order)."""


def split_drops(
    native: Any, checks: Sequence[ExpectationSpec], *, dialect_hook: DialectHook
) -> DropSplit:
    drops = [c for c in checks if c.kind in ROW_CHECKS and c.action == "drop"]
    frame = _eager(native)
    original = list(frame.columns)
    if not drops:
        return DropSplit(nw.to_native(frame), nw.to_native(frame.head(0)), [], [])
    marked = _eager(violation_columns(native, drops, dialect_hook=dialect_hook))
    flags = [violation_name(i) for i in range(len(drops))]
    bad = nw.any_horizontal(*(nw.col(f) for f in flags), ignore_nulls=False)
    return DropSplit(
        nw.to_native(marked.filter(~bad).select(*original)),
        nw.to_native(marked.filter(bad)),
        [d.name for d in drops],
        flags,
    )
