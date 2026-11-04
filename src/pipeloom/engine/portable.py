"""Narwhals helpers engines may reuse. The only module of the core that imports narwhals."""

from __future__ import annotations

import re
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, time
from decimal import Decimal
from typing import Any, Literal

import narwhals.stable.v2 as nw

from pipeloom.diagnostics import JsonValue
from pipeloom.engine.errors import WriteKeyError
from pipeloom.engine.types import Column, Schema, WriteConfig

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


def encode_partition_value(value: Any) -> JsonValue:
    """A partition value as JSON: ISO text for dates, times and timestamps (aware ones in UTC),
    a string for a decimal, None for null and NaN."""
    value = _plain(value)
    if isinstance(value, datetime):
        return (value.astimezone(UTC) if value.tzinfo else value).isoformat()
    if isinstance(value, date | time):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if value is None or isinstance(value, str | int | float | bool):
        return value
    return str(value)


def decode_partition_value(dtype: str, raw: JsonValue) -> Any:
    """Read back what `encode_partition_value` wrote, given the column's logical type."""
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


def partition_key(values: Sequence[Any]) -> tuple[JsonValue, ...]:
    return tuple(encode_partition_value(v) for v in values)


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


def split_partitions(
    native: Any, partition_by: Sequence[str]
) -> list[tuple[tuple[JsonValue, ...], Any]]:
    """One (encoded partition values, native frame) per distinct tuple of `partition_by`; null is
    a value. An empty frame gives no parts."""
    frame = _eager(native)
    out = []
    # ponytail: one filter per partition (O(parts x rows)); a group-by split when that hurts.
    for values in distinct_partitions(frame, partition_by):
        part = frame.filter(_in_partitions(partition_by, [values]))
        out.append((partition_key(values), nw.to_native(part)))
    return out


def distinct_partitions(native: Any, partition_by: Sequence[str]) -> list[tuple[Any, ...]]:
    """The distinct partition tuples of a frame, as plain Python values."""
    frame = _eager(native).select(*partition_by).unique(maintain_order=True)
    return [tuple(_plain(v) for v in row) for row in frame.iter_rows()]


def _in_partitions(partition_by: Sequence[str], values: Sequence[tuple[Any, ...]]) -> Any:
    """Expression: the row's partition is one of `values` (null equals null)."""
    hit: Any = nw.lit(False)
    for tup in values:
        row: Any = nw.lit(True)
        for column, v in zip(partition_by, tup, strict=True):
            col = nw.col(column)
            row = row & (col.is_null() if v is None else (col == v).fill_null(False))
        hit = hit | row
    return hit


def drop_partitions(
    native: Any, values: Sequence[tuple[Any, ...]], partition_by: Sequence[str]
) -> tuple[Any, int]:
    """`native` without the rows of the partitions in `values`, and how many rows that removed.
    ponytail: one predicate per partition value, fine for hundreds, not for many thousands."""
    frame = _eager(native)
    if not values:
        return nw.to_native(frame), 0
    hit = _in_partitions(partition_by, values)
    return nw.to_native(frame.filter(~hit)), int(frame.filter(hit).shape[0])


def replace_partition_frames(
    current: Any, batch: Any, partition_by: Sequence[str]
) -> tuple[Any, int]:
    """Swap every partition present in `batch`; both frames share their columns. Returns the new
    frame and the number of rows removed from `current`."""
    if not partition_by:
        raise WriteKeyError("replace_partitions needs partition_by")
    check_columns(batch, partition_by, "partition_by")
    check_columns(current, partition_by, "partition_by")
    kept, deleted = drop_partitions(current, distinct_partitions(batch, partition_by), partition_by)
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
