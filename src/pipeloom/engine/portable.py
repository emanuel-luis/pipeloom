"""Narwhals helpers engines may reuse. The only module of the core that imports narwhals."""

from __future__ import annotations

import re
from typing import Any, Literal

import narwhals.stable.v2 as nw

from pipeloom.engine.types import Column, Schema

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
