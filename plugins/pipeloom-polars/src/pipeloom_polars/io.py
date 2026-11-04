"""Reading and writing files, and the logical-to-polars type mapping."""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any, Final

import polars as pl

from pipeloom.engine.types import Schema

EXTENSIONS: Final = {"parquet": "parquet", "csv": "csv", "ndjson": "ndjson"}
"""Part file extension by catalog format."""

_SIMPLE: dict[str, Any] = {
    "int8": pl.Int8,
    "int16": pl.Int16,
    "int32": pl.Int32,
    "int64": pl.Int64,
    "int128": pl.Int128,
    "uint8": pl.UInt8,
    "uint16": pl.UInt16,
    "uint32": pl.UInt32,
    "uint64": pl.UInt64,
    "float32": pl.Float32,
    "float64": pl.Float64,
    "string": pl.String,
    "boolean": pl.Boolean,
    "date": pl.Date,
    "time": pl.Time,
    "binary": pl.Binary,
}
if hasattr(pl, "UInt128"):  # polars 1.34 and later; the floor is 1.30
    _SIMPLE["uint128"] = pl.UInt128
_TIMESTAMP = re.compile(r"timestamp\[(ms|us|ns)(?:, ([^\]]+))?\]")
_DECIMAL = re.compile(r"decimal\((\d+),(\d+)\)")


def polars_dtype(logical: str) -> Any | None:
    """The polars dtype of a logical type, or None for one this engine cannot name."""
    simple = _SIMPLE.get(logical)
    if simple is not None:
        return simple
    ts = _TIMESTAMP.fullmatch(logical)
    if ts:
        return pl.Datetime(ts.group(1), ts.group(2))  # type: ignore[arg-type]
    dec = _DECIMAL.fullmatch(logical)
    if dec:
        return pl.Decimal(int(dec.group(1)), int(dec.group(2)))
    return None


def typed_columns(columns: Schema) -> dict[str, Any]:
    """The polars dtype of every column this engine can name; the others are left out."""
    out: dict[str, Any] = {}
    for col in columns:
        dtype = polars_dtype(col.dtype)
        if dtype is not None:
            out[col.name] = dtype
    return out


def scan(fmt: str, location: str, options: Mapping[str, Any]) -> pl.LazyFrame:
    """A lazy frame over a source file, a directory or a glob."""
    if fmt == "parquet":
        return pl.scan_parquet(location, **options)
    if fmt == "csv":
        return pl.scan_csv(location, **options)
    if fmt == "ndjson":
        return pl.scan_ndjson(location, **options)
    if fmt == "json":
        return pl.read_json(location, **options).lazy()
    raise ValueError(f"polars engine cannot read format '{fmt}'")


def scan_part(fmt: str, path: Path, schema: Schema) -> pl.LazyFrame:
    """A lazy frame over one committed part file.

    csv and ndjson lose type information, so the stored logical types are applied column by
    column; a column this engine cannot name keeps what polars infers. A column the file lacks
    is ignored here (`align_frame` adds it).
    """
    if fmt == "parquet":
        return pl.scan_parquet(str(path))
    known = typed_columns(schema)
    if fmt == "csv":
        return pl.scan_csv(str(path), schema_overrides=known)
    if fmt == "ndjson":
        # A decimal is a JSON number there: read it as text and cast, or the schema fails.
        decimals = {n: t for n, t in known.items() if isinstance(t, pl.Decimal)}
        read = {n: pl.String if n in decimals else t for n, t in known.items()}
        frame = pl.scan_ndjson(str(path), schema_overrides=read)
        have = set(frame.collect_schema().names())
        return frame.with_columns(*(pl.col(n).cast(t) for n, t in decimals.items() if n in have))
    raise ValueError(f"cannot read a table stored as '{fmt}'")


def align_frame(frame: Any, schema: Schema) -> Any:  # LazyFrame or DataFrame, same kind out
    """Columns in `schema` order; a missing one is a typed null, the others are cast."""
    have = set(frame.collect_schema().names())
    exprs = []
    for col in schema:
        expr = pl.col(col.name) if col.name in have else pl.lit(None)
        dtype = polars_dtype(col.dtype)
        exprs.append((expr if dtype is None else expr.cast(dtype)).alias(col.name))
    return frame.select(exprs)


def concat(frames: Sequence[Any]) -> pl.LazyFrame:
    out: pl.LazyFrame = pl.concat([f.lazy() for f in frames], how="vertical_relaxed")
    return out


def write(df: pl.DataFrame, fmt: str, path: Path) -> None:
    if fmt == "parquet":
        df.write_parquet(path)
    elif fmt == "csv":
        df.write_csv(path)
    elif fmt == "ndjson":
        df.write_ndjson(path)
    else:
        raise ValueError(f"cannot write format '{fmt}'")
