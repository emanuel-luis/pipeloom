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

_SIMPLE: Final[Mapping[str, Any]] = {
    "int8": pl.Int8,
    "int16": pl.Int16,
    "int32": pl.Int32,
    "int64": pl.Int64,
    "int128": pl.Int128,
    "uint8": pl.UInt8,
    "uint16": pl.UInt16,
    "uint32": pl.UInt32,
    "uint64": pl.UInt64,
    "uint128": pl.UInt128,
    "float32": pl.Float32,
    "float64": pl.Float64,
    "string": pl.String,
    "boolean": pl.Boolean,
    "date": pl.Date,
    "time": pl.Time,
    "binary": pl.Binary,
}
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


def typed_schema(columns: Schema) -> dict[str, Any] | None:
    """A polars schema for `columns`, or None when any type is unknown (infer instead)."""
    out: dict[str, Any] = {}
    for col in columns:
        dtype = polars_dtype(col.dtype)
        if dtype is None:
            return None
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


def scan_parts(fmt: str, files: Sequence[Path], schema: Schema) -> pl.LazyFrame:
    """A lazy frame over the committed part files of a table."""
    paths = [str(f) for f in files]
    if fmt == "parquet":
        return pl.scan_parquet(paths)
    typed = typed_schema(schema)
    if fmt == "csv":
        return pl.concat([pl.scan_csv(p, schema=typed) for p in paths])
    if fmt == "ndjson":
        return pl.concat([pl.scan_ndjson(p, schema=typed) for p in paths])
    raise ValueError(f"cannot read a table stored as '{fmt}'")


def write(df: pl.DataFrame, fmt: str, path: Path) -> None:
    if fmt == "parquet":
        df.write_parquet(path)
    elif fmt == "csv":
        df.write_csv(path)
    elif fmt == "ndjson":
        df.write_ndjson(path)
    else:
        raise ValueError(f"cannot write format '{fmt}'")
