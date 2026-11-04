"""Reading and writing files, and the logical-to-Arrow type mapping."""

from __future__ import annotations

import glob
import re
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any, Final

import pandas as pd
import pyarrow as pa

from pipeloom.engine.types import Schema

EXTENSIONS: Final = {"parquet": "parquet", "csv": "csv", "ndjson": "ndjson"}
"""Part file extension by catalog format."""

_SIMPLE: Final[Mapping[str, Any]] = {
    "int8": pa.int8(),
    "int16": pa.int16(),
    "int32": pa.int32(),
    "int64": pa.int64(),
    "uint8": pa.uint8(),
    "uint16": pa.uint16(),
    "uint32": pa.uint32(),
    "uint64": pa.uint64(),
    "float32": pa.float32(),
    "float64": pa.float64(),
    "string": pa.string(),
    "boolean": pa.bool_(),
    "date": pa.date32(),
    "time": pa.time64("us"),
    "binary": pa.binary(),
}
_TIMESTAMP = re.compile(r"timestamp\[(ms|us|ns)(?:, ([^\]]+))?\]")
_DECIMAL = re.compile(r"decimal\((\d+),(\d+)\)")


def arrow_type(logical: str) -> Any | None:
    """The Arrow type of a logical type, or None for one this engine cannot name."""
    simple = _SIMPLE.get(logical)
    if simple is not None:
        return simple
    ts = _TIMESTAMP.fullmatch(logical)
    if ts:
        return pa.timestamp(ts.group(1), tz=ts.group(2))
    dec = _DECIMAL.fullmatch(logical)
    if dec:
        return pa.decimal128(int(dec.group(1)), int(dec.group(2)))
    return None


def _concat(frames: list[pd.DataFrame]) -> pd.DataFrame:
    return frames[0] if len(frames) == 1 else pd.concat(frames, ignore_index=True)


def _files(location: str) -> list[str]:
    if not any(ch in location for ch in "*?["):
        return [location]
    found = sorted(glob.glob(location))
    if not found:
        raise FileNotFoundError(f"no file matches '{location}'")
    return found


def _read_one(fmt: str, path: str, options: Mapping[str, Any]) -> pd.DataFrame:
    # Arrow-backed columns keep nulls in integer columns and keep the logical types.
    kw: dict[str, Any] = {"dtype_backend": "pyarrow", **options}
    if fmt == "parquet":
        return pd.read_parquet(path, **kw)
    if fmt == "csv":
        return pd.read_csv(path, **kw)
    if fmt == "ndjson":
        return pd.read_json(path, lines=True, **{"dtype": False, **kw})
    if fmt == "json":
        return pd.read_json(path, **{"dtype": False, **kw})
    raise ValueError(f"pandas engine cannot read format '{fmt}'")


def read(fmt: str, location: str, options: Mapping[str, Any]) -> pd.DataFrame:
    """A DataFrame over a source file or a glob."""
    return _concat([_read_one(fmt, p, options) for p in _files(location)])


def read_parts(fmt: str, files: Sequence[Path], schema: Schema) -> pd.DataFrame:
    """The committed part files of a table, concatenated, with the stored logical types."""
    frames = []
    for path in files:
        df = _read_one(fmt, str(path), {})
        if fmt != "parquet":  # csv and ndjson lose the types; parquet keeps them
            for col in schema:
                dtype = arrow_type(col.dtype)
                if dtype is None or col.name not in df.columns:
                    continue  # an unnamed type keeps what the reader inferred
                series = df[col.name]
                if pa.types.is_date(dtype) and pd.api.types.is_string_dtype(series):
                    series = series.str.slice(0, 10)  # ndjson writes a date as an ISO datetime
                df[col.name] = series.astype(pd.ArrowDtype(dtype))
        frames.append(df)
    return _concat(frames)


def write(df: pd.DataFrame, fmt: str, path: Path) -> None:
    if fmt == "parquet":
        df.to_parquet(path, engine="pyarrow", index=False)
    elif fmt == "csv":
        df.to_csv(path, index=False, lineterminator="\n")
    elif fmt == "ndjson":
        df.to_json(path, orient="records", lines=True, date_format="iso")
    else:
        raise ValueError(f"cannot write format '{fmt}'")
