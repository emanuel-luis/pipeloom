"""Spark types to logical types and back.

`to_logical` reads a Spark type by its class name and attributes, so it needs no Spark import.
"""

from __future__ import annotations

import re
from typing import Any, Final

_SIMPLE: Final = {
    "ByteType": "int8",
    "ShortType": "int16",
    "IntegerType": "int32",
    "LongType": "int64",
    "FloatType": "float32",
    "DoubleType": "float64",
    "StringType": "string",
    "VarcharType": "string",
    "CharType": "string",
    "BooleanType": "boolean",
    "BinaryType": "binary",
    "DateType": "date",
    "TimestampType": "timestamp[us, UTC]",
    "TimestampNTZType": "timestamp[us]",
    "DayTimeIntervalType": "duration[us]",
    "NullType": "null",
}


def to_logical(dt: Any) -> str:
    """The logical type of a Spark type, `unknown` for one with no logical form."""
    name = type(dt).__name__
    simple = _SIMPLE.get(name)
    if simple is not None:
        return simple
    if name == "DecimalType":
        return f"decimal({dt.precision},{dt.scale})"
    if name == "ArrayType":
        return f"list<{to_logical(dt.elementType)}>"
    if name == "StructType":
        return "struct<" + ",".join(f"{f.name}:{to_logical(f.dataType)}" for f in dt.fields) + ">"
    return "unknown"


_WIDER: Final = {"uint8": "int16", "uint16": "int32", "uint32": "int64", "uint64": "decimal(20,0)"}
_TIMESTAMP = re.compile(r"timestamp\[(ms|us|ns)(?:, ([^\]]+))?\]")
_DECIMAL = re.compile(r"decimal\((\d+),(\d+)\)")
_DDL: Final = {
    "int8": "tinyint",
    "int16": "smallint",
    "int32": "int",
    "int64": "bigint",
    "float32": "float",
    "float64": "double",
    "string": "string",
    "boolean": "boolean",
    "binary": "binary",
    "date": "date",
    "null": "void",
    "duration[us]": "interval day to second",
}
_CLASSES: Final = {
    "tinyint": "ByteType",
    "smallint": "ShortType",
    "int": "IntegerType",
    "bigint": "LongType",
    "float": "FloatType",
    "double": "DoubleType",
    "string": "StringType",
    "boolean": "BooleanType",
    "binary": "BinaryType",
    "date": "DateType",
    "timestamp": "TimestampType",
    "timestamp_ntz": "TimestampNTZType",
    "void": "NullType",
    "interval day to second": "DayTimeIntervalType",
}


def spark_ddl(logical: str) -> str:
    """The Spark SQL type name of a logical type.

    Spark has no unsigned integers, so they widen (`uint64` to `decimal(20,0)`), and nanosecond or
    millisecond timestamps become microseconds. Raises ValueError for a type Spark cannot hold
    (the write reports PLM201).
    """
    logical = _WIDER.get(logical, logical)
    ts = _TIMESTAMP.fullmatch(logical)
    if ts:
        return "timestamp" if ts.group(2) else "timestamp_ntz"
    if _DECIMAL.fullmatch(logical):
        return logical
    ddl = _DDL.get(logical)
    if ddl is None:
        raise ValueError(f"the spark engine has no type for '{logical}'")
    return ddl


def to_spark(logical: str) -> Any:
    """The `pyspark.sql.types` instance of a logical type (see `spark_ddl`)."""
    from pyspark.sql import types

    ddl = spark_ddl(logical)
    dec = _DECIMAL.fullmatch(ddl)
    if dec:
        return types.DecimalType(int(dec.group(1)), int(dec.group(2)))
    return getattr(types, _CLASSES[ddl])()
