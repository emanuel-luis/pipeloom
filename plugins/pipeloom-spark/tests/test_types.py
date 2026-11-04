"""Type mapping tables, driven by fake Spark types (no pyspark)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pytest

from pipeloom_spark.types import spark_ddl, to_logical


def fake(name: str, **attrs: Any) -> Any:
    cls = type(name, (), {})
    obj = cls()
    for key, value in attrs.items():
        setattr(obj, key, value)
    return obj


@dataclass
class Field:
    name: str
    dataType: Any


@pytest.mark.parametrize(
    ("spark", "logical"),
    [
        ("ByteType", "int8"),
        ("ShortType", "int16"),
        ("IntegerType", "int32"),
        ("LongType", "int64"),
        ("FloatType", "float32"),
        ("DoubleType", "float64"),
        ("StringType", "string"),
        ("VarcharType", "string"),
        ("CharType", "string"),
        ("BooleanType", "boolean"),
        ("BinaryType", "binary"),
        ("DateType", "date"),
        ("TimestampType", "timestamp[us, UTC]"),
        ("TimestampNTZType", "timestamp[us]"),
        ("DayTimeIntervalType", "duration[us]"),
        ("NullType", "null"),
        ("MapType", "unknown"),
    ],
)
def test_to_logical_simple(spark: str, logical: str) -> None:
    assert to_logical(fake(spark)) == logical


def test_to_logical_nested() -> None:
    dec = fake("DecimalType", precision=18, scale=2)
    assert to_logical(dec) == "decimal(18,2)"
    arr = fake("ArrayType", elementType=dec)
    assert to_logical(arr) == "list<decimal(18,2)>"
    struct = fake("StructType", fields=[Field("a", fake("LongType")), Field("b", arr)])
    assert to_logical(struct) == "struct<a:int64,b:list<decimal(18,2)>>"


@pytest.mark.parametrize(
    ("logical", "ddl"),
    [
        ("int8", "tinyint"),
        ("int64", "bigint"),
        ("uint8", "smallint"),
        ("uint16", "int"),
        ("uint32", "bigint"),
        ("uint64", "decimal(20,0)"),
        ("float32", "float"),
        ("decimal(18,2)", "decimal(18,2)"),
        ("timestamp[us, UTC]", "timestamp"),
        ("timestamp[ns, UTC]", "timestamp"),
        ("timestamp[ms]", "timestamp_ntz"),
        ("null", "void"),
    ],
)
def test_spark_ddl(logical: str, ddl: str) -> None:
    assert spark_ddl(logical) == ddl


@pytest.mark.parametrize("logical", ["unknown", "time", "int128", "list<int64>"])
def test_spark_ddl_rejects_what_spark_cannot_hold(logical: str) -> None:
    with pytest.raises(ValueError, match="no type"):
        spark_ddl(logical)
