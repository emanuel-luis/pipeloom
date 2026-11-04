"""SQL text helpers: pure strings, no Spark."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta, timezone
from decimal import Decimal

from pipeloom_spark.sqltext import literal, partition_predicate, quote_ident, wrap_inputs


def test_quote_ident_doubles_backticks() -> None:
    assert quote_ident("a`b") == "`a``b`"


def test_wrap_inputs_binds_aliases_as_ctes_and_doubles_braces() -> None:
    template, kwargs = wrap_inputs('select {"a": 1} as j from src join other;  ', ["src", "other"])
    assert kwargs == {"src": "_pl_in0", "other": "_pl_in1"}
    assert template == (
        "WITH `src` AS (SELECT * FROM {_pl_in0}), `other` AS (SELECT * FROM {_pl_in1}) "
        'SELECT * FROM (\nselect {{"a": 1}} as j from src join other\n)'
    )


def test_wrap_inputs_without_aliases_leaves_the_query_alone() -> None:
    assert wrap_inputs("select '{x}' ;", []) == ("select '{x}'", {})


def test_string_literals_are_hex() -> None:
    text = "it's \\ é"
    assert literal(text, "string") == f"decode(X'{text.encode().hex()}','UTF-8')"
    assert literal(None, "string") == "NULL"


def test_literals() -> None:
    assert literal(7, "int64") == "7"
    assert literal(True, "boolean") == "TRUE"
    assert literal(1.5, "float64") == "CAST('1.5' AS DOUBLE)"
    assert literal(date(2026, 1, 2), "date") == "DATE '2026-01-02'"
    assert literal(Decimal("12.34"), "decimal(18,2)") == "CAST('12.34' AS DECIMAL(18,2))"
    assert literal(datetime(1970, 1, 1, 0, 0, 1, 5, tzinfo=UTC), "timestamp[us, UTC]") == (
        "timestamp_micros(1000005)"
    )
    offset = datetime(1970, 1, 1, 1, tzinfo=timezone(timedelta(hours=1)))
    assert literal(offset, "timestamp[us, UTC]") == "timestamp_micros(0)"
    assert literal(datetime(2026, 1, 2, 3, 4, 5), "timestamp[us]") == (
        "TIMESTAMP_NTZ '2026-01-02 03:04:05.000000'"
    )


def test_partition_predicate_shapes() -> None:
    assert partition_predicate(["p"], [], ["string"]) == "FALSE"
    assert partition_predicate(["p"], [(1,), (2,)], ["int64"]) == "`p` IN (1, 2)"
    assert partition_predicate(["p"], [(1,), (None,)], ["int64"]) == "`p` IN (1) OR `p` IS NULL"
    assert partition_predicate(["p"], [(None,)], ["int64"]) == "`p` IS NULL"
    assert partition_predicate(["a", "b"], [(1, None), (2, 3)], ["int64", "int64"]) == (
        "(`a` <=> 1 AND `b` <=> NULL) OR (`a` <=> 2 AND `b` <=> 3)"
    )
