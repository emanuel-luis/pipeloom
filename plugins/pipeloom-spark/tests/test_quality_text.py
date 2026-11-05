"""Expectation SQL text: pure strings, no Spark."""

from __future__ import annotations

from typing import Any

import pytest

from pipeloom.engine.types import Column, ExpectationSpec
from pipeloom_spark.quality import aggregate_sql, failed_sql, violation_sql
from pipeloom_spark.sqltext import literal

SCHEMA = (
    Column("id", "int64"),
    Column("f", "float64"),
    Column("s", "string"),
    Column("ts", "timestamp[us, UTC]"),
    Column("d", "date"),
)
F = "CASE WHEN isnan(`f`) THEN NULL ELSE `f` END"


def spec(
    kind: str, columns: tuple[str, ...], action: str = "warn", **params: Any
) -> ExpectationSpec:
    return ExpectationSpec(kind, kind, action, columns, params)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("check", "text"),
    [
        (spec("not_null", ("id",)), "coalesce((`id`) IS NULL, false)"),
        (spec("not_null", ("f",)), f"coalesce(({F}) IS NULL, false)"),
        (
            spec("accepted_values", ("id",), values=[1, 2]),
            "coalesce(((`id`) IS NOT NULL AND NOT ((`id`) IN (1, 2))), false)",
        ),
        (
            spec("range", ("f",), min=0, max=10),
            (
                f"coalesce((({F}) IS NOT NULL AND (({F}) < CAST('0' AS DOUBLE) OR ({F}) > "
                "CAST('10' AS DOUBLE))), false)"
            ),
        ),
        (
            spec("range", ("id",), max=5),
            "coalesce(((`id`) IS NOT NULL AND ((`id`) > 5)), false)",
        ),
        (
            spec("regex", ("s",), pattern="^a\\d'"),
            (
                "coalesce(((`s`) IS NOT NULL AND NOT (CAST((`s`) AS STRING) RLIKE "
                f"{literal(chr(94) + 'a' + chr(92) + 'd' + chr(39), 'string')})), false)"
            ),
        ),
        (spec("expr", (), expr="id > 1"), "coalesce(NOT (id > 1), false)"),
    ],
)
def test_violation_text(check: ExpectationSpec, text: str) -> None:
    assert violation_sql(check, SCHEMA) == text


def test_several_columns_violate_when_any_does() -> None:
    assert violation_sql(spec("not_null", ("id", "s")), SCHEMA) == (
        "coalesce((`id`) IS NULL OR (`s`) IS NULL, false)"
    )


def test_aggregates_come_in_one_list() -> None:
    checks = [
        spec("not_null", ("id",), "drop"),
        spec("regex", ("s",), pattern="a"),
        spec("unique", ("id", "f")),
        spec("row_count", ()),
        spec("freshness", ("ts",)),
        spec("freshness", ("d",)),
    ]
    assert aggregate_sql(checks, SCHEMA) == [
        "count(1)",
        "count_if(__pipeloom_v0)",
        "count_if(__pipeloom_v1)",
        "count_if(__pipeloom_v0)",
        f"count_if((`id`) IS NOT NULL AND ({F}) IS NOT NULL) - count(DISTINCT `id`, {F})",
        "count(1)",
        "max(unix_micros(`ts`))",
        "max(`d`)",
    ]


def test_no_drop_check_means_no_dropped_count() -> None:
    assert aggregate_sql([spec("not_null", ("id",))], SCHEMA) == [
        "count(1)",
        "count_if(__pipeloom_v0)",
    ]


def test_failed_lists_names_in_order() -> None:
    assert failed_sql(["a", "b"]) == (
        f"array_compact(array(CASE WHEN __pipeloom_v0 THEN {literal('a', 'string')} END, "
        f"CASE WHEN __pipeloom_v1 THEN {literal('b', 'string')} END))"
    )
