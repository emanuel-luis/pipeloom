"""The polars engine declares sql_dialect postgres: sqlglot has no polars dialect, and
pl.SQLContext follows PostgreSQL syntax. These pin the constructs `ctx.sql` translates to it."""

from __future__ import annotations

from datetime import date

import polars as pl
import pytest

from pipeloom.runtime.sql import translate
from pipeloom_polars.spec import CAPABILITIES

pytest.importorskip("sqlglot")

FRAME = pl.LazyFrame(
    {
        "id": [1, 2, 3],
        "name": ["Ana", "bob", "Cid"],
        "d": [date(2024, 1, 1), date(2024, 2, 1), date(2024, 3, 1)],
    }
)


def run(query: str, dialect: str) -> list[dict[str, object]]:
    text = translate(query, dialect, CAPABILITIES.sql_dialect)
    out = pl.SQLContext(frames={"t": FRAME}, eager=False).execute(text)
    return out.collect().to_dicts()


@pytest.mark.parametrize(
    ("dialect", "query", "expected"),
    [
        # date literals
        ("spark", "select id from t where d >= DATE '2024-02-01'", [{"id": 2}, {"id": 3}]),
        ("tsql", "select id from t where d >= CAST('2024-02-01' AS DATE)", [{"id": 2}, {"id": 3}]),
        # casts
        ("spark", "select cast(id as string) as s from t where id = 1", [{"s": "1"}]),
        ("duckdb", "select id::varchar as s from t where id = 1", [{"s": "1"}]),
        # string concatenation
        ("spark", "select concat(name, '!') as s from t where id = 1", [{"s": "Ana!"}]),
        ("tsql", "select name + '!' as s from t where id = 1", [{"s": "Ana!"}]),
        # case-insensitive like
        ("spark", "select id from t where name ilike 'b%'", [{"id": 2}]),
        ("duckdb", "select id from t where name ilike 'B%'", [{"id": 2}]),
        # row limits
        ("tsql", "select top 1 id from t order by id", [{"id": 1}]),
        ("spark", "select id from t order by id limit 2", [{"id": 1}, {"id": 2}]),
    ],
)
def test_common_constructs_translate_to_polars(
    dialect: str, query: str, expected: list[dict[str, object]]
) -> None:
    assert run(query, dialect) == expected


def test_the_engine_dialect_is_never_translated() -> None:
    query = "select id from t where name || '' = 'Ana'"
    assert translate(query, "postgres", "postgres") is query
    assert run(query, "postgres") == [{"id": 1}]
