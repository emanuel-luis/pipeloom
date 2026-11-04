"""`can_widen`, `diff_schemas` and the small value types of plugin API (0, 4)."""

from __future__ import annotations

import pytest

from pipeloom.engine.types import Column, ColumnChange, SchemaDiff, can_widen, diff_schemas


@pytest.mark.parametrize(
    ("old", "new", "ok"),
    [
        ("int32", "int64", True),
        ("int64", "int32", False),
        ("int64", "int64", True),
        ("uint32", "int64", True),
        ("uint32", "uint64", True),
        ("uint64", "int64", False),
        ("int64", "uint64", False),
        ("int32", "float64", True),
        ("float32", "float64", True),
        ("float64", "float32", False),
        ("float64", "int64", False),
        ("decimal(10,2)", "decimal(12,2)", True),
        ("decimal(10,2)", "decimal(12,4)", True),
        ("decimal(10,2)", "decimal(14,4)", True),
        ("decimal(10,2)", "decimal(10,1)", False),
        ("timestamp[ms]", "timestamp[us]", True),
        ("timestamp[us]", "timestamp[ns]", True),
        ("timestamp[ns]", "timestamp[us]", False),
        ("timestamp[us, UTC]", "timestamp[ns, UTC]", True),
        ("timestamp[us]", "timestamp[ns, UTC]", False),
        ("null", "string", True),
        ("string", "int64", False),
    ],
)
def test_can_widen(old: str, new: str, ok: bool) -> None:
    assert can_widen(old, new) is ok


def test_diff_schemas_by_name_ignoring_order_and_nullability() -> None:
    target = (Column("a", "int64"), Column("b", "string", nullable=False), Column("c", "string"))
    batch = (Column("b", "string"), Column("a", "int64"), Column("c", "string"))
    assert diff_schemas(target, batch).empty


def test_diff_schemas_reports_added_removed_and_changed() -> None:
    target = (Column("a", "int64"), Column("b", "string"), Column("c", "float64"))
    batch = (
        Column("a", "string"),
        Column("c", "int32"),
        Column("D", "int64"),
        Column("e", "int64"),
    )
    diff = diff_schemas(target, batch)
    # c: int32 widens to float64, so it is not a change; names are case-sensitive
    assert diff.changes == (
        ColumnChange("type_changed", "a", "int64", "string"),
        ColumnChange("removed", "b", "string", None),
        ColumnChange("added", "D", None, "int64"),
        ColumnChange("added", "e", None, "int64"),
    )
    assert diff.to_json()[0] == {
        "kind": "type_changed",
        "column": "a",
        "old": "int64",
        "new": "string",
    }
    assert SchemaDiff(()).empty and not diff.empty
