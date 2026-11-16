"""Row inference, casts, CSV reading and the multiset diff of unit tests (ADR-0045)."""

from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path

import pytest

from pipeloom.quality.rows import coerce, diff_rows, infer_dtype, read_csv_rows
from pipeloom.testing import reference


def test_reference_re_exports_the_diff() -> None:
    assert reference.diff_rows is diff_rows


@pytest.mark.parametrize(
    ("values", "dtype"),
    [
        ([True, None], "boolean"),
        ([1, 2], "int64"),
        ([1, 2.5], "float64"),
        (["a"], "string"),
        ([date(2026, 1, 1)], "date"),
        ([datetime(2026, 1, 1)], "timestamp[us]"),
        ([datetime(2026, 1, 1, tzinfo=UTC)], "timestamp[us, UTC]"),
        ([None, float("nan")], None),
        ([1, "a"], "string"),
    ],
)
def test_infer_inline(values: list[object], dtype: str | None) -> None:
    assert infer_dtype(values) == dtype


@pytest.mark.parametrize(
    ("values", "dtype"),
    [(["1", None], "int64"), (["1", "2.5"], "float64"), (["1", "x"], "string"), ([None], None)],
)
def test_infer_csv(values: list[str | None], dtype: str | None) -> None:
    assert infer_dtype(values, from_csv=True) == dtype


def test_coerce() -> None:
    assert coerce("3", "int64") == 3
    assert coerce(2.0, "int32") == 2
    assert coerce("1.5", "float64") == 1.5
    assert coerce("true", "boolean") is True
    assert coerce(7, "string") == "7"
    assert coerce("1.10", "decimal(18,2)") == Decimal("1.10")
    assert coerce("2026-01-02", "date") == date(2026, 1, 2)
    assert coerce("2026-01-02T03:04:05", "timestamp[us, UTC]") == datetime(
        2026, 1, 2, 3, 4, 5, tzinfo=UTC
    )
    assert coerce(datetime(2026, 1, 2, tzinfo=UTC), "timestamp[us]") == datetime(2026, 1, 2)
    assert coerce(float("nan"), "int64") is None
    for bad, dtype in ((1.5, "int64"), ("x", "boolean"), ("x", "date")):
        with pytest.raises(ValueError):
            coerce(bad, dtype)


def test_read_csv_rows(tmp_path: Path) -> None:
    path = tmp_path / "rows.csv"
    path.write_text("id,label\n1,a\n2,\n", encoding="utf-8")
    assert read_csv_rows(path) == (
        ["id", "label"],
        [{"id": "1", "label": "a"}, {"id": "2", "label": None}],
    )


def test_diff_is_a_multiset_over_the_expect_columns() -> None:
    expected = [{"id": 1, "f": 0.1 + 0.2}, {"id": 1, "f": 0.3}, {"id": 2, "f": None}]
    actual = [
        {"id": 2, "f": float("nan"), "extra": "x"},
        {"id": 1, "f": 0.3},
        {"id": 1, "f": 0.30000000000001},
    ]
    assert diff_rows(expected, actual, ["id", "f"]).ok


def test_diff_lists_missing_and_unexpected_rows() -> None:
    stamp = datetime(2026, 1, 1)
    diff = diff_rows(
        [{"id": 1, "t": stamp}, {"id": 2, "t": stamp}, {"id": 2, "t": stamp}],
        [{"id": 1, "t": stamp.replace(tzinfo=UTC)}, {"id": 2, "t": stamp}, {"id": 3, "t": stamp}],
        ["id", "t"],
    )
    assert diff.missing == [{"id": 2, "t": stamp}]
    assert diff.unexpected == [{"id": 3, "t": stamp}]
    assert not diff.ok
