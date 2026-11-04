"""The pure-Python reference: quality counts, drops, merge, replace_partitions and align."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pytest

from pipeloom.engine.errors import WriteKeyError
from pipeloom.engine.types import Column, DeleteSpec, ExpectationSpec, QuarantineMeta, WriteConfig
from pipeloom.testing import reference as ref

NAN = float("nan")


def spec(
    name: str, kind: str, action: str = "warn", cols: tuple[str, ...] = (), **params: Any
) -> ExpectationSpec:
    return ExpectationSpec(name, kind, action, cols, params)  # type: ignore[arg-type]


ROWS = [
    {"id": 1, "x": 0.0, "s": "abc"},
    {"id": 2, "x": 10.0, "s": "xbc"},
    {"id": None, "x": NAN, "s": None},
    {"id": 2, "x": 11.0, "s": "ab"},
]


def test_row_checks_follow_sql_check_semantics() -> None:
    checks = [
        spec("nn", "not_null", cols=("id", "x")),
        spec("rng", "range", "drop", ("x",), min=0, max=10),
        spec("rx", "regex", "drop", ("s",), pattern="^ab"),
        spec("acc", "accepted_values", cols=("s",), values=["abc", "ab"]),
    ]
    got = ref.evaluate_rows(ROWS, checks)
    assert got.total_rows == 4
    assert got.failed == {"nn": 1, "rng": 1, "rx": 1, "acc": 1}
    assert got.dropped == 2  # x=11 and "xbc" fail a drop check; the all-null row passes
    assert got.aggregates == {}


def test_aggregates() -> None:
    checks = [
        spec("u", "unique", cols=("id",)),
        spec("n", "row_count"),
        spec("f", "freshness", cols=("id",)),
    ]
    got = ref.evaluate_rows(ROWS, checks)
    assert got.aggregates == {"u": 1, "n": 4, "f": 2}  # a null key is not a duplicate
    assert ref.evaluate_rows([], checks).aggregates == {"u": 0, "n": 0, "f": None}


def test_expr_uses_the_given_predicate() -> None:
    check = spec("e", "expr", "drop", expr="ignored")

    def pred(r: Any) -> bool | None:
        return None if r["id"] is None else bool(r["id"] > 1)

    got = ref.evaluate_rows(ROWS, [check], exprs={"e": pred})
    assert got.failed == {"e": 1} and got.dropped == 1  # null passes
    with pytest.raises(ValueError, match="predicate"):
        ref.evaluate_rows(ROWS, [check])


def test_apply_drops_lists_failing_checks_in_declaration_order() -> None:
    checks = [
        spec("a", "range", "drop", ("x",), min=0, max=10),
        spec("b", "regex", "drop", ("s",), pattern="^ab"),
        spec("w", "not_null", "warn", ("id",)),
    ]
    meta = QuarantineMeta("r1", datetime(2026, 1, 1, tzinfo=UTC))
    kept, dropped = ref.apply_drops_rows(ROWS, checks, meta)
    assert [r["id"] for r in kept] == [1, None]
    assert dropped is not None and [r["_pipeloom_failed"] for r in dropped] == [["b"], ["a"]]
    assert dropped[0]["_pipeloom_run_id"] == "r1" and dropped[0]["_pipeloom_ts"] == meta.ts
    assert ref.apply_drops_rows(ROWS, checks, None)[1] is None


def test_align_adds_drops_and_casts() -> None:
    schema = (Column("b", "float64"), Column("new", "int64"))
    assert ref.align_rows([{"a": 1, "b": 2}], schema) == [{"b": 2.0, "new": None}]


CUR = [{"id": 1, "v": 10}, {"id": 2, "v": 20}, {"id": None, "v": 0}]


def test_merge_insert_update_and_untouched_rows() -> None:
    cfg = WriteConfig("merge", unique_key=("id",))
    out = ref.merge_rows(CUR, [{"id": 2, "v": 25}, {"id": 3, "v": 30}, {"id": None, "v": 1}], cfg)
    assert (out.inserted, out.updated, out.deleted) == (2, 1, 0)
    want = [CUR[0], {"id": 2, "v": 25}, {"id": 3, "v": 30}, CUR[2], {"id": None, "v": 1}]
    assert sorted(out.rows, key=ref.sort_key) == sorted(want, key=ref.sort_key)


def test_merge_condition_and_deletes() -> None:
    cfg = WriteConfig(
        "merge", unique_key=("id",), update_condition="c", deletes=DeleteSpec("v", -1, "hard")
    )
    batch = [{"id": 1, "v": 5}, {"id": 2, "v": -1}, {"id": 7, "v": -1}]
    out = ref.merge_rows(CUR, batch, cfg, condition=lambda s, t: s["v"] > t["v"] or s["v"] < 0)
    assert (out.inserted, out.updated, out.deleted) == (0, 0, 1)  # id 1: 5 < 10, no update
    assert {r["id"] for r in out.rows} == {1, None}
    soft = WriteConfig("merge", unique_key=("id",), deletes=DeleteSpec("v", -1, "soft"))
    out = ref.merge_rows(CUR, batch, soft)
    assert (out.inserted, out.updated, out.deleted) == (0, 1, 1)
    assert ref.merge_written(out, soft) == 2
    assert {r["id"]: r["v"] for r in out.rows if r["id"] is not None} == {1: 5, 2: -1}


def test_merge_rejects_bad_keys() -> None:
    cfg = WriteConfig("merge", unique_key=("id",))
    with pytest.raises(WriteKeyError):
        ref.merge_rows(CUR, [{"id": 5, "v": 1}, {"id": 5, "v": 2}], cfg)
    with pytest.raises(WriteKeyError):
        ref.merge_rows(CUR, [{"k": 1}], cfg)
    with pytest.raises(WriteKeyError):
        ref.merge_rows(CUR, [], WriteConfig("merge"))


def test_replace_partitions_swaps_only_the_batch_partitions() -> None:
    cur = [{"p": "a", "v": 1}, {"p": "b", "v": 2}, {"p": None, "v": 3}]
    rows, deleted = ref.replace_partitions_rows(cur, [{"p": None, "v": 9}], ("p",))
    assert deleted == 1
    want = [{"p": "a", "v": 1}, {"p": "b", "v": 2}, {"p": None, "v": 9}]
    assert sorted(rows, key=ref.sort_key) == sorted(want, key=ref.sort_key)
    with pytest.raises(WriteKeyError):
        ref.replace_partitions_rows(cur, [{"v": 1}], ("p",))
