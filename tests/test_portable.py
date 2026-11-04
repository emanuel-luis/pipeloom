from __future__ import annotations

import json
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any

import pytest

from pipeloom.engine import portable
from pipeloom.engine.errors import WriteKeyError
from pipeloom.engine.types import Column, DeleteSpec, WriteConfig


def polars_frame(lazy: bool = False) -> Any:
    pl = pytest.importorskip("polars")
    df = pl.DataFrame(
        {"id": [1, 2, 3], "name": ["a", "b", "c"], "d": [date(2026, 1, i) for i in (1, 2, 3)]}
    )
    return df.lazy() if lazy else df


def pandas_frame() -> Any:
    pd = pytest.importorskip("pandas")
    return pd.DataFrame(
        {"id": [1, 2, 3], "name": ["a", "b", "c"], "d": pd.to_datetime(["2026-01-01"] * 3)}
    )


@pytest.fixture(
    params=[
        pytest.param(lambda: polars_frame(), id="polars"),
        pytest.param(lambda: polars_frame(lazy=True), id="polars-lazy"),
        pytest.param(pandas_frame, id="pandas"),
    ]
)
def frame(request: pytest.FixtureRequest) -> Any:
    return request.param()


def test_schema_count_max(frame: Any) -> None:
    cols = portable.schema(frame)
    assert cols[0] == Column("id", "int64") and cols[1].name == "name"
    assert portable.count(frame) == 3
    assert portable.max_value(frame, "id") == 3


def test_filter_since_keeps_the_frame_kind(frame: Any) -> None:
    out = portable.filter_since(frame, "id", 2)
    assert type(out) is type(frame) and portable.count(out) == 2
    assert portable.max_value(portable.filter_since(frame, "id", 9), "id") is None


def test_frame_api_round_trip(frame: Any) -> None:
    nwf = portable.to_frame_api(frame, "narwhals")
    assert nwf is not frame
    assert type(portable.from_frame_api(nwf)) is type(frame)
    assert portable.from_frame_api(frame) is frame
    assert portable.to_frame_api(frame, "native") is frame
    with pytest.raises(ValueError):
        portable.to_frame_api(frame, "other")  # type: ignore[arg-type]


def test_logical_dtypes_polars() -> None:
    pl = pytest.importorskip("polars")
    df = pl.DataFrame(
        {
            "i8": pl.Series([1], dtype=pl.Int8),
            "u32": pl.Series([1], dtype=pl.UInt32),
            "f32": pl.Series([1.0], dtype=pl.Float32),
            "cat": pl.Series(["a"], dtype=pl.Categorical),
            "b": [True],
            "ts": [datetime(2026, 1, 1)],
            "tsz": pl.Series([datetime(2026, 1, 1)]).dt.replace_time_zone("UTC"),
            "dur": pl.Series([1], dtype=pl.Duration("ms")),
            "dec": pl.Series([Decimal("1.50")], dtype=pl.Decimal(10, 2)),
            "bin": [b"x"],
            "lst": [[1]],
            "st": [{"a": 1, "b": "x"}],
            "nul": pl.Series([None], dtype=pl.Null),
        }
    )
    got = {c.name: c.dtype for c in portable.schema(df)}
    assert got == {
        "i8": "int8",
        "u32": "uint32",
        "f32": "float32",
        "cat": "string",
        "b": "boolean",
        "ts": "timestamp[us]",
        "tsz": "timestamp[us, UTC]",
        "dur": "duration[ms]",
        "dec": "decimal(10,2)",
        "bin": "binary",
        "lst": "list<int64>",
        "st": "struct<a:int64,b:string>",
        "nul": "unknown",
    }


def test_pandas_dtypes() -> None:
    got = {c.name: c.dtype for c in portable.schema(pandas_frame())}
    assert got["name"] == "string" and got["d"].startswith("timestamp[")


def test_pandas_max_value_is_plain_and_serialisable() -> None:
    import json

    pd = pytest.importorskip("pandas")
    from pipeloom.state.codec import encode_value

    cases = [
        (pd.DataFrame({"x": pd.Series([], dtype="datetime64[ns]")}), None),
        (pd.DataFrame({"x": pd.Series([], dtype="Int64")}), None),
        (pd.DataFrame({"x": pd.Series([None, None], dtype="Int64")}), None),
        (pd.DataFrame({"x": pd.Series([], dtype="float64")}), None),
        (pd.DataFrame({"x": [1, 2]}), 2),
        (pd.DataFrame({"x": pd.Series([1, 2], dtype="Int64")}), 2),
        (pd.DataFrame({"x": [1.5]}), 1.5),
        (pd.DataFrame({"x": pd.to_datetime(["2026-01-02"])}), datetime(2026, 1, 2)),
    ]
    for frame, expected in cases:
        value = portable.max_value(frame, "x")
        assert value == expected and type(value) is type(expected)
        json.dumps(encode_value(value))


# -- merge, replace_partitions and partition values ----------------------------------------------


def _kv(kind: str, rows: list[tuple[Any, ...]]) -> Any:
    cols = ["id", "label", "v"]
    data = {c: [r[i] for r in rows] for i, c in enumerate(cols)}
    if kind == "polars":
        pl = pytest.importorskip("polars")
        return pl.DataFrame(data, schema={"id": pl.Int64, "label": pl.String, "v": pl.Int64})
    pd = pytest.importorskip("pandas")
    return pd.DataFrame(data).astype({"id": "Int64", "label": "string", "v": "Int64"})


def _cond(kind: str) -> Any:
    if kind == "polars":
        pl = pytest.importorskip("polars")
        return lambda df, cond: df.select(pl.sql_expr(cond).alias("c")).to_series()
    return lambda df, cond: df.eval(cond, engine="python")


def _out(frame: Any) -> list[tuple[Any, ...]]:
    nw_frame = portable.nw.from_native(frame)
    return sorted(nw_frame.iter_rows(), key=repr)


@pytest.fixture(params=["polars", "pandas"])
def kind(request: pytest.FixtureRequest) -> str:
    pytest.importorskip(request.param)
    return str(request.param)


def test_merge_frames_inserts_updates_and_skips_null_keys(kind: str) -> None:
    cur = _kv(kind, [(1, "a", 10), (2, "b", 20), (None, "old", 0)])
    bat = _kv(kind, [(2, "b2", 30), (3, "c", 5), (None, "n1", 1), (None, "n2", 2)])
    out = portable.merge_frames(
        cur, bat, WriteConfig("merge", unique_key=("id",)), condition_hook=None
    )
    assert (out.inserted, out.updated, out.deleted) == (3, 1, 0)
    assert len(_out(out.frame)) == 6


def test_merge_frames_condition_and_deletes(kind: str) -> None:
    cur = _kv(kind, [(1, "a", 10), (2, "b", 20), (3, "c", 30)])
    bat = _kv(kind, [(1, "gone", 1), (2, "gone", 99), (9, "gone", 0), (4, "d", 4)])
    for mode, rows in (("hard", 3), ("soft", 4)):
        cfg = WriteConfig(
            "merge",
            unique_key=("id",),
            update_condition="source.v > target.v",
            deletes=DeleteSpec("label", "gone", mode),  # type: ignore[arg-type]
        )
        out = portable.merge_frames(cur, bat, cfg, condition_hook=_cond(kind))
        assert (out.inserted, out.updated, out.deleted) == (1, 0, 1)
        assert len(_out(out.frame)) == rows


def test_merge_frames_rejects_bad_keys(kind: str) -> None:
    cur = _kv(kind, [(1, "a", 1)])
    with pytest.raises(WriteKeyError):
        portable.merge_frames(
            cur,
            _kv(kind, [(3, "x", 1), (3, "y", 2)]),
            WriteConfig("merge", unique_key=("id",)),
            condition_hook=None,
        )
    with pytest.raises(WriteKeyError):
        portable.merge_frames(
            cur, cur, WriteConfig("merge", unique_key=("nope",)), condition_hook=None
        )
    with pytest.raises(WriteKeyError):
        portable.merge_frames(cur, cur, WriteConfig("merge"), condition_hook=None)


def test_replace_partition_frames_treats_null_as_a_value(kind: str) -> None:
    cur = _kv(kind, [(1, "a", 1), (2, "a", 2), (3, "b", 3), (4, None, 4)])
    bat = _kv(kind, [(9, "a", 9), (8, None, 8)])
    out, deleted = portable.replace_partition_frames(cur, bat, ("label",))
    assert deleted == 3 and sorted(r[0] for r in _out(out)) == [3, 8, 9]
    with pytest.raises(WriteKeyError):
        portable.replace_partition_frames(cur, bat, ())


def _typed(kind: str, case: str) -> Any:
    """A frame of one partition column `p` (three rows) plus `v`, built natively."""
    ns = [1767225600000000001, 1767225600000000002, 1767225600000000001]
    t = datetime(2026, 1, 1, tzinfo=UTC)
    values: dict[str, Any] = {
        "float": [1.0, float("nan"), None],
        "ns": ns,
        "tz": [t, None, t],
        "dec": [Decimal("1.50"), Decimal("2.25"), None],
        "date": [date(2026, 1, 1), date(2026, 1, 2), None],
    }
    if kind == "polars":
        pl = pytest.importorskip("polars")
        dtypes: dict[str, Any] = {
            "float": pl.Float64,
            "ns": pl.Int64,
            "tz": pl.Datetime("us", "UTC"),
            "dec": pl.Decimal(10, 2),
            "date": pl.Date,
        }
        col = pl.Series("p", values[case], dtype=dtypes[case])
        if case == "ns":
            col = col.cast(pl.Datetime("ns"))
        return pl.DataFrame({"p": col, "v": [1, 2, 3]})
    pd = pytest.importorskip("pandas")
    pa = pytest.importorskip("pyarrow")
    types: dict[str, Any] = {
        "float": pa.float64(),
        "ns": pa.int64(),
        "tz": pa.timestamp("us", tz="UTC"),
        "dec": pa.decimal128(10, 2),
        "date": pa.date32(),
    }
    arr = pa.array(values[case], type=types[case])
    if case == "ns":
        arr = arr.cast(pa.timestamp("ns"))
    return pd.DataFrame({"p": pd.Series(arr, dtype=pd.ArrowDtype(arr.type)), "v": [1, 2, 3]})


# ns: two distinct values, one repeated; the others: NaN and null (float) or null form one group.
_GROUPS = {"float": [2, 1], "ns": [2, 1], "tz": [2, 1], "dec": [1, 1, 1], "date": [1, 1, 1]}


@pytest.mark.parametrize("case", list(_GROUPS))
def test_partition_groups_are_exact_for_every_type(kind: str, case: str) -> None:
    frame = _typed(kind, case)
    groups = list(portable.partition_groups(frame, ("p",)))
    counts = sorted((portable.count(f) for _, f in groups), reverse=True)
    assert counts == sorted(_GROUPS[case], reverse=True) and sum(counts) == 3
    assert len({k for k, _ in groups}) == len(groups)
    for key, _ in groups:
        json.dumps(key)
    keys = portable.partition_keys(frame, ("p",))
    assert keys == [k for k, _ in groups]
    kept, removed = portable.drop_partitions(frame, {keys[0]}, ("p",))
    assert removed == counts_of(groups, keys[0]) and portable.count(kept) == 3 - removed
    out, deleted = portable.replace_partition_frames(frame, frame, ("p",))
    assert deleted == 3 and portable.count(out) == 3


def counts_of(groups: list[Any], key: Any) -> int:
    return next(portable.count(f) for k, f in groups if k == key)


def test_ns_keys_keep_nanoseconds(kind: str) -> None:
    keys = portable.partition_keys(_typed(kind, "ns"), ("p",))
    assert keys == [("2026-01-01T00:00:00.000000001",), ("2026-01-01T00:00:00.000000002",)]


def test_decimal_keys_ignore_scale(kind: str) -> None:
    assert portable.partition_keys(_typed(kind, "dec"), ("p",))[0] == ("1.5",)


def test_partition_groups_of_an_empty_frame(kind: str) -> None:
    frame = portable.empty_like(_kv(kind, [(1, "a", 1)]))
    assert list(portable.partition_groups(frame, ("label",))) == []


def test_decode_partition_value() -> None:
    assert portable.decode_partition_value("date", "2026-01-02") == date(2026, 1, 2)
    assert portable.decode_partition_value("decimal(10,2)", "1.5") == Decimal("1.50")
    stamp = portable.decode_partition_value(
        "timestamp[us, UTC]", "2026-01-02T03:04:05.000000000+00:00"
    )
    assert stamp == datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)
    assert portable.decode_partition_value("int64", None) is None


def test_rewrite_qualified_skips_literals() -> None:
    cond = "source.v > target.v and label != 'source.x'"
    assert portable.rewrite_qualified(cond) == "v > __pipeloom_t_v and label != 'source.x'"
