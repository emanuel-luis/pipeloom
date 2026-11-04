from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any

import pytest

from pipeloom.engine import portable
from pipeloom.engine.types import Column


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
