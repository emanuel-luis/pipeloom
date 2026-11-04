"""Engine behavior beyond the contract, on classic local Spark."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from pipeloom.engine.types import Column
from pipeloom.platform import Identity
from pipeloom.runtime.context import RuntimeContext
from pipeloom.testing.clock import FakeClock
from pipeloom_spark.catalog import DeltaPathCatalog
from pipeloom_spark.engine import SparkEngine
from pipeloom_spark.spec import SparkEngineOptions

pytestmark = [pytest.mark.spark, pytest.mark.timeout(300)]


def _ctx(tmp_path: Path) -> RuntimeContext:
    return RuntimeContext(
        run_id="r1",
        invocation_id="i1",
        project="p",
        target="t",
        project_dir=tmp_path,
        clock=FakeClock(),
        identity=Identity("tester", "host"),
        vars={},
    )


def _session(tmp_path: Path) -> Any:
    engine = SparkEngine(SparkEngineOptions(mode="active"))
    return engine.session(_ctx(tmp_path), catalog=DeltaPathCatalog(str(tmp_path)))


def test_braces_and_aliases_in_sql(classic_spark: Any, tmp_path: Path) -> None:
    with _session(tmp_path) as s:
        a = s.relation(classic_spark.range(3).toDF("id"))
        out = s.sql("select id, '{\"k\": 1}' as j from a where id > 0;", {"a": a})
        rows = out.native.collect()
        assert [r["id"] for r in rows] == [1, 2] and rows[0]["j"] == '{"k": 1}'
        assert [c.name for c in s.schema(out)] == ["id", "j"]
        with pytest.raises(Exception):  # noqa: B017 - Spark's AnalysisException
            s.sql("select * from a", {})


def test_unit_scope_adds_and_removes_its_tag(classic_spark: Any, tmp_path: Path) -> None:
    with _session(tmp_path) as s:
        with s.unit_scope("u/1"):
            tags = classic_spark.sparkContext.getLocalProperty("spark.job.tags")
            assert tags == "pipeloom:r1:u_1"
            kept = s.persist(s.relation(classic_spark.range(2)))
            assert kept.native.is_cached
        assert not classic_spark.sparkContext.getLocalProperty("spark.job.tags")
        assert not kept.native.is_cached


def test_align_filter_since_and_max_value_on_timestamps(classic_spark: Any, tmp_path: Path) -> None:
    with _session(tmp_path) as s:
        rel = s.relation(
            classic_spark.createDataFrame([(1,), (2,)], "id int").selectExpr(
                "id", "timestamp_micros(id * 1000000) as ts"
            )
        )
        got = s.align(rel, (Column("ts", "timestamp[us, UTC]"), Column("x", "uint8")))
        assert [(c.name, c.dtype) for c in s.schema(got)] == [
            ("ts", "timestamp[us, UTC]"),
            ("x", "int16"),
        ]
        bound = datetime(1970, 1, 1, 0, 0, 2, tzinfo=UTC)
        assert s.count(s.filter_since(rel, "ts", bound)) == 1
        assert s.count(s.filter_since(rel, "ts", bound, inclusive=False)) == 0
        assert s.max_value(rel, "ts") == bound


def test_the_active_session_survives_the_engine_session(classic_spark: Any, tmp_path: Path) -> None:
    with _session(tmp_path):
        pass
    assert classic_spark.range(1).count() == 1
