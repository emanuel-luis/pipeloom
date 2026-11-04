from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from pipeloom.engine.protocols import Catalog, Engine, EngineSession, Materializer, Reader
from pipeloom.engine.types import Column, Namespace, ReadSpec, Relation, TargetRef, WriteConfig
from pipeloom.platform import Identity, SystemClock
from pipeloom.runtime.context import RuntimeContext
from pipeloom.testing.memory import CATALOG_SPEC, SPEC, MemoryCatalog, MemoryEngine

ROWS = (
    {"id": 1, "name": "a", "ts": 10, "score": 1.5},
    {"id": 2, "name": "b", "ts": 20, "score": None},
    {"id": 3, "name": None, "ts": 30, "score": 2.5},
)
T = TargetRef(None, "main", "out")
CTX = RuntimeContext(
    run_id="r",
    invocation_id="i",
    project="p",
    target="dev",
    project_dir=Path("."),
    clock=SystemClock(),
    identity=Identity("u", "h"),
    vars={},
)


@pytest.fixture
def engine() -> MemoryEngine:
    return MemoryEngine({"raw": ROWS})


def test_structural_protocol_checks(engine: MemoryEngine) -> None:
    assert isinstance(engine, Engine)
    assert isinstance(MemoryCatalog(), Catalog)
    with engine.session(CTX) as session:
        assert isinstance(session, EngineSession)
        assert isinstance(session.reader("memory"), Reader)
        assert isinstance(session.materializer("append"), Materializer)


def test_capabilities_and_spec() -> None:
    caps = MemoryEngine().capabilities
    assert caps.formats == {"memory"} and caps.strategies == {"append", "overwrite"}
    assert caps.sql_models and caps.idempotent_writes
    assert not (caps.views or caps.cancel or caps.bytes_metrics or caps.portable)
    assert SPEC.capabilities == caps and SPEC.group == "engines"
    assert CATALOG_SPEC.compatible_engines == {"memory"}
    assert isinstance(SPEC.factory(SPEC.config_model(tables={"t": [{"a": 1}]})), MemoryEngine)


def test_read_by_location_and_by_table(engine: MemoryEngine) -> None:
    with engine.session(CTX) as s:
        assert s.reader("memory").read(ReadSpec("memory", location="raw")).native == ROWS
        engine.store["main.t"] = ROWS[:1]
        assert s.reader("table").read(ReadSpec("table", table=TargetRef(None, "main", "t")))
        with pytest.raises(KeyError):
            s.reader("memory").read(ReadSpec("memory", location="nope"))
        with pytest.raises(ValueError, match="parquet"):
            s.reader("parquet")


def test_materialize_append_and_overwrite(engine: MemoryEngine) -> None:
    with engine.session(CTX) as s:
        rel = s.reader("memory").read(ReadSpec("memory", location="raw"))
        cfg = WriteConfig("append")
        assert s.materializer("append").materialize(rel, T, cfg, "t1").rows_written == 3
        s.materializer("append").materialize(rel, T, cfg, "t2")
        assert len(engine.store["main.out"]) == 6
        one = s.sql("select * from x where id = 1", {"x": rel})
        res = s.materializer("overwrite").materialize(one, T, WriteConfig("overwrite"), "t3")
        assert res.rows_written == 1 and engine.store["main.out"] == one.native
        with pytest.raises(ValueError, match="merge"):
            s.materializer("merge")


def test_materialize_is_idempotent_per_token(engine: MemoryEngine) -> None:
    rel = Relation(ROWS, "memory")
    cfg = WriteConfig("append")
    other_target = TargetRef(None, "main", "o2")
    with engine.session(CTX) as s:
        first = s.materializer("append").materialize(rel, T, cfg, "same")
    with engine.session(CTX) as s:  # tokens outlive a session
        again = s.materializer("append").materialize(rel, T, cfg, "same")
        other = s.materializer("append").materialize(rel, other_target, cfg, "same")
    assert again == first and first.rows_written == 3  # a replayed token returns the first result
    assert len(engine.store["main.out"]) == 3
    assert len(engine.store["main.o2"]) == 3 and other.rows_written == 3  # per target


def test_sql_over_inputs(engine: MemoryEngine) -> None:
    with engine.session(CTX) as s:
        raw = Relation(ROWS, "memory")
        out = s.sql("select name, ts * 2 as t2 from raw where id >= 2 order by id", {"raw": raw})
        assert out.native == ({"name": "b", "t2": 40}, {"name": None, "t2": 60})
        joined = s.sql(
            "select r.id, d.label from raw r join dim d on d.id = r.id",
            {"raw": raw, "dim": Relation(({"id": 1, "label": "x"},), "memory")},
        )
        assert joined.native == ({"id": 1, "label": "x"},)
        assert s.sql("select 1 as one", {}).native == ({"one": 1},)
        assert s.count(s.sql("select * from e", {"e": Relation((), "memory")})) == 0


def test_schema_count_filter_max(engine: MemoryEngine) -> None:
    with engine.session(CTX) as s:
        rel = Relation(ROWS, "memory")
        assert s.schema(rel) == (
            Column("id", "int64"),
            Column("name", "string"),
            Column("ts", "int64"),
            Column("score", "float64"),
        )
        assert s.count(rel) == 3
        kept = s.filter_since(rel, "ts", 20)
        assert [r["id"] for r in kept.native] == [2, 3]
        assert s.max_value(rel, "ts") == 30 and s.max_value(kept, "score") == 2.5
        assert s.max_value(s.filter_since(rel, "ts", 99), "ts") is None
        assert s.schema(Relation((), "memory")) == ()
        s.cancel("unit")  # no-op


def test_session_cleanup_runs_on_error(engine: MemoryEngine) -> None:
    with pytest.raises(RuntimeError), engine.session(CTX):
        raise RuntimeError("boom")


def test_catalog_sees_engine_writes() -> None:
    engine = MemoryEngine()
    catalog = MemoryCatalog(engine.store)
    assert not catalog.exists(T) and catalog.table_schema(T) is None
    with engine.session(CTX) as s:
        rel = Relation(ROWS, "memory")
        s.materializer("overwrite").materialize(rel, T, WriteConfig("overwrite"), "t")
    assert catalog.exists(T)
    schema: Any = catalog.table_schema(T)
    assert schema[0] == Column("id", "int64")
    catalog.ensure_namespace(Namespace(None, "main"))
    catalog.ensure_namespace(Namespace(None, "main"))
    assert catalog.namespaces == {Namespace(None, "main")}
