"""`EngineContract`: what every engine must do, gated by the capabilities it declares.

Subclass it in a test module and override the `engine_factory` fixture::

    class TestMyEngine(EngineContract):
        @pytest.fixture
        def engine_factory(self):
            return lambda tmp_path: EngineHarness(engine=..., catalog=..., ...)

A test whose capability the engine does not declare is skipped, never failed.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from pipeloom.engine.errors import WriteKeyError
from pipeloom.engine.protocols import Catalog, Engine, EngineSession
from pipeloom.engine.types import (
    Column,
    DeleteSpec,
    ExpectationSpec,
    IntermediateRef,
    QuarantineMeta,
    ReadSpec,
    Relation,
    Schema,
    TargetRef,
    WriteConfig,
    WriteResult,
)
from pipeloom.platform import Identity
from pipeloom.runtime.context import RuntimeContext
from pipeloom.runtime.schema_policy import SchemaRejected, resolve_columns
from pipeloom.testing import reference
from pipeloom.testing.clock import FakeClock

Rows = Sequence[Mapping[str, Any]]


@dataclass
class EngineHarness:
    """What the suite needs from an engine under test."""

    engine: Engine
    catalog: Catalog
    make_native: Callable[[Rows, Schema], Any]
    """Build the engine's own frame from rows and a logical schema."""
    to_rows: Callable[[Any], list[dict[str, Any]]]
    """The rows of a frame, as plain dicts."""
    write_source: Callable[[str, Path, Rows, Schema], None]
    """Write a source file of format `fmt` at `path`; the suite then reads it with that path."""
    failing_relation: Callable[[EngineSession], Relation]
    """A relation that raises when it is written."""
    settings_snapshot: Callable[[], Mapping[str, Any]]
    """The engine's global settings; the suite checks a session leaves them as it found them."""
    expr_cases: Sequence[tuple[str, Callable[[Mapping[str, Any]], bool | None]]] = ()
    """`expr` checks: the engine-dialect string and the Python predicate it stands for (True
    passes, False fails, None passes). Columns: id int64, f float64. Needed for `quality`."""
    update_condition: str = ""
    """An engine-dialect `update_condition` meaning `source.v > target.v` (int64 column v)."""
    typed_nulls: bool = True
    """`align` gives a column it adds the declared type, not an untyped null. False only for
    engines that infer types from values (the memory engine)."""
    schema_on_read: bool = False
    """Old data files read through the table's current schema: a column added, dropped or widened
    later still reads back. The schema-change matrix skips its writing cells without it."""
    slow_relation: Callable[[EngineSession, float], Relation] | None = None
    """Blocks up to the given seconds inside a unit scope and raises once `cancel` fires."""


def requires(h: EngineHarness, cap: str, value: str | None = None) -> None:
    """Skip the test unless the engine declares the capability (and `value` in a set one)."""
    declared = getattr(h.engine.capabilities, cap)
    if value is None:
        ok = bool(declared)
    else:
        ok = value in declared
    if not ok:
        pytest.skip(f"engine does not declare {cap}" + (f" {value}" if value else ""))


SCHEMA = (Column("id", "int64"), Column("label", "string"))
ROWS = ({"id": 1, "label": "a"}, {"id": 2, "label": "b"}, {"id": 3, "label": None})
TYPED_SCHEMA = (
    Column("i", "int64"),
    Column("f", "float64"),
    Column("s", "string"),
    Column("b", "boolean"),
    Column("d", "date"),
    Column("t", "timestamp[us, UTC]"),
    Column("n", "decimal(18,2)"),
)
TYPED_ROWS = (
    {
        "i": 1,
        "f": 1.5,
        "s": "x",
        "b": True,
        "d": date(2026, 1, 2),
        "t": datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC),
        "n": Decimal("12.34"),
    },
    {"i": None, "f": None, "s": None, "b": None, "d": None, "t": None, "n": None},
)


KV_SCHEMA = (Column("id", "int64"), Column("label", "string"), Column("v", "int64"))
KV_ROWS = ({"id": 1, "label": "a", "v": 10}, {"id": 2, "label": "b", "v": 20})
QUALITY_SCHEMA = (
    Column("id", "int64"),
    Column("f", "float64"),
    Column("s", "string"),
    Column("ts", "timestamp[us, UTC]"),
)
_T = [datetime(2026, 1, d, tzinfo=UTC) for d in (1, 2, 3)]
QUALITY_ROWS = (
    {"id": 1, "f": 1.0, "s": "abc", "ts": _T[0]},
    {"id": 2, "f": 10.0, "s": "abd", "ts": _T[1]},  # 10.0 sits on the range boundary
    {"id": 3, "f": float("nan"), "s": "xyz", "ts": _T[2]},  # NaN counts as null
    {"id": None, "f": 11.0, "s": None, "ts": None},
    {"id": 2, "f": -1.0, "s": "ab1", "ts": _T[1]},  # duplicate id
    {"id": None, "f": 3.0, "s": "abx", "ts": _T[0]},  # two null keys are not duplicates
)


MATRIX_SCHEMA = (Column("id", "int64"), Column("part", "string"), Column("v", "int64"))
MATRIX_SEED = ({"id": 1, "part": "a", "v": 10}, {"id": 2, "part": "b", "v": 20})
_ID_PART = (Column("id", "int64"), Column("part", "string"))
MATRIX_BATCHES: dict[str, tuple[Schema, Rows]] = {
    "added": (
        (*MATRIX_SCHEMA, Column("extra", "string")),
        (
            {"id": 2, "part": "b", "v": 21, "extra": "x"},
            {"id": 3, "part": "c", "v": 30, "extra": "y"},
        ),
    ),
    "removed": (_ID_PART, ({"id": 2, "part": "b"}, {"id": 3, "part": "c"})),
    "widened": (
        (*_ID_PART, Column("v", "float64")),
        ({"id": 2, "part": "b", "v": 21.0}, {"id": 3, "part": "c", "v": 30.0}),
    ),
    "incompatible": (
        (*_ID_PART, Column("v", "string")),
        ({"id": 2, "part": "b", "v": "21"}, {"id": 3, "part": "c", "v": "30"}),
    ),
}
_BASE = [(c.name, c.dtype) for c in MATRIX_SCHEMA]
_PLUS = [*_BASE, ("extra", "string")]
POLICY_NAMES = ("fail", "ignore", "append_new_columns", "sync_all_columns")
# Final columns per policy and scenario; None means the policy rejects the change (PLM202).
MATRIX_COLUMNS: dict[str, dict[str, list[tuple[str, str]] | None]] = {
    "fail": {"added": None, "removed": None, "widened": None, "incompatible": None},
    "ignore": {"added": _BASE, "removed": _BASE, "widened": _BASE, "incompatible": _BASE},
    "append_new_columns": {"added": _PLUS, "removed": _BASE, "widened": None, "incompatible": None},
    "sync_all_columns": {
        "added": _PLUS,
        "removed": _BASE[:2],
        "widened": [("id", "int64"), ("part", "string"), ("v", "float64")],
        "incompatible": None,
    },
}


def _norm(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Rows with NaN replaced by a marker, so row sets compare equal."""
    return [
        {k: ("NaN" if isinstance(v, float) and v != v else v) for k, v in r.items()}  # noqa: PLR0124
        for r in rows
    ]


def _target(name: str) -> TargetRef:
    return TargetRef(None, "main", name)


def _key(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(rows, key=lambda r: str(sorted(r.items(), key=str)))


class EngineContract:
    """Subclass and provide the `engine_factory` fixture."""

    @pytest.fixture
    def engine_factory(self) -> Callable[[Path], EngineHarness]:
        raise NotImplementedError("override engine_factory in the subclass")

    @pytest.fixture
    def h(self, engine_factory: Callable[[Path], EngineHarness], tmp_path: Path) -> EngineHarness:
        return engine_factory(tmp_path)

    @contextmanager
    def session(self, h: EngineHarness, tmp_path: Path) -> Iterator[EngineSession]:
        ctx = RuntimeContext(
            run_id="contract",
            invocation_id="contract",
            project="contract",
            target="test",
            project_dir=tmp_path,
            clock=FakeClock(),
            identity=Identity("tester", "host"),
            vars={},
        )
        with h.engine.session(ctx, catalog=h.catalog) as s:
            yield s

    def write(
        self, s: EngineSession, h: EngineHarness, name: str, rows: Rows, strategy: str, token: str
    ) -> Any:
        rel = s.relation(h.make_native(rows, SCHEMA))
        h.catalog.ensure_namespace(_target(name).namespace)
        return s.materializer(strategy).materialize(
            rel, _target(name), WriteConfig(strategy), token
        )

    def read_table(self, s: EngineSession, h: EngineHarness, name: str) -> list[dict[str, Any]]:
        rel = s.reader("table").read(ReadSpec("table", table=_target(name)))
        return _key(h.to_rows(rel.native))

    # -- shape ------------------------------------------------------------------------------

    def test_capabilities_shape(self, h: EngineHarness) -> None:
        caps = h.engine.capabilities
        for field in ("formats", "strategies", "materializations"):
            assert isinstance(getattr(caps, field), frozenset)
        assert h.engine.name and h.engine.version
        assert "table" in caps.materializations

    # -- reading ----------------------------------------------------------------------------

    def test_reads_every_declared_format(self, h: EngineHarness, tmp_path: Path) -> None:
        for fmt in sorted(h.engine.capabilities.formats):
            path = tmp_path / f"src_{fmt}"
            h.write_source(fmt, path, ROWS, SCHEMA)
            with self.session(h, tmp_path) as s:
                rel = s.reader(fmt).read(ReadSpec(fmt, location=path.as_posix()))
                assert _key(h.to_rows(rel.native)) == _key([dict(r) for r in ROWS]), fmt

    def test_logical_types_round_trip_on_parquet(self, h: EngineHarness, tmp_path: Path) -> None:
        requires(h, "formats", "parquet")
        path = tmp_path / "typed.parquet"
        h.write_source("parquet", path, TYPED_ROWS, TYPED_SCHEMA)
        with self.session(h, tmp_path) as s:
            rel = s.reader("parquet").read(ReadSpec("parquet", location=path.as_posix()))
            assert {c.name: c.dtype for c in s.schema(rel)} == {
                c.name: c.dtype for c in TYPED_SCHEMA
            }
            assert h.to_rows(rel.native) == [dict(r) for r in TYPED_ROWS]

    def test_schema_returns_logical_types(self, h: EngineHarness, tmp_path: Path) -> None:
        with self.session(h, tmp_path) as s:
            rel = s.relation(h.make_native(ROWS, SCHEMA))
            assert [(c.name, c.dtype) for c in s.schema(rel)] == [(c.name, c.dtype) for c in SCHEMA]

    def test_schema_of_typed_columns(self, h: EngineHarness, tmp_path: Path) -> None:
        with self.session(h, tmp_path) as s:
            rel = s.relation(h.make_native(TYPED_ROWS, TYPED_SCHEMA))
            assert [(c.name, c.dtype) for c in s.schema(rel)] == [
                (c.name, c.dtype) for c in TYPED_SCHEMA
            ]

    # -- SQL --------------------------------------------------------------------------------

    def test_sql_binds_aliases_without_leaking(self, h: EngineHarness, tmp_path: Path) -> None:
        requires(h, "sql_models")
        with self.session(h, tmp_path) as s:
            rel = s.relation(h.make_native(ROWS, SCHEMA))
            out = s.sql("select id from src where id > 1", {"src": rel})
            assert sorted(r["id"] for r in h.to_rows(out.native)) == [2, 3]
            with pytest.raises(Exception):  # noqa: B017 - each engine has its own error
                s.sql("select id from src", {}).native  # noqa: B018

    # -- writing ----------------------------------------------------------------------------

    def test_overwrite_replaces_data(self, h: EngineHarness, tmp_path: Path) -> None:
        requires(h, "strategies", "overwrite")
        with self.session(h, tmp_path) as s:
            self.write(s, h, "t_over", ROWS, "overwrite", "r:1")
            self.write(s, h, "t_over", ROWS[:1], "overwrite", "r:2")
            assert self.read_table(s, h, "t_over") == _key([dict(ROWS[0])])

    def test_append_adds_rows(self, h: EngineHarness, tmp_path: Path) -> None:
        requires(h, "strategies", "append")
        with self.session(h, tmp_path) as s:
            self.write(s, h, "t_app", ROWS[:1], "append", "r:1")
            self.write(s, h, "t_app", ROWS[1:], "append", "r:2")
            assert self.read_table(s, h, "t_app") == _key([dict(r) for r in ROWS])

    def test_a_failure_during_overwrite_keeps_the_old_data(
        self, h: EngineHarness, tmp_path: Path
    ) -> None:
        requires(h, "strategies", "overwrite")
        with self.session(h, tmp_path) as s:
            self.write(s, h, "t_keep", ROWS, "overwrite", "r:1")
            with pytest.raises(Exception):  # noqa: B017 - each engine has its own error
                s.materializer("overwrite").materialize(
                    h.failing_relation(s), _target("t_keep"), WriteConfig("overwrite"), "r:2"
                )
            assert self.read_table(s, h, "t_keep") == _key([dict(r) for r in ROWS])

    def test_metrics(self, h: EngineHarness, tmp_path: Path) -> None:
        requires(h, "strategies", "overwrite")
        with self.session(h, tmp_path) as s:
            result = self.write(s, h, "t_met", ROWS, "overwrite", "r:1")
            assert result.rows_written == len(ROWS)
            if h.engine.capabilities.bytes_metrics:
                assert result.bytes_written is not None

    def test_two_threads_materialize_at_once(self, h: EngineHarness, tmp_path: Path) -> None:
        requires(h, "strategies", "overwrite")
        with self.session(h, tmp_path) as s:
            errors: list[BaseException] = []

            def work(name: str) -> None:
                try:
                    self.write(s, h, name, ROWS, "overwrite", f"r:{name}")
                except BaseException as exc:  # noqa: BLE001 - reported below
                    errors.append(exc)

            threads = [threading.Thread(target=work, args=(f"t_th{i}",)) for i in range(2)]
            for t in threads:
                t.start()
            for t in threads:
                t.join()
            assert not errors
            assert all(
                self.read_table(s, h, f"t_th{i}") == _key([dict(r) for r in ROWS]) for i in range(2)
            )

    # -- relation helpers -------------------------------------------------------------------

    def test_count_max_value_and_filter_since(self, h: EngineHarness, tmp_path: Path) -> None:
        with self.session(h, tmp_path) as s:
            rel = s.relation(h.make_native(ROWS, SCHEMA))
            assert s.count(rel) == 3
            assert s.max_value(rel, "id") == 3
            kept = s.filter_since(rel, "id", 2)
            assert sorted(r["id"] for r in h.to_rows(kept.native)) == [2, 3]

    def test_persist_is_equivalent(self, h: EngineHarness, tmp_path: Path) -> None:
        with self.session(h, tmp_path) as s:
            rel = s.relation(h.make_native(ROWS, SCHEMA))
            kept = s.persist(rel)
            assert _key(h.to_rows(kept.native)) == _key(h.to_rows(rel.native))
            assert s.count(kept) == s.count(rel)

    def test_relation_rejects_a_foreign_type(self, h: EngineHarness, tmp_path: Path) -> None:
        with self.session(h, tmp_path) as s, pytest.raises(TypeError):
            s.relation(object())

    # -- views ------------------------------------------------------------------------------

    def test_a_view_sees_an_upstream_overwrite(self, h: EngineHarness, tmp_path: Path) -> None:
        requires(h, "views")
        requires(h, "sql_models")  # the view is a SQL query
        requires(h, "strategies", "overwrite")
        with self.session(h, tmp_path) as s:
            self.write(s, h, "t_up", ROWS, "overwrite", "r:1")
            view = _target("v_up")
            h.catalog.ensure_namespace(view.namespace)
            s.create_view(
                view,
                "select id from up",
                {"up": ReadSpec("table", table=_target("t_up"))},
            )
            self.write(s, h, "t_up", ROWS[:1], "overwrite", "r:2")
            rel = s.reader("table").read(ReadSpec("table", table=view))
            assert [r["id"] for r in h.to_rows(rel.native)] == [1]

    # -- settings ---------------------------------------------------------------------------

    def test_a_session_leaves_global_settings_alone(self, h: EngineHarness, tmp_path: Path) -> None:
        before = dict(h.settings_snapshot())
        with self.session(h, tmp_path) as s:
            s.count(s.relation(h.make_native(ROWS, SCHEMA)))
        assert dict(h.settings_snapshot()) == before

    # -- write strategies -------------------------------------------------------------------

    def put(
        self,
        s: EngineSession,
        h: EngineHarness,
        name: str,
        rows: Rows,
        schema: Schema,
        cfg: WriteConfig,
        token: str,
    ) -> WriteResult:
        h.catalog.ensure_namespace(_target(name).namespace)
        rel = s.relation(h.make_native(rows, schema))
        return s.materializer(cfg.strategy).materialize(rel, _target(name), cfg, token)

    def table_rows(self, s: EngineSession, h: EngineHarness, name: str) -> list[dict[str, Any]]:
        return _key(_norm(self.read_table(s, h, name)))

    def merge_case(
        self,
        h: EngineHarness,
        tmp_path: Path,
        name: str,
        batch: Rows,
        cfg: WriteConfig,
        seed: Rows = KV_ROWS,
    ) -> tuple[WriteResult, list[dict[str, Any]], reference.MergeResult]:
        """Seed a table, merge `batch`, and return (result, rows read back, reference outcome)."""
        requires(h, "strategies", "merge")
        cond = None
        if cfg.update_condition is not None:
            cond = lambda src, tgt: src["v"] > tgt["v"]
        with self.session(h, tmp_path) as s:
            self.put(s, h, name, seed, KV_SCHEMA, WriteConfig("overwrite"), "seed")
            result = self.put(s, h, name, batch, KV_SCHEMA, cfg, "r:merge")
            got = self.table_rows(s, h, name)
        want = reference.merge_rows(seed, batch, cfg, condition=cond)
        return result, got, want

    def test_merge_inserts_and_updates(self, h: EngineHarness, tmp_path: Path) -> None:
        batch = ({"id": 2, "label": "b2", "v": 30}, {"id": 3, "label": "c", "v": 5})
        cfg = WriteConfig("merge", unique_key=("id",))
        result, got, want = self.merge_case(h, tmp_path, "t_merge", batch, cfg)
        assert got == _key(_norm(want.rows))
        assert (result.rows_inserted, result.rows_updated) == (1, 1)
        assert result.rows_written == 2

    def test_merge_update_condition(self, h: EngineHarness, tmp_path: Path) -> None:
        if not h.update_condition:
            pytest.skip("harness gives no update_condition")
        batch = ({"id": 1, "label": "low", "v": 1}, {"id": 2, "label": "high", "v": 99})
        cfg = WriteConfig("merge", unique_key=("id",), update_condition=h.update_condition)
        result, got, want = self.merge_case(h, tmp_path, "t_mcond", batch, cfg)
        assert got == _key(_norm(want.rows))
        assert result.rows_updated == 1

    @pytest.mark.parametrize("mode", ["hard", "soft"])
    def test_merge_deletes(self, h: EngineHarness, tmp_path: Path, mode: str) -> None:
        batch = (
            {"id": 1, "label": "gone", "v": 0},
            {"id": 9, "label": "gone", "v": 0},  # unmatched delete: ignored
            {"id": 4, "label": "new", "v": 4},
        )
        deletes = DeleteSpec("label", "gone", "hard" if mode == "hard" else "soft")
        cfg = WriteConfig("merge", unique_key=("id",), deletes=deletes)
        result, got, want = self.merge_case(h, tmp_path, f"t_mdel_{mode}", batch, cfg)
        assert got == _key(_norm(want.rows))
        assert result.rows_deleted == want.deleted == 1
        assert result.rows_inserted == want.inserted == 1
        assert result.rows_updated == want.updated
        assert result.rows_written == want.inserted + want.updated + (
            want.deleted if mode == "soft" else 0
        )

    @pytest.mark.parametrize("mode", ["hard", "soft"])
    def test_merge_deletes_obey_the_update_condition(
        self, h: EngineHarness, tmp_path: Path, mode: str
    ) -> None:
        if not h.update_condition:
            pytest.skip("harness gives no update_condition")
        batch = (
            {"id": 1, "label": "gone", "v": 1},  # condition false (1 < 10): the delete is skipped
            {"id": 2, "label": "gone", "v": 99},  # condition true: deleted
        )
        deletes = DeleteSpec("label", "gone", "hard" if mode == "hard" else "soft")
        cfg = WriteConfig(
            "merge", unique_key=("id",), deletes=deletes, update_condition=h.update_condition
        )
        result, got, want = self.merge_case(h, tmp_path, f"t_mdelc_{mode}", batch, cfg)
        assert got == _key(_norm(want.rows))
        assert result.rows_deleted == want.deleted == 1
        assert result.rows_updated == want.updated
        assert result.rows_written == want.inserted + want.updated + (
            want.deleted if mode == "soft" else 0
        )

    def test_merge_null_keys_never_match(self, h: EngineHarness, tmp_path: Path) -> None:
        batch = ({"id": None, "label": "n1", "v": 1}, {"id": None, "label": "n2", "v": 2})
        cfg = WriteConfig("merge", unique_key=("id",))
        seed = (*KV_ROWS, {"id": None, "label": "old", "v": 0})  # a null key in the target too
        result, got, want = self.merge_case(h, tmp_path, "t_mnull", batch, cfg, seed)
        assert got == _key(_norm(want.rows))
        assert len(got) == 5  # nothing matched: both batch rows were inserted
        assert result.rows_inserted == 2 and not result.rows_updated

    def test_merge_rejects_a_duplicate_key(self, h: EngineHarness, tmp_path: Path) -> None:
        batch = ({"id": 3, "label": "x", "v": 1}, {"id": 3, "label": "y", "v": 2})
        with pytest.raises(WriteKeyError):
            self.merge_case(h, tmp_path, "t_mdup", batch, WriteConfig("merge", unique_key=("id",)))

    def test_replace_partitions(self, h: EngineHarness, tmp_path: Path) -> None:
        requires(h, "strategies", "replace_partitions")
        schema = (Column("part", "string"), Column("v", "int64"))
        current = (
            {"part": "a", "v": 1},
            {"part": "a", "v": 2},
            {"part": "b", "v": 3},
            {"part": None, "v": 4},
        )
        batch = ({"part": "a", "v": 9}, {"part": None, "v": 8})
        cfg = WriteConfig("replace_partitions", partition_by=("part",))
        with self.session(h, tmp_path) as s:
            self.put(s, h, "t_rp", current, schema, WriteConfig("overwrite"), "seed")
            result = self.put(s, h, "t_rp", batch, schema, cfg, "r:rp")
            got = self.table_rows(s, h, "t_rp")
        want, deleted = reference.replace_partitions_rows(current, batch, ("part",))
        assert got == _key(_norm(want))
        assert result.rows_deleted == deleted == 3

    @pytest.mark.parametrize("strategy", ["append", "overwrite", "merge", "replace_partitions"])
    def test_a_replayed_token_writes_once(
        self, h: EngineHarness, tmp_path: Path, strategy: str
    ) -> None:
        requires(h, "idempotent_writes")
        requires(h, "strategies", strategy)
        cfg = WriteConfig(
            strategy, unique_key=("id",), partition_by=("label",), commit_meta={"watermark": "w1"}
        )
        with self.session(h, tmp_path) as s:
            first = self.put(s, h, f"t_idem_{strategy}", KV_ROWS, KV_SCHEMA, cfg, "r:same")
            assert not first.skipped and first.commit_meta == {"watermark": "w1"}
            second = self.put(s, h, f"t_idem_{strategy}", KV_ROWS, KV_SCHEMA, cfg, "r:same")
            assert second.skipped and second.commit_meta == {"watermark": "w1"}
            assert replace(second, skipped=False) == first
            assert s.applied_token(_target(f"t_idem_{strategy}"), "r:same") == second
            assert s.applied_token(_target(f"t_idem_{strategy}"), "r:other") is None
            assert self.table_rows(s, h, f"t_idem_{strategy}") == _key(_norm(KV_ROWS))

    # -- schema alignment -------------------------------------------------------------------

    def test_align_adds_drops_and_widens(self, h: EngineHarness, tmp_path: Path) -> None:
        source = (Column("id", "int64"), Column("label", "string"), Column("junk", "string"))
        target = (Column("label", "string"), Column("id", "float64"), Column("extra", "int64"))
        rows = ({"id": 1, "label": "a", "junk": "x"}, {"id": 2, "label": None, "junk": "y"})
        with self.session(h, tmp_path) as s:
            out = s.align(s.relation(h.make_native(rows, source)), target)
            want = reference.align_rows(rows, target)
            self.assert_schema(h, s.schema(out), target, want)
            assert _key(h.to_rows(out.native)) == _key(want)

    def assert_schema(
        self, h: EngineHarness, got: Schema, target: Schema, rows: Sequence[Mapping[str, Any]]
    ) -> None:
        assert [c.name for c in got] == [c.name for c in target]
        for col, wanted in zip(got, target, strict=True):
            if h.typed_nulls or any(r[wanted.name] is not None for r in rows):
                assert col.dtype == wanted.dtype, col.name

    def test_align_casts_a_null_typed_column(self, h: EngineHarness, tmp_path: Path) -> None:
        source = (Column("id", "int64"), Column("n", "null"))
        rows = ({"id": 1, "n": None}, {"id": 2, "n": None})
        target = (Column("id", "int64"), Column("n", "string"))
        with self.session(h, tmp_path) as s:
            try:
                native = h.make_native(rows, source)
            except Exception:  # noqa: BLE001 - this engine cannot build a null-typed column
                pytest.skip("engine cannot build a null-typed column")
            out = s.align(s.relation(native), target)
            want = reference.align_rows(rows, target)
            self.assert_schema(h, s.schema(out), target, want)
            assert _key(h.to_rows(out.native)) == _key(want)

    # -- schema-change policy ---------------------------------------------------------------

    @pytest.mark.parametrize("strategy", ["append", "merge", "replace_partitions"])
    @pytest.mark.parametrize("policy", POLICY_NAMES)
    @pytest.mark.parametrize("scenario", list(MATRIX_BATCHES))
    def test_schema_policy_matrix(
        self, h: EngineHarness, tmp_path: Path, strategy: str, policy: str, scenario: str
    ) -> None:
        """Seed a table, write a batch that differs from it, and check the final schema and rows
        against the reference. `columns` of the write is what the core's policy decides."""
        requires(h, "strategies", strategy)
        batch_schema, batch_rows = MATRIX_BATCHES[scenario]
        name = f"t_sp_{strategy}_{policy}_{scenario}"
        cfg = WriteConfig(strategy, unique_key=("id",), partition_by=("part",))
        with self.session(h, tmp_path) as s:
            self.put(s, h, name, MATRIX_SEED, MATRIX_SCHEMA, WriteConfig("overwrite"), "seed")
            target_schema = h.catalog.table_schema(_target(name))
            batch = s.relation(h.make_native(batch_rows, batch_schema))
            want_names = MATRIX_COLUMNS[policy][scenario]
            if want_names is None:
                with pytest.raises(SchemaRejected):
                    resolve_columns(
                        policy,
                        target_schema,
                        s.schema(batch),
                        strategy=strategy,
                        full_refresh=False,
                    )
                return
            columns, diff, action = resolve_columns(
                policy,
                target_schema,
                s.schema(batch),
                strategy=strategy,
                full_refresh=False,
                keys=("id", "part"),
            )
            assert [(c.name, c.dtype) for c in columns] == want_names
            ignored = policy == "ignore" or (
                policy == "append_new_columns" and scenario == "removed"
            )
            assert not diff.empty and action == ("ignored" if ignored else "applied")
            if not h.schema_on_read:
                pytest.skip("engine does not read old files through the commit schema yet")
            aligned = s.align(batch, columns)
            write = replace(cfg, columns=columns)
            h.catalog.ensure_namespace(_target(name).namespace)
            s.materializer(strategy).materialize(aligned, _target(name), write, "r:policy")
            got = self.table_rows(s, h, name)
            rel = s.reader("table").read(ReadSpec("table", table=_target(name)))
            got_schema = s.schema(rel)
        old = reference.align_rows(MATRIX_SEED, columns)
        new = reference.align_rows(batch_rows, columns)
        if strategy == "append":
            want = [*old, *new]
        elif strategy == "merge":
            want = reference.merge_rows(old, new, cfg).rows
        else:
            want = reference.replace_partitions_rows(old, new, ("part",))[0]
        assert got == _key(_norm(want))
        self.assert_schema(h, got_schema, columns, want)

    @pytest.mark.parametrize("strategy", ["append", "merge", "replace_partitions"])
    @pytest.mark.parametrize("policy", POLICY_NAMES)
    def test_schema_policy_never_drops_or_nulls_a_key(
        self, h: EngineHarness, tmp_path: Path, strategy: str, policy: str
    ) -> None:
        """A batch without a key column (unique_key or partition_by) is a write key error under
        every policy, before anything is aligned or written."""
        requires(h, "strategies", strategy)
        name = f"t_spk_{strategy}_{policy}"
        schema, rows = (Column("part", "string"), Column("v", "int64")), ({"part": "c", "v": 1},)
        with self.session(h, tmp_path) as s:
            self.put(s, h, name, MATRIX_SEED, MATRIX_SCHEMA, WriteConfig("overwrite"), "seed")
            batch = s.schema(s.relation(h.make_native(rows, schema)))
            for keys in (("id",), ("part", "id")):
                with pytest.raises(WriteKeyError):
                    resolve_columns(
                        policy,
                        h.catalog.table_schema(_target(name)),
                        batch,
                        strategy=strategy,
                        full_refresh=False,
                        keys=keys,
                    )

    # -- filter_since and max_value ---------------------------------------------------------

    @pytest.mark.parametrize(
        ("dtype", "values"),
        [
            ("timestamp[us, UTC]", [datetime(2026, 1, d, tzinfo=UTC) for d in (1, 2, 3)]),
            ("timestamp[us]", [datetime(2026, 1, d) for d in (1, 2, 3)]),
            ("date", [date(2026, 1, d) for d in (1, 2, 3)]),
            ("int64", [1, 2, 3]),
            ("string", ["a", "b", "c"]),
        ],
    )
    def test_filter_since_bounds(
        self, h: EngineHarness, tmp_path: Path, dtype: str, values: list[Any]
    ) -> None:
        rows = [{"x": v} for v in [*values, None]]
        with self.session(h, tmp_path) as s:
            rel = s.relation(h.make_native(rows, (Column("x", dtype),)))
            inclusive = s.filter_since(rel, "x", values[1])
            exclusive = s.filter_since(rel, "x", values[1], inclusive=False)
            assert sorted(r["x"] for r in h.to_rows(inclusive.native)) == values[1:]
            assert [r["x"] for r in h.to_rows(exclusive.native)] == values[2:]

    def test_max_value_is_a_plain_python_value(self, h: EngineHarness, tmp_path: Path) -> None:
        expected = {
            "i": int,
            "f": float,
            "s": str,
            "d": date,
            "t": datetime,
            "n": Decimal,
        }
        with self.session(h, tmp_path) as s:
            rel = s.relation(h.make_native(TYPED_ROWS, TYPED_SCHEMA))
            for column, kind in expected.items():
                assert type(s.max_value(rel, column)) is kind, column
            empty = s.relation(h.make_native((), TYPED_SCHEMA))
            assert s.max_value(empty, "i") is None

    # -- quality ----------------------------------------------------------------------------

    def quality_case(
        self, h: EngineHarness
    ) -> tuple[list[ExpectationSpec], dict[str, reference.RowPredicate]]:
        requires(h, "quality")
        checks = [
            ExpectationSpec("id_set", "not_null", "warn", ("id",), {}),
            ExpectationSpec(
                "s_known", "accepted_values", "drop", ("s",), {"values": ["abc", "abd"]}
            ),
            ExpectationSpec("f_range", "range", "drop", ("f",), {"min": 0, "max": 10}),
            ExpectationSpec("s_ab", "regex", "warn", ("s",), {"pattern": "ab"}),
            ExpectationSpec("id_unique", "unique", "warn", ("id",), {}),
            ExpectationSpec("rows", "row_count", "warn", (), {}),
            ExpectationSpec("fresh", "freshness", "warn", ("ts",), {}),
        ]
        exprs: dict[str, reference.RowPredicate] = {}
        for i, (sql, predicate) in enumerate(h.expr_cases):
            checks.append(ExpectationSpec(f"expr{i}", "expr", "drop", (), {"expr": sql}))
            exprs[f"expr{i}"] = predicate
        return checks, exprs

    def test_evaluate_matches_the_reference(self, h: EngineHarness, tmp_path: Path) -> None:
        checks, exprs = self.quality_case(h)
        with self.session(h, tmp_path) as s:
            rel = s.relation(h.make_native(QUALITY_ROWS, QUALITY_SCHEMA))
            got = s.evaluate(rel, checks)
        want = reference.evaluate_rows(QUALITY_ROWS, checks, exprs=exprs)
        assert got.total_rows == want.total_rows
        assert dict(got.failed) == dict(want.failed)
        assert got.dropped == want.dropped
        assert dict(got.aggregates) == dict(want.aggregates)

    def test_apply_drops_matches_the_reference(self, h: EngineHarness, tmp_path: Path) -> None:
        checks, exprs = self.quality_case(h)
        meta = QuarantineMeta("run-1", datetime(2026, 2, 3, 4, 5, 6, tzinfo=UTC))
        with self.session(h, tmp_path) as s:
            rel = s.relation(h.make_native(QUALITY_ROWS, QUALITY_SCHEMA))
            out = s.apply_drops(rel, checks, quarantine=meta)
            assert out.quarantined is not None
            kept = _key(_norm(h.to_rows(out.kept.native)))
            dropped = _key(_norm(h.to_rows(out.quarantined.native)))
            assert s.apply_drops(rel, checks, quarantine=None).quarantined is None
        want_kept, want_dropped = reference.apply_drops_rows(
            QUALITY_ROWS, checks, meta, exprs=exprs
        )
        assert kept == _key(_norm(want_kept))
        assert want_dropped is not None
        assert dropped == _key(_norm(want_dropped))

    # -- intermediate -----------------------------------------------------------------------

    def intermediate(self, tmp_path: Path, run_id: str, model: str = "m") -> IntermediateRef:
        location = tmp_path / "artifacts" / "runs" / run_id / "intermediate" / model
        return IntermediateRef(run_id, model, location.as_posix())

    def put_intermediate(
        self, s: EngineSession, h: EngineHarness, ref: IntermediateRef, rows: Rows, token: str
    ) -> WriteResult:
        return s.write_intermediate(s.relation(h.make_native(rows, SCHEMA)), ref, token)

    def read_intermediate(
        self, s: EngineSession, h: EngineHarness, ref: IntermediateRef
    ) -> list[dict[str, Any]]:
        rel = s.reader("intermediate").read(ReadSpec("intermediate", location=ref.location))
        return _key(h.to_rows(rel.native))

    def test_intermediate_write_then_read_equals_input(
        self, h: EngineHarness, tmp_path: Path
    ) -> None:
        requires(h, "materializations", "intermediate")
        ref = self.intermediate(tmp_path, "r1")
        with self.session(h, tmp_path) as s:
            result = self.put_intermediate(s, h, ref, ROWS, "r1:m")
            assert result.rows_written == len(ROWS) and not result.skipped
            assert self.read_intermediate(s, h, ref) == _key([dict(r) for r in ROWS])

    def test_intermediate_same_token_writes_once(self, h: EngineHarness, tmp_path: Path) -> None:
        requires(h, "materializations", "intermediate")
        ref = self.intermediate(tmp_path, "r1")
        with self.session(h, tmp_path) as s:
            self.put_intermediate(s, h, ref, ROWS, "r1:m")
            again = self.put_intermediate(s, h, ref, ROWS[:1], "r1:m")
            assert again.skipped
            assert self.read_intermediate(s, h, ref) == _key([dict(r) for r in ROWS])

    def test_intermediate_a_new_token_replaces_the_data(
        self, h: EngineHarness, tmp_path: Path
    ) -> None:
        requires(h, "materializations", "intermediate")
        ref = self.intermediate(tmp_path, "r1")
        with self.session(h, tmp_path) as s:
            self.put_intermediate(s, h, ref, ROWS, "t1")
            second = self.put_intermediate(s, h, ref, ROWS[:1], "t2")
            assert not second.skipped
            assert self.read_intermediate(s, h, ref) == _key([dict(ROWS[0])])

    def test_intermediate_second_run_id_has_its_own_folder(
        self, h: EngineHarness, tmp_path: Path
    ) -> None:
        requires(h, "materializations", "intermediate")
        first, second = self.intermediate(tmp_path, "r1"), self.intermediate(tmp_path, "r2")
        assert first.location != second.location
        with self.session(h, tmp_path) as s:
            self.put_intermediate(s, h, first, ROWS, "r1:m")
            self.put_intermediate(s, h, second, ROWS[:1], "r2:m")
            assert self.read_intermediate(s, h, first) == _key([dict(r) for r in ROWS])
            assert self.read_intermediate(s, h, second) == _key([dict(ROWS[0])])
        assert Path(first.location).is_dir() and Path(second.location).is_dir()

    # -- cancel -----------------------------------------------------------------------------

    def test_cancel_stops_a_slow_relation(self, h: EngineHarness, tmp_path: Path) -> None:
        requires(h, "cancel")
        slow = h.slow_relation
        if slow is None:
            pytest.skip("harness gives no slow_relation")
        entered, finished = threading.Event(), threading.Event()
        errors: list[BaseException] = []
        with self.session(h, tmp_path) as s:

            def work() -> None:
                with s.unit_scope("u1"):
                    entered.set()
                    try:
                        slow(s, 30.0)
                    except Exception as exc:  # noqa: BLE001 - the engine's own cancel error
                        errors.append(exc)
                finished.set()

            threading.Thread(target=work, daemon=True).start()
            assert entered.wait(5)
            time.sleep(0.05)
            s.cancel("u1")
            assert finished.wait(5), "cancel did not stop the work within 5 s"
            assert errors
            s.cancel("never-started")  # unknown units are ignored
