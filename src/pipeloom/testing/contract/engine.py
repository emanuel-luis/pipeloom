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
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from pipeloom.engine.protocols import Catalog, Engine, EngineSession
from pipeloom.engine.types import Column, ReadSpec, Relation, Schema, TargetRef, WriteConfig
from pipeloom.platform import Identity
from pipeloom.runtime.context import RuntimeContext
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

    def test_the_same_token_writes_once(self, h: EngineHarness, tmp_path: Path) -> None:
        requires(h, "idempotent_writes")
        strategy = "append" if "append" in h.engine.capabilities.strategies else "overwrite"
        with self.session(h, tmp_path) as s:
            first = self.write(s, h, "t_tok", ROWS, strategy, "r:same")
            second = self.write(s, h, "t_tok", ROWS, strategy, "r:same")
            assert first == second
            assert self.read_table(s, h, "t_tok") == _key([dict(r) for r in ROWS])

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
        with self.session(h, tmp_path) as s:
            result = self.write(s, h, "t_met", ROWS, "overwrite", "r:1")
            assert result.rows_written == len(ROWS)
            if h.engine.capabilities.bytes_metrics:
                assert result.bytes_written is not None

    def test_two_threads_materialize_at_once(self, h: EngineHarness, tmp_path: Path) -> None:
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
