"""Plugin API (0, 4): run start time, session before lock, unit scope from read to write."""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from contextlib import AbstractContextManager, contextmanager
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest
from pydantic import Field

from pipeloom.api import Project, ProjectOptions
from pipeloom.config.types import ProjectPath
from pipeloom.engine.protocols import Catalog, Engine, EngineSession, Materializer, Reader
from pipeloom.engine.types import (
    IntermediateRef,
    ReadSpec,
    Relation,
    TargetRef,
    WriteConfig,
    WriteResult,
)
from pipeloom.plugins import API_VERSION, OptionsModel, PluginSpec
from pipeloom.runtime.context import RuntimeContext
from pipeloom.runtime.run import RunOptions, resolve_run_started_at
from pipeloom.state.sqlite import SqliteStore
from pipeloom.state.types import LockInfo, RunRecord
from pipeloom.testing.clock import FakeClock
from pipeloom.testing.memory import CAPABILITIES, MemoryCatalog, MemoryEngine
from tests.conftest import make_project

LOG: list[str] = []
CONTEXTS: list[RuntimeContext] = []
STARTS: list[datetime | None] = []
NOW = datetime(2026, 11, 4, 9, 30, tzinfo=UTC)
EARLIER = NOW - timedelta(days=1)


def _record(started: datetime) -> RunRecord:
    return RunRecord("r", "p", "t", "0" * 64, "running", started)


# -- resolution order --------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("flag", "prior", "unit_mode", "expected", "conflict"),
    [
        (None, None, False, NOW, False),  # the owner records the run: now
        (None, None, True, None, False),  # a unit invocation without a record has none
        (EARLIER, None, False, EARLIER, False),  # the flag
        (EARLIER, None, True, EARLIER, False),
        (None, _record(EARLIER), False, EARLIER, False),  # a reused run id keeps its start
        (None, _record(EARLIER), True, EARLIER, False),  # a unit invocation reads the record
        (EARLIER, _record(EARLIER), False, EARLIER, False),  # same value: fine
        (NOW, _record(EARLIER), False, None, True),  # disagreement
        (NOW, _record(EARLIER), True, None, True),
    ],
)
def test_resolution_order(
    flag: datetime | None,
    prior: RunRecord | None,
    unit_mode: bool,
    expected: datetime | None,
    conflict: bool,
) -> None:
    got, diag = resolve_run_started_at(flag, prior, NOW, unit_mode=unit_mode)
    assert got == expected
    assert (diag is not None and diag.code == "PLX108") is conflict
    if diag is not None:  # names both values
        assert "2026-11-04T09:30:00" in diag.message and "2026-11-03T09:30:00" in diag.message


def test_flag_with_another_offset_is_the_same_instant() -> None:
    plus3 = EARLIER.astimezone(timezone(timedelta(hours=3)))
    got, diag = resolve_run_started_at(plus3, _record(EARLIER), NOW, unit_mode=False)
    assert diag is None and got == EARLIER


# -- recording engine, store and the order of calls ---------------------------------------------


class _Opts(OptionsModel):
    pass


class _RecReader:
    def __init__(self, inner: Reader) -> None:
        self._inner = inner

    def read(self, spec: ReadSpec) -> Relation:
        LOG.append("read")
        return self._inner.read(spec)


class _RecMaterializer:
    def __init__(self, inner: Materializer) -> None:
        self._inner = inner
        self.strategy = inner.strategy

    def materialize(
        self, rel: Relation, target: TargetRef, cfg: WriteConfig, token: str
    ) -> WriteResult:
        LOG.append("write")
        STARTS.append(cfg.run_started_at)
        return self._inner.materialize(rel, target, cfg, token)


class _RecSession:
    def __init__(self, inner: EngineSession) -> None:
        self._inner = inner

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)

    def reader(self, fmt: str) -> Reader:
        return _RecReader(self._inner.reader(fmt))

    def materializer(self, strategy: str) -> Materializer:
        return _RecMaterializer(self._inner.materializer(strategy))

    def sql(self, query: str, inputs: Mapping[str, Relation]) -> Relation:
        LOG.append("body")
        return self._inner.sql(query, inputs)

    def write_intermediate(self, rel: Relation, ref: IntermediateRef, token: str) -> WriteResult:
        STARTS.append(ref.run_started_at)
        return self._inner.write_intermediate(rel, ref, token)

    @contextmanager
    def unit_scope(self, unit_id: str) -> Iterator[None]:
        LOG.append("scope+")
        try:
            with self._inner.unit_scope(unit_id):
                yield
        finally:
            LOG.append("scope-")


class _RecEngine(MemoryEngine):
    @contextmanager
    def session(self, ctx: RuntimeContext, *, catalog: Catalog) -> Iterator[EngineSession]:
        LOG.append("session+")
        CONTEXTS.append(ctx)
        try:
            with super().session(ctx, catalog=catalog) as inner:
                yield _RecSession(inner)
        finally:
            LOG.append("session-")


class _RecStore(SqliteStore):
    def get_run(self, run_id: str) -> Any:
        LOG.append("get_run")
        return super().get_run(run_id)

    def lock(self, scope: str, run_id: str) -> AbstractContextManager[LockInfo]:
        LOG.append("lock+")
        inner = super().lock(scope, run_id)

        @contextmanager
        def held() -> Iterator[LockInfo]:
            with inner as info:
                yield info
            LOG.append("lock-")

        return held()


class _StoreOpts(OptionsModel):
    path: ProjectPath = Field(".pipeloom/state.db", validate_default=True)


ENGINE: PluginSpec[Engine] = PluginSpec(
    "rec",
    "rec",
    "engines",
    API_VERSION,
    lambda _o: _RecEngine({"/raw": ({"x": 1},)}),
    _Opts,
    capabilities=CAPABILITIES,
)
CATALOG: PluginSpec[Catalog] = PluginSpec(
    "rec", "rec", "catalogs", API_VERSION, lambda _o: MemoryCatalog(), _Opts
)
STORE: PluginSpec[Any] = PluginSpec(
    "rec", "rec", "state_stores", API_VERSION, lambda o: _RecStore(o.path), _StoreOpts
)


MODEL_A = "  - {name: a, sql: select x from raw, inputs: {raw: {source: raw}}}\n"


@pytest.fixture
def project(tmp_path: Path) -> Project:
    return build(tmp_path, MODEL_A)


def build(tmp_path: Path, models: str) -> Project:
    LOG.clear()
    CONTEXTS.clear()
    STARTS.clear()
    make_project(
        tmp_path,
        {
            "pipeloom.yml": "config_version: 1\nname: p\ntargets:\n  t:\n"
            "    engine: {kind: rec}\n    catalog: {kind: rec}\n    state: {kind: rec}\n",
            "models/m.yml": "sources:\n  - {name: raw, format: memory, location: /raw}\n"
            "models:\n" + models,
        },
    )
    here = "tests.runtime.test_api04"
    opts = ProjectOptions(
        project_dir=tmp_path,
        target="t",
        plugins=(f"{here}:ENGINE", f"{here}:CATALOG", f"{here}:STORE"),
        env={},
    )
    return Project(opts, clock=FakeClock(), invocation_id="inv")


def test_unit_scope_spans_read_body_and_write(project: Project) -> None:
    assert project.run(RunOptions(run_id="r1")).exit_code == 0
    inner = LOG[LOG.index("scope+") : LOG.index("scope-") + 1]
    assert inner == ["scope+", "read", "body", "write", "scope-"]
    assert LOG.count("scope+") == 1


def test_session_opens_before_the_lock_and_closes_after(project: Project) -> None:
    assert project.run(RunOptions(run_id="r1")).exit_code == 0
    assert LOG[0] == "session+" and LOG[-1] == "session-"
    assert LOG.index("session+") < LOG.index("lock+") < LOG.index("scope+")
    assert LOG.index("scope-") < LOG.index("lock-") < LOG.index("session-")


def test_session_opens_before_any_state_call(project: Project) -> None:
    assert project.run(RunOptions(run_id="r1")).exit_code == 0
    assert LOG.index("session+") < LOG.index("get_run") < LOG.index("lock+")


def test_run_options_reject_a_naive_start_and_a_start_without_run_id() -> None:
    naive = datetime(2026, 1, 2, 3, 4, 5)
    with pytest.raises(ValueError, match="timezone-aware"):
        RunOptions(run_id="r1", run_started_at=naive)
    with pytest.raises(ValueError, match="needs run_id"):
        RunOptions(run_started_at=naive.replace(tzinfo=UTC))


def test_context_carries_start_and_artifacts_root(project: Project, tmp_path: Path) -> None:
    given = datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)
    assert project.run(RunOptions(run_id="r1", run_started_at=given)).exit_code == 0
    assert STARTS[-1] == given
    assert CONTEXTS[-1].run_started_at is None  # the session opened before the start resolved
    assert CONTEXTS[-1].artifacts_root == (tmp_path / "target").resolve().as_posix()
    # the same run id again keeps the recorded start, with or without the flag
    assert project.run(RunOptions(run_id="r1")).exit_code == 0
    assert STARTS[-1] == given
    clash = project.run(RunOptions(run_id="r1", run_started_at=given + timedelta(hours=1)))
    assert clash.exit_code == 2 and [d.code for d in clash.diagnostics] == ["PLX108"]


def test_an_intermediate_write_carries_the_run_start(tmp_path: Path) -> None:
    models = MODEL_A + (
        "  - {name: i, materialized: intermediate, sql: select x from raw, "
        "inputs: {raw: {source: raw}}}\n"
    )
    given = datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)
    outcome = build(tmp_path, models).run(RunOptions(run_id="r1", run_started_at=given))
    assert outcome.exit_code == 0, outcome.diagnostics
    assert STARTS.count(given) == 2  # the table write and the intermediate write


def test_a_new_run_without_flag_starts_now(project: Project) -> None:
    assert project.run(RunOptions()).exit_code == 0
    ctx = CONTEXTS[-1]  # the generated run id is stamped with the same instant
    assert STARTS[-1] == datetime.strptime(ctx.run_id[:16], "%Y%m%dT%H%M%SZ").replace(tzinfo=UTC)
