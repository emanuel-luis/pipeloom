"""A catalog that fails to prepare a namespace, for PLM201."""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from contextlib import contextmanager

from pydantic import Field

from pipeloom.config.types import ProjectPath
from pipeloom.engine.errors import TransientError, WriteRefusedError
from pipeloom.engine.protocols import Catalog, Engine, EngineSession, Materializer
from pipeloom.engine.types import (
    ExpectationSpec,
    Namespace,
    QualityCounts,
    Relation,
    StateStoreCapabilities,
)
from pipeloom.plugins import API_VERSION, OptionsModel, PluginSpec
from pipeloom.runtime.context import RuntimeContext
from pipeloom.state.sqlite import SqliteStore
from pipeloom.state.types import StateKey, StateStore, Watermark
from pipeloom.testing.memory import CAPABILITIES as MEMORY_CAPABILITIES
from pipeloom.testing.memory import MemoryCatalog, MemoryEngine, MemorySession


class FailingCatalog(MemoryCatalog):
    def ensure_namespace(self, ns: Namespace) -> None:
        raise OSError("disk full")


class RefusingSession(MemorySession):
    """A session whose writes are refused by an idempotency guard, for PLM207."""

    def materializer(self, strategy: str) -> Materializer:
        raise WriteRefusedError("txnVersion is behind the table's last version for this model")


class RefusingEngine(MemoryEngine):
    name = "refusing"

    @contextmanager
    def session(self, ctx: RuntimeContext, *, catalog: Catalog) -> Iterator[EngineSession]:
        yield RefusingSession(self.store, self._tokens)


class FlakySession(MemorySession):
    """A session whose counting fails with a transient error, for the contract's null check."""

    def evaluate(self, rel: Relation, checks: Sequence[ExpectationSpec]) -> QualityCounts:
        raise TransientError("cluster lost")


class FlakyEngine(MemoryEngine):
    name = "flaky"

    @contextmanager
    def session(self, ctx: RuntimeContext, *, catalog: Catalog) -> Iterator[EngineSession]:
        yield FlakySession(self.store, self._tokens)


class _Options(OptionsModel):
    pass


FLAKY_ENGINE: PluginSpec[Engine] = PluginSpec(
    "flaky",
    "flaky",
    "engines",
    API_VERSION,
    lambda _opts: FlakyEngine(),
    _Options,
    capabilities=MEMORY_CAPABILITIES,
)
FLAKY_CATALOG: PluginSpec[Catalog] = PluginSpec(
    "flaky",
    "flaky",
    "catalogs",
    API_VERSION,
    lambda _opts: MemoryCatalog(),
    _Options,
    compatible_engines=frozenset({"flaky"}),
)


FAILING: PluginSpec[Catalog] = PluginSpec(
    "failing",
    "failing",
    "catalogs",
    API_VERSION,
    lambda _opts: FailingCatalog(),
    _Options,
    compatible_engines=frozenset({"memory"}),
)


REFUSING_ENGINE: PluginSpec[Engine] = PluginSpec(
    "refusing",
    "refusing",
    "engines",
    API_VERSION,
    lambda _opts: RefusingEngine(),
    _Options,
    capabilities=MEMORY_CAPABILITIES,
)
REFUSING_CATALOG: PluginSpec[Catalog] = PluginSpec(
    "refusing",
    "refusing",
    "catalogs",
    API_VERSION,
    lambda _opts: MemoryCatalog(),
    _Options,
    compatible_engines=frozenset({"refusing"}),
)


# -- a memory engine that outlives one `Project.run`, and a state store that misbehaves on cue --

KEEP: dict[str, MemoryEngine] = {}
"""The engine every `keepmem` build returns: tables and tokens survive between runs."""
FAULT: dict[str, bool] = {"crash": False, "move": False}
"""`crash`: the watermark swap raises. `move`: another run commits first, so the swap loses."""


def reset_keep() -> MemoryEngine:
    KEEP["engine"] = MemoryEngine()
    FAULT.update(crash=False, move=False)
    return KEEP["engine"]


class _KeepOptions(OptionsModel):
    pass


KEEP_ENGINE: PluginSpec[Engine] = PluginSpec(
    "keepmem",
    "keepmem",
    "engines",
    API_VERSION,
    lambda _opts: KEEP["engine"],
    _KeepOptions,
    capabilities=MEMORY_CAPABILITIES,
)
KEEP_CATALOG: PluginSpec[Catalog] = PluginSpec(
    "keepmem",
    "keepmem",
    "catalogs",
    API_VERSION,
    lambda _opts: MemoryCatalog(store=KEEP["engine"].store),
    _KeepOptions,
    compatible_engines=frozenset({"keepmem"}),
)


class FaultyStore(SqliteStore):
    def cas_watermark(self, key: StateKey, expected: Watermark | None, new: Watermark) -> bool:
        if FAULT["crash"]:
            raise RuntimeError("killed before the watermark swap")
        if FAULT["move"]:
            other = Watermark(999, new.dtype, "other-run", new.updated_at)
            super().cas_watermark(key, expected, other)
        return super().cas_watermark(key, expected, new)


class _FaultyOptions(OptionsModel):
    path: ProjectPath = Field(".pipeloom/state.db", validate_default=True)


FAULTY: PluginSpec[StateStore] = PluginSpec(
    "faulty",
    "faulty",
    "state_stores",
    API_VERSION,
    lambda opts: FaultyStore(opts.path),
    _FaultyOptions,
    capabilities=StateStoreCapabilities(shared=False),
)
