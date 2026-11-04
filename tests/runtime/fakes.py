"""A catalog that fails to prepare a namespace, for PLM201."""

from __future__ import annotations

from pydantic import Field

from pipeloom.config.types import ProjectPath
from pipeloom.engine.protocols import Catalog, Engine
from pipeloom.engine.types import Namespace, StateStoreCapabilities
from pipeloom.plugins import API_VERSION, OptionsModel, PluginSpec
from pipeloom.state.sqlite import SqliteStore
from pipeloom.state.types import StateKey, StateStore, Watermark
from pipeloom.testing.memory import CAPABILITIES as MEMORY_CAPABILITIES
from pipeloom.testing.memory import MemoryCatalog, MemoryEngine


class FailingCatalog(MemoryCatalog):
    def ensure_namespace(self, ns: Namespace) -> None:
        raise OSError("disk full")


class _Options(OptionsModel):
    pass


FAILING: PluginSpec[Catalog] = PluginSpec(
    "failing",
    "failing",
    "catalogs",
    API_VERSION,
    lambda _opts: FailingCatalog(),
    _Options,
    compatible_engines=frozenset({"memory"}),
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
