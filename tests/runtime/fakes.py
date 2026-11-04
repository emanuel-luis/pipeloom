"""A catalog that fails to prepare a namespace, for PLM201."""

from __future__ import annotations

from pipeloom.engine.protocols import Catalog
from pipeloom.engine.types import Namespace
from pipeloom.plugins import API_VERSION, OptionsModel, PluginSpec
from pipeloom.testing.memory import MemoryCatalog


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
