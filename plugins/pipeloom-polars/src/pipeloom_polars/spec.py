"""The plugin spec. It imports no dataframe library: the engine module loads on first use."""

from __future__ import annotations

from typing import Any

from pipeloom.engine.protocols import Engine
from pipeloom.engine.types import EngineCapabilities
from pipeloom.plugins import API_VERSION, OptionsModel, PluginSpec

CAPABILITIES = EngineCapabilities(
    formats=frozenset({"parquet", "csv", "json", "ndjson"}),
    strategies=frozenset({"append", "overwrite", "merge", "replace_partitions"}),
    materializations=frozenset({"table", "view", "ephemeral", "incremental"}),
    sql_models=True,
    views=True,
    idempotent_writes=True,
    cancel=False,
    bytes_metrics=True,
    portable=True,
    quality=True,
)


class PolarsOptions(OptionsModel):
    """The polars engine has no options."""


def _build(opts: Any) -> Engine:
    from pipeloom_polars.engine import PolarsEngine

    return PolarsEngine()


SPEC: PluginSpec[Engine] = PluginSpec(
    "polars",
    "polars",
    "engines",
    API_VERSION,
    _build,
    PolarsOptions,
    capabilities=CAPABILITIES,
    description="Polars lazy frames; SQL through pl.SQLContext; tables on the filesystem catalog.",
)
