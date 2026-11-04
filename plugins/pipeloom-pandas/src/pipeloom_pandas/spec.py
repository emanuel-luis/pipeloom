"""The plugin spec. It imports no dataframe library: the engine module loads on first use."""

from __future__ import annotations

from typing import Any

from pipeloom.engine.protocols import Engine
from pipeloom.engine.types import EngineCapabilities
from pipeloom.plugins import API_VERSION, OptionsModel, PluginSpec

CAPABILITIES = EngineCapabilities(
    formats=frozenset({"parquet", "csv", "json", "ndjson"}),
    strategies=frozenset({"append", "overwrite", "merge", "replace_partitions"}),
    materializations=frozenset({"table", "ephemeral", "incremental"}),
    sql_models=False,
    views=False,
    idempotent_writes=True,
    cancel=False,
    bytes_metrics=True,
    portable=True,
    quality=True,
)


class PandasOptions(OptionsModel):
    """The pandas engine has no options."""


def _build(opts: Any) -> Engine:
    from pipeloom_pandas.engine import PandasEngine

    return PandasEngine()


SPEC: PluginSpec[Engine] = PluginSpec(
    "pandas",
    "pandas",
    "engines",
    API_VERSION,
    _build,
    PandasOptions,
    capabilities=CAPABILITIES,
    description="Eager pandas DataFrames; tables on the filesystem catalog; no SQL in 0.2.",
)
