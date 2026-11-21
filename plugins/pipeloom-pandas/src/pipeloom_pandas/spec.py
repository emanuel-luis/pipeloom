"""The plugin spec. It imports no dataframe library: the engine module loads on first use."""

from __future__ import annotations

from importlib.util import find_spec
from typing import Any

from pipeloom.engine.protocols import Engine
from pipeloom.engine.types import EngineCapabilities
from pipeloom.plugins import API_VERSION, OptionsModel, PluginSpec

# ctx.sql runs through DuckDB when it is installed (`pipeloom-pandas[sql]`, ADR-0068). find_spec
# only looks for the package: nothing is imported here.
SQL = find_spec("duckdb") is not None

CAPABILITIES = EngineCapabilities(
    formats=frozenset({"parquet", "csv", "json", "ndjson"}),
    strategies=frozenset({"append", "overwrite", "merge", "replace_partitions", "scd2"}),
    materializations=frozenset({"table", "ephemeral", "incremental", "intermediate"}),
    sql_models=SQL,
    views=False,
    idempotent_writes=True,
    cancel=False,
    bytes_metrics=True,
    portable=True,
    quality=True,
    interval_writes=True,
    # Without DuckDB there is no SQL: ctx.sql raises SqlUnsupportedError (PLK606 warns first).
    sql_dialect="duckdb" if SQL else None,
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
    description="Eager pandas DataFrames; tables on the filesystem catalog; SQL through DuckDB.",
)
