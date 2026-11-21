"""The plugin specs. They import no DuckDB: the engine and catalog modules load on first use."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Annotated, Any

from pydantic import AfterValidator, Field, ValidationInfo

from pipeloom.config.types import ProjectPath, resolve_project_path
from pipeloom.engine.protocols import Catalog, Engine
from pipeloom.engine.types import CatalogCapabilities, EngineCapabilities
from pipeloom.plugins import API_VERSION, OptionsModel, PluginSpec

CAPABILITIES = EngineCapabilities(
    formats=frozenset({"parquet", "csv", "delta"}),
    strategies=frozenset({"append", "overwrite", "merge", "replace_partitions", "scd2"}),
    materializations=frozenset({"table", "view", "ephemeral", "incremental", "intermediate"}),
    sql_models=True,
    views=True,
    idempotent_writes=True,
    cancel=True,
    bytes_metrics=False,
    portable=True,
    quality=True,
    sql_dialect="duckdb",
)

MEMORY = ":memory:"


class DuckDBEngineOptions(OptionsModel):
    extension_directory: ProjectPath | None = None
    """Where DuckDB installs and loads extensions; DuckDB's own default (under the home
    directory) when unset. Pin it in the project so CI can preinstall it."""
    extensions: tuple[str, ...] = ()
    """Extensions to load when the session opens, for example `httpfs`. `delta` loads on the
    first Delta read without being listed."""
    install_extensions: bool = False
    """Install a missing extension from the network into `extension_directory`. Off: a missing
    extension is an error (PLX121) and nothing is downloaded."""
    config: Mapping[str, str | int | bool] = Field(default_factory=dict)
    """DuckDB configuration options for the session's connection, for example `threads` or
    `memory_limit`."""


def _database(value: str, info: ValidationInfo) -> str:
    return value if value == MEMORY else resolve_project_path(value, info)


class DuckDBCatalogOptions(OptionsModel):
    path: Annotated[str, AfterValidator(_database)] = Field(
        "target/pipeloom.duckdb", validate_default=True
    )
    """The database file that holds tables without a catalog, or `:memory:`. A model catalog
    `<name>` is the file `<name>.duckdb` next to it (in memory too, for `:memory:`)."""


def _engine(opts: Any) -> Engine:
    from pipeloom_duckdb.engine import DuckDBEngine

    return DuckDBEngine(opts)


def _catalog(opts: Any) -> Catalog:
    from pipeloom_duckdb.catalog import DuckDBCatalog

    return DuckDBCatalog(opts.path)


ENGINE_SPEC: PluginSpec[Engine] = PluginSpec(
    "duckdb",
    "duckdb",
    "engines",
    API_VERSION,
    _engine,
    DuckDBEngineOptions,
    capabilities=CAPABILITIES,
    description="DuckDB relations and SQL; tables in a DuckDB database file.",
)

CATALOG_SPEC: PluginSpec[Catalog] = PluginSpec(
    "duckdb",
    "duckdb",
    "catalogs",
    API_VERSION,
    _catalog,
    DuckDBCatalogOptions,
    compatible_engines=frozenset({"duckdb"}),
    capabilities=CatalogCapabilities(views=True),
    description="Tables and views in a DuckDB database file; a model catalog is a sibling file.",
)
