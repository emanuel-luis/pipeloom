"""The plugin specs. They import no Spark library: the engine and catalog modules load on use."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Annotated, Any, Literal

from pydantic import AfterValidator, Field, ValidationInfo, model_validator

from pipeloom.config.types import resolve_project_path
from pipeloom.engine.protocols import Catalog, Engine
from pipeloom.engine.types import CatalogCapabilities, EngineCapabilities, StateStoreCapabilities
from pipeloom.plugins import API_VERSION, OptionsModel, PluginSpec
from pipeloom.state.types import StateStore
from pipeloom_spark.sqltext import has_scheme

CAPABILITIES = EngineCapabilities(
    formats=frozenset({"delta", "parquet", "csv", "json", "ndjson"}),
    strategies=frozenset({"append", "overwrite", "merge", "replace_partitions", "scd2"}),
    materializations=frozenset({"table", "ephemeral", "view", "incremental", "intermediate"}),
    sql_models=True,
    views=True,
    idempotent_writes=True,
    cancel=True,
    bytes_metrics=True,
    portable=True,
    quality=True,
)


class SparkEngineOptions(OptionsModel):
    mode: Literal["auto", "local", "connect", "active"] = "auto"
    """auto: an existing session or a Databricks runtime means active; a remote URL means connect;
    otherwise local."""
    master: str = "local[*]"
    """Spark master, local mode only."""
    remote: str | None = None
    """Spark Connect URL (sc://host:port[/;params]), connect mode only. A URL that starts with
    "local" starts a local Connect server."""
    app_name: str = "pipeloom"
    conf: Mapping[str, str] = Field(default_factory=dict)
    """Spark conf applied when the session is built. Rejected with mode active: a session the
    engine did not build is never configured."""
    delta: bool = True
    """Local and local Connect sessions: add the Delta jars, SQL extension and catalog."""
    txn_guard: bool = True
    """Set txnAppId and txnVersion on Delta writes."""


def _root(value: str, info: ValidationInfo) -> str:
    # A URI (s3://, abfss://, dbfs:/ ...) is not under the project directory: keep it as is.
    return value if has_scheme(value) else resolve_project_path(value, info)


RootPath = Annotated[str, AfterValidator(_root)]


class DeltaPathOptions(OptionsModel):
    root: RootPath = Field(".", validate_default=True)
    """Directory under which `<schema>/<name>` Delta tables live."""


class TableStateOptions(OptionsModel):
    table: str | None = None
    """Catalog table name, for example `ops._pipeloom_state`."""
    path: RootPath | None = None
    """Directory or URI of a path-based Delta table. Set exactly one of `table` and `path`."""
    session: SparkEngineOptions = Field(default_factory=SparkEngineOptions)
    """Used only when no Spark session exists yet; the store reuses the engine's session."""

    @model_validator(mode="after")
    def _one_location(self) -> TableStateOptions:
        if (self.table is None) == (self.path is None):
            raise ValueError("set exactly one of `table` and `path`")
        return self


class SparkCatalogOptions(OptionsModel):
    catalog: str | None = None
    """Spark catalog name; the session's current catalog when unset."""


def _engine(opts: Any) -> Engine:
    from pipeloom_spark.engine import SparkEngine

    return SparkEngine(opts)


def _delta_path(opts: Any) -> Catalog:
    from pipeloom_spark.catalog import DeltaPathCatalog

    return DeltaPathCatalog(opts.root)


def _spark_catalog(opts: Any) -> Catalog:
    from pipeloom_spark.catalog import SparkSessionCatalog

    return SparkSessionCatalog(opts.catalog)


def _table_state(opts: Any) -> StateStore:
    from pipeloom_spark.state import DeltaStateStore

    return DeltaStateStore(opts)


ENGINE_SPEC: PluginSpec[Engine] = PluginSpec(
    "spark",
    "spark",
    "engines",
    API_VERSION,
    _engine,
    SparkEngineOptions,
    capabilities=CAPABILITIES,
    description="Spark DataFrames and SQL, local, through Spark Connect or an active session.",
)

DELTA_PATH_SPEC: PluginSpec[Catalog] = PluginSpec(
    "delta_path",
    "delta_path",
    "catalogs",
    API_VERSION,
    _delta_path,
    DeltaPathOptions,
    compatible_engines=frozenset({"spark"}),
    capabilities=CatalogCapabilities(views=False),
    description="Delta tables at <root>/<schema>/<name>, no metastore.",
)

SPARK_CATALOG_SPEC: PluginSpec[Catalog] = PluginSpec(
    "spark_catalog",
    "spark_catalog",
    "catalogs",
    API_VERSION,
    _spark_catalog,
    SparkCatalogOptions,
    compatible_engines=frozenset({"spark"}),
    capabilities=CatalogCapabilities(views=True),
    description="Tables and views by name through the Spark session catalog.",
)

TABLE_STATE_SPEC: PluginSpec[StateStore] = PluginSpec(
    "table",
    "table",
    "state_stores",
    API_VERSION,
    _table_state,
    TableStateOptions,
    capabilities=StateStoreCapabilities(shared=True),
    description="State in one Delta table, shared by every process that can reach it.",
)
