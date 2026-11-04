"""The plugin specs. They import no Spark library: the engine and catalog modules load on use."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Annotated, Any, Literal

from pydantic import AfterValidator, Field, ValidationInfo

from pipeloom.config.types import resolve_project_path
from pipeloom.engine.protocols import Catalog, Engine
from pipeloom.engine.types import CatalogCapabilities, EngineCapabilities
from pipeloom.plugins import API_VERSION, OptionsModel, PluginSpec
from pipeloom_spark.sqltext import has_scheme

CAPABILITIES = EngineCapabilities(
    formats=frozenset({"delta", "parquet", "csv", "json", "ndjson"}),
    strategies=frozenset(),  # the Delta strategies arrive with the write path
    materializations=frozenset({"table", "ephemeral"}),
    sql_models=True,
    views=False,
    idempotent_writes=False,
    cancel=True,
    bytes_metrics=False,
    portable=True,
    quality=False,
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
