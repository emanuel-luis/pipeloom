"""Unity Catalog: `catalog.schema.name` tables and views through the Spark session."""

from __future__ import annotations

from pipeloom.engine.types import CatalogCapabilities, Namespace
from pipeloom_spark.catalog import SparkSessionCatalog


class UnityCatalog(SparkSessionCatalog):
    """Always three-part, backticked identifiers. A target whose naming left the catalog empty
    gets the catalog from the options."""

    capabilities = CatalogCapabilities(views=True)

    def __init__(self, catalog: str) -> None:
        super().__init__(catalog)

    def ensure_namespace(self, ns: Namespace) -> None:
        # CREATE SCHEMA IF NOT EXISTS needs CREATE SCHEMA on the catalog even when the schema
        # exists, so a principal that only holds USE SCHEMA must not run it.
        if self.spark.catalog.databaseExists(self._parts(ns.catalog, ns.schema)):
            return
        super().ensure_namespace(ns)
