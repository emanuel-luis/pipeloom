"""The plugin spec. It imports no Databricks library and no engine."""

from __future__ import annotations

from typing import Any

from pydantic import Field

from pipeloom.engine.protocols import Catalog, SecretProvider
from pipeloom.engine.types import CatalogCapabilities
from pipeloom.plugins import API_VERSION, OptionsModel, PluginSpec
from pipeloom_databricks.options import DatabricksDeployOptions


class UnityOptions(OptionsModel):
    catalog: str = Field(min_length=1)
    """Unity Catalog catalog used when a target's naming leaves the catalog empty.

    Never empty: an empty catalog would give two-part names that resolve against the
    session's current catalog."""


class SecretsOptions(OptionsModel):
    """No options: the connection comes from the environment, never from a chosen profile."""


def _unity(opts: Any) -> Catalog:
    from pipeloom_databricks.unity import UnityCatalog

    return UnityCatalog(opts.catalog)


def _secrets(_opts: Any) -> SecretProvider:
    from pipeloom_databricks.secrets import DatabricksSecretProvider

    return DatabricksSecretProvider()


def _options(opts: Any) -> DatabricksDeployOptions:
    # The core binds and records the options but never builds a renderer (ADR-0034).
    assert isinstance(opts, DatabricksDeployOptions)
    return opts


RENDERER_SPEC: PluginSpec[DatabricksDeployOptions] = PluginSpec(
    "databricks",
    "databricks",
    "renderers",
    API_VERSION,
    _options,
    DatabricksDeployOptions,
    description="Render a plan as Databricks Jobs settings JSON and bundle YAML.",
)

UNITY_SPEC: PluginSpec[Catalog] = PluginSpec(
    "unity",
    "unity",
    "catalogs",
    API_VERSION,
    _unity,
    UnityOptions,
    compatible_engines=frozenset({"spark"}),
    capabilities=CatalogCapabilities(views=True),
    description="Unity Catalog tables and views as three-part names through the Spark session.",
)

SECRETS_SPEC: PluginSpec[SecretProvider] = PluginSpec(
    "databricks",
    "databricks",
    "secret_providers",
    API_VERSION,
    _secrets,
    SecretsOptions,
    description="Reads Databricks secret scopes: ${secret:databricks/<scope>/<key>}.",
)
