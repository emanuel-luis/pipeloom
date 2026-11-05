"""The plugin spec. It imports no Databricks library and no engine."""

from __future__ import annotations

from typing import Any

from pipeloom.plugins import API_VERSION, PluginSpec
from pipeloom_databricks.options import DatabricksDeployOptions


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
