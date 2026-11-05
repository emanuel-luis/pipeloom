"""The options of the `databricks` deploy block. No Databricks import."""

from __future__ import annotations

from typing import Annotated, Literal

from pydantic import Field, JsonValue

from pipeloom.config.types import Identifier
from pipeloom.plugins import OptionsModel


class ServerlessCompute(OptionsModel):
    kind: Literal["serverless"] = "serverless"
    environment_version: str = "5"
    """Serverless environment version, as a string. Version 1 runs Python 3.10, below the floor."""
    dependencies: list[str] | None = None
    """pip requirement lines. Unset installs the pinned pipeloom distributions."""


class JobClusterCompute(OptionsModel):
    kind: Literal["job_cluster"]
    new_cluster: dict[str, JsonValue]
    """The `new_cluster` object of the Jobs API."""
    libraries: list[dict[str, JsonValue]] | None = None
    """Task libraries. Unset installs the pinned pipeloom distributions from PyPI."""


Compute = Annotated[ServerlessCompute | JobClusterCompute, Field(discriminator="kind")]


class ComputeRule(OptionsModel):
    select: list[str]
    """Selectors; the rule applies to the models they match."""
    exclude: list[str] = Field(default_factory=list)
    compute: Identifier
    """Key of an entry in `compute`."""


class PipelineOptions(OptionsModel):
    select: list[str] = Field(default_factory=list)
    """Selectors; empty means the whole plan."""
    exclude: list[str] = Field(default_factory=list)


def _main() -> dict[str, PipelineOptions]:
    return {"main": PipelineOptions()}


def _default() -> dict[str, Compute]:
    return {"default": ServerlessCompute()}


class DatabricksDeployOptions(OptionsModel):
    pipelines: dict[Identifier, PipelineOptions] = Field(default_factory=_main)
    """One job per pipeline."""
    compute: dict[Identifier, Compute] = Field(default_factory=_default)
    default_compute: Identifier = "default"
    compute_rules: list[ComputeRule] = Field(default_factory=list)
    """The first rule that matches a model wins."""
    task_grouping: Literal["none", "by_tag", "by_folder"] = "none"
    """none: one task per unit. by_tag: by the model's first tag. by_folder: by its folder."""
    workspace_root: str | None = None
    """Where a direct deploy uploads the project. Default /Workspace/Users/<me>/.pipeloom."""
    settings: dict[str, JsonValue] = Field(default_factory=dict)
    """Job settings passed through (schedule, notifications, run_as, tags ...)."""
