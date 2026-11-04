"""Configuration models for pipeloom.yml, model files and `_defaults.yml`.

Every model is frozen and forbids unknown fields. Attribute docstrings become the descriptions
in the exported JSON Schema.
"""

from __future__ import annotations

import keyword
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
from pydantic_core import PydanticCustomError

from pipeloom.config.types import Duration, Identifier, RelativePosixPath

Materialized = Literal["table", "incremental", "view", "ephemeral", "intermediate"]
Strategy = Literal["append", "overwrite", "replace_partitions", "merge", "scd2"]
OnSchemaChange = Literal["fail", "ignore", "append_new_columns", "sync_all_columns"]
OnViolation = Literal["warn", "drop", "fail"]


class _Model(BaseModel):
    model_config = ConfigDict(
        frozen=True,
        extra="forbid",
        use_attribute_docstrings=True,
        protected_namespaces=(),
    )


def _exactly_one(model: BaseModel, fields: tuple[str, ...]) -> None:
    given = [f for f in fields if getattr(model, f) is not None]
    if len(given) != 1:
        raise PydanticCustomError(
            "pipeloom_exclusive",
            "exactly one of {fields} is required",
            {"fields": ", ".join(fields)},
        )


class PluginBlock(BaseModel):
    """A plugin selection: `kind` plus the options of that plugin."""

    model_config = ConfigDict(
        frozen=True, extra="allow", use_attribute_docstrings=True, protected_namespaces=()
    )

    kind: str
    """Name of the plugin."""


class SinkBlock(PluginBlock):
    """An event sink."""

    failure_budget: int = 5
    """Consecutive failures before the sink is disabled."""
    buffered: bool = False
    """Run the sink on a background thread."""
    buffer_size: int = 10_000
    """Events held by a buffered sink before the oldest is dropped."""


class ArtifactsConfig(_Model):
    root: str | None = None
    """Directory or volume path where artifacts are written."""
    intermediate_ttl: Duration = 86400
    """How long the persisted results of `intermediate` models are kept after their run ended.
    0 deletes them when a run succeeds. Default 1d."""


class RuntimeSettings(_Model):
    """Settings a target may override."""

    engine: PluginBlock | None = None
    """Engine plugin."""
    catalog: PluginBlock | None = None
    """Catalog plugin."""
    naming: PluginBlock | None = None
    """Naming strategy plugin."""
    state: PluginBlock | None = None
    """State store plugin."""
    workers: int | None = Field(default=None, ge=1)
    """Maximum concurrent units."""
    boundary: Literal["process", "task"] | None = None
    """Whether units share a process or run as separate tasks."""
    cancel_grace: Duration | None = None
    """Time a stopped or timed-out unit gets to finish before it is killed. Default 30s."""
    artifacts: ArtifactsConfig | None = None
    """Where artifacts go."""


class TargetConfig(RuntimeSettings):
    """One deployment target."""


class WriteConfig(_Model):
    strategy: Strategy | None = None
    """How rows reach the target."""
    unique_key: list[str] | None = None
    """Columns that identify a row (merge and scd2)."""
    update_condition: str | None = None
    """Extra condition for updating a matched row (merge)."""
    deletes: DeletesConfig | None = None
    """How deleted rows are recognised and applied."""
    on_schema_change: OnSchemaChange = "fail"
    """Policy when the output schema differs from the target."""
    options: dict[str, Any] = Field(default_factory=dict)
    """Engine-specific write options."""
    table_properties: dict[str, str] = Field(default_factory=dict)
    """Properties set on the target table."""
    tracked_columns: list[str] | None = None
    """Columns whose change opens a new scd2 row; default all."""


class DeletesConfig(_Model):
    column: str
    """Column that marks a deleted row."""
    value: Any = True
    """Value of the column that means deleted."""
    mode: Literal["hard", "soft"] = "hard"
    """Remove the row, or keep it flagged."""


class IncrementalConfig(_Model):
    input: Identifier
    """Alias of the input that is filtered."""
    column: str | None = None
    """Column compared with the watermark."""
    lookback: Duration = 0
    """Reprocess this much before the watermark."""
    watermark_source: Literal["state", "target"] = "state"
    """Where the watermark is read from."""


class CheckConfig(_Model):
    not_null: list[str] | None = None
    accepted_values: AcceptedValuesCheck | None = None
    range: RangeCheck | None = None
    regex: RegexCheck | None = None
    unique: list[str] | None = None
    row_count: RowCountCheck | None = None
    freshness: FreshnessCheck | None = None

    @model_validator(mode="after")
    def _one_check(self) -> CheckConfig:
        _exactly_one(self, tuple(type(self).model_fields))
        return self


class AcceptedValuesCheck(_Model):
    column: str
    values: list[Any]


class RangeCheck(_Model):
    column: str
    min: float | None = None
    max: float | None = None


class RegexCheck(_Model):
    column: str
    pattern: str


class RowCountCheck(_Model):
    min: int | None = None
    max: int | None = None


class FreshnessCheck(_Model):
    column: str
    max_age: Duration


class ExpectationConfig(_Model):
    name: Identifier
    """Name shown in results."""
    check: CheckConfig | None = None
    """A portable check."""
    expr: str | None = None
    """An engine-dialect predicate."""
    on_violation: OnViolation = "fail"
    """What happens to rows or runs that violate it."""

    @model_validator(mode="after")
    def _check_or_expr(self) -> ExpectationConfig:
        _exactly_one(self, ("check", "expr"))
        return self


class QualityConfig(_Model):
    quarantine: bool = False
    """Write dropped rows to a quarantine table."""
    max_drop_ratio: float | None = Field(default=None, ge=0, le=1)
    """Fail the unit when more than this share of rows is dropped."""


class ModelSettings(_Model):
    """Settings a model may inherit from defaults."""

    materialized: Materialized | None = None
    """How the model is stored."""
    write: WriteConfig | None = None
    """Write behaviour."""
    incremental: IncrementalConfig | None = None
    """Incremental processing."""
    partition_by: list[str] | None = None
    """Partition columns."""
    cluster_by: list[str] | None = None
    """Clustering columns."""
    expectations: list[ExpectationConfig] | None = None
    """Data quality expectations."""
    quality: QualityConfig | None = None
    """Quarantine settings."""
    tags: list[str] | None = None
    """Tags used by selectors."""
    retries: int | None = Field(default=None, ge=0)
    """Retries after a failure."""
    retry_delay: Duration | None = None
    """Base delay between retries."""
    retry_on: Literal["transient", "any"] | None = None
    """Which failures are retried."""
    timeout: Duration | None = None
    """Time limit per attempt."""
    frame_api: Literal["native", "narwhals"] | None = None
    """Frame type passed to Python bodies."""
    schema_: str | None = Field(default=None, alias="schema")
    """Schema (namespace) of the target relation."""
    meta: dict[str, Any] | None = None
    """Free-form metadata."""


class ResolvedModelConfig(_Model):
    """ModelSettings with concrete defaults, after defaults cascade."""

    materialized: Materialized = "table"
    write: WriteConfig = Field(default_factory=WriteConfig)
    incremental: IncrementalConfig | None = None
    partition_by: list[str] = Field(default_factory=list)
    cluster_by: list[str] = Field(default_factory=list)
    expectations: list[ExpectationConfig] = Field(default_factory=list)
    quality: QualityConfig = Field(default_factory=QualityConfig)
    tags: list[str] = Field(default_factory=list)
    retries: int = Field(default=0, ge=0)
    retry_delay: Duration = 1
    retry_on: Literal["transient", "any"] = "transient"
    timeout: Duration | None = None
    frame_api: Literal["native", "narwhals"] = "native"
    schema_: str | None = Field(default=None, alias="schema")
    meta: dict[str, Any] = Field(default_factory=dict)


class InputConfig(_Model):
    ref: Identifier | None = None
    """Name of another model."""
    source: Identifier | None = None
    """Name of a declared source."""

    @model_validator(mode="after")
    def _ref_or_source(self) -> InputConfig:
        _exactly_one(self, ("ref", "source"))
        return self


class ModelConfig(ModelSettings):
    """One model."""

    name: Identifier
    """Unique model name."""
    description: str | None = None
    """What the model holds."""
    sql: str | None = None
    """SQL body."""
    python: str | None = Field(default=None, pattern=r"^[A-Za-z_][\w.]*:[A-Za-z_]\w*$")
    """Python body as `module:function`."""
    inputs: dict[Identifier, InputConfig] = Field(default_factory=dict)
    """Local aliases bound to refs or sources."""

    @model_validator(mode="after")
    def _sql_or_python(self) -> ModelConfig:
        _exactly_one(self, ("sql", "python"))
        return self

    @field_validator("inputs")
    @classmethod
    def _alias_names(cls, value: dict[str, InputConfig]) -> dict[str, InputConfig]:
        for alias in value:
            if alias == "ctx" or keyword.iskeyword(alias):
                raise ValueError(f"'{alias}' cannot be an input alias")
        return value


class SourceConfig(_Model):
    name: Identifier
    """Unique source name."""
    description: str | None = None
    """What the source holds."""
    format: str | None = None
    """File or table format."""
    location: str | None = None
    """Path or URI of the data."""
    table: str | None = None
    """Table name in the catalog."""
    options: dict[str, Any] = Field(default_factory=dict)
    """Reader options."""
    tags: list[str] = Field(default_factory=list)
    """Tags used by selectors."""
    meta: dict[str, Any] = Field(default_factory=dict)
    """Free-form metadata."""

    @model_validator(mode="after")
    def _location_or_table(self) -> SourceConfig:
        _exactly_one(self, ("location", "table"))
        return self


class ModelFile(_Model):
    """A `*.yml` file under model_paths."""

    sources: list[SourceConfig] = Field(default_factory=list)
    models: list[ModelConfig] = Field(default_factory=list)


class DefaultsFile(_Model):
    """A `_defaults.yml` file."""

    models: ModelSettings = Field(default_factory=ModelSettings)


class ProjectDefaults(_Model):
    models: ModelSettings = Field(default_factory=ModelSettings)
    """Defaults for every model."""


class EventsConfig(_Model):
    sinks: list[SinkBlock] = Field(default_factory=list)
    """Event sinks."""


class ProjectConfig(RuntimeSettings):
    """The pipeloom.yml file."""

    config_version: Literal[1]
    """Schema version of this file."""
    name: Identifier
    """Project name."""
    model_paths: list[RelativePosixPath] = Field(default_factory=lambda: ["models"])
    """Directories that hold model files."""
    python_paths: list[RelativePosixPath] = Field(default_factory=lambda: ["."])
    """Directories added to sys.path when models run."""
    vars: dict[str, Any] = Field(default_factory=dict)
    """Variables for ${var:name}."""
    defaults: ProjectDefaults = Field(default_factory=ProjectDefaults)
    """Project-wide defaults."""
    targets: dict[Identifier, TargetConfig] = Field(min_length=1)
    """Deployment targets."""
    default_target: Identifier | None = None
    """Target used when none is selected."""
    ephemeral_fanout_warn: int = 3
    """Warn when an ephemeral model feeds more units than this."""
    events: EventsConfig = Field(default_factory=EventsConfig)
    """Event sinks."""
