"""Configuration models for pipeloom.yml, model files and `_defaults.yml`.

Every model is frozen and forbids unknown fields. Attribute docstrings become the descriptions
in the exported JSON Schema.
"""

from __future__ import annotations

import json
import keyword
from typing import Annotated, Any, Literal

from pydantic import (
    BaseModel,
    ConfigDict,
    Discriminator,
    Field,
    Tag,
    field_validator,
    model_validator,
)
from pydantic_core import PydanticCustomError

from pipeloom.config.types import Duration, Identifier, RelativePosixPath
from pipeloom.engine.types import LOGICAL_TYPE_PATTERN

Materialized = Literal["table", "incremental", "view", "ephemeral", "intermediate"]
Strategy = Literal["append", "overwrite", "replace_partitions", "merge", "scd2"]
OnSchemaChange = Literal["fail", "ignore", "append_new_columns", "sync_all_columns"]
OnViolation = Literal["warn", "drop", "fail"]
SOURCE_REF_PATTERN = r"^[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)?$"


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
    deploy: PluginBlock | None = None
    """How an orchestrator deploys and runs the plan: a `renderers` plugin and its options."""


class TargetConfig(RuntimeSettings):
    """One deployment target."""

    sources: dict[Identifier, dict[str, Any]] | None = None
    """Source group overrides, deep-merged over the groups; an explicit null removes a key."""


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
    catalog: str | None = Field(default=None, min_length=1)
    """Catalog of the target relation; wins over the naming strategy's catalog."""
    sql_dialect: str | None = Field(default=None, min_length=1)
    """sqlglot dialect of the model's `ctx.sql` queries; translated when it differs from the
    engine's (needs the sqlglot extra)."""
    meta: dict[str, Any] | None = None
    """Free-form metadata."""

    @field_validator("sql_dialect")
    @classmethod
    def _known_dialect(cls, value: str | None) -> str | None:
        """A sqlglot dialect name when sqlglot is installed; any string otherwise (PLC513 then
        reports a translation it cannot do)."""
        if value is None:
            return value
        try:
            from sqlglot.dialects.dialect import Dialects
        except ImportError:
            return value
        known = sorted(d.value for d in Dialects if d.value)
        if value not in known:
            raise ValueError(f"unknown sql_dialect '{value}'; sqlglot knows {', '.join(known)}")
        return value


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
    catalog: str | None = Field(default=None, min_length=1)
    """Catalog of the target relation; wins over the naming strategy's catalog."""
    sql_dialect: str | None = None
    """Kept out of the checksum and the manifest while None, so older plans keep their hash."""
    meta: dict[str, Any] = Field(default_factory=dict)
    description: str | None = Field(default=None, exclude=True)
    """What the model holds. Excluded from dumps, so it stays out of checksums and the manifest."""


class FolderSettings(ModelSettings):
    """One folder of the `models:` tree in pipeloom.yml: settings written as `+key`, and
    sub-folders written as plain keys."""

    model_config = ConfigDict(alias_generator=lambda name: "+" + name)
    schema_: str | None = Field(default=None, alias="+schema")
    """Schema (namespace) of the target relation."""
    folders: dict[str, FolderSettings] = Field(default_factory=dict, alias="folders")
    """Sub-folders by name. YAML writes them as plain keys; validation gathers them here."""

    @model_validator(mode="before")
    @classmethod
    def _gather_folders(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        out = {k: v for k, v in data.items() if str(k).startswith("+")}
        folders = {k: v for k, v in data.items() if not str(k).startswith("+")}
        return {**out, "folders": folders} if folders else out


class InputConfig(_Model):
    ref: Identifier | None = None
    """Name of another model."""
    source: str | None = Field(default=None, pattern=SOURCE_REF_PATTERN)
    """Name of a declared source, or `group.table` for a table of a source group."""

    @model_validator(mode="after")
    def _ref_or_source(self) -> InputConfig:
        _exactly_one(self, ("ref", "source"))
        return self


def _unique_columns(value: list[ColumnConfig] | None) -> list[ColumnConfig] | None:
    names = [c.name for c in value or ()]
    dupes = sorted({n for n in names if names.count(n) > 1})
    if dupes:
        raise ValueError(f"duplicate column name(s) {', '.join(dupes)}")
    return value


class ColumnConfig(_Model):
    """One column of a model contract."""

    name: str = Field(min_length=1)
    """Column name, case-sensitive."""
    dtype: str = Field(pattern=LOGICAL_TYPE_PATTERN)
    """Logical type, for example int64, string, decimal(18,2) or timestamp[us, UTC]."""
    nullable: bool = True
    """False makes a null in the column a contract violation."""
    description: str | None = Field(default=None, exclude=True)
    """What the column holds. Not part of the checksum or the plan hash."""


class RelationshipsCheck(_Model):
    """Every non-null `column` value of the model must exist in `field` of `to`."""

    column: str
    """Column of the model that holds the key."""
    to: InputConfig
    """The parent: another model (ref) or a source."""
    field: str
    """Column of the parent the key must match."""


class DataCheckConfig(_Model):
    not_null: list[str] | None = None
    unique: list[str] | None = None
    relationships: RelationshipsCheck | None = None

    @model_validator(mode="after")
    def _one_check(self) -> DataCheckConfig:
        _exactly_one(self, tuple(type(self).model_fields))
        return self


class DataTestConfig(_Model):
    """A check run against the whole written table after the write (ADR-0045)."""

    name: Identifier
    """Name shown in results."""
    check: DataCheckConfig
    """The check."""
    on_violation: Literal["warn", "fail"] = "fail"
    """warn records the count; fail fails the unit with PLQ303 (the data stays written)."""


class GivenConfig(_Model):
    """Rows for a unit test: inline or from a CSV file."""

    rows: list[dict[str, Any]] | None = None
    """The rows, one mapping per row."""
    csv: RelativePosixPath | None = None
    """A CSV file with a header, relative to the project dir; an empty field is null."""
    columns: list[ColumnConfig] | None = None
    """Logical types of the columns; others are taken from the upstream contract or inferred."""

    @model_validator(mode="after")
    def _rows_or_csv(self) -> GivenConfig:
        _exactly_one(self, ("rows", "csv"))
        return self


class UnitTestConfig(_Model):
    """A unit test: the model body run on given input rows, compared with expected rows."""

    name: Identifier
    """Name of the case, unique per model."""
    given: dict[Identifier, GivenConfig] = Field(default_factory=dict)
    """Rows for every input alias of the model."""
    expect: GivenConfig
    """Expected output rows; only their columns are compared, in any order."""


class FileUnitTestConfig(UnitTestConfig):
    """A unit test in a file's top-level `unit_tests` list."""

    model: Identifier
    """The model the case tests."""


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
    columns: list[ColumnConfig] | None = None
    """Model contract: the exact output columns, checked before every write (PLM208)."""
    unit_tests: list[UnitTestConfig] | None = None
    """Cases `pipeloom test` runs on given rows (ADR-0045)."""
    data_tests: list[DataTestConfig] | None = None
    """Checks `pipeloom build` runs against the written table (ADR-0045)."""

    @model_validator(mode="after")
    def _sql_or_python(self) -> ModelConfig:
        _exactly_one(self, ("sql", "python"))
        return self

    @field_validator("columns")
    @classmethod
    def _unique_columns(cls, value: list[ColumnConfig] | None) -> list[ColumnConfig] | None:
        return _unique_columns(value)

    @field_validator("inputs")
    @classmethod
    def _alias_names(cls, value: dict[str, InputConfig]) -> dict[str, InputConfig]:
        for alias in value:
            if alias == "ctx" or keyword.iskeyword(alias):
                raise ValueError(f"'{alias}' cannot be an input alias")
        return value


class DecoratorArgs(ModelSettings):
    """The arguments of `@model`, as plain data (ADR-0050 point 1)."""

    name: Identifier | None = None
    description: str | None = None
    inputs: dict[Identifier, InputConfig | Identifier] = Field(default_factory=dict)
    columns: list[ColumnConfig] | None = None
    data_tests: list[DataTestConfig] | None = None

    @field_validator("name")
    @classmethod
    def _not_ctx(cls, value: str | None) -> str | None:
        if value == "ctx":
            raise ValueError("'ctx' is reserved for the model context")
        return value

    @field_validator("meta")
    @classmethod
    def _json_meta(cls, value: dict[str, Any] | None) -> dict[str, Any] | None:
        """In YAML every value is JSON; a Python value may not be (ADR-0050 point 10)."""
        try:
            json.dumps(value, allow_nan=False)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"meta values must be JSON: {exc}") from None
        return value

    @field_validator("columns")
    @classmethod
    def _unique_columns(cls, value: list[ColumnConfig] | None) -> list[ColumnConfig] | None:
        return _unique_columns(value)

    @field_validator("inputs")
    @classmethod
    def _alias_names(cls, value: dict[str, InputConfig | str]) -> dict[str, InputConfig | str]:
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


class SourceTableConfig(_Model):
    """One table of a source group. Keys it leaves out come from the group."""

    description: str | None = None
    """What the table holds."""
    location: str | None = None
    """Path joined to the group location."""
    table: str | None = None
    """Table name, qualified by the group schema. Without location or table, the key is used."""
    schema_: str | None = Field(default=None, alias="schema")
    """Schema that qualifies the table name; replaces the group schema."""
    format: str | None = None
    """File or table format; replaces the group format."""
    options: dict[str, Any] | None = None
    """Reader options, merged over the group options."""
    tags: list[str] | None = None
    """Tags added to the group tags."""
    meta: dict[str, Any] | None = None
    """Free-form metadata, merged over the group meta."""

    @model_validator(mode="after")
    def _not_both(self) -> SourceTableConfig:
        if self.location is not None and self.table is not None:
            raise PydanticCustomError(
                "pipeloom_exclusive", "at most one of location, table is allowed"
            )
        return self


class SourceGroupConfig(_Model):
    """A group of source tables that share a format, a location or a schema."""

    description: str | None = None
    """What the group holds."""
    format: str | None = None
    """File or table format of its tables."""
    location: str | None = None
    """Base path or URI that table locations are joined to."""
    schema_: str | None = Field(default=None, alias="schema")
    """Schema that qualifies table names."""
    options: dict[str, Any] = Field(default_factory=dict)
    """Reader options."""
    tags: list[str] = Field(default_factory=list)
    """Tags used by selectors."""
    meta: dict[str, Any] = Field(default_factory=dict)
    """Free-form metadata."""
    tables: dict[Identifier, SourceTableConfig | None] = Field(default_factory=dict)
    """Tables by name. An empty entry reads the table named by its key."""


class PropertiesEntry(_Model):
    """A properties entry: the description, settings and contract of a model defined elsewhere."""

    name: Identifier
    """Name of the model it describes."""
    description: str | None = None
    """What the model holds."""
    config: ModelSettings | None = None
    """Model settings, below the model's own and above the `models:` tree."""
    columns: list[ColumnConfig] | None = None
    """Model contract, used when the model declares none itself."""
    data_tests: list[DataTestConfig] | None = None
    """Checks run against the written table, used when the model sets none itself (ADR-0045)."""
    unit_tests: list[UnitTestConfig] | None = None
    """Cases `pipeloom test` runs on given rows, keyed by input alias (ADR-0045)."""

    @field_validator("columns")
    @classmethod
    def _unique_columns(cls, value: list[ColumnConfig] | None) -> list[ColumnConfig] | None:
        return _unique_columns(value)


def _entry_kind(value: Any) -> str:
    """An entry with `sql` or `python` (or one that is not a mapping) is a model."""
    if isinstance(value, dict) and "sql" not in value and "python" not in value:
        return "properties"
    return "model"


ModelFileEntry = Annotated[
    Annotated[ModelConfig, Tag("model")] | Annotated[PropertiesEntry, Tag("properties")],
    Discriminator(_entry_kind),
]


class ModelFile(_Model):
    """A `*.yml` file under model_paths."""

    sources: list[SourceConfig] = Field(default_factory=list)
    models: list[ModelFileEntry] = Field(default_factory=list)
    """Models (entries with `sql` or `python`) and properties entries (all others)."""
    unit_tests: list[FileUnitTestConfig] = Field(default_factory=list)
    """Unit tests of models defined in any file."""


class PropertiesFile(_Model):
    """A properties file under model_paths."""

    models: list[PropertiesEntry] = Field(default_factory=list)
    """Properties entries, one per model."""


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
    model_packages: list[Annotated[str, Field(pattern=r"^[A-Za-z_]\w*(\.[A-Za-z_]\w*)*$")]] = Field(
        default_factory=list
    )
    """Installed packages whose modules hold models, by dotted name."""
    vars: dict[str, Any] = Field(default_factory=dict)
    """Variables for ${var:name}."""
    defaults: ProjectDefaults = Field(default_factory=ProjectDefaults)
    """Project-wide defaults."""
    models: FolderSettings = Field(default_factory=FolderSettings)
    """Model settings by folder: `+key` settings apply to every model below that folder, and
    plain keys name sub-folders of model_paths. Root `+key` settings are project defaults."""
    sources: dict[Identifier, SourceGroupConfig] = Field(default_factory=dict)
    """Source groups by name."""
    targets: dict[Identifier, TargetConfig] = Field(min_length=1)
    """Deployment targets."""
    default_target: Identifier | None = None
    """Target used when none is selected."""
    ephemeral_fanout_warn: int = 3
    """Warn when an ephemeral model feeds more units than this."""
    events: EventsConfig = Field(default_factory=EventsConfig)
    """Event sinks."""
