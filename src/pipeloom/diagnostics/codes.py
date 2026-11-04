"""The diagnostic code catalog. Codes are never reused."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from types import MappingProxyType
from typing import Final, Literal

from pipeloom.diagnostics import Severity


class Layer(StrEnum):
    L0 = "L0 parse"
    L1 = "L1 schema"
    L2 = "L2 references"
    L3 = "L3 graph"
    L4 = "L4 plan semantics"
    L5 = "L5 capability"
    L6 = "L6 static code"
    L7 = "L7 preflight"
    PLUGINS = "Plugins"
    EVENTS = "Events"
    RUNTIME = "Runtime"
    RENDERER = "Renderers"


@dataclass(frozen=True, slots=True)
class CodeInfo:
    code: str
    layer: Layer
    severity: Severity
    title: str
    explanation: str
    status: Literal["active", "reserved"]
    since: str


_PREFIX_LAYER: Final[Mapping[str, Layer]] = {
    "PLY": Layer.L0,
    "PLS": Layer.L1,
    "PLR": Layer.L2,
    "PLG": Layer.L3,
    "PLP": Layer.L4,
    "PLC": Layer.L5,
    "PLK": Layer.L6,
    "PLF": Layer.L7,
    "PLU": Layer.PLUGINS,
    "PLE": Layer.EVENTS,
    "PLX": Layer.RUNTIME,
    "PLM": Layer.RUNTIME,
    "PLQ": Layer.RUNTIME,
    "PLD": Layer.RENDERER,
    "PLA": Layer.RENDERER,
}

E, W = Severity.ERROR, Severity.WARNING

# (code, severity, title, explanation); active in 0.1
_ACTIVE: Final = (
    (
        "PLY001",
        E,
        "YAML syntax error",
        (
            "The file is not valid YAML, holds more than one document, contains a recursive alias, "
            "expands an alias above 100000 nodes, or uses an unsupported construct."
        ),
    ),
    (
        "PLY002",
        E,
        "Duplicate key",
        (
            "A mapping repeats a key. Keys that arrive through a merge key are exempt: "
            "explicit keys win."
        ),
    ),
    (
        "PLY003",
        E,
        "Invalid encoding",
        "The file is not valid UTF-8 (a UTF-8 BOM is accepted) or has characters YAML forbids.",
    ),
    (
        "PLY004",
        E,
        "File missing or unreadable",
        "pipeloom.yml or a model file does not exist or cannot be read.",
    ),
    (
        "PLS101",
        E,
        "Unknown field",
        "A key is not part of the schema. The message suggests the closest known field.",
    ),
    ("PLS102", E, "Invalid value", "A value has the wrong type or fails a constraint."),
    (
        "PLS103",
        E,
        "Unsupported config_version",
        "config_version is missing or not 1. It is checked first and stops schema validation.",
    ),
    (
        "PLS104",
        E,
        "Literal value in a secret-like field",
        "A field whose name suggests a secret holds a literal. Use ${secret:...} or ${env:...}.",
    ),
    ("PLS105", E, "Required field missing", "A mapping lacks a required field."),
    (
        "PLS106",
        E,
        "Unresolved interpolation",
        "${env:NAME} is unset with no default, or ${var:name} names an unknown variable.",
    ),
    ("PLS107", E, "Malformed interpolation", "A ${env:...} or ${var:...} expression is malformed."),
    (
        "PLS108",
        E,
        "Exactly one of several fields required",
        "Exactly one of a set of fields must be given, for example sql or python.",
    ),
    (
        "PLR206",
        W,
        "model_paths entry does not exist",
        "A directory listed in model_paths is missing, so no models are read from it.",
    ),
    (
        "PLR204",
        E,
        "Unknown target or plugin kind",
        (
            "The selected target is not in targets, or an engine, catalog, naming or sink block "
            "names a kind that no installed plugin provides."
        ),
    ),
    (
        "PLR207",
        E,
        "No target selected",
        (
            "Several targets exist and none was chosen. Use --target, PIPELOOM_TARGET or "
            "default_target."
        ),
    ),
    (
        "PLR201",
        E,
        "Unknown ref",
        "An input names a model that does not exist. The hint suggests the closest name.",
    ),
    ("PLR202", E, "Duplicate model name", "Two models share a name. Names are unique per project."),
    (
        "PLR203",
        E,
        "Unknown source",
        "An input names a source that no model file declares.",
    ),
    ("PLR205", E, "Duplicate source", "Two sources share a name. Names are unique per project."),
    (
        "PLG301",
        E,
        "Cycle in the graph",
        "Models depend on each other in a loop. The message lists the path.",
    ),
    ("PLG302", E, "Self-reference", "A model lists itself among its own inputs."),
    (
        "PLG303",
        W,
        "Selector matched nothing",
        "A selector atom matched no model. This is a warning, not an error.",
    ),
    (
        "PLG304",
        E,
        "Unknown selector method",
        "A selector uses a method other than name, tag, path or materialized. Exit code 2.",
    ),
    (
        "PLG305",
        E,
        "Malformed selector",
        "A selector cannot be parsed, for example an empty atom or a depth of 0. Exit code 2.",
    ),
    (
        "PLU001",
        E,
        "Plugin API mismatch",
        (
            "A plugin was built for another plugin API version. While the major version is 0 "
            "the versions must match exactly."
        ),
    ),
    (
        "PLU002",
        E,
        "Duplicate plugin kind",
        "Two installed plugins provide the same kind in one group, and a config names it.",
    ),
    (
        "PLU003",
        E,
        "Plugin failed to load",
        (
            "A plugin module cannot be imported, does not resolve to a PluginSpec, or its group "
            "or kind differs from the entry point that registered it."
        ),
    ),
    (
        "PLE401",
        W,
        "Sink disabled",
        (
            "An event sink failed failure_budget times in a row and was disabled. "
            "The other sinks keep receiving events and get a sink.disabled event."
        ),
    ),
    (
        "PLP401",
        E,
        "merge or scd2 without unique_key",
        "A model writes with merge or scd2 but sets no write.unique_key.",
    ),
    (
        "PLP402",
        E,
        "incremental without column",
        "A model is materialized as incremental but sets no incremental.column to compare with the watermark.",
    ),
    (
        "PLP403",
        E,
        "Aggregate expectation with drop",
        "An aggregate check (unique, row_count, freshness) cannot drop rows. Use warn or fail.",
    ),
    (
        "PLP404",
        E,
        "incremental.input not in inputs",
        "incremental.input names an alias that is not one of the model's inputs.",
    ),
    (
        "PLP405",
        E,
        "Write option not applied by this strategy",
        "A declared write option would be ignored: unique_key, update_condition or deletes with a strategy that does not use them, tracked_columns outside scd2, write on an ephemeral model, or incremental on a model that is not incremental.",
    ),
    (
        "PLP406",
        W,
        "Ephemeral fan-out above threshold",
        "An ephemeral model is inlined in more units than ephemeral_fanout_warn. Make it an intermediate to compute it once.",
    ),
    (
        "PLP407",
        E,
        "replace_partitions without partition_by",
        "The replace_partitions strategy needs partition_by to know which partitions to replace.",
    ),
    (
        "PLP408",
        W,
        "Ephemeral model with no consumer",
        "No persisted model reads this ephemeral model, directly or through other ephemeral models, so it never runs.",
    ),
    (
        "PLC501",
        E,
        "Strategy unsupported by engine",
        "The write strategy is not in the selected engine's capabilities.",
    ),
    (
        "PLC502",
        E,
        "SQL model on an engine without SQL",
        "A SQL model targets an engine that cannot run SQL.",
    ),
    (
        "PLC503",
        E,
        "boundary task with a non-shared state store",
        "Units run as separate tasks, but the state store is local to one process or machine.",
    ),
    (
        "PLC504",
        E,
        "Catalog incompatible with engine",
        "The catalog plugin does not list the selected engine in its compatible engines.",
    ),
    (
        "PLC505",
        E,
        "Non-idempotent append with retries",
        "An append model retries on an engine without idempotent writes, which could write the same rows twice.",
    ),
    (
        "PLC506",
        E,
        "Source format unsupported",
        "A source declares a format the selected engine cannot read.",
    ),
    (
        "PLC507",
        E,
        "View without views support",
        "A model is materialized as a view on an engine without catalog views.",
    ),
    (
        "PLC508",
        E,
        "frame_api narwhals without portable engine",
        "A Python model asks for Narwhals frames on an engine whose frames are not portable.",
    ),
    (
        "PLK601",
        E,
        "Callable file or function not found",
        "The Python module is not under python_paths, cannot be parsed, or does not define the function.",
    ),
    (
        "PLK602",
        E,
        "Callable parameters do not match inputs",
        "The function's parameters must be ctx followed by one parameter per input alias (any alias is accepted with **kwargs).",
    ),
    (
        "PLK604",
        W,
        "Name bound at module level by something other than a def",
        "The callable's name is bound by an assignment, import or class, so its parameters cannot be checked.",
    ),
)

# Active since 0.2.
_ACTIVE_02: Final = (
    (
        "PLX101",
        E,
        "Model raised",
        "A model's body raised, or returned a value its engine cannot use as a relation.",
    ),
    (
        "PLM201",
        E,
        "Write failed",
        "The engine failed to write a model's result or to create its view.",
    ),
    (
        "PLM204",
        E,
        "Input read failed",
        "An input of a model could not be read from its source or from the catalog.",
    ),
    (
        "PLC509",
        E,
        "Materialization unsupported by engine",
        "The materialization is not in the selected engine's capabilities. No engine offers intermediate yet.",
    ),
    (
        "PLP409",
        E,
        "View is not a plain SQL model",
        "A view must be a SQL model that reads no ephemeral model, because its query is stored in the catalog as written.",
    ),
    (
        "PLX103",
        E,
        "Plan hash mismatch",
        "The plan hash of the manifest or of the compiled project differs from --expect-plan-hash.",
    ),
    (
        "PLX105",
        E,
        "Run lock held",
        "Another run holds the lock of this project and target. The message names the holder, its run and when its lease expires.",
    ),
    (
        "PLX106",
        E,
        "Unit process exited without a result",
        "A unit started under boundary task exited without writing its attempt file. The excerpt holds the last lines of its log.",
    ),
    (
        "PLX107",
        E,
        "Manifest invalid",
        "The manifest cannot be read, has another schema, was edited after it was written, or belongs to another project or target.",
    ),
    (
        "PLG306",
        E,
        "Unknown unit",
        "A unit: selector names a unit that the project does not have. Ephemeral models are not units.",
    ),
    (
        "PLK603",
        W,
        "SQL references undeclared table",
        "A model's SQL reads a table that is neither an input alias nor a CTE. Checked only when sqlglot is installed (extra: sqlglot).",
    ),
    (
        "PLK605",
        W,
        "SQL not parseable",
        "sqlglot could not parse the model's SQL, so its tables were not checked. Checked only when sqlglot is installed.",
    ),
)

# Active since 0.3.
_ACTIVE_03: Final = (
    (
        "PLX102",
        E,
        "Timeout",
        "A unit attempt ran longer than its timeout. A timed-out unit is not retried and its descendants are skipped.",
    ),
    (
        "PLM202",
        E,
        "Schema change rejected",
        "The batch schema differs from the existing target and the model's on_schema_change policy does not allow it.",
    ),
    (
        "PLM206",
        E,
        "Write key invalid",
        "A unique_key or partition_by column is missing from the batch, or a merge batch repeats a unique_key.",
    ),
    (
        "PLM203",
        E,
        "Watermark CAS conflict",
        "The stored watermark changed while the unit ran, so the new one was not saved. The data was already written. Another run on the same target is the usual cause.",
    ),
    (
        "PLM205",
        E,
        "Incremental watermark unusable",
        "The incremental column is missing from the input, its type differs from the stored watermark, or a lookback is set on a column that is not a timestamp or date.",
    ),
    (
        "PLP410",
        W,
        "append with lookback reloads rows",
        "An incremental model appends with a lookback above zero, so rows inside the window are written again. Use merge with a unique_key.",
    ),
    (
        "PLQ301",
        E,
        "Expectation failed",
        "An expectation with on_violation: fail found violating rows, or the share of dropped rows exceeded quality.max_drop_ratio. The unit fails before writing; the counts are still recorded in run_results.",
    ),
    (
        "PLQ302",
        E,
        "Expectation references an unknown column",
        "An expectation names a column the model output lacks, or its expr cannot be evaluated by the engine.",
    ),
    (
        "PLP411",
        E,
        "Expectations on a view or ephemeral model",
        "A view or an ephemeral model writes no rows of its own, so it cannot carry expectations. Put them on a table that reads it.",
    ),
    (
        "PLP412",
        E,
        "Quarantine relation collides with a model",
        "A model's quarantine table is named <model>__quarantine, and another model already has that name in the same schema.",
    ),
    (
        "PLC510",
        E,
        "Expectations on an engine without quality",
        "The selected engine does not declare the quality capability, so it cannot evaluate expectations.",
    ),
)


# Reserved: in the catalog, not emitted yet. (code, severity, title, since)
_RESERVED: Final = (
    ("PLF701", E, "Engine unreachable", "0.2"),
    ("PLF702", E, "Source missing", "0.2"),
    ("PLF703", E, "Target schema drift detected", "0.2"),
    ("PLX104", E, "Cancelled", "0.3"),
    ("PLD001", E, "Task limit exceeded", "0.4"),
    ("PLD002", E, "Submit above 100 tasks", "0.4"),
    ("PLA001", E, "Unsupported execution mode", "0.4"),
)


def _build() -> Mapping[str, CodeInfo]:
    out: dict[str, CodeInfo] = {}
    for since, table in (("0.1", _ACTIVE), ("0.2", _ACTIVE_02), ("0.3", _ACTIVE_03)):
        for code, sev, title, expl in table:
            out[code] = CodeInfo(code, _PREFIX_LAYER[code[:3]], sev, title, expl, "active", since)
    for code, sev, title, since in _RESERVED:
        out[code] = CodeInfo(
            code,
            _PREFIX_LAYER[code[:3]],
            sev,
            title,
            "Reserved: not emitted yet.",
            "reserved",
            since,
        )
    return MappingProxyType(dict(sorted(out.items())))


CATALOG: Final[Mapping[str, CodeInfo]] = _build()


def info(code: str) -> CodeInfo:
    """Return the catalog entry for a code; raise KeyError for an unknown one."""
    return CATALOG[code]


def render_markdown() -> str:
    """Render the catalog as a docs page: one section per code, grouped by layer."""
    lines = ["# Error codes", ""]
    for layer in Layer:
        entries = [c for c in CATALOG.values() if c.layer is layer]
        if not entries:
            continue
        lines += [f"## {layer.value}", ""]
        for c in entries:
            lines += [
                f"### {c.code} {{#{c.code.lower()}}}",
                "",
                f"{c.title}. Severity: {c.severity.value}. Status: {c.status}. Since: {c.since}.",
                "",
                c.explanation,
                "",
            ]
    return "\n".join(lines)
