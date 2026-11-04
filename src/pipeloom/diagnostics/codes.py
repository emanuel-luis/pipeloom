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
)

# Reserved: in the catalog, not emitted yet. (code, severity, title, since)
_RESERVED: Final = (
    ("PLP401", E, "merge or scd2 without unique_key", "0.1"),
    ("PLP402", E, "incremental without column", "0.1"),
    ("PLP403", E, "Aggregate expectation with drop", "0.1"),
    ("PLP404", E, "incremental.input not in inputs", "0.1"),
    ("PLP405", E, "Write option not applied by this strategy", "0.1"),
    ("PLP406", W, "Ephemeral fan-out above threshold", "0.1"),
    ("PLP407", E, "replace_partitions without partition_by", "0.1"),
    ("PLP408", W, "Ephemeral model with no consumer", "0.1"),
    ("PLC501", E, "Strategy unsupported by engine", "0.1"),
    ("PLC502", E, "SQL model on an engine without SQL", "0.1"),
    ("PLC503", E, "boundary task with a non-shared state store", "0.2"),
    ("PLC504", E, "Catalog incompatible with engine", "0.1"),
    ("PLC505", E, "Non-idempotent append with retries", "0.1"),
    ("PLC506", E, "Source format unsupported", "0.1"),
    ("PLC507", E, "View without views support", "0.1"),
    ("PLC508", E, "frame_api narwhals without portable engine", "0.1"),
    ("PLK601", E, "Callable file or function not found", "0.1"),
    ("PLK602", E, "Callable parameters do not match inputs", "0.1"),
    ("PLK603", W, "SQL references undeclared table", "0.2"),
    ("PLK604", W, "Name bound at module level by something other than a def", "0.1"),
    ("PLF701", E, "Engine unreachable", "0.2"),
    ("PLF702", E, "Source missing", "0.2"),
    ("PLF703", E, "Target schema drift detected", "0.2"),
    ("PLE401", W, "Sink disabled", "0.1"),
    ("PLX101", E, "Model raised", "0.3"),
    ("PLX102", E, "Timeout", "0.3"),
    ("PLX103", E, "Plan hash mismatch", "0.3"),
    ("PLX104", E, "Cancelled", "0.3"),
    ("PLM201", E, "Write failed", "0.3"),
    ("PLM202", E, "Schema change rejected", "0.3"),
    ("PLM203", E, "Watermark CAS conflict", "0.3"),
    ("PLQ301", E, "Expectation failed", "0.3"),
    ("PLD001", E, "Task limit exceeded", "0.4"),
    ("PLD002", E, "Submit above 100 tasks", "0.4"),
    ("PLA001", E, "Unsupported execution mode", "0.4"),
)


def _build() -> Mapping[str, CodeInfo]:
    out: dict[str, CodeInfo] = {}
    for code, sev, title, expl in _ACTIVE:
        out[code] = CodeInfo(code, _PREFIX_LAYER[code[:3]], sev, title, expl, "active", "0.1")
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
