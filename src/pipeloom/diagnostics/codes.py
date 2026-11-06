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
        "A selector uses a method other than name, tag, path, materialized, unit, state or result. Exit code 2.",
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
        "The materialization is not in the selected engine's capabilities.",
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
        "PLX104",
        E,
        "Cancelled",
        "A unit was still running when the run was stopped and did not finish within the grace period, or the stop was hard. The run ends cancelled with exit code 130.",
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
        "A model's quarantine table is named <model>__quarantine, and another model already has that name in the same catalog and schema.",
    ),
    (
        "PLP413",
        E,
        "Intermediate model with a write option",
        "An intermediate model is rewritten whole at a location of its own. It cannot be incremental, use a strategy other than overwrite, or set partition_by.",
    ),
    (
        "PLC510",
        E,
        "Expectations on an engine without quality",
        "The selected engine does not declare the quality capability, so it cannot evaluate expectations or count nulls in non-nullable contract columns.",
    ),
)


# Active since 0.4.
_ACTIVE_04: Final = (
    (
        "PLX108",
        E,
        "Run start time conflict",
        "--run-started-at differs from the start the run recorded when it first ran. A reused run id keeps its recorded start. Exit code 2.",
    ),
    (
        "PLM207",
        E,
        "Write refused by the idempotency guard",
        "The engine skipped the write because this model already has a later transaction version: a newer run, an overlapping run started with --no-lock, or a host clock that ran behind. The engine refuses before it changes any data. Wait for the other run, check the clocks, or turn the guard off in the engine options.",
    ),
)


# Active since 0.5.
_ACTIVE_05: Final = (
    (
        "PLX109",
        E,
        "Secret reference not resolved",
        "A ${secret:provider/path} reference in a plugin's options could not be resolved when the command executed: the provider is unknown or failed, the resolved options are invalid, or the plugin could not be built with them. The message names the provider and path, never a value.",
    ),
    (
        "PLD001",
        E,
        "Task limit exceeded",
        "A Databricks job would hold more than 1,000 tasks, its finalize task included. Set task_grouping in the deploy block, or split the project into pipelines.",
    ),
    (
        "PLD003",
        E,
        "Task groups form a cycle",
        "With task_grouping set, the units of one group depend on units of another group in both directions, so the grouped tasks cannot be ordered. Group by another key or turn grouping off.",
    ),
    (
        "PLD004",
        E,
        "Invalid Databricks deploy options for this plan or target",
        "The deploy block cannot be rendered: an unknown pipeline or compute key, environment_version 1, more than 10 serverless environments in a job, a key in settings that pipeloom owns, a ${secret:...} reference, an artifacts root that is unset or relative, a state store that is not shared, a group that spans two computes, or an ephemeral run on a job cluster.",
    ),
    (
        "PLD005",
        W,
        "Compute rule matches no model",
        "No model of the plan matches the select selectors of a compute rule, so the rule never applies.",
    ),
    (
        "PLD006",
        E,
        "More than one job carries the pipeloom_job tag",
        "Two or more Databricks jobs hold the same pipeloom_job tag, so deploy cannot tell which one it owns. Delete or untag the extra jobs; the message lists their ids.",
    ),
    (
        "PLD007",
        E,
        "Job or run not found",
        "No Databricks job carries the pipeloom_job tag of this pipeline, or the run asked for does not exist. Deploy the job first, or pass --ensure to run so it deploys one; check the run id and the pipeline.",
    ),
    (
        "PLD008",
        E,
        "Deployed job is out of date",
        "The job in the workspace was deployed from other settings than the project renders now (its pipeloom_settings_hash tag differs). Deploy again, or pass --ensure to run so it deploys first.",
    ),
    (
        "PLD009",
        E,
        "Databricks API request failed",
        "A call to the Databricks REST API returned an error. The message carries the HTTP status, the error code and the message of the API, with registered secret values masked. Check the connection, the permissions of the caller and the workspace path.",
    ),
    (
        "PLD010",
        E,
        "No Databricks connection configured",
        "The command needs a workspace and none is configured. Pass --profile, set DATABRICKS_CONFIG_PROFILE, set DATABRICKS_HOST with its credentials, or run on a Databricks runtime. pipeloom never picks a profile by itself.",
    ),
    (
        "PLD011",
        E,
        "Task ended without a unit result",
        "A Databricks task failed before its unit wrote an attempt file, for example because the cluster was lost, so the unit has no result of its own. Finalize records the unit as failed with the termination message of the task. Read the task's log on the run page; repair reruns it.",
    ),
    (
        "PLD012",
        E,
        "Project file cannot be uploaded",
        "A project file is above 10 MB, is a notebook (a Databricks notebook header or an .ipynb file), is unreadable or lies outside the project folder, so it cannot be stored as a plain workspace file.",
    ),
    (
        "PLD013",
        E,
        "Dynamic value reference not resolved in a task parameter",
        "A parameter of a Databricks task still holds a {{...}} reference, so the platform did not substitute it. The task stops before it runs anything. Check the job's parameters and the Databricks runtime that runs the task.",
    ),
    (
        "PLD014",
        E,
        "Run is still active",
        "A repair was asked for a Databricks job run that has not ended. Wait for it to finish or cancel it, then repair.",
    ),
    (
        "PLD015",
        W,
        "Selection covers part of a task group",
        "With task_grouping set, a task runs every unit of its group, so a selection that names only some of them still runs the whole group. Select the whole group, or turn grouping off, to run exactly the units chosen.",
    ),
)


# Active since 0.6.
_ACTIVE_06: Final = (
    (
        "PLA001",
        E,
        "Unsupported execution mode",
        "The Airflow plugin was configured with an execution mode it does not support. DOCKER is not supported in 0.6; use local, virtualenv, kubernetes or databricks.",
    ),
    (
        "PLA002",
        E,
        "Invalid Airflow plugin configuration",
        "A configuration of the Airflow plugin cannot be used: the manifest load method without a manifest path, a relative path, a virtualenv mode without a directory, a kubernetes mode without an image, a pod_kwargs or operator_args key that pipeloom sets itself, an unknown asset_uri placeholder, or a full_refresh param that is not a boolean.",
    ),
    (
        "PLA003",
        W,
        "Selection renders no units",
        "The DAG's select and exclude selectors match no unit of the plan, so the DAG would hold no unit task.",
    ),
    (
        "PLA004",
        E,
        "Unit task ended without a visible unit result",
        "An Airflow task ended without an attempt file for its unit under artifacts.root. When the task succeeded, the root is not shared between the workers and the finalize task. The unit is recorded as failed, so the run fails.",
    ),
    (
        "PLA005",
        E,
        "Unsupported Airflow version",
        "The plugin supports Airflow 3.2 up to, not including, 4.0.",
    ),
    (
        "PLA006",
        W,
        "PROJECT compile cache unusable",
        "The directory that holds the compile cache of the project load method cannot be read or written. The project compiles on every DAG parse until it is fixed.",
    ),
    (
        "PLA007",
        W,
        "Target boundary is not task",
        "Each unit runs in its own process, so the target should set boundary: task. Then PLC503 checks that the state store is shared between workers.",
    ),
)


# Active since 0.8.
_ACTIVE_08: Final = (
    (
        "PLG307",
        E,
        "State selector without reference",
        "A state: or result: selector atom was used without --state. Point --state at a folder that holds the reference manifest.json. Exit code 2.",
    ),
    (
        "PLG308",
        E,
        "No run results in the reference",
        "A result: selector atom needs the reference's run_results.json, and the --state folder has none. Exit code 2.",
    ),
    (
        "PLX110",
        E,
        "Reference state unusable",
        "The --state folder cannot serve as a reference: it is missing, its manifest.json is unreadable or belongs to another project, or its run_results.json is unreadable, has another schema or belongs to another project. Compile again with --stable to write a fresh reference.",
    ),
    (
        "PLX111",
        E,
        "Defer refused",
        "--defer cannot be used here: the invocation is a unit invocation, the boundary is task (children rerun units without the reference), or the reference was compiled for another catalog kind. Use --boundary process, or build the parents here. Exit code 2.",
    ),
    (
        "PLX112",
        W,
        "Parent not deferred",
        "With --defer, an unselected parent is read from this target instead of the reference: it is an intermediate model, it is missing from the reference, or the reference has no relation for it.",
    ),
    (
        "PLX113",
        E,
        "Run cannot be retried",
        "pipeloom retry --from names a run that cannot be retried: its folder or run_results.json is missing (it never existed, never finished or is still running), it belongs to another project, or it ran another target. Retry with the target of that run.",
    ),
)


# Active since 0.9.
_ACTIVE_09: Final = (
    (
        "PLM208",
        E,
        "Model contract violated",
        "The model output does not match its declared columns: a column is missing or extra, a logical type differs from the declared dtype, or a column with nullable: false holds a null (NaN counts as null). The check runs after the expectations and before the schema policy, the quarantine write and the table write, so nothing is written. Permanent.",
    ),
    (
        "PLP414",
        E,
        "Contract on a view or ephemeral model",
        "A view or an ephemeral model writes no batch of its own, so its columns cannot be checked. Put the contract on a table that reads it.",
    ),
    (
        "PLX117",
        E,
        "State store cannot list runs",
        "pipeloom runs needs StateStore.list_runs (plugin API 0.5), and the configured state store does not provide it. Use --artifacts to read the run results under the artifacts root instead, or upgrade the store plugin.",
    ),
    (
        "PLX118",
        E,
        "Run not found",
        "pipeloom runs show, or runs ls --before, names a run id that the state store (or, with --artifacts, the artifacts root) does not hold for this project and target. Check the target, or list the runs with pipeloom runs ls.",
    ),
    (
        "PLP416",
        E,
        "scd2 options conflict",
        "A model writing with scd2 sets an option the strategy cannot apply: an empty tracked_columns, a tracked column that is part of unique_key, deletes with mode: soft, or a unique_key, tracked_columns, partition_by, cluster_by, deletes.column or contract column named valid_from, valid_to or is_current (scd2 adds those columns itself).",
    ),
)


# Reserved: in the catalog, not emitted yet. (code, severity, title, since)
_RESERVED: Final = (
    ("PLF701", E, "Engine unreachable", "0.2"),
    ("PLF702", E, "Source missing", "0.2"),
    ("PLF703", E, "Target schema drift detected", "0.2"),
    ("PLD002", E, "Submit task limit (never emitted, PLD001 covers it)", "0.4"),
)


def _build() -> Mapping[str, CodeInfo]:
    out: dict[str, CodeInfo] = {}
    for since, table in (
        ("0.1", _ACTIVE),
        ("0.2", _ACTIVE_02),
        ("0.3", _ACTIVE_03),
        ("0.4", _ACTIVE_04),
        ("0.5", _ACTIVE_05),
        ("0.6", _ACTIVE_06),
        ("0.8", _ACTIVE_08),
        ("0.9", _ACTIVE_09),
    ):
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


DOCS_BASE_URL: Final = "https://emanuel-luis.github.io/pipeloom/latest/"


def docs_url(code: str) -> str:
    """The error-code page entry of a code (the anchor is the `{#plx101}` id render_markdown sets)."""
    return f"{DOCS_BASE_URL}reference/errors/#{code.lower()}"


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
