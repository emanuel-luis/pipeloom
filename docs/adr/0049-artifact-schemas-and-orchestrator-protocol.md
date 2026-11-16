# ADR-0049: Artifact schemas, the unit graph and the orchestrator protocol

## Context

Principle 3 of the architecture says the artifacts are the contract: an orchestrator, a catalog or a dashboard reads `manifest.json`, `run_results.json`, the event stream and the attempt files, and never imports pipeloom. The roadmap's export theme for 1.0 asks to publish that contract. Today only the config files have JSON Schemas (`schema/v1/project.json`, `models.json`, `defaults.json`). The artifacts are described in prose in `docs/reference/artifacts.md` and pinned by golden tests, and the reference page says a schema "will follow once the field set stops moving". 1.0 is that point.

Two orchestrator plugins exist (Databricks and Airflow), and each one re-derives the same facts from the manifest: which units exist, their parents, their relations and the command line of each unit. Kestra, Argo or GitHub Actions would each need the same code again. [ADR-0034](0034-core-orchestration-hooks.md) already defines the behavior an orchestrator relies on (`run --select unit:<id> --run-id --expect-plan-hash`, `finalize --unit-states`), and [ADR-0038](0038-airflow-execution-modes-and-artifact-store.md) defines retries and the shared artifacts root, but nothing states it as one protocol.

The artifacts come from two kinds of code. Events are pydantic models (`pipeloom.events.model`): an `Event` envelope and one `EventPayload` subclass per event type. The manifest is built from the plan models (`PlanNode`, `PlanSource`, `PlanMetadata`). Run results and unit states are plain dicts: a results row is `ROW_DEFAULTS` in `artifacts/results.py` plus the fields a unit fills, and the unit-states reader in `runtime/finalize_run.py` checks the document by hand.

## Options

- **Schemas from pydantic only.** One source, but run results and unit states have no model, and adding models only to describe them would be a second source for the same rows.
- **Schemas written by hand only.** Full control of the text, but the event payloads would drift from the classes that emit them.
- **Per artifact: generated where a model exists, written by hand where none does.** Chosen. A test ties each hand-written schema to the code that writes the document.
- **Closed objects (`additionalProperties: false`).** A consumer's validator catches every unexpected field, but any field a 1.x release adds breaks every consumer that validates. That would make every addition a new major schema.
- **Open objects.** Consumers ignore fields they do not know, so 1.x can add fields. The published schema cannot catch a field pipeloom emits by mistake, so pipeloom's own tests use a closed variant. Chosen.
- **Unit graph in a new command, in `compile`, or in `graph`.** `graph` already prints the model graph as text, DOT and JSON; a `--units` flag on it adds one concept to an existing command instead of a new one.

## Decision

**Five schemas.** pipeloom 1.0 publishes a JSON Schema (draft 2020-12) per artifact, under the same base URL as the config schemas, `https://emanuel-luis.github.io/pipeloom/latest/schema/v1/`:

| Document | Tag | Schema |
|---|---|---|
| `manifest.json` | `pipeloom.manifest/1` | `manifest.json` |
| `run_results.json` and attempt files | `pipeloom.run_results/1` | `run_results.json` |
| one event (a JSON line, a jsonl file line) | `pipeloom.event/1` | `event.json` |
| `--unit-states` / `--unit-states-json` | `pipeloom.unit_states/1` | `unit_states.json` |
| `graph --units --output json` | `pipeloom.unit_graph/1` | `unit_graph.json` |

Every document has a required `schema` property whose value is its tag (a `const` in the schema). `run_results.json`, `event.json` and `unit_states.json` are published first; `manifest.json` follows once the 0.9 config changes have landed, and `unit_graph.json` comes with the command below. All five are part of 1.0.

**Generation.** `pipeloom.artifacts.schema.artifact_schemas()` returns the schemas by file name, and the docs build (`tools/docs_hooks.py`) writes each to `schema/v1/<name>` with the same formatting as the config schemas. The event schema is the `Event` envelope plus, for every `EventPayload` subclass defined in `pipeloom.events.model`, an `if type == <TYPE> then data: <payload>` rule; an event type without a rule keeps an open `data` object. The manifest schema has a hand-written top level and `$defs` generated from `PlanNode`, `PlanSource` and `PlanMetadata`. The run results and unit states schemas are written by hand; a test asserts that the row properties are exactly the keys of `ROW_DEFAULTS`. `UNIT_STATES_SCHEMA` moves to `pipeloom.artifacts.schema`, and `runtime/finalize_run.py` keeps re-exporting it. `pipeloom schema export` still exports only the config schemas.

**Attempt files.** An attempt file (`units/<unit_id>/attempt-<n>.json`) is one `run_results/1` row and is validated with `run_results.json#/$defs/result`. It has no tag of its own.

**Open objects and the strict variant.** The published schemas leave objects open: the generator removes the `additionalProperties: false` that pydantic emits for `extra="forbid"`. A consumer must ignore fields it does not know. pipeloom's tests validate against a strict variant (`close()` adds `additionalProperties: false` to every object schema with `properties`, except the parts of a conditional), so a field emitted by mistake fails CI. pipeloom's own readers stay strict where they are today, since a newer core reads files an older one wrote.

**What stays within `/1`.**

- A new field is optional and never enters `required`. It has a default or may be absent.
- Event types and `skip_reason` are open strings: a consumer must accept a value it does not know.
- `status` (results rows, the run, unit states), `level` (events) and `materialized` (manifest) are closed enums. A new value is a breaking change and needs `/2`.
- Within `manifest/1` the plan hash and node checksum formulas are frozen. A field added to the manifest stays out of the hash and has a default, so an older manifest keeps its hash.
- Anything a consumer must understand to act correctly needs `/2`.

**The unit graph.** `pipeloom graph --units` prints the execution units, and only with `--output json` (other outputs exit 2). A new `--manifest PATH` reads the plan from that file, so the plan hash in the output equals the manifest's; without it `graph` compiles. Selection works like `finalize`: a unit is in scope when its persisted node is selected. The output, tagged `pipeloom.unit_graph/1`, has `pipeloom_version`, `project`, `target`, `plan_hash`, `selectors`, the `placeholders` list and three parts:

- `units`, in topological order with ties broken by id. Each has `unit_id`, `unique_id`, `nodes`, `materialized`, `relation`, `relation_name`, `assets`, `parents`, `tags`, the retry settings (`retries`, `retry_delay_s`, `retry_on`, `timeout_s`) and `argv`, the command line that runs it.
- `edges`, `{"from": parent, "to": child}`, only among selected units; `parents` likewise.
- `finalize.argv`, the command line that closes the run.

An `argv` is a list with the placeholders `{project_dir}`, `{manifest}` and `{run_id}`; the unit id, target and plan hash are literal, and the selectors are copied into the finalize command. Assets are `pipeloom://{project}/{target}/{relation_name}`, percent-encoded like the Airflow plugin's default; intermediates and models without a relation get none. The pydantic models live in `pipeloom.artifacts.unit_graph`.

**The orchestrator protocol.** A docs page states what any orchestrator does, all of it behavior that exists today:

1. Prepare: `compile --stable --out manifest.json`, then `graph --units --output json --manifest manifest.json`.
2. Pick one run id per orchestrator run, matching `[A-Za-z0-9._-]{1,64}` ([ADR-0013](0013-run-identity-and-reruns.md)), and keep it across retries and repairs. Give every invocation the same `--run-started-at` (or `PIPELOOM_RUN_STARTED_AT`).
3. Run each unit with its `argv`. Optional: `--orchestrator-id`, `--log-format json`, `--cancel-grace`, `--full-refresh`. A plan hash mismatch is PLX103; exit 1 with no new attempt file is a setup failure and is not retried.
4. Exit codes: 0 succeeded; 2 is a usage error and never retried; 130 was cancelled and may be retried; for 1 the orchestrator reads the newest attempt file's `error.category` and retries when attempts remain and the category is `transient` or the unit's `retry_on` is `any`. The orchestrator owns retries, their delay and the timeout (`timeout_s` plus the cancel grace plus a margin), and stops a unit with SIGTERM, then SIGKILL after the grace. Unit invocations never retry inside pipeloom (`retries=0` in unit mode).
5. Order: a unit starts only after its parents succeeded, and the same unit never runs twice at once. Unit invocations take no lock.
6. Storage: every invocation sees one artifacts root, state store and catalog ([ADR-0038](0038-airflow-execution-modes-and-artifact-store.md)).
7. Finalize always runs ([ADR-0034](0034-core-orchestration-hooks.md)), with `--unit-states` or `--unit-states-json` for units that left no attempt file. It exits 0, 1 or 130, a failure wins over a cancellation, a rerun is safe (`replay`), and PLX105 means a full `pipeloom run` holds the run.

The docs carry one recipe: a GitHub Actions workflow with one job and ordered steps that runs the TPC-H example through this protocol. One job is enough to prove it, because hosted runners share no filesystem and the protocol needs one artifacts root.

**Amendments.** This ADR amends [ADR-0010](0010-events-sinks-artifacts.md): the event envelope and the artifacts it lists now have published JSON Schemas, and the compatibility rules above govern changes to them. It amends [ADR-0034](0034-core-orchestration-hooks.md): the `pipeloom.unit_states/1` document it defines is published as `unit_states.json`, and `graph --units` gives an orchestrator the per-unit command lines it previously built from the manifest.

## Consequences

The artifacts become a published contract that tools in any language can validate. Every golden run results document and event, and the output of a real run (events, `run_results.json`, attempt files), are checked against the strict variant in CI, and the schemas themselves are golden files, so a change to a payload class or to `ROW_DEFAULTS` shows up as a golden diff in review. Branches that add fields must update the schema in the same change, which is the intent. The published schema cannot catch an extra field; only pipeloom's tests can. A new orchestrator needs a recipe, not a plugin. A future `/2` of any document is a breaking change under the stability policy of [ADR-0016](0016-versioning-and-plugin-api.md).

## Status

Accepted.

## Date

2026-11-15
