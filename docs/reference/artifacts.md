# Run artifacts

A run writes its files under the artifacts root, `target` by default (set it with `artifacts.root`
on the target). Paths in artifacts are POSIX.

## Layout

```text
target/
  manifest.json            copy of the latest run's manifest
  run_results.json         copy of the latest run's results
  runs/<run_id>/
    manifest.json          the plan that ran (pipeloom.manifest/1)
    run_results.json       the outcome of the run (pipeloom.run_results/1)
    events/<invocation_id>.jsonl   events of each invocation
    units/<unit_id>/attempt-<n>.json   one file per attempt of a unit
    logs/<unit_id>-attempt-<n>.log     output of a task-boundary unit
    intermediate/<model>/          result of an `intermediate` model, deleted after `artifacts.intermediate_ttl`
```

Finalize writes `run_results.json` into the run folder, then copies it and the manifest to the
root. The run folder is the source of truth; the root copies are for convenience.

Finalize also deletes the `intermediate/` folder of other runs once they are idle. A finished run
is judged by the `intermediate_ttl` it recorded. A run with no `run_results.json`, or whose
manifest is newer than its results (resumed, then possibly crashed again), is judged by the newest
modification time under its run folder and expires after `max(ttl, 1h)` of inactivity, so an
active run is never deleted.

## Attempt files

Each unit writes `units/<unit_id>/attempt-<n>.json` when the attempt ends, whatever the outcome.
The file holds one results row, the same object that appears in `run_results.json`. The runner
writes it, so it exists for a unit that ran in a child process too. A reused run id keeps counting:
the next attempt is one above the highest file. A unit that started under `boundary: task` and
left no attempt file is reported as failed with PLX106, with the end of its log as the excerpt.

## run_results.json

`schema` is `pipeloom.run_results/1`. Top-level keys:

| Key | Content |
|---|---|
| `schema` | The schema name and version. |
| `metadata` | `pipeloom_version`, `generated_at`, `invocation_id`, `plugins`. |
| `args` | The options of the invocation: `select`, `exclude`, `manifest`, `run_id`, `full_refresh`, `fail_fast`, `workers`, `boundary`, `lock`. |
| `run` | One object for the whole run, below. |
| `results` | One row per persisted model, sorted by `unit_id`. |

`run` holds `run_id`, `parent_run_id` (null, or the run `pipeloom retry --from` retried), `invocation_ids` (every invocation that
worked on the run), `project`, `target`, `plan_hash`, `selector`, `full_refresh`, `status`
(`succeeded`, `failed` or `cancelled`), `started_at`, `ended_at`, `duration_ms`, `counts`
(`total` and one number per unit status), `totals` (`rows_written` and `bytes_written` of
succeeded units), `identity` (`user`, `host`, `orchestrator`, `orchestrator_ids`),
`pipeloom_version`, `plugins`, `python_version`, `git_sha` (null outside a git repository),
`exit_code`, `intermediate_ttl` (seconds; the TTL this run was finalized with, which other runs
use to judge its intermediates) and `warnings` (run-level messages, such as
`intermediate_cleanup_failed: <run_id>: <error>` when an expired folder could not be deleted).

Every row in `results` has every key. Ephemeral models have no row: they appear in the
`ephemeral_inlined` list of the units that inline them.

| Field | Meaning |
|---|---|
| `unique_id`, `unit_id` | Model id (`model.<project>.<name>`) and execution unit id. |
| `ephemeral_inlined` | Ephemeral models computed inside this unit. |
| `materialization`, `strategy`, `relation_name` | What was built and where. |
| `status` | `succeeded`, `failed`, `skipped` or `cancelled`. |
| `skip_reason` | `upstream_failed` or `fail_fast` for a skipped unit; `not_run` (or the orchestrator's own reason) when `pipeloom finalize` found no result; else null. |
| `attempt`, `retries_used` | Attempt number; retries are 0 in 0.2. |
| `started_at`, `ended_at`, `duration_ms`, `queue_wait_ms` | Timing. Timestamps are RFC 3339 UTC. |
| `rows_read` | Rows read per input alias; null where the engine is lazy (polars). |
| `rows_written`, `rows_inserted`, `bytes_written` | Write metrics; null when the engine does not report them. |
| `full_refresh`, `is_incremental` | Whether the unit was rebuilt from scratch; incremental is false in 0.2. |
| `schema_hash` | sha256 of the canonical output schema (name, dtype, nullable, in column order). |
| `error` | Null, or `code`, `category` (`transient` or `permanent`), `message`, `hint`, `exception_type`, `trace_excerpt`. |
| `engine`, `engine_version`, `executor` | What ran the unit (`thread` or `process`). |
| `idempotency_token` | `<run_id>:<unit_id>`. |

Incremental and merge models fill `rows_updated`, `rows_deleted`, `target_version`,
`is_incremental`, `watermark_before` and `watermark_after` (ISO text for dates and timestamps),
and `warnings` carries `watermark_seeded: target` when the state store had no watermark.
Fields that later versions fill stay null or empty: `rows_dropped`, `rows_quarantined`,
`bytes_read`, `schema_changes` and `expectations`. `orchestrator_ids` holds the ids from `--orchestrator-id`.

## JSON Schemas

Each document names its schema in a `schema` property, and each schema is published next to the
config schemas ([ADR-0049](../adr/0049-artifact-schemas-and-orchestrator-protocol.md)):

| Document | Schema |
|---|---|
| `run_results.json` (`pipeloom.run_results/1`) | [run_results.json](https://emanuel-luis.github.io/pipeloom/latest/schema/v1/run_results.json) |
| an attempt file | `#/$defs/result` of run_results.json |
| one event (`pipeloom.event/1`) | [event.json](https://emanuel-luis.github.io/pipeloom/latest/schema/v1/event.json) |
| `--unit-states` (`pipeloom.unit_states/1`) | [unit_states.json](https://emanuel-luis.github.io/pipeloom/latest/schema/v1/unit_states.json) |
| `graph --units --output json` (`pipeloom.unit_graph/1`) | [unit_graph.json](https://emanuel-luis.github.io/pipeloom/latest/schema/v1/unit_graph.json) |

Compatibility within a version: consumers must ignore fields they do not know, since the schemas
leave objects open. A 1.x release may add optional fields, new event types and new skip reasons
and stays on `/1`: skip reasons are open strings both in the row `skip_reason` and in the
`unit.skipped` event. Every other enumerated value is closed: `status` of a row, of the run, of
`run.completed` and of a unit state, the event `level`, the error `category` (`transient` or
`permanent`), `unit.cancelled` `reason`, `model.input_read` `kind`, `model.schema_changed`
`action`, `run.started` `boundary`, `state.watermark_committed` `source` and `model.log`
`level`. A new value of one of them, or any change a consumer must understand, moves the
document to `/2`.

## Unit graph

`pipeloom graph --units --output json` prints the execution units an orchestrator runs one by
one, tagged `pipeloom.unit_graph/1`. With `--manifest PATH` it reads the plan from that manifest,
so `plan_hash` is the manifest's; without it the command compiles the project. A unit is in scope
when its persisted model is selected; `parents` and `edges` only name units in scope.

| Key | Meaning |
|---|---|
| `project`, `target`, `plan_hash` | From the plan. |
| `selectors` | `select` and `exclude` as given; the finalize command repeats them. `finalize` has no `--state`, so when a selector uses `state:` or `result:` it gets `--select unit:<id>` for each unit instead. |
| `placeholders` | `project_dir`, `manifest` and `run_id`: written as `{name}` in every `argv`. |
| `units` | Topological order, ties broken by unit id. Each has `unit_id`, `unique_id`, `nodes`, `materialized`, `relation` (`catalog`, `schema`, `name`) and `relation_name` (null without a relation), `assets`, `parents`, `tags`, `retries`, `retry_delay_s`, `retry_on`, `timeout_s` and `argv`. |
| `edges` | `{"from": parent, "to": child}`. |
| `finalize.argv` | The command line that closes the run. |

An `argv` starts with `pipeloom`; the unit id, target and plan hash are literal, and every
`--plugin` given to `graph` is repeated at the end, so each process can load the same kinds. Assets are
`pipeloom://{project}/{target}/{relation_name}` with each part percent-encoded; intermediates and
models without a relation have none. The schema is
[unit_graph.json](https://emanuel-luis.github.io/pipeloom/latest/schema/v1/unit_graph.json).

## Events

`events/<invocation_id>.jsonl` holds the events of one invocation, one JSON object per line. An
invocation that runs a single unit writes only its own file; the owning invocation forwards the
unit events it receives, keeping the original invocation id and sequence number. Order events
across files by (`ts`, `invocation_id`, `seq`).
