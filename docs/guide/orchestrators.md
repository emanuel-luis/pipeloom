# Orchestrator protocol

An orchestrator runs a pipeloom plan one unit at a time and closes the run when the units are done.
Airflow and Databricks do it through plugins. Anything else (GitHub Actions, Kestra, Argo, a shell
script) can follow the same protocol with only the command line and the files pipeloom writes. The
protocol is [ADR-0049](../adr/0049-artifact-schemas-and-orchestrator-protocol.md); this page is
the short version, and [the GitHub Actions recipe](recipe-github-actions.md) is a working example.

## Terms

- **Run**: one orchestrator run of the plan, named by a run id. It has one folder,
  `<artifacts root>/runs/<run_id>/`.
- **Unit**: the smallest thing the orchestrator schedules, one persisted model plus the ephemeral
  models inlined into it. The unit id is the model name.
- **Artifacts root**: the folder that holds the runs, `target/` in the project by default.
- **Attempt file**: `units/<unit_id>/attempt-<n>.json` in the run folder, written by every unit
  invocation. It is one row of `run_results.json`.

## 1. Prepare

Compile once, then read the unit graph from that manifest, so every unit runs the same plan:

```console
$ pipeloom compile --project-dir <dir> --target <target> --stable --out manifest.json
$ pipeloom graph --units --output json --project-dir <dir> --target <target> --manifest manifest.json > units.json
```

`--stable` leaves the timestamps out of the manifest, so the same project always gives the same
bytes. `--out` is relative to the project directory unless it is absolute. The unit graph
(`pipeloom.unit_graph/1`) has the plan hash, the `units` in topological order, the `edges`, and
`finalize.argv`. Each unit lists its `parents`, its `retries`, `retry_delay_s`, `retry_on` and
`timeout_s`, and its `argv`.

## 2. Run id

Pick one run id per orchestrator run: 1 to 64 letters, digits, dots, underscores or hyphens
([ADR-0013](../adr/0013-run-identity-and-reruns.md)). Use the same id for every unit, for the
finalize call, and for every retry or repair of that run. Prefix it with the orchestrator so ids do
not collide, such as `gha-<run_id>-<attempt>`.

## 3. Start time

Give every invocation the same start time, with a UTC offset. Either pass `--run-started-at` to
each command or export `PIPELOOM_RUN_STARTED_AT` once. Engines use it to make writes idempotent,
and a run id that already recorded its start keeps it. The option needs `--run-id`.

## 4. Run a unit

The `argv` of a unit is a command line with three placeholders: `{project_dir}`, `{manifest}` and
`{run_id}`. Replace them as whole values and run the result. The unit id, the target and the plan
hash are already in it. For the example in the recipe a unit looks like this:

```console
$ pipeloom run --project-dir examples/tpch_polars --target dev --manifest manifest.json \
    --select unit:orders --run-id gha-1-1 --expect-plan-hash <sha256>
```

You may add these options to a unit command: `--orchestrator-id KEY=VALUE` (repeatable, stored in
the attempt file, the run results and every event), `--log-format json`, `--cancel-grace DURATION`
and `--full-refresh`. A unit invocation takes no run lock, writes no `run_results.json` and never
retries on its own.

## 5. Plan hash mismatch and setup failures

`--expect-plan-hash` stops the unit with PLX103 when its plan is not the one you prepared. That is
an orchestrator bug, not a data problem, so do not retry it. More generally, a unit that exits 1
without writing a new attempt file failed during setup (a bad config, a missing plugin, an
unreachable catalog). Treat it as permanent and do not retry it.

## 6. Exit codes and retries

| Exit code | Meaning | What the orchestrator does |
|---|---|---|
| 0 | The unit succeeded. | Go on. |
| 1 | The unit failed. | Read the newest attempt file. |
| 2 | Usage error. | Never retry. |
| 130 | The unit was cancelled. | May retry. |

For exit 1, read `error.category` in the newest attempt file. Retry when attempts remain and the
category is `transient`, or when the unit's `retry_on` is `any`. The orchestrator owns the retry
count, the delay and the timeout: use `retries`, `retry_delay_s` and `timeout_s` from the unit
graph. Pipeloom does not retry inside a unit invocation, so retries are never counted twice.
Enforce `timeout_s` by stopping the process with SIGTERM, then SIGKILL after the cancel grace.
Allow `timeout_s` plus the grace plus a margin before you stop it. More on the settings in
[Retries, timeouts and cancellation](retries-and-cancellation.md).

## 7. Order

Start a unit only after all its `parents` succeeded. Never run the same unit twice at once.
Units that do not depend on each other may run in parallel. A unit whose parent failed or was
skipped is not started; tell finalize about it (see below).

## 8. Shared storage

Every invocation, including finalize, must see the same artifacts root, the same state store and
the same catalog ([ADR-0038](../adr/0038-airflow-execution-modes-and-artifact-store.md)). On one
machine that is a folder. Across machines it takes a shared volume and a remote catalog and state
store. Pipeloom does not copy artifacts between machines for you.

## 9. Finalize

Always run `finalize`, also when a unit failed: it is the call that writes `run_results.json` and
closes the run ([ADR-0034](../adr/0034-core-orchestration-hooks.md)). Use `finalize.argv` from the
unit graph, with the same placeholders. Options you may add: `--orchestrator-id KEY=VALUE` and
`--run-started-at`.

For each unit in scope, finalize takes the newest attempt file. A unit that left none takes the
state you pass with `--unit-states PATH` (a JSON file) or `--unit-states-json TEXT` (the same
document inline), and a unit with neither is recorded as skipped with `not_run`, which does not
fail the run. The states document is `pipeloom.unit_states/1`: a map from unit id to `status`
(`skipped`, `cancelled`, `failed` or `excluded`) plus an optional `reason`, `code`, `message` and
`orchestrator_ids`. An excluded unit gets no row.

Finalize exits 0 when the run succeeded, 1 when any unit failed, and 130 when it was cancelled.
A failure wins over a cancellation. Run it again and it rewrites the same results; the second
`run.completed` event carries `replay: true`. PLX105 means a full `pipeloom run` holds the run, so
a run is driven either by the orchestrator or by one `pipeloom run`, never both.

## 10. Outputs

Everything lands in `<artifacts root>/runs/<run_id>/`: `manifest.json`, `run_results.json`, the
attempt files under `units/`, the logs, and one event file per invocation under `events/`. Each
document has a published JSON Schema, so a consumer in any language can validate it:

| Document | Tag | Schema |
|---|---|---|
| `manifest.json` | `pipeloom.manifest/1` | [manifest.json](https://emanuel-luis.github.io/pipeloom/latest/schema/v1/manifest.json) |
| `run_results.json`, attempt files | `pipeloom.run_results/1` | [run_results.json](https://emanuel-luis.github.io/pipeloom/latest/schema/v1/run_results.json) |
| one event | `pipeloom.event/1` | [event.json](https://emanuel-luis.github.io/pipeloom/latest/schema/v1/event.json) |
| unit states | `pipeloom.unit_states/1` | [unit_states.json](https://emanuel-luis.github.io/pipeloom/latest/schema/v1/unit_states.json) |
| unit graph | `pipeloom.unit_graph/1` | [unit_graph.json](https://emanuel-luis.github.io/pipeloom/latest/schema/v1/unit_graph.json) |

See [Run artifacts](../reference/artifacts.md) for the fields.

## 11. Stability

The protocol and the five schemas are part of pipeloom 1.0. A new field is optional, so a consumer
must ignore fields it does not know. Event types and `skip_reason` are open strings. `status`
values are a closed set, and a new one needs a new schema version. Anything a consumer must
understand to act correctly is a breaking change.
