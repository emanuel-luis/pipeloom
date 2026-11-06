# ADR-0013: Run identity, idempotency, reruns

Architecture: [Section 17, ADR-0013 text](../architecture.md#17-adr-list-and-diagrams)

## Context

A run can span several processes and orchestrator tasks, and tasks can be retried or repaired. Metadata, events and writes from all of them must join into one run, and a retry must not duplicate data.

## Options

- **A fresh `run_id` per attempt.** Simple, but splits one logical run into many.
- **A stable `run_id` with an attempt counter,** derived deterministically by orchestrators.

## Decision

- `run_id` is either caller-supplied or generated as `YYYYMMDDTHHMMSSZ-<6 hex>`, matching `[A-Za-z0-9._-]{1,64}`.
- Orchestrators derive it deterministically: `dbx-<job_run_id>` or `af-<hash>`.
- Orchestrator retries and repairs keep the `run_id` and increment the `attempt`.
- Local `pipeloom retry --from <run_id>` creates a new run with `parent_run_id` that runs the failed, skipped and cancelled units of the parent (see the 0.8 amendment).
- Writes are idempotent through the token `<run_id>:<model>`.

## Consequences

- A cleared-and-rerun Airflow task continues the same pipeloom run.
- Engines without idempotent writes declare `idempotent_writes=False`, and validation forbids retries on non-idempotent append (PLC505).
- Run records go to the state store for history.

## Status

Accepted. Amended in 0.8 (`pipeloom retry`).

## Date

2026-10-29

## Amendment (0.8): pipeloom retry

### Decision

- `pipeloom retry --from RUN_ID` runs the units that run RUN_ID did not finish, in a new run. The parent is the folder `<artifacts root>/runs/RUN_ID` of the bound target. It is read with the `--state` reference loader of [ADR-0042](0042-state-selection-and-defer.md): its `manifest.json` and `run_results.json`.
- The selection is `result:fail`, `result:skip` and `result:cancelled`, only for the values that occur in the parent's rows, against a reference built from the parent folder. It is deliberately not `result:fail+`: the `+` follows the current graph and would pull in descendants the parent never selected, for example when the parent ran with `--select`. The rows of the parent already say what it selected, so the failed units and the descendants that were skipped because of them are exactly the set to run again. A parent with none of the three has nothing to retry: the command prints `run <id> has nothing to retry` and exits 0 without a run folder.
- The retry also selects every `intermediate` model that a retried unit reads, directly or through other intermediate models, even when the parent built it. An intermediate model lives under the folder of the run that built it, so the retry cannot read the parent's copy; it builds its own, which is safe because an intermediate model always overwrites.
- Inherited defer never defers a model whose row in the parent is `succeeded`. Such a model is unselected in the retry and would otherwise be read from the reference, so the retry would build on production upstreams instead of what the parent built in this target. Those models are read from this target (`RunOptions.not_deferred`).
- The vars are read before the project loads, so a model that needs `${var:x}` loads with the parent's vars. The parent folder is found under the target's artifacts root, or under `<project dir>/target` when the project cannot bind yet. If the parent cannot be found and the project fails to load, the load errors stay, with a hint to pass the vars with `--vars`.
- The command takes no `--select`, `--exclude`, `--manifest`, `--expect-plan-hash`, `--run-started-at`, `--state` or `--defer`. It inherits from the parent: the vars of its manifest (`--vars` on top), `full_refresh` (an explicit `--full-refresh` or `--no-full-refresh` wins), and `state` and `defer` from the `args` of its `run_results.json`. Select and exclude are not inherited.
- The project is compiled fresh, so a fixed model is the point of the retry. A plan hash that differs from the parent's is expected and is not checked.
- The retry gets its own run id (generated, or `--run-id`, which must differ from `--from`) and records the parent as `parent_run_id` in `run_results.json`, the state store record and the `run.started` event. Reusing the id of a retry continues that retry and keeps its parent.
- A parent that has no `run_results.json` (it never existed, never finished or is still running), that belongs to another project or that ran another target is PLX113. A parent that is still running and holds the run lock gets PLX105 from the normal lock path.

### Risk

A unit can fail after its write committed (PLM203). The retry runs under a new run id, so the idempotency token `<run_id>:<model>` is new and an append writes the rows again. Retries that must not duplicate keep the original run id instead (run again with `--run-id` set to the parent's id, which continues that run and replays tokens).
