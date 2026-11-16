# ADR-0048: Airflow WATCHER mode

Architecture: [Section 13, The Airflow plugin](../architecture.md#13-the-airflow-plugin)

Amends [ADR-0012](0012-airflow-rendering-and-modes.md) and [ADR-0038](0038-airflow-execution-modes-and-artifact-store.md). No core change.

Status: Proposed. Set to Accepted after the live check.

## Context

In LOCAL, VIRTUALENV and KUBERNETES modes every unit is its own Airflow task and its own process: each one starts Python, imports the engine, binds the project and checks the plan hash. On a wide DAG of short models that start-up cost dominates, and the core scheduler, which already runs units in parallel with in-process retries, goes unused. A user wants one `pipeloom run` for the whole selection and still one Airflow task per model, with its state, retries, logs and asset events.

## Options

- **One task for the run.** A single task runs `pipeloom run`. Cheap, but the graph, per-model state and asset events are lost.
- **Producer and watchers.** One producer task runs the selection; one deferrable watcher per unit reads the files the run writes and reports that unit. The watchers cost a triggerer slot each instead of a worker slot.
- **Producer pushes states.** The producer sets the state of the unit tasks through the Airflow API. It needs API credentials on the worker and breaks the retry semantics of the unit tasks.

## Decision

1. Config: `ExecutionMode.WATCHER = "watcher"`. New `ExecutionConfig` fields: `watcher_workers` (`--workers` of the producer's run; None keeps the target's), `watcher_poll_interval` (10 seconds), `watcher_timeout` (86400 seconds: the producer's `execution_timeout` and each watcher's deadline for one try) and `watcher_artifacts_root` (an absolute path that overrides the root read at parse time). `virtualenv_dir` is optional, as in VIRTUALENV. Watchers always defer, so there is no deferrable switch. `RECHECK_S = 300` and `VISIBILITY_GRACE_S = 120` are constants. PLA002 also covers a worker count below 1, a non-positive poll interval or timeout, and a WATCHER selection made only of `unit:` selectors (the producer would run in unit mode). PLA007 is not reported: one process runs every unit.
2. Retries: the producer has Airflow `retries=0`, because the core scheduler applies the model retries in process, and a producer try above 1 is skipped, so it runs once per DAG run. Each watcher gets the model's `retries` and `retry_delay`. A failure the producer already reported fails the watcher without a retry; a rerun's failure maps exit codes as LOCAL does.
3. Graph: the producer `pipeloom_run` is a root task with no edge to the watchers. Watchers follow the plan edges, so a failed unit leaves its descendants `upstream_failed`. Each watcher has one asset outlet per model, and `group_by` nests watchers. `pipeloom_finalize` (trigger rule `all_done`) runs after the producer and every leaf. The producer's `priority_weight` is the task count plus 2, with `weight_rule="absolute"`, so it starts first.
4. Producer command: `python -m pipeloom run --log-format json`, the project and plan arguments (`--manifest` in MANIFEST mode, always `--expect-plan-hash`), the rendered `--select` and `--exclude`, `--run-id`, `--run-started-at`, `--full-refresh`, the orchestrator ids, `--vars` and `--workers` when set. An empty rendered selection is passed as `--select name:*`: a run without selectors keeps the selection a non-stable manifest recorded at compile time, while the DAG renders every unit. Exit 0 or 1 is a success (the results belong to the watchers; finalize fails the DAG run when the pipeloom run failed), once `runs/<run id>/run_results.json` is visible under the resolved root within the grace, else PLA008. Exit 2 or 130 fails the producer.
5. Artifacts root: resolved at parse time from `<project_dir>/pipeloom.yml` read as plain YAML, as the core does: `targets.<t>.artifacts` replaces the top-level block whole, `root` falls back to `target`, and a relative root is joined to the execution project dir. An unreadable `pipeloom.yml`, or a `${...}` reference in the root without the override, is PLA008. The project is never bound on the worker for this (it would import user modules and break MANIFEST mode). The triggerer must mount the root at the same path.
6. Watcher state machine. Inputs are the unit's files and the producer's task state. The producer is in progress while its state is not terminal and the files hold no final verdict. At try 2 or later, when the producer is not in progress, the watcher reruns its unit (same run id, unit mode, one attempt). Otherwise the files decide: the latest attempt succeeded is a success; timed out or cancelled is a failure; failed with no retry left under the unit's policy (`should_retry` of the core) is a failure. With no final attempt and `run_results.json` present: a `skipped` row runs the unit here (its parent was rerun), a `cancelled` row is a failure, and a terminal row without its attempt file is invisible (PLA004 after the grace). With no results and the producer ended, PLA009 after the grace, which fails the try so a watcher with retries reruns the unit. Else the watcher defers. On success it logs the attempt summary and the tail of the unit's log; on failure the code, message and trace excerpt.
7. Trigger: `PipeloomWatcherTrigger(run_dir, unit_id, retries, retry_on, poll_interval, recheck_after)` reads files only, every interval, in a thread. It yields `done`, `run`, `invisible` (after the grace) or, after `recheck_after`, `pending`, on which the watcher checks the producer's state on a worker and defers again with the remaining deadline. The trigger never reads task states (a triggerer deadlock fixed in Airflow 3.2.2).
8. Shared storage: the watcher lists the unit folder and also opens `attempt-1` up to `attempt-<retries + 1>` by name, since an NFS listing can lag behind a file. Partial or invalid JSON is not visible. Waiting is bounded by the grace.
9. Rerun and finalize: a unit invocation takes no lock and writes no results; `pipeloom_finalize`, cleared with Downstream, merges the new attempt and overwrites the producer's `run_results.json`.
10. Codes: PLA008 (WATCHER artifacts root not resolved) and PLA009 (WATCHER producer ended without run results).

## Consequences

- One process start per DAG run instead of one per unit, and the core scheduler's parallelism and in-process retries apply. Watchers hold triggerer slots, not worker slots, while they wait.
- The triggerer needs pipeloom-airflow and the artifacts root mounted at the same path as the workers. On NFS, `lookupcache=positive` and a low `acdirmax` keep the grace short.
- If a producer's worker dies, its child may keep running; PLA009 and a watcher retry can then run a unit twice.
- A watcher retry after a watch timeout can rerun a unit the producer finished meanwhile.
- To confirm on a live Airflow: `try_number` rises after a clear, an `upstream_failed` task goes back to try 1, and a watcher can defer again from `execute_complete`.
