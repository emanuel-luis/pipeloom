# ADR-0011: Databricks job rendering

Architecture: [Section 12, The Databricks plugin](../architecture.md#12-the-databricks-plugin)

## Context

A plan must run as one Databricks job, with run history, repair and the platform's own scheduling, and the same plan must also run locally.

## Options

- **One task per unit as `python_wheel_task`, in a saved job.** Gives run history, repair, `only`-subset runs and up to 1,000 tasks.
- **`runs/submit` (ephemeral).** No saved job, but limited to 100 tasks and no repair history. Kept for `--ephemeral` dev runs.
- **Fewer, fused tasks.** Needed only above the task limit (`databricks.task_grouping`).

## Decision

Each unit becomes one `python_wheel_task` running `pipeloom run --manifest ... --select unit:<id> --expect-plan-hash <h> --run-id ...`.

- Task keys are sanitized model names, truncated with a short hash suffix (assumed 100-character limit, unverified).
- Compute: serverless by default, with one `environments[]` entry pinning the pipeloom distributions to the running version (`environment_version` is an assumed field, unverified). `compute: job_cluster` is the alternative.
- The default is a saved job found by tag `pipeloom_job=<project>.<pipeline>.<target>`: `jobs/create`, or `reset` when it exists, then `run-now`. Settings: `max_concurrent_runs: 1`, queueing, parameters `pipeloom_run_id` and `full_refresh`.
- `pipeloom databricks bundle` writes a deterministic `resources/pipeloom_<project>.job.yml`. `pipeloom databricks deploy` uses the SDK directly.
- An empty `pipeloom_run_id` resolves to `dbx-<job.run_id>`. Each task writes its unit file to `artifacts.root` and publishes `taskValues` (capped well under 48 KiB). A final `pipeloom__finalize` task (`run_if: ALL_DONE`) writes `run_results.json`.
- Above 1,000 tasks the plan fails with PLD001 and a hint to use task grouping or split by selector. No automatic job chaining before 1.0.

## Consequences

- One job definition, whether it is triggered from the CLI, the Databricks UI or Airflow (ADR-0012).
- Repair keeps the job run id, so the pipeloom `run_id` is stable and the attempt number increments.
- `pipeloom databricks run --select` resolves to task keys plus finalize and uses `only`.

## Status

Accepted

## Date

2026-10-29
