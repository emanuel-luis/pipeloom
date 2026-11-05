# ADR-0012: Airflow rendering and modes

Architecture: [Section 13, The Airflow plugin](../architecture.md#13-the-airflow-plugin)

## Context

Teams that orchestrate with Airflow need the pipeline graph as DAG tasks, with Airflow retries, assets and the same run identity as other orchestrators. Airflow DAG files are parsed often, so parsing must be safe and cheap.

## Options

For the DATABRICKS mode:

- **(a) `DatabricksWorkflowTaskGroup` with `DatabricksTaskOperator` children.** Builds the job spec from Airflow operators at parse time. That duplicates the pipeloom renderer, forces job name `<dag_id>.<group_id>`, fixes `max_concurrent_runs=1`, and disables "Repair All Failed Tasks" on Airflow 3.
- **(b) One `run-now` of the bundle-deployed job, with per-model status mirroring.**

For the plugin overall: support Airflow 2 and 3, or Airflow 3 only.

## Decision

Airflow 3 only (`airflow.sdk`, version >= 3.0). Dropping Airflow 2 support was accepted on 2026-10-29.

- Load methods: `MANIFEST` (recommended) reads a precompiled `manifest.json`. `PROJECT` compiles YAML, which is safe because no user code runs (ADR-0003). Neither imports engines or connects to anything.
- Modes: `LOCAL`, `VIRTUALENV`, `KUBERNETES` (wraps `KubernetesPodOperator`), `DATABRICKS`. `DOCKER` is later.
- Shared behavior: one Airflow task per unit with Airflow owning retries, trigger rule `all_success`, a `pipeloom_finalize` task with `all_done`, and `rid = af-` plus 16 hex of sha256(dag_id + Airflow run_id), stable across task retries. With `emit_assets`, each model task declares `Asset` outlets.
- DATABRICKS mode uses (b). A deferrable `launch` task finds or creates the job and calls `run-now` with `pipeloom_run_id = rid`, or repair if a failed run exists for `rid`. Each model gets a deferrable sensor that mirrors the state of its `task_key`.

## Consequences

- One job definition however the run is triggered, and repair stays in Databricks.
- Airflow task retries of a sensor only re-poll. Reruns go through `launch`.
- Polling load is about one call per sensor per interval, with a configurable interval and an optional shared poll cache.
- The shared artifact store for LOCAL and KUBERNETES modes is still open, with fsspec as the leading option (decide in 0.6).

## Status

Accepted. Amended by [ADR-0037](0037-airflow-packaging-and-rendering.md): the floor is Airflow 3.2, the run id hashes NUL-separated parts, and the package has no hard Airflow dependency.

## Date

2026-10-29
