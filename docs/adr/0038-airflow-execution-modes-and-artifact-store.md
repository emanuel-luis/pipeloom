# ADR-0038: Airflow execution modes and artifact store

Architecture: [Section 13, The Airflow plugin](../architecture.md#13-the-airflow-plugin)

Amends [ADR-0012](0012-airflow-rendering-and-modes.md), [ADR-0020](0020-execution-model-0-2.md) and [ADR-0037](0037-airflow-packaging-and-rendering.md).

## Context

ADR-0037 renders a plan into one Airflow task per unit but runs nothing. A task must now run its unit, report the outcome to Airflow in a way its retries understand, stop cleanly when Airflow kills it, and a last task must close the pipeloom run. Airflow 3 parses the DAG file before every task, and its workers may be separate machines.

Checked on Airflow 3.3.2 (and 3.2.2 for the floor): a `DAG` subclass with its own `__init__` builds and serializes (`DagSerialization.serialize_dag`), as astronomer-cosmos does; `RuntimeTaskInstance.get_task_states(dag_id, task_ids=, run_ids=)` returns `{run_id: {task_id: state}}`; `airflow.sdk.exceptions` has `AirflowFailException`; `TaskGroup(parent_group=None)` passed explicitly does not attach to the enclosing group.

## Options

- **Artifact store.** One shared POSIX directory for every task and the finalize task, with a sentinel for a unit whose task succeeded but whose result is not visible; or fsspec URIs in the core; or relaying attempt files through XCom.
- **Process model.** A child process per task running the core CLI; or running the core in the worker process.
- **Retries.** Map the core's retry classification to Airflow exceptions; or let Airflow retry every failure.
- **DAG class.** Subclass `DAG` and `TaskGroup`; or a factory returning a plain `DAG`.

## Decision

The target's `artifacts.root` must be one directory that every unit task and the finalize task see (any path under the LocalExecutor, a network volume otherwise). fsspec is deferred: create-once files and atomic replace are not portable across its backends, and state stores and filesystem catalogs need shared storage anyway.

Each unit task runs `python -m pipeloom run --log-format json --select unit:<id>` with the DAG run's run id and orchestrator ids, in a new session (its own process group), with `sys.executable` (`local`) or the interpreter of `virtualenv_dir` (`virtualenv`; a missing interpreter fails without retry). Child events are logged line by line. Exit 0 succeeds; 2 fails without retry; 130 fails with retry; 1 fails without retry when the unit's terminal error is permanent and `retry_on` is `transient`, else with retry. Model `retries` and `retry_delay` become the task's; the execution timeout is the model timeout plus the cancel grace plus 300 s, so the core times out first. `on_kill` sends SIGTERM to the process group and SIGKILL after the cancel grace.

The finalize task (`pipeloom_finalize`, trigger rule `all_done`, one retry) reads the states of the unit tasks, maps them to unit states (a `success` or `failed` task without an attempt file becomes a failed unit with PLA004) and runs `pipeloom finalize --unit-states <temp file>`. Exit 1 fails it without retry, so the DAG run fails when the pipeloom run failed.

`PipeloomDag` subclasses `DAG` and `PipeloomTaskGroup` subclasses `TaskGroup`; both build the plan in their constructor. Errors raise `PipeloomAirflowError`, which Airflow shows as the file's import error; warnings go to the `pipeloom_airflow` logger. The Airflow version is checked first (PLA005).

Deviations from the 0.6 plan. A stable manifest carries an empty `selected`, and `pipeloom finalize --manifest` without a selector keeps it, so the core would see no unit in scope and drop every external state; without a render selection the finalize task therefore passes `--select unit:<id>` for every rendered unit. The core behavior is reported as a follow-up rather than changed here. The `kubernetes` and `databricks` modes report PLA001 until their operators arrive. `--unit-states-json` is not added, since only the pod finalize needs it.

## Consequences

Units on unshared storage fail loudly through PLA004, but state stores and filesystem catalogs on unshared paths still misbehave; PLA007 and the docs mitigate that. A task costs a child interpreter start on top of the DAG parse. Airflow-importing tests run in a separate CI job and in WSL, with a throwaway home and sqlite database checked by the test conftest.

## Status

Accepted.

## Date

2026-11-05
