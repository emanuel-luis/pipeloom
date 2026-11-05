# ADR-0039: Airflow DATABRICKS mode

Architecture: [Section 13, The Airflow plugin](../architecture.md#13-the-airflow-plugin)

Amends [ADR-0012](0012-airflow-rendering-and-modes.md), [ADR-0035](0035-databricks-rendering-and-job-lifecycle.md) and [ADR-0036](0036-databricks-client-and-connection.md).

## Context

ADR-0037 renders one Airflow task per Databricks task key, but DATABRICKS mode reported PLA001. The units run inside a Databricks job, so Airflow must start that job, show each task's result, and fail the DAG run when the pipeloom run fails. Airflow workers and triggerers are long-lived shared processes: they must not pick up a Databricks profile or `DATABRICKS_*` variables meant for something else, and a deferred task must not store a secret.

## Options

- **Client.** The Databricks Airflow provider, which pulls the SQL connector, pandas and pyarrow; or `pipeloom_databricks.api` with the Airflow connection mapped by pipeloom.
- **Polling.** Every sensor defers on its own trigger (one API call per task per interval); or the launch defers on one whole-run trigger and the sensors read its final snapshot; or an in-memory hub per triggerer.
- **A cleared launch.** Start a new run; or find the run by its pipeloom run id and attach to it, repairing it when a task failed.

## Decision

- No provider dependency. The extra `databricks` adds `pipeloom-databricks~=X.Y.0`. `pipeloom_databricks.api.connect_explicit(host, auth_type, token | client_id and client_secret)` builds the SDK configuration with the auth type set and a config file that does not exist, so neither `~/.databrickscfg` nor a worker's `DATABRICKS_*` variables change the auth. The connection fields follow the provider's convention (host; token in the password or `extra.token`; `extra.service_principal_oauth` with the client id as login and the secret as password). Azure-only extras and missing values are PLD010, naming the connection id and never a value. Secrets go through Airflow's `mask_secret`; the trigger serializes only the connection id.
- The launch compiles the project on the worker (PLX103 when its plan hash is not the DAG's) and calls `start_or_attach`: no run with the DAG run's pipeloom run id goes through `run_job` (PLD007, PLD008, or a deploy with `databricks_ensure_job`; the run id is the idempotency key); an active or succeeded run is attached; an ended run with failed, cancelled or skipped tasks is repaired, and each rerun key's next attempt number is recorded as its baseline. An existing run is continued as it was started, without the drift check.
- `databricks_poll` has two modes. `per_task` is the default: the launch returns `{v, run_id, job_id, job_run_id, run_page_url, repair_id, baseline, action}` right after run-now or repair, and each sensor makes one synchronous `get_run`; without a verdict it defers on `DatabricksRunTrigger(task_key=key)` with the sensor's `timeout`, which is `ExecutionConfig.databricks_timeout`. The trigger polls every interval, retries a transient `ApiError` (429, 5xx, status 0) up to 5 times in a row with sleeps of one, two, then four intervals, and yields an error event on any other API error or when the retries run out. `shared`: the launch defers on `DatabricksRunTrigger(task_key=None)` until the run is over (after a repair, until that repair is over, with the rules of `wait_for_run`), and `execute_complete` adds the run's `snapshot` to that XCom; sensors read it and make no API call. No process-global state is kept in the triggerer. Every deferral carries `timeout=ExecutionConfig.databricks_timeout` (default 24 hours, PLA002 unless positive), because `execution_timeout` is reserved and the trigger itself never gives up; a timed-out launch fails that try, and its retry finds the same run and attaches to it.
- Verdicts: SUCCESS is success; SKIPPED, EXCLUDED and UPSTREAM_* are skipped; FAILED, TIMEDOUT, CANCELED, INTERNAL_ERROR and MAXIMUM_CONCURRENT_RUNS_REACHED fail once the attempt is the last one Databricks makes or the run is over; while the run is active an attempt below its baseline is ignored; a task an ended run does not list was skipped, and any other state of an ended run is a failure. A failed sensor raises `AirflowFailException` with the task message and the run page URL; sensors have no retries.
- Graph: `pipeloom_launch` -> one sensor per key (plan edges, asset outlets) -> `pipeloom_finalize`, a sensor on the job's `pipeloom__finalize` task with trigger rule `all_done`, which fails when Databricks finalize failed (it exits 1 on a failed run).

## Consequences

- A DAG in DATABRICKS mode needs the project files and pipeloom-databricks on the worker that runs the launch, and pipeloom-airflow[databricks] on the triggerer.
- `per_task` costs one API call per waiting sensor per interval, so a wide DAG should raise `databricks_poll_interval` or use `shared`, where one call per interval is made but a sensor's result shows only when the whole run is over.
- The blueprint's `airflow_e2e` `dag.test()` of a DATABRICKS-mode DAG is deferred: the test client factory builds a new in-memory `FakeDatabricks` for each client the launch and the trigger make, so a run started by one is unknown to the other, and nothing in the fake plays the job's tasks to an end. It needs a file-backed fake that advances runs, which arrives with the live-workflow step of PR41. The launch, trigger and sensors are covered in process instead (`tests/airflow/test_databricks_*.py`), and the DAG file parses in a real DagBag.
- Repair baselines assume Databricks numbers a repaired task's attempt after the earlier ones; only a live workspace confirms it, and the terminal-run rules keep a wrong guess from hanging a sensor.
