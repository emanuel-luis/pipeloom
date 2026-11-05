# pipeloom-airflow

The Apache Airflow renderer for [pipeloom](https://github.com/emanuel-luis/pipeloom). It turns a
compiled plan into the tasks of an Airflow DAG: one task per execution unit, plus a finalize task
that closes the run.

```console
$ pip install "pipeloom-airflow[airflow]"
```

Airflow is not a hard dependency. The extra pulls `apache-airflow>=3.2,<4`. Everything in this
release of the package is pure Python: configuration, loading a plan from a manifest or by
compiling the project, the task graph, the command lines a task runs and how Airflow task states
map to unit states. Importing the package never imports `airflow`.

- Load a plan from a stable manifest (`pipeloom compile --stable`), or compile the project on
  the DAG parse path with a small on-disk cache keyed by the project files, the installed
  pipeloom versions, the target and the variables.
- Every task runs `python -m pipeloom run --select unit:<id>` with a run id derived from the DAG
  id and the Airflow run id, so a retried task and the finalize task agree on the run.
- The artifact root of the target must be one directory that every task and the finalize task
  can see (a shared volume). A task that succeeded without a visible result file fails the run
  with PLA004.
- Diagnostics: PLA001 (unsupported execution mode), PLA002 (invalid configuration), PLA003
  (the selection renders no unit), PLA004 (a task ended without a unit result), PLA005
  (unsupported Airflow version), PLA006 (the compile cache cannot be used), PLA007 (the target
  boundary is not task).
- `PipeloomDag` (a `DAG` subclass) and `PipeloomTaskGroup` (a `TaskGroup` subclass) build the
  tasks at parse time. Both add nothing else to the DAG; `PipeloomDag` adds the param
  `full_refresh` (default false) unless you define it.
- Execution modes `local` (the worker's own interpreter) and `virtualenv`
  (`ExecutionConfig.virtualenv_dir`, created on every worker with pipeloom installed). Each task
  runs the core in a child process group; killing the task sends SIGTERM, then SIGKILL after
  the cancel grace (the core default of 30 seconds; the manifest does not carry the target's
  `cancel_grace`, and the same value sets a pod's termination grace). Exit 2, and exit 1 with a permanent error under
  `retry_on: transient`, fail the task without a retry.
- The finalize task (`pipeloom_finalize`, trigger rule `all_done`, one retry) reads the states
  of the unit tasks and runs `pipeloom finalize --unit-states`; it fails when the run failed, so
  the DAG run fails too.

```python
from datetime import datetime
from pipeloom_airflow.config import ProjectConfig
from pipeloom_airflow.dag import PipeloomDag

dag = PipeloomDag(
    dag_id="shop",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    project_config=ProjectConfig("/srv/shop", manifest_path="/srv/shop/target/manifest.json"),
    target="prod",
)
```

### Kubernetes mode

```console
$ pip install "pipeloom-airflow[kubernetes]"
```

`ExecutionMode.KUBERNETES` runs every unit, and the finalize step, as a pod built by
`KubernetesPodOperator` (cncf-kubernetes provider `>=10.25,<11`). Set
`ExecutionConfig.kubernetes=KubernetesConfig(image=..., namespace=..., python=..., pod_kwargs=...)`;
`project_dir` and `manifest_path` of the execution config are paths inside the pod. `pod_kwargs`
passes volumes, mounts, resources and a service account straight through, and
`ExecutionConfig.env` becomes pod environment variables. The artifact root must be shared storage
mounted at the same path in every pod (a ReadWriteMany claim or an object-store CSI mount, given
through `volumes` and `volume_mounts`). Pods are never deferrable and push no XCom, the arguments
are set when the task runs, and `retry_on` is not enforced because Airflow sees only the pod's
result. The finalize pod gets the unit states inline (`--unit-states-json`); above 120 KiB (one Linux exec argument is capped at 128 KiB) it
fails with PLA002, so split the project with `render_config.select`.

### Databricks mode

```console
$ pip install "pipeloom-airflow[databricks]"
```

`ExecutionMode.DATABRICKS` runs the project as the Databricks job that `pipeloom-databricks`
renders, without the Databricks Airflow provider. The DAG gets a `pipeloom_launch` task, one
sensor per Databricks task key (with the plan's edges) and a `pipeloom_finalize` sensor on the
job's finalize task (trigger rule `all_done`) that fails when the pipeloom run failed.

- The launch compiles the project at `project_dir` on the worker, checks its plan hash against
  the DAG's (PLX103), and starts the job run named after the DAG run. A missing job is PLD007 and
  a job deployed from other settings is PLD008, unless `databricks_ensure_job=True` deploys it. A
  retried or cleared launch attaches to the same run, or repairs it when a task failed;
  Databricks owns the task retries. Launch retries default to 2.
- The Airflow connection `databricks_conn_id` (default `databricks_default`) holds the host and a
  token in its password (or `extra.token`); with `extra.service_principal_oauth` true, the OAuth
  client id is the login and its secret the password. No profile, `~/.databrickscfg` or
  `DATABRICKS_*` variable is read; a connection that cannot be used is PLD010.
- `databricks_poll="per_task"` (the default) mirrors the run live: the launch returns right after
  run-now or repair, and each sensor makes one `get_run` call. Without a verdict yet it defers on
  its own trigger, which polls every `databricks_poll_interval` seconds (30 by default), rides out
  up to 5 transient API errors in a row (429, 5xx, no answer) with doubling sleeps, and fails the
  sensor on any other API error or when the retries run out. A sensor gives up after
  `databricks_timeout` seconds (24 hours by default). Cost: one API call per waiting sensor per interval.
- `databricks_poll="shared"`: the launch defers on one trigger that polls the whole run, keeps
  the final snapshot in its XCom, and the sensors read it without calling the API. One call per
  interval in total, but a task's result shows only when the whole run is over.
- The triggerer needs `pipeloom-airflow[databricks]` installed in both modes. In `shared` mode the
  launch's wait also ends after `databricks_timeout` seconds; the launch then retries and
  attaches to the same run.

Airflow does not run on Windows, so neither do the DAG classes.

License: Apache-2.0.
