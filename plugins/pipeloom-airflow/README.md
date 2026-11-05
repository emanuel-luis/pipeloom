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
  the target's cancel grace. Exit 2, and exit 1 with a permanent error under
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

The `databricks` mode arrives in a later release of this package. Airflow does not run on
Windows, so neither do the DAG classes.

License: Apache-2.0.
