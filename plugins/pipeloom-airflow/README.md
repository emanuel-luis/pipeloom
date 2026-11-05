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
- The DAG and operator classes arrive in the next releases of this package.

License: Apache-2.0.
