# ADR-0037: Airflow packaging and rendering

Architecture: [Section 13, The Airflow plugin](../architecture.md#13-the-airflow-plugin)

Amends [ADR-0005](0005-packaging.md) and [ADR-0012](0012-airflow-rendering-and-modes.md).

## Context

pipeloom 0.6 runs a plan from Apache Airflow. ADR-0005 set the distribution layout and ADR-0012 sketched the Airflow modes and left versions and mechanics open. This ADR covers packaging and rendering, which are pure and need no Airflow; the execution modes, the artifact store and the Databricks mode arrive in later records and amend this one.

Facts checked against the public Airflow sources and PyPI metadata: 3.3.2 is the latest stable release and 3.2.0 is the first with Python 3.14 support; core requirements carry no platform markers and Airflow does not support Windows; `airflow.sdk` exports `DAG`, `TaskGroup`, `BaseOperator`, `BaseSensorOperator` and `Asset`; the DagBag lives in `airflow.models.dagbag` on 3.1 and in `airflow.dag_processing.dagbag` on 3.3; `AirflowSkipException` is in `airflow.sdk.exceptions`. Airflow 3 parses the DAG file before every task.

## Options

The open questions of the 0.6 plan, each with the option chosen first.

- **Floor.** Airflow `>=3.2,<4` (first with Python 3.14, tested in CI), or `>=3.1`, or `>=3.0` as ADR-0012 wrote.
- **Dependency.** Airflow as an extra with a runtime check (as pyspark in ADR-0031), or a hard dependency.
- **Artifact store.** A shared POSIX path with a sentinel for a missing result, or fsspec URIs in the core, or an XCom relay of attempt files.
- **Run id.** A hash of NUL-separated parts with the group id, or the literal `dag_id` plus `run_id`.
- **Shared Databricks polling.** A deferred launch snapshot, or an in-memory hub per triggerer.
- **CI.** Make the `airflow` job a required check after two green runs on main.
- **Deferrable pods.** Forbidden in 0.6, or supported through `trigger_reentry`.

## Decision

`pipeloom-airflow` is a workspace member that depends on `pipeloom~=X.Y.0` and has no hard Airflow dependency. The extra `airflow` pulls `apache-airflow>=3.2,<4`; the floor is checked when a DAG is built (PLA005). Its pure modules (configs, loading, rendering, command lines, state mapping, ids) run on every CI cell. The root extra `airflow` pins the plugin. Importing the package imports nothing (PEP 562) and a test proves `airflow` stays out of `sys.modules`.

One import seam: only `pipeloom_airflow._compat` and `_compat_k8s` import `airflow` or `kubernetes`. Import-linter forbids the core from importing the plugin or Airflow, and the pure modules from importing Airflow, Kubernetes or the Databricks SDK; indirect imports are allowed because the subprocess test covers them.

Rendering is a pure function of a loaded plan and the configs. There is one Airflow task per unit; `group_by` (`none`, `folder`, `tag`) only nests visual task groups. Ids: unsafe characters become `_`; an id above 200 characters, a colliding id or one equal to a reserved id (`pipeloom_finalize`, `pipeloom_launch`) ends in `_` and 8 hex of the sha256 of the raw name. The run id is `af-` plus 16 hex of the sha256 of `dag_id`, `run_id` and, inside a task group, the group id, separated by NUL, so two inputs never concatenate alike and two groups of one DAG run never share a run. Model retries become Airflow retries; a unit invocation keeps `retries=0`; `max_attempts` is `retries + 1`; the execution timeout is the model timeout plus `cancel_grace` plus 300 s, with the core default of 30 s for the grace because the manifest does not carry it.

A plan loads from a stable manifest, or by compiling the project (PROJECT). PROJECT results are cached on disk under a key that is the sha256 of the canonical JSON of the cache schema, the installed `pipeloom*` versions, the target, the variables and the path, size and mtime of `pipeloom.yml` and every file under `model_paths` and `python_paths` (dot entries, `__pycache__` and the artifacts root skipped). An entry also stores a fingerprint (sha256 per name) of the environment variables the plan read, and a hit needs equal fingerprints, so a secret value never lands on disk. An entry is two files, `<key>.entry.json` and `<key>.manifest.json` (a stable manifest), written to a temporary name and replaced into place; the newest 8 are kept. A cache that cannot be read or written is PLA006 and the project still compiles. Compiling runs no user code (ADR-0003).

Asset URIs follow a template (default `pipeloom://{project}/{target}/{relation}`) whose values are percent-encoded. Case files for the PLA codes use a new `python:` kind (`module:function` returning diagnostics), so they run without Airflow. Airflow-importing tests will run in a separate `airflow` CI job on Ubuntu, and DAG-bag parses in a subprocess with an explicit environment and a throwaway `AIRFLOW_HOME`. The recommended option of the other open questions is recorded here and applies to the later records: a shared POSIX artifact root with a PLA004 sentinel, a deferred launch snapshot for shared polling, the `airflow` job required after two green runs on main, and no deferrable pods.

Deviations from the 0.6 plan, all in this first half. PLA004 and PLA005 are active from the start, because pure modules emit them (`states`, `version`). A MANIFEST load checks only the target, since the configs carry no expected project name. The cache key reads `pipeloom.yml` as plain YAML for `model_paths`, `python_paths` and artifacts roots, so an interpolated root is not skipped (the key only becomes more sensitive). A cache entry names the sha256 of its manifest, so interleaved writers that share a key cannot pair one writer's entry with another's manifest; a mismatch is a miss. DATABRICKS mode needs `pipeloom-databricks`, which this distribution does not declare; without it the render reports PLA001. `finalize` takes its unit states from `--unit-states PATH`. A `--unit-states-json` flag for small documents is a possible future core flag and is not emitted.

## Consequences

A machine that renders or parses a DAG needs the extra, and the pure code is testable everywhere. Airflow internals drift between minors, so every Airflow import sits in two modules. A project compile can run at task start when the cache misses; the stat-based key keeps that rare. The plugin does not record `model_paths` in the plan, so folder groups use the folder every rendered model shares as the model path. The DOCKER mode is PLA001 until it exists.

## Status

Accepted. A second half, covering the execution modes and the artifact store, follows with the operators.

## Date

2026-11-05
