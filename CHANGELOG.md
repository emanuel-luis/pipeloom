# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses
[Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

- `pipeloom-airflow` DAG classes and the `local` and `virtualenv` execution modes (ADR-0038): `PipeloomDag`, `PipeloomTaskGroup`, `PipeloomRunOperator` and `PipeloomFinalizeOperator`. Each unit runs as `python -m pipeloom run` in its own process group, a killed task stops it (SIGTERM, then SIGKILL after the cancel grace), and the finalize task passes the Airflow task states to `pipeloom finalize --unit-states` and fails the DAG run when the run failed. PLA005 is checked when a DAG is built; the `databricks` mode still reports PLA001.
- `pipeloom-airflow` `kubernetes` mode and extra (ADR-0038): `PipeloomKubernetesOperator` and `PipeloomKubernetesFinalizeOperator` build on `KubernetesPodOperator` from `apache-airflow-providers-cncf-kubernetes>=10.25,<11`. Each unit and the finalize step is a pod of `KubernetesConfig.image`; `pod_kwargs` (volumes, resources, service account) and `ExecutionConfig.env` pass through, pods are never deferrable, and the run id is set in `execute`. Only `pipeloom_airflow._compat_k8s` imports the provider; PLA001 now also reports a missing provider and PLA002 a reserved `pod_kwargs` key such as `deferrable`.
- `pipeloom finalize --unit-states-json TEXT` takes the `pipeloom.unit_states/1` document inline (same schema and errors as `--unit-states`; giving both exits 2).
- The `airflow` dependency group, the `airflow` CI job (locked latest and the 3.2.2 floor), `scripts/wsl-airflow-tests.sh` and `make airflow-test`. Airflow tests opt in with `PIPELOOM_TEST_AIRFLOW=1` and refuse to start unless Airflow points at a throwaway home and sqlite database.
- `pipeloom-databricks`: the `unity` catalog (`catalog: {kind: unity, catalog: main}`, Spark engine only; three-part backticked names, the schema is created only when absent) and the `databricks` secret provider (`${secret:databricks/<scope>/<key>}`, no profile auto-selection, failures are PLX109 without the value).
- Orchestration hooks in the core (ADR-0034, plugin API stays (0, 4)): a `deploy` runtime key, bound against the `renderers` group and never built by the core, with its validated options recorded in the manifest as `metadata.deploy` outside the plan hash; `manifest_bytes(plan, stable=True)` and `write_manifest(..., stable=True)` give byte-stable manifests.
- The `pipeloom.commands` entry-point group adds plugin commands to the CLI. They are discovered from metadata and imported only when invoked; built-in names win, duplicate names resolve by distribution name, a command that cannot be imported exits 1, and `PIPELOOM_DISABLE_PLUGIN_AUTOLOAD` hides them. `plugins list --group commands` lists them.
- `${secret:provider/path}` references in plugin options resolve at run time (`run`), through the `secret_providers` group, and are masked in events, attempt files, state rows and `run_results.json`, in both boundaries, also under secret-like keys. `$${secret:...}` stays literal text. They are never resolved by compile or validate, and never reach the manifest. PLX109 reports one that cannot be resolved (the blueprint's PLX108 was already taken).
- `intermediate` materialization on the memory, polars and pandas engines (ADR-0030, amends ADR-0007): the result is written to `<artifacts.root>/runs/<run_id>/intermediate/<model>` and read by its consumers; `relation_name` in `run_results` is that location. PLP413 rejects `incremental`, a strategy other than overwrite and `partition_by` on an intermediate.
- `artifacts.intermediate_ttl` (default 1d; 0 deletes at the end of a succeeded run). Finalize deletes expired intermediates; a deletion error is reported in the new `run.warnings` of `run_results`, never as a failure.
- `EngineContract` gains the intermediate tests (write then read, repeated token, one folder per run id).
- Plugin API (0, 4), ADR-0030: `RuntimeContext` gains `run_started_at` and `artifacts_root`; `EngineSession` gains `write_intermediate`; `CatalogCapabilities(views=)` and `IntermediateRef` are new types. Plugins built for (0, 3) fail with PLU001.
- `run --run-started-at ISO8601` (environment `PIPELOOM_RUN_STARTED_AT`) sets the start time every engine sees. It needs an offset and `--run-id`; a reused run id keeps its recorded start, and a different value is PLX108 (exit 2).
- The engine session opens before any state-store call and the run lock (so it has no `run_started_at`; engines read `WriteConfig.run_started_at`, new and additive), and `unit_scope` now spans a unit's reads, body, checks and write.
- PLC507 also fires when the catalog declares no views (`CatalogCapabilities(views=False)`).
- `run --orchestrator-id KEY=VALUE` (repeatable) records orchestrator ids in every attempt file, in `orchestrator_ids` of each result row and of the run, and in the `context` of every event (`orchestrator_id.<key>`). Bad keys or values are exit 2. `RunOptions.orchestrator_ids` is the API.
- `pipeloom finalize --run-id ID` (`Project.finalize`) merges the unit results of a run an orchestrator drove unit by unit: attempt file, else the state in `--unit-states` (`pipeloom.unit_states/1`), else `skipped`: `upstream_failed` when an upstream failed, otherwise the new reason `not_run`, which does not fail the run; `excluded` drops the unit. It refuses (PLX105, exit 1) while a `pipeloom run` of the same run id is live (its lock is held and its run record is `running`). It records the run, deletes expired intermediates, emits `run.completed` and rewrites the same result when repeated. Exit 0, 1 or 130; a failed unit wins over a cancelled one.
- `write_json_once` creates the file with an exclusive open when the file system refuses hard links (EPERM, ENOTSUP, EOPNOTSUPP, EXDEV, ENOSYS), such as a network volume.
- `pipeloom-spark` (ADR-0031): a Spark engine, new in this release, with the `delta_path` and `spark_catalog` catalogs. It reads `delta`, `parquet`, `csv`, `json` and `ndjson`, runs SQL models with DataFrame-bound aliases (no session temp views), cancels through Spark job tags, and opens a `local`, `connect` or `active` session; it never sets conf on a session it did not build. pyspark is not a hard dependency: `pipeloom[spark]` pulls `pipeloom-spark[local]` (`pyspark[connect]` 4.0.1 to 4.2, `delta-spark` 4.1 to 4.4).
- CI has a `spark` job (Java 17, classic then Connect), not yet a required check, and `typecheck` covers the Spark plugin. `make spark-test` prints the WSL recipe. Spark tests opt in with `PIPELOOM_TEST_SPARK=classic|connect`.
- `tools/versions.py` accepts a root extra that pins a distribution with an extra, as in `pipeloom-spark[local]==X.Y.Z`.
- Spark Delta writes (ADR-0032): `append`, `overwrite`, `merge` and `replace_partitions` (`replaceWhere`), the `view`, `incremental` and `intermediate` materializations, idempotent writes (`txnAppId`, `txnVersion` and a token in `userMetadata`; a MERGE is followed by an empty token commit) and row and byte metrics read from the commit history. No Spark conf is set.
- PLM207: Delta skipped the write because a later transaction version exists for the model (a newer or overlapping run, or clock skew); permanent for retries. `txn_guard: false` turns the guard off.
- `pipeloom-databricks` (ADR-0035, first half), new in this release: a renderer for the `deploy` block (`kind: databricks`) that turns a plan into Databricks Jobs settings JSON, bundle YAML and `runs/submit` bodies. Rendering is pure (no network, no SDK, no credentials) and byte-deterministic. Supports serverless and job-cluster compute, `compute_rules`, several pipelines, `task_grouping` (`none`, `by_tag`, `by_folder`) and a finalize task. Install with `pipeloom[databricks]`.
- PLD001 (more than 1,000 tasks), PLD003 (task groups form a cycle), PLD004 (invalid Databricks deploy options) and the warning PLD005 (a compute rule matches no model). PLD002 stays reserved and is never emitted.
- `tools/versions.py` checks and bumps a member that pins another member (`pipeloom-spark~=X.Y.0` in `pipeloom-databricks`). `import-linter` keeps the core free of `pipeloom_databricks` and `databricks`, and the SDK out of every plugin module except the client ones.
- `pipeloom databricks deploy` (ADR-0036): uploads the project to the workspace, content addressed, and creates or resets one job per pipeline, found by its `pipeloom_job` tag. `--dry-run` prints the settings and connects to nothing; `--profile` is a group option and no profile is ever picked automatically (`DATABRICKS_CONFIG_PROFILE`, `DATABRICKS_HOST`, the runtime, else PLD010). The client calls the Jobs API 2.2 through the SDK's `api_client.do`; `pipeloom_databricks.api` and `pipeloom_databricks.testing.fake.FakeDatabricks` are new.
- `pipeloom databricks run` (start the deployed job, `--ensure`, `--ephemeral`, `--select`, `--wait`) and `pipeloom databricks repair` (rerun failed, cancelled and skipped tasks). The finalize task now reads task states from the Jobs API, so a task that died without an attempt file fails or cancels the run. `pipeloom_databricks.api` gains `run_job`, `find_run`, `repair` and `task_states`; `simulate_job` takes a `FakeDatabricks` to publish task states.
- PLD007 (job or run not found), PLD008 (deployed job out of date), PLD011 (task ended without a unit result), PLD014 (run still active) and the warning PLD015 (a selection covers part of a task group; the whole group runs). `run` and `repair` take `--timeout SECONDS` (default 24 hours) for `--wait`.
- PLD006 (two jobs carry the same `pipeloom_job` tag), PLD009 (Databricks API request failed), PLD010 (no Databricks connection configured) and PLD012 (a project file cannot be uploaded: a notebook, above 10 MB or outside the project).
- `EngineContract`: `test_metrics` and `test_two_threads_materialize_at_once` skip for an engine without the `overwrite` strategy.
- `pipeloom-airflow` (ADR-0037, first half), new in this release: the pure part of the Airflow renderer. Configs, plan loading from a stable manifest or by compiling the project (with an on-disk cache keyed by files, versions, target, variables and an environment fingerprint), the task graph (one task per unit, visual groups, retry and timeout mapping, asset URIs), the `pipeloom run` and `finalize` command lines, the run id (`af-` plus 16 hex) and the mapping from Airflow task states to `pipeloom finalize --unit-states`. Airflow is not a dependency: the extra `airflow` pulls `apache-airflow>=3.2,<4`, and importing the package never imports `airflow`. Install with `pipeloom[airflow]`.
- PLA001 (unsupported execution mode, was reserved), PLA002 (invalid plugin configuration), PLA003 (selection renders no units), PLA004 (task ended without a visible unit result), PLA005 (unsupported Airflow version), PLA006 (compile cache unusable) and PLA007 (target boundary is not task).
- Case files can use `python: "module:function"` to run a scenario that returns diagnostics.

## [0.3.0] - 2026-11-04

pipeloom 0.3 adds incremental models, merge and replace_partitions, schema-change policies, expectations with quarantine, retries, timeouts and cancellation, on plugin API (0, 3).

### Added

- Guides for incremental models, merge, replace_partitions and schema changes, data quality, and retries, timeouts and cancellation; architecture sections 5, 7, 8, 9, 10, 11, 15 and 16 describe the 0.3 contract as built, and the state-machine diagram shows `retrying -> cancelled`. ADR-0024 and ADR-0026 now say that the polars and pandas engines implement quality, merge and replace_partitions.
- Cancellation: the first SIGINT or SIGTERM (Ctrl+C or Ctrl+Break on Windows) stops starting units and gives running ones `cancel_grace` to finish; a second signal kills them at once. The run ends `cancelled` with exit code 130, emits `run.cancel_requested`, and unfinished units report PLX104.
- `run --cancel-grace DURATION` and the `cancel_grace` setting (default 30s); `Project.run(..., control=RunControl())` stops a run from the API.
- Plugin API (0, 3), ADR-0024: `EngineSession` gains `filter_since(..., inclusive=)`, `evaluate`, `apply_drops`, `align`, `applied_token`, `unit_scope` and `is_transient`; `cancel` becomes active. `WriteConfig` gains `partition_by`, `update_condition`, `deletes`, `columns` and `commit_meta`; `WriteResult` gains `skipped` and `commit_meta`. `EngineCapabilities.quality` marks engines that implement expectations. Plugins built for (0, 2) fail with PLU001.
- `pipeloom.engine.types`: `DeleteSpec`, `ColumnChange`, `SchemaDiff`, `ExpectationSpec`, `QualityCounts`, `QuarantineMeta`, `DropResult`, `can_widen` and `diff_schemas`. `pipeloom.engine.errors`: `TransientError`, `CommitConflict` and `WriteKeyError`.
- `pipeloom.testing.reference`: a pure-Python reference for quality counts, drops, merge, replace_partitions and align. The memory engine now implements merge, replace_partitions, quality, cancel and the `incremental` materialization on top of it.
- `EngineContract` gains the strategy matrix, idempotent replay per strategy, `align`, `filter_since` bounds, `max_value` types, quality counts and drops against the reference, and cancel. `EngineHarness` gains `expr_cases`, `update_condition` and `slow_relation`.
- Diagnostic PLM206 (write key invalid) is emitted by the runner.
- Expectations and quarantine, ADR-0028: row checks (not_null, accepted_values, range, regex, expr) warn, drop or fail; aggregate checks (unique, row_count, freshness) warn or fail on the batch. Engines count, the core judges (`pipeloom.quality`). Dropped rows go to `<model>__quarantine` with `_pipeloom_run_id`, `_pipeloom_failed` and `_pipeloom_ts`; `quality.max_drop_ratio` fails a unit that drops too much. The polars and pandas engines declare `quality`. A pandas `expr` or `update_condition` may only use column names, literals and operators: `@name`, calls and attribute access are rejected (PLQ302).
- `model.quality_evaluated` event, `expectations[]`, `rows_dropped`, `rows_quarantined` and `warnings[]` in `run_results`, and WARN lines on the console. PLQ301, PLQ302, PLP411, PLP412 and PLC510 are active.
- Schema-change policy, ADR-0026: `write.on_schema_change` (fail, ignore, append_new_columns, sync_all_columns) now applies to append, merge and replace_partitions on an existing target. `pipeloom.runtime.schema_policy.resolve_columns` decides the final schema, the runner aligns the batch and passes it as `WriteConfig.columns`. Overwrite and full refresh replace the schema and report the diff.
- Event `model.schema_changed` (relation, policy, action, changes), `schema_changes[]` in run_results, a console line, and PLM202 (schema change rejected) is active with a case file.
- `EngineContract` gains the schema-change policy matrix and `EngineHarness.schema_on_read`; engines that do not read old files through the table schema yet skip the cells that write a changed schema.
- `pipeloom.engine.portable.rewrite_qualified` and an `inclusive` option on `portable.filter_since`.
- Filesystem tables v2, ADR-0025: commit files carry per-file row counts and partition values, and a commit takes a lock (30 second lease) and checks an expected version, raising the transient `CommitConflict` on a clash. /1 tables stay readable and upgrade on their next commit.
- The polars and pandas engines declare `merge` and `replace_partitions`. With `partition_by` every strategy writes one file per partition; replace_partitions swaps files without reading the others; an empty batch still records its token. A merge rewrites the whole table in 0.3.
- `pipeloom.engine.portable`: `merge_frames`, `replace_partition_frames`, partition value encoding and `empty_like`. `pipeloom.builtins.fs_write` holds the write logic the two engines share, and `FsTable.read_aligned` reads every part file through the commit schema.
- Incremental models, ADR-0027: `materialized: incremental` runs on the memory, polars and pandas engines. `pipeloom.runtime.incremental` resolves the watermark (seeding it from the target when the state store has none), filters the input with `incremental.lookback` and commits the new watermark after the write with a compare-and-swap. `ctx.is_incremental`, `ctx.watermark` and `ctx.lower_bound` are set for Python models.
- Retries and timeouts, ADR-0029: `retries`, `retry_delay` and `retry_on` now work. A transient failure (a `TransientError`, a connection or timeout error, or an `OSError` while reading, writing or touching state) moves the unit to RETRYING and runs it again after an exponential backoff with jitter and a cap; `retry_on: any` retries every failure. The scheduler owns retries, with no sleeps (`Executor.wait_any(timeout)`), and `--select unit:...` never retries. Events `unit.retrying` and `unit.timed_out`, a `RETRY` and `TIMEOUT` console line, and `retries_used` in run_results.
- A unit that exceeds its `timeout` ends TIMED_OUT with PLX102 and its descendants are skipped; it is not retried. The thread executor runs one daemon thread per attempt, cancels the engine work once, keeps the timed_out attempt file and drops the late result; `run.completed` reports `abandoned_threads`. The process executor starts children in their own process group and kills a child that outlives `timeout` plus the cancel grace. `pipeloom.runtime.control` adds `CancelToken` and `RunControl`; `RunOptions` gains `cancel_grace` and `control`; `pipeloom.artifacts.io.write_json_once` makes the first terminal attempt file win.
- `SimExecutor` takes a script of `SimAttempt` per unit, `cancel_latency` and `stop_at`.
- A write stores its watermark in the token's commit metadata, and the runner checks the token before reading anything: a retry with the same run id commits the original batch's watermark even when new input arrived. The replay also restores the quality outcome (`expectations`, `rows_dropped`, `rows_quarantined`, `warnings`) without evaluating or quarantining again. Replayed writes emit `model.written` with `skipped: true`.
- Event `state.watermark_committed`; run_results rows fill `is_incremental`, `watermark_before`, `watermark_after`, `rows_updated`, `rows_deleted` and `target_version`; `warnings[]` records `watermark_seeded: target`.
- Diagnostics PLM203 (watermark changed while the unit ran), PLM205 (watermark unusable) and PLP410 (append with a lookback) are active, each with a case file.
- `pipeloom.plan.model.default_strategy`; the sqlite and json state stores treat a stored value equal to `new` and written by the same run as a successful `cas_watermark`, and `StateStoreContract` checks it.

### Changed

- The release workflow publishes each distribution from its own job and GitHub environment (`pypi` for the core, `pypi-<plugin>` for each plugin), because PyPI accepts only one pending publisher per environment. Plugin jobs run after the core and only when their distribution is in the release.

### Fixed

- The json state store retries a lease file whose release is still pending on Windows (`PermissionError` from `O_EXCL`) instead of failing the lock or the watermark compare-and-swap.

## [0.2.0] - 2026-11-04

pipeloom 0.2 runs plans: a scheduler, thread and process executors, polars and pandas engines, a filesystem catalog, state stores and run artifacts.

### Added

- PLK603 (SQL reads a table that is not an input alias or a CTE) and PLK605 (SQL not parseable), both warnings. They run only when sqlglot is installed (extra `pipeloom[sqlglot]`); without it nothing is imported or reported.
- Docs: the guide `Running a pipeline`, the reference `Run artifacts`, a quickstart that runs on polars, and the 0.2 edits of the architecture document and the state machine diagram.
- `pipeloom-polars`, the first plugin distribution (`plugins/pipeloom-polars/`, install with `pipeloom[polars]`): the polars engine on lazy frames, SQL and views through `pl.SQLContext`, tables on the filesystem catalog with exact token replay. It passes `EngineContract`; see `docs/plugins/polars.md`.
- `pipeloom-pandas` (`plugins/pipeloom-pandas/`, install with `pipeloom[pandas]`): the pandas engine on eager DataFrames with Arrow-backed reads, tables on the filesystem catalog with exact token replay. No SQL models and no views in 0.2 (PLC502, PLC507). It passes `EngineContract`; see `docs/plugins/pandas.md`.
- `tests/fixtures/smoke-pandas`: the smoke project on the pandas target (Python models only); CI runs it from the built wheels.
- uv workspace (`members = ["plugins/*"]`) with one lockfile; the `polars` extra of `pipeloom` pins `pipeloom-polars` to the same version.
- `frame_api: narwhals` is wired in the runner: models receive Narwhals frames and a Narwhals result is converted back. PLC508 still rejects it on a non-portable engine.
- `tools/versions.py` (`check [--tag vX.Y.Z]` and `bump X.Y.Z`) keeps every distribution on one version.
- `tests/fixtures/smoke`: a small project on the polars engine that CI runs from the built wheels.
- CI builds every distribution, smoke tests the core wheel alone and all wheels together, and covers the plugin in typecheck and coverage. The release workflow builds each distribution into `dist/<name>/` and publishes each with its own step. ADR-0023.
- `pipeloom run`: runs the selected models with `-s/--select`, `--exclude`, `--manifest`, `--expect-plan-hash`, `--run-id`, `--full-refresh [true|false]`, `--fail-fast`, `--no-lock`, `--workers` (`PIPELOOM_WORKERS`), `--boundary` and `--warn-error`. Exit codes: 0, 1, 2 and 130.
- Process executor for `boundary: task`: each unit runs as `python -m pipeloom run --select unit:<id>`, its events are relayed to the parent with their original invocation id and sequence, and its logs go to `runs/<run_id>/logs/`.
- Unit mode (`--select unit:<id>`): no lock, no `run.*` events and no results file. The `unit:` selector method and `apply_to_plan` select over a manifest.
- Run console output with one line per unit, a summary, the failure block and the artifacts line.
- Diagnostics PLX103 (plan hash mismatch), PLX105 (run lock held), PLX106 (unit process died without a result), PLX107 (manifest invalid) and PLG306 (unknown unit).
- `pipeloom.testing.contract.engine`: `EngineHarness`, `requires` and `EngineContract`, gated by the capabilities an engine declares. `EventBus.forward` relays events from another process, and a per-invocation run file sink writes `runs/<run_id>/events/<invocation_id>.jsonl`.
- Filesystem table format (`pipeloom.builtins.fs_table`): part files plus an atomically replaced `_pipeloom/commit.json` with a format version, an idempotency token log (last 100), overwrite garbage collection and view definitions. ADR-0022.
- The `filesystem` catalog is implemented, with options `root` and `format` (`parquet`, `csv` or `ndjson`); it works with the polars and pandas engines.
- `pipeloom.engine.portable`: Narwhals helpers (`to_frame_api`, `from_frame_api`, `schema`, `count`, `max_value`, `filter_since`, `logical_dtype`). `narwhals>=2.0` is a core dependency, and an import-linter contract keeps it confined to that module.
- State stores in `pipeloom.state`: `StateKey`, `Watermark`, `RunRecord`, `UnitRecord`, `LockInfo`, `LockHeld` and the `StateStore` protocol, with a JSON codec and a run lock that is a 60 s lease renewed by a heartbeat thread every 20 s.
- Built-in `sqlite` state store (WAL, one connection per call, schema version 1) and `json` directory state store (atomic file replace, `O_EXCL` lock files, stale lock takeover). `sqlite` is local to one machine; `json` is shared.
- The `state` block of a target is bound and validated; without one, `sqlite` at `.pipeloom/state.db` is used. Relative state paths resolve against the project directory.
- PLC503: `boundary: task` with a state store that is not shared.
- `pipeloom.testing.contract.state.StateStoreContract`, a pytest suite state store plugins subclass.
- Import contracts (layers, `pipeloom.testing` isolation, no dataframe or SQL parsing library in the core) checked by `uv run lint-imports` in the `lint` CI job and `make lint`.
- Unit state machine with a checked transition table, and a ready-queue scheduler with skip-downstream, `--fail-fast` and stop requests.
- Thread executor and a simulated executor for tests; `unit.queued`, `unit.skipped` and `unit.cancelled` event payloads.
- ADR-0020, the execution model for 0.2.
- Model runner: `pipeloom.runtime.runner.ModelRunner` runs one unit (reads inputs, runs a SQL or Python body, writes a table or creates a view, records the attempt). `ModelContext`, `CallableLoader` (puts `python_paths` on `sys.path` for the run only), `EphemeralMemo` (an ephemeral model shared by several units is computed once) and `ErrorInfo` with a trace excerpt and a transient or permanent category.
- `Project.run(RunOptions, bus)` and `pipeloom.runtime.run.execute_run` run the selected units on the thread executor, take the run lock, record the run in the state store and finalize. Reusing a run id continues the run.
- Run artifacts in `pipeloom.artifacts`: `RunPaths`, `write_json_atomic`, `run_results.json` (`pipeloom.run_results/1`, one row per persisted model) and finalize, which copies `manifest.json` and `run_results.json` to the artifacts root (`target` by default).
- Events `run.started`, `run.compiled`, `run.completed`, `unit.started`, `unit.succeeded`, `unit.failed`, `model.input_read`, `model.written` and `model.log`.
- Codes PLX101 (model raised), PLM201 (write failed), PLM204 (input read failed), PLC509 (materialization unsupported by the engine) and PLP409 (a view must be plain SQL).
- `MemoryCatalog(root=...)` and the memory engine option `root` keep tables as JSON files, so tests can share them between processes. The memory engine supports views.
- ADR-0021, the plugin API (0, 2).

### Changed

- Plugin API version is (0, 2): `Engine.version`, `Engine.session(ctx, *, catalog)`, and `EngineSession.relation`, `persist` and `create_view`. Plugins built for (0, 1) fail with PLU001.
- A model with a materialization the engine does not declare (`incremental`, `intermediate`) is rejected with PLC509.
- The import-linter layers include `pipeloom.artifacts` next to `pipeloom.state`, and imports under `TYPE_CHECKING` are ignored.

## [0.1.0] - 2026-11-04

First functional release: configuration, layered validation, the dependency graph, selectors,
the plan and manifest, the event bus and the CLI. Running plans comes in 0.2.

### Added

- Diagnostics package with the full error-code catalog (active and reserved codes), text and JSON rendering.
- YAML loader with YAML 1.2 core scalars, source marks, duplicate-key detection, merge keys and an alias expansion limit (ADR-0019).
- `${env:...}` and `${var:...}` interpolation, and a secret-like field check (PLS104).
- Pydantic configuration models for `pipeloom.yml`, model files and `_defaults.yml`, with schema errors mapped to file locations and did-you-mean hints.
- `pipeloom validate` with `--project-dir` (`PIPELOOM_PROJECT_DIR`), `--vars` and `--warn-error`; exit codes 0, 1, 2 and 130.
- Python API: `pipeloom.api.Project` and `ProjectOptions`; `pipeloom.platform` clock and identity seams.
- Plugin system: `pipeloom.plugins` with `PluginSpec`, `PluginRegistry`, `API_VERSION` and `check_api`; entry-point discovery that reads metadata only, `--plugin module:attr`, `PIPELOOM_DISABLE_PLUGIN_AUTOLOAD`, and codes PLU001 to PLU003 (ADR-0017).
- Built-in plugins from a lazy in-code table: `memory` engine and catalog, `none`, `prefix` and `per_user` naming, `env` secret provider, and the `filesystem` catalog spec (implementation in 0.2).
- Engine contracts: `pipeloom.engine` types and protocols, `RuntimeContext`, and `pipeloom.testing.memory` (MemoryEngine and MemoryCatalog on sqlite3).
- Target binding: `--target` (`PIPELOOM_TARGET`), `default_target`, runtime merge and plugin option validation, with codes PLR204 and PLR207. `Project.bind()` in the API.
- `pipeloom schema export` writes JSON Schemas for `pipeloom.yml`, model files and `_defaults.yml`, with plugin options as `oneOf` branches.
- `pipeloom plugins list` with `--group`, `--output`, `--check` and `--plugin`.
- Model settings resolution: `_defaults.yml` cascades from the model's folder up to its model_paths root, then `defaults.models`; mappings merge deeply, lists replace and `tags` is a deduplicated union. Unique ids `model.<project>.<name>` and `source.<project>.<name>`.
- L2 reference checks (PLR201, PLR202, PLR203, PLR205) with did-you-mean hints.
- Dependency graph with deterministic topological order, depth-limited ancestors and descendants, and cycle detection (PLG301, PLG302).
- Selector engine: space union, comma intersection, `N+` and `+N` graph operators, and the `name`, `tag`, `path` and `materialized` methods (PLG303 to PLG305).
- `Project.resolve()` in the API runs load, bind, resolve, L2 and graph.
- `pipeloom ls` (`--output text|json`) and `pipeloom graph` (`--output text|dot|mermaid|json`), both with repeatable `-s/--select` and `--exclude`. A malformed selector (PLG304, PLG305) exits 2; a selector that matches nothing (PLG303) is a warning.
- Typed events (`pipeloom.event/1`) and a synchronous event bus with failure budgets, redaction and context-manager close; `console`, `jsonl` and buffered sinks; code PLE401 (ADR-0018).
- `--log-format text|json` (`PIPELOOM_LOG_FORMAT`) on every command; `console` and `jsonl` sinks configurable under `events.sinks`.
- `pipeloom.testing.RecordingSink` and `FakeClock`.
- Plan model (`pipeloom.plan`): frozen `Plan`, `PlanNode`, `ExecutionUnit`; one unit per non-ephemeral model with its inlined ephemeral ancestors, identical for both boundaries; target relations from the bound naming strategy.
- Manifest `pipeloom.manifest/1` with `plan_hash` and per-node checksums (CRLF read as LF), `write_manifest`, `read_manifest` and `compute_plan_hash`.
- L4 plan semantics (PLP401 to PLP408), L5 capability checks read from the engine spec without importing it (PLC501, PLC502, PLC504 to PLC508) and L6 static checks of Python callables (PLK601, PLK602, PLK604).
- `Project.validate()` and `Project.compile(selection, boundary)` in the API; `pipeloom validate` now runs L0 to L6.
- `pipeloom compile` with the selection options, `--boundary`, `--out` and `--warn-error`; it emits `plan.compiled` and `artifact.written`.
- Generated docs: error codes, JSON Schemas under `schema/v1/`, the command-line reference, a quickstart and the schema guide.

### Changed

- `pipeloom validate` binds the selected target and checks plugin options, and accepts `--target` and `--plugin`.
- Diagnostics and the run summary go to stderr through the event bus; the summary line now reads `<command> finished in N ms | exit C | E error(s), W warning(s)`. `rich>=13.7` is a new dependency.
- PLS104 matches secret words in key segments (`max_tokens` is no longer flagged) and ignores booleans.
- The CLI needs `click>=8.2`; runtime dependencies now include pydantic and PyYAML.
- CI tests Python 3.11 to 3.14 on Ubuntu, Windows and macOS now that the repository is public.
- PLE401 names the exception type and the sink's configured path instead of the OS error text.

## [0.0.2] - 2026-10-29

0.0.1 was never published: PyPI rejects that file name because an unrelated, deleted project
once used it.

### Added

- Package skeleton with `pipeloom --version` and `__version__`.
- CI, release and docs workflows.
- Apache-2.0 license, contributing guide, code of conduct, security policy and governance.

[Unreleased]: https://github.com/emanuel-luis/pipeloom/compare/v0.3.0...HEAD
[0.3.0]: https://github.com/emanuel-luis/pipeloom/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/emanuel-luis/pipeloom/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/emanuel-luis/pipeloom/compare/v0.0.2...v0.1.0
[0.0.2]: https://github.com/emanuel-luis/pipeloom/releases/tag/v0.0.2
- `pipeloom databricks bundle --target T --out DIR` (command added by `pipeloom-databricks` through `pipeloom.commands`) writes a job file and a stable manifest per pipeline into a Declarative Automation Bundle, with `${workspace.file_path}` paths; `--check` lists stale files and exits 1. Tasks run the new `pipeloom_databricks_task` wrapper, which resolves the run id, rejects unresolved `{{...}}` parameters (PLD013), calls the core in process, sets a size-capped task value (best effort) and exits with the core's code.
- `pipeloom_databricks.testing.jobsim.simulate_job` runs a rendered job locally through the wrapper, with `run_if`, `only` and repairs.
- PLD013: a task parameter still holds a `{{...}}` reference.
- `pipeloom-spark`: with `DATABRICKS_RUNTIME_VERSION` set and no active session, mode `active` attaches with `SparkSession.builder.getOrCreate()` and no conf (a wheel task starts without a session). The session is never configured or stopped, and `active_session()` now uses `getActiveSession()`.
