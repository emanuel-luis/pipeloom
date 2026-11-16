# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses
[Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

- The published JSON Schema for the manifest, `schema/v1/manifest.json` (`pipeloom.manifest/1`): a hand-written top level as `compile` writes it, with or without `--stable`, and node, source and metadata objects generated from the plan models; `materialized` is a closed enum. Objects are open, so consumers ignore unknown fields; CI validates the manifest goldens and real compiles of the reference project and the TPC-H examples against a strict variant ([ADR-0049](docs/adr/0049-artifact-schemas-and-orchestrator-protocol.md)).
- Version check and `[tool.pipeloom]` ([ADR-0016](docs/adr/0016-versioning-and-plugin-api.md#version-check)): every distribution declares `config-version = 1` and `plugin-api = "1.0"`. `tools/versions.py check` compares the declarations with `CONFIG_VERSION` and `API_VERSION` in the source, requires every distribution to match the root, and requires every `PluginSpec` to pass `API_VERSION`. `check`, `bump` and `--tag` accept prereleases such as `1.0.0rc1`, with members pinned `pipeloom~=1.0.0rc1`; the lint job of CI runs the check, and the release workflow marks a prerelease tag as a GitHub prerelease.

### Changed

- Breaking for plugin authors: the plugin API is (1, 0). A plugin built for (0, 5) fails with PLU001. From now on the core accepts a plugin with the same major and a minor not above its own. `config_version: 1` stays the only accepted value, now named by `CONFIG_VERSION` in `pipeloom.config.types`.

## [0.9.0] - 2026-11-16

pipeloom 0.9 makes models Python-first. A model is a function marked with `@model`; `pipeloom.yml` holds the project and a `models:` tree of folder defaults, and properties files next to the models configure and describe them, with the decorator winning as in dbt. The compiler reads model files with `ast` and imports only the modules whose decorators have dynamic arguments, and SQL runs through `ctx.sql`, translated to the target engine. YAML model declarations are removed: see the [migration guide](docs/guide/migrate-0.9.md). The release also adds model contracts, `pipeloom test` and `pipeloom build` with post-write data tests, the `scd2` write strategy, run history and the `webhook` sink, the new `pipeloom-openlineage` distribution with Airflow facets, CSV sample data on Spark and Databricks, and the Airflow WATCHER mode.

### Added

- Airflow WATCHER mode (`ExecutionMode.WATCHER`, [ADR-0048](docs/adr/0048-airflow-watcher-mode.md)): one producer task runs the whole selection in a single `pipeloom run`, and one deferrable watcher per model reads the unit's files, reports its result and declares its asset. Clearing a failed watcher with Downstream reruns that one unit. New `ExecutionConfig` fields `watcher_workers`, `watcher_poll_interval`, `watcher_timeout` and `watcher_artifacts_root`; the triggerer needs `pipeloom-airflow` and the artifacts root at the same path. New codes: PLA008 (WATCHER artifacts root not resolved, or the run left no `run_results.json`) and PLA009 (producer ended without run results). The examples gain the `tpch_polars_watcher` DAG and a development-wheel hook in the Astro `Dockerfile`.
- The 1.0 stability and deprecation policy: [ADR-0016 amendment](docs/adr/0016-versioning-and-plugin-api.md#amendment-10-stability-and-deprecation-policy) and a new [Stability and deprecations](docs/reference/stability.md) page. It lists what is stable from 1.0.0, what counts as breaking, and how a deprecation is announced (CHANGELOG, a `PLV` warning, `DeprecationWarning`) and removed (next major, after a minor that warned). The `PLV` prefix is reserved; no code is added yet. Documentation only.
- Model contracts: an optional `columns` list on a model, each entry with `name`, a logical `dtype` and `nullable`. Before every write the batch must have exactly those columns and types, and no null in a non-nullable column; otherwise the unit fails with PLM208 and nothing is written. A contract on a view or an ephemeral model is PLP414. Declared columns appear as `nodes[].columns` in the manifest (only when declared, so other manifests, checksums and plan hashes are unchanged) and on the docs site model page. The engine contract suite gains contract cells ([ADR-0044](docs/adr/0044-model-contracts.md)).
- Unit tests: `unit_tests` cases in a model's properties entry give rows per input alias (inline or CSV) and the expected rows. `pipeloom test` runs each body on the target's engine without writing anything and compares the rows as a multiset; a difference is PLX114 with a row diff, a case that cannot run is PLX115, and a wrong case reference is PLR208. Unit tests never change a manifest or a plan hash ([ADR-0045](docs/adr/0045-unit-and-data-tests.md)).
- Data tests: a `data_tests` list on `@model` or in a model's properties entry (`not_null`, `unique`, and `relationships` with `column`, `to` and `field`) runs against the whole written table after the write, under `pipeloom build`. Results join `results[].expectations` with `scope: table`; `on_violation: fail` is PLQ303 (permanent, the data stays written) and `warn` adds a warning. PLP415 rejects data tests on ephemeral models and relationship parents that are the model itself, a descendant, an ephemeral model, an intermediate model or close a cycle among input and relationship edges. `data_tests` appears in the manifest only when declared ([ADR-0045](docs/adr/0045-unit-and-data-tests.md)).
- `pipeloom.DataTest` (alias of the data test config) is exported from `pipeloom` and `pipeloom.authoring` for the `data_tests=` keyword of `@model`; the AST resolver evaluates it statically.
- `pipeloom build` runs the unit tests of the selection, stops with exit 1 before any run or write when one fails, then runs with data tests on. It takes the flags of `run`; `run_results.json` `args` gains `data_tests`, which `retry --from` inherits ([ADR-0045](docs/adr/0045-unit-and-data-tests.md)).
- Every `results[].expectations` entry gains an additive `scope` field (`batch` or `table`); entries replayed from older commits get `batch`.
- Plugin API (0, 5): `EngineSession.from_rows`, `to_rows` and `orphans`, implemented by the memory, polars, pandas and Spark engines, with portable narwhals versions in `pipeloom.engine.portable` and `orphans_rows` in `pipeloom.testing.reference`. A third-party engine without them fails only when a test uses them. The engine contract suite gains row round-trip, unit case, orphans and data-tests-after-write cells.
- Plugin API (0, 5), [ADR-0047](docs/adr/0047-run-history-and-webhook.md): `StateStore.list_runs(project, target, limit, before=None)` returns the runs of one project and target, newest first, paged by a run id. The sqlite, json and Spark `table` stores implement it, `page_runs` does the ordering for any store, and the state store contract suite checks it. Plugins built for (0, 4) fail with PLU001; every first-party plugin moves with the core.
- `pipeloom runs ls` lists the last runs of a target (`--limit`, `--before RUN_ID` for the next page, `--output json`) and `pipeloom runs show RUN_ID` shows one run with its unit attempts. Both read the state store, or with `--artifacts` the `runs/*/run_results.json` files under the artifacts root. New codes: PLX117 (state store cannot list runs) and PLX118 (run not found).
- A built-in `webhook` sink posts selected events as JSON (`text` for Slack and Teams, plus the event) to a URL given as `${secret:...}`, which is never logged. By default it posts once when a run fails; `events`, `statuses`, `min_level`, `timeout` and `max_failures` tune it. After `max_failures` failed posts in a row it stops and the bus reports one `sink.disabled` warning. stdlib HTTP only, meant for `buffered: true`.
- The `scd2` write strategy on polars, pandas and Spark: the table keeps every version of a key with `valid_from`, `valid_to` and `is_current`. A change in a tracked column (`tracked_columns`, by default every column but the key and the delete column) closes the current version at the run start and opens a new one; hard deletes close it. Null or repeated keys are PLM206, and conflicting scd2 options are the new PLP416. Spark writes it as one Delta `MERGE INTO` under the usual claim and token commits. The engine contract suite gains scd2 cells ([ADR-0008 amendment](docs/adr/0008-incremental-and-state.md#amendment-09-scd2)).
- Published JSON Schemas for the run artifacts under `schema/v1/`: `run_results.json` (its `#/$defs/result` also covers attempt files), `event.json` (envelope plus one `data` schema per event type) and `unit_states.json`. Objects are open, so consumers ignore unknown fields; CI validates the goldens and a real run against a strict variant ([ADR-0049](docs/adr/0049-artifact-schemas-and-orchestrator-protocol.md)).
- `pipeloom graph --units --output json` prints the selected execution units of the plan, tagged `pipeloom.unit_graph/1`: per unit its nodes, relation, assets, parents, tags, retry settings and `argv`, plus the edges among the selected units and the finalize `argv`. Each `argv` has the placeholders `{project_dir}`, `{manifest}` and `{run_id}` and repeats the `--plugin` specs given to `graph`. `--manifest PATH` reads the plan from a manifest so `plan_hash` is the manifest's. `--units` with another output, or `--manifest` without `--units`, exits 2. The schema is published as `schema/v1/unit_graph.json` ([ADR-0049](docs/adr/0049-artifact-schemas-and-orchestrator-protocol.md)).
- An orchestrator protocol page and a GitHub Actions recipe. The page states what any orchestrator does with `graph --units`, `run --select unit:<id>` and `finalize`: run ids, one start time, exit codes, who retries, ordering, shared storage and the outputs. The recipe is a workflow that runs the TPC-H polars example unit by unit in this repository's CI ([ADR-0049](docs/adr/0049-artifact-schemas-and-orchestrator-protocol.md)).
- `run.completed` gains an additive `replay` field: true when `pipeloom finalize` finds the run already finished with the same status (an orchestrator retried the finalize task). The `webhook` sink skips replays, so a failed run posts once.
- Sample data on Spark and Databricks: the `tpch_spark` example reads its `region` table from `data/region.csv`, committed with the project, and the same source reads a copy on a Databricks volume through a var. The engine contract suite gains a cell that reads one hand-written CSV on every engine that declares `csv`, and the `databricks-live` workflow checks a CSV source read from the volume (`csv_source_on_volume`). See [CSV sample data](docs/plugins/spark.md#csv-sample-data).
- A new distribution, `pipeloom-openlineage` (`pip install "pipeloom[openlineage]"`), with the `openlineage` sink: the run is the OpenLineage job `<project>` and each persisted model the job `<project>.<model>`, with uuid5 run ids shared by every process of a run, START, COMPLETE, FAIL and ABORT events, input and output datasets from the manifest, and the schema, outputStatistics, dataQualityAssertions, errorMessage, parent, jobType and sourceCodeLocation facets. Options `namespace`, `dataset_namespace`, `transport` and `inside_airflow`; inside an Airflow task it stays silent by default ([ADR-0046](docs/adr/0046-openlineage.md)). Run ids include the project, so two projects with the same `--run-id` stay apart.
- The pipeloom-airflow unit operators implement `get_openlineage_facets_on_start`, `..._on_complete` and `..._on_failure`, so the Airflow OpenLineage provider reports each unit task with its datasets and facets once: LOCAL and VIRTUALENV build them from the child's events, KUBERNETES from the manifest; DATABRICKS keeps the sink. Without `pipeloom-openlineage` or the provider the hooks return nothing. `examples/marquez/compose.yml` and `tools/marquez_check.py` check a run against Marquez; guide: [Lineage with OpenLineage](docs/guide/openlineage.md).
- Sinks may define an optional `plan_ready(PlanInfo)` hook: the bus hands it the run's manifest and project dir right after `run.started`, in unit invocations and in `finalize`. Additive within plugin API (0, 5).
- `model.written` gains an additive `columns` field: the table's columns after the write (`name`, `dtype`, `nullable`), null for intermediates, views and replayed writes.
- Python-first models ([ADR-0050](docs/adr/0050-python-first-authoring.md)): a model is a function marked with `@model(...)`, whose keywords are the model settings and whose parameters are its inputs, bound by name to a model or a source table, or through `inputs=` with `ref()` and `source()`. `var()`, the typed classes `Write`, `Incremental`, `Column`, `Expectation`, `Quality`, `Deletes` and the check helpers come from `pipeloom`. Discovery reads decorators from the AST and imports only modules with dynamic arguments; `model_packages` adds installed packages. `ctx.sql(query, dialect=, inputs=)` runs SQL over the inputs, translated with the `sqlglot` extra when a declared `sql_dialect` differs from the engine's. Properties files (any `*.yml` under `model_paths`) describe models, the `models:` tree in `pipeloom.yml` sets folder defaults with `+` keys, and sources are groups of tables in `pipeloom.yml`. `schema export` writes `properties.json`. New codes: PLY005, PLR209, PLR210, PLC513, PLK606, PLX120.

### Changed

- Breaking: the manifest node body is always `{kind: python, callable, file}`, and `node.file` and `node.line` point at the decorator. Every plan hash and node checksum changes once.
- Breaking: settings resolve from the decorator, then properties, then the `models:` tree, then built-in defaults. PLK602 now means a function whose parameters cannot take its inputs (`*args`, positional-only parameters, an `inputs=` key that is not a parameter). PLS108 is now a source table with both `location` and `table`. PLP409 narrows to a view that inlines an ephemeral model.

### Removed

- Breaking: YAML model declarations. Model entries with `sql:` or `python:`, flat `sources:` lists in model files, `_defaults.yml` and `defaults.models` are gone; such keys are PLS101 with a hint to the migration guide. A manifest with a `kind: sql` body is PLX107. `models.json` and `defaults.json` are no longer exported or published under `latest/`. PLK601, PLK604, PLC502 and PLR205 are retired: the catalog keeps them, marked "Retired in 0.9", and they are no longer emitted.
- Migrating from 0.8: see [the migration guide](docs/guide/migrate-0.9.md).

### Fixed

- A source location with a URI scheme such as `file:/data/x.csv` or `dbfs:/Volumes/...` is no longer joined to the project directory; only relative paths are.
- On Windows, `boundary: task` runs each child in its own job object, so a timed-out, cancelled or finished unit no longer leaves a grandchild running (POSIX already swept the child's process group). If the parent dies, Windows ends the whole tree. When Windows refuses a job, the executor falls back to `taskkill /T /F` ([ADR-0029 amendment](docs/adr/0029-retries-timeouts-cancellation.md#amendment-10-windows-job-objects)).

## [0.8.0] - 2026-11-06

pipeloom 0.8 is about the first hour and fast loops. `pipeloom init` scaffolds a project with editor completion set up (polars and pandas projects run out of the box), and `pipeloom new model` adds models to it. `pipeloom explain` prints what a diagnostic code means. `--state` with the `state:modified`, `state:new` and `result:` selectors picks what changed or what failed, and `--defer` reads the parents a run does not build from a reference such as production. `pipeloom retry --from` reruns what a run did not finish. `pipeloom docs generate` and `docs serve` write a one-file docs and lineage site. Plugin API stays (0, 4). Artifacts only gain fields: `run_results` `args` (`state`, `defer`), `run.parent_run_id`, `model.input_read` (`relation`, `deferred`) and `run.started` (`parent_run_id`).

### Added

- `pipeloom init [DIRECTORY]` scaffolds a new project, like `dbt init`: a `dev` target for polars, pandas, spark or databricks, one example model per layer (bronze, silver, gold), a `.gitignore` and a README. polars and pandas projects run out of the box on a small CSV in `data/`. Flags `--name`, `--engine`, `--catalog` (databricks), `--no-input` and `--force`; it prompts for the rest only when stdin is a terminal. `--force` keeps existing files and lists them as skipped ([ADR-0041](docs/adr/0041-pipeloom-init.md)).
- `pipeloom init` writes the schema modeline in every YAML file, including `_defaults.yml`, plus `.vscode/settings.json` (maps `yaml.schemas` to the published schemas) and `.vscode/extensions.json` (recommends `redhat.vscode-yaml`), so completion works with only the YAML extension ([ADR-0041](docs/adr/0041-pipeloom-init.md)).
- `pipeloom new model NAME [--sql|--python] [--folder PATH]` adds a model file under `models/`: one block-style entry with the schema modeline and, for Python, a `build(ctx, **inputs)` stub that passes PLK602. The default kind is Python on a pandas default target, SQL otherwise ([ADR-0041](docs/adr/0041-pipeloom-init.md)).
- `pipeloom explain <CODE>` prints the catalog entry of a diagnostic code (title, layer, severity, status, the version it appeared in and the explanation) and the link to its entry on the error-code page. The code is case-insensitive; an unknown one is a usage error (exit 2) with a did-you-mean. The console failure block gains an `explain: pipeloom explain <CODE>` line. It reads no project.
- Selection by state. `--state DIR` on `run`, `compile`, `ls` and `graph` points at a reference `manifest.json` (and optionally `run_results.json`), for example one from `compile --stable`. New selector methods: `state:modified` (new, or the checksum or the inputs differ; a modified ephemeral model also marks the models that inline it), `state:new`, and `result:fail|skip|cancelled|succeeded` against the reference run results. New codes: PLG307 (state selector without `--state`), PLG308 (no run results in the reference) and PLX110 (reference unusable) ([ADR-0042](docs/adr/0042-state-selection-and-defer.md)).
- `pipeloom run --defer` (with `--state`) reads the parents the run does not build from the relations of the reference manifest, so a dev run of one model reads the production upstreams. The plan hash does not change. PLX111 refuses it for unit invocations, `boundary: task` and another catalog kind; PLX112 warns when a parent cannot be deferred. `run_results.json` `args` gains `state` and `defer`, and `model.input_read` gains `relation` and `deferred` ([ADR-0042](docs/adr/0042-state-selection-and-defer.md)).
- `pipeloom retry --from RUN_ID` runs again the units that run did not finish (failed, timed out, skipped, cancelled) in a new run, with `parent_run_id` set in `run_results.json`, the state record and `run.started`. The project is compiled fresh; vars, `--full-refresh`, `--state` and `--defer` are inherited from the parent and the flags win. A parent with nothing to retry prints a message and exits 0. New code: PLX113 (run cannot be retried). `Project.retry` is the API ([ADR-0013](docs/adr/0013-run-identity-and-reruns.md) amendment).
- `pipeloom docs generate` writes one self-contained `index.html` (default `target/docs/`) that opens offline: an inline SVG lineage graph, a filterable model table, a page section per model and source (config, SQL or Python body, inputs, expectations) and, when a `run_results.json` is present, each model's last status, rows and error. It compiles the project, or reads `--manifest`; `--run-results` picks the results file. `pipeloom docs serve` serves the folder on localhost with the stdlib. The site is read only, so the non-goal is now "a scheduler service, or any UI that starts, schedules or changes runs" ([ADR-0043](docs/adr/0043-docs-site.md)).

## [0.7.0] - 2026-11-05

pipeloom 0.7 adds a `catalog` setting per model and per folder, so a project can keep each layer in its own catalog, and `pipeloom compile --stable`. Plugin API stays (0, 4). Plan hashes change once, so Databricks jobs deployed with an earlier version show as stale (PLD008) until they are deployed again.

### Added

- A `catalog` setting per model and per folder default, next to `schema`, so each layer can write to its own catalog (for example `pipeloom_bronze.tpch.orders`). It cascades like `schema` and wins over the naming strategy's `catalog` option. pipeloom creates schemas, never catalogs. An empty catalog is rejected (PLS102). The resolved config gains a field, so node checksums and plan hashes change once ([ADR-0040](docs/adr/0040-per-model-catalog.md)). Plugin API stays (0, 4).
- `pipeloom compile --stable` writes a reproducible manifest: `generated_at`, `invocation_id` and the selection are blank, so one project and target always give the same bytes. `pipeloom run --manifest` on such a manifest runs every model unless `--select` or `--exclude` is given.

### Changed

- The `filesystem` and `delta_path` catalogs lay a table out at `<root>/<catalog>/<schema>/<name>/` when the target has a catalog; they used to ignore it. Migration: a project whose naming option `catalog` was set on a path catalog moves its tables under a directory of that name, or removes the option. A source with a three-part `table: a.b.c` now reads `<root>/a/b/c` instead of `<root>/b/c` on these catalogs: move its files under a directory named `a`, or write the table as `b.c`.

### Fixed

- PLP412 (quarantine table collides with a model) compares the effective catalog and schema of both tables, including the naming strategy's defaults. A model that left its schema or catalog unset no longer escapes the check when the other model sets the same value explicitly.

## [0.6.1] - 2026-11-05

pipeloom 0.6.1 fixes the Databricks plugin on serverless compute, found by running the `databricks-live` workflow against a serverless workspace. Jobs deployed with 0.6.0 show as stale (PLD008) until they are deployed again.

### Fixed

- A repaired Databricks task keeps the run's start time. `{{job.start_time}}` is the start of the latest repair, so the rerun units passed a start the run had not recorded and stopped with PLX108. The task wrapper now reads the job run's `start_time` from the Jobs API and keeps `--start-ms` only as a fallback.
- `pipeloom run` and `pipeloom finalize` with `--manifest` now bind the project with the manifest's vars (an explicit `--vars` still wins). This fixes Databricks jobs ignoring the vars given at deploy time.
- The Databricks task entry point returns on success instead of raising `SystemExit(0)`. Serverless runs it under IPython, which reported every successful task as failed.
- The console log writes plain lines on Databricks. Rich took the job's IPython kernel for Jupyter and rendered each line as its own HTML block, with large gaps between lines in the job output.
- `pipeloom databricks repair` passes the run's job parameters again. Without them the repair resolved `pipeloom_run_id` to its empty default, so the rerun units started a new pipeloom run (`dbx-<job run id>`) instead of continuing the original one.
- Finalize maps a task in state DISABLED to `excluded`: serverless reports a task left out by `only` as DISABLED, not EXCLUDED.
- Serverless tasks are rendered with `disable_auto_optimization: true`. Auto-optimization retried a failed task on top of `max_retries`, so a model with `retries: 0` ran twice. The settings hash changes, so deployed jobs show as stale (PLD008) until they are deployed again.

## [0.6.0] - 2026-11-05

pipeloom 0.6 adds the `pipeloom-airflow` plugin: `PipeloomDag` and `PipeloomTaskGroup` render a project as one Airflow task per unit, run in LOCAL, VIRTUALENV or KUBERNETES mode, or mirror a deployed Databricks job in DATABRICKS mode. Airflow 3.2 or later; Airflow is an optional extra. Plugin API stays (0, 4).

### Added

- `pipeloom-airflow` (ADR-0037), new in this release: the pure part of the Airflow renderer. Configs, plan loading from a stable manifest or by compiling the project (with an on-disk cache keyed by files, versions, target, variables and an environment fingerprint), the task graph (one task per unit, visual groups, retry and timeout mapping, asset URIs), the `pipeloom run` and `finalize` command lines, the run id (`af-` plus 16 hex) and the mapping from Airflow task states to `pipeloom finalize --unit-states`. Airflow is not a dependency: the extra `airflow` pulls `apache-airflow>=3.2,<4`, and importing the package never imports `airflow`. Install with `pipeloom-airflow[airflow]` (or `pipeloom[airflow]` next to an existing Airflow).
- PLA001 (unsupported execution mode, was reserved), PLA002 (invalid plugin configuration), PLA003 (selection renders no units), PLA004 (task ended without a visible unit result), PLA005 (unsupported Airflow version), PLA006 (compile cache unusable) and PLA007 (target boundary is not task).
- Case files can use `python: "module:function"` to run a scenario that returns diagnostics.
- `pipeloom-airflow` DAG classes and the `local` and `virtualenv` execution modes (ADR-0038): `PipeloomDag`, `PipeloomTaskGroup`, `PipeloomRunOperator` and `PipeloomFinalizeOperator`. Each unit runs as `python -m pipeloom run` in its own process group, a killed task stops it (SIGTERM, then SIGKILL after the core default grace of 30 s), and the finalize task passes the Airflow task states to `pipeloom finalize --unit-states` and fails the DAG run when the run failed. PLA005 is checked when a DAG is built.
- `pipeloom-airflow` `kubernetes` mode and extra (ADR-0038): `PipeloomKubernetesOperator` and `PipeloomKubernetesFinalizeOperator` build on `KubernetesPodOperator` from `apache-airflow-providers-cncf-kubernetes>=10.25,<11`. Each unit and the finalize step is a pod of `KubernetesConfig.image`; `pod_kwargs` (volumes, resources, service account) and `ExecutionConfig.env` pass through, pods are never deferrable, and the run id is set in `execute`. Only `pipeloom_airflow._compat_k8s` imports the provider; PLA001 now also reports a missing provider and PLA002 a reserved `pod_kwargs` key such as `deferrable`.
- `pipeloom finalize --unit-states-json TEXT` takes the `pipeloom.unit_states/1` document inline (same schema and errors as `--unit-states`; giving both exits 2).
- `pipeloom-airflow` `databricks` mode and extra (ADR-0039): `PipeloomDatabricksRunOperator` starts the Databricks job run named after the DAG run (or attaches to it, or repairs it when a task failed), defers on one `DatabricksRunTrigger` until the run is over and keeps its snapshot in XCom; one `PipeloomDatabricksTaskSensor` per task key and one on the job's finalize task mirror the results. The Airflow connection maps to explicit credentials, never a profile (PLD010 when it cannot be used). `databricks_poll="per_task"` is the default and mirrors the run live: the launch returns right after run-now or repair, and each sensor makes one `get_run` call and, without a verdict, defers on its own trigger (bounded by `databricks_timeout`, 24 hours by default, 5 transient API errors in a row are retried with doubling sleeps). `databricks_poll="shared"` keeps one whole-run trigger and sensors that read its final snapshot.
- `pipeloom_databricks.api.connect_explicit` builds a client from a host and a token or OAuth client credentials, ignoring `~/.databrickscfg` and `DATABRICKS_*` variables.
- The `airflow` dependency group, the `airflow` CI job (locked latest and the 3.2.2 floor), `scripts/wsl-airflow-tests.sh` and `make airflow-test`. Airflow tests opt in with `PIPELOOM_TEST_AIRFLOW=1` and refuse to start unless Airflow points at a throwaway home and sqlite database.
- Docs: a new Airflow plugin page (installation with constraint files, configs, load methods and the PROJECT cache, modes, artifact store per executor, run id, retries per mode, `full_refresh`, assets, finalize, the Databricks connection, poll modes and their API cost, limits).
- Docs: architecture sections 7, 11, 13, 14, 15 and 16 describe the 0.6 Airflow plugin as built (floor and extras, `ExecutionConfig`, artifact store, retry table, `shared` poll mode, PLA001 to PLA007, `orchestrator_ids` keys, connection handling, the `airflow` CI job and its isolation and parse budgets); the shared artifact store question is decided. The `07-airflow-modes` diagram shows the run id formula, the shared artifact root and the poll modes.

## [0.5.0] - 2026-11-05

pipeloom 0.5 adds the `pipeloom-databricks` plugin: rendering to Databricks Jobs and bundles, direct deploy, run, repair and finalize with task states, the `unity` catalog and a `databricks` secret provider. The core gains generic orchestration hooks: the `deploy` block, plugin commands, runtime secret resolution, `--orchestrator-id` and `pipeloom finalize`. Plugin API stays (0, 4).

### Added

- `pipeloom-databricks`: the `unity` catalog (`catalog: {kind: unity, catalog: main}`, Spark engine only; three-part backticked names, the schema is created only when absent) and the `databricks` secret provider (`${secret:databricks/<scope>/<key>}`, no profile auto-selection, failures are PLX109 without the value).
- Docs for 0.5: the Databricks plugin page (install, connection rules, deploy block, compute, grouping, bundle and direct deploy, run, repair, finalize, task values, secrets, Unity catalog, limits), a CLI reference page for `pipeloom databricks`, the architecture sections 4, 6, 7, 11, 12, 13, 14, 15 and 16 as delivered, and the Databricks diagram redrawn for the shipped design.
- `databricks-live`, a manually dispatched workflow (protected environment `databricks-live`, never triggered by regular CI) that deploys `tests/fixtures/databricks_live`, runs, selects and repairs it on a real workspace and prints `CHECK <fact> ok|failed` for the facts no offline test can confirm. `tools/databricks_live.py` holds the checks; its flow is unit-tested against `FakeDatabricks`. CONTRIBUTING explains the environment and secrets.
- Orchestration hooks in the core (ADR-0034, plugin API stays (0, 4)): a `deploy` runtime key, bound against the `renderers` group and never built by the core, with its validated options recorded in the manifest as `metadata.deploy` outside the plan hash; `manifest_bytes(plan, stable=True)` and `write_manifest(..., stable=True)` give byte-stable manifests.
- The `pipeloom.commands` entry-point group adds plugin commands to the CLI. They are discovered from metadata and imported only when invoked; built-in names win, duplicate names resolve by distribution name, a command that cannot be imported exits 1, and `PIPELOOM_DISABLE_PLUGIN_AUTOLOAD` hides them. `plugins list --group commands` lists them.
- `${secret:provider/path}` references in plugin options resolve at run time (`run`), through the `secret_providers` group, and are masked in events, attempt files, state rows and `run_results.json`, in both boundaries, also under secret-like keys. `$${secret:...}` stays literal text. They are never resolved by compile or validate, and never reach the manifest. PLX109 reports one that cannot be resolved (the blueprint's PLX108 was already taken).
- `run --orchestrator-id KEY=VALUE` (repeatable) records orchestrator ids in every attempt file, in `orchestrator_ids` of each result row and of the run, and in the `context` of every event (`orchestrator_id.<key>`). Bad keys or values are exit 2. `RunOptions.orchestrator_ids` is the API.
- `pipeloom finalize --run-id ID` (`Project.finalize`) merges the unit results of a run an orchestrator drove unit by unit: attempt file, else the state in `--unit-states` (`pipeloom.unit_states/1`), else `skipped`: `upstream_failed` when an upstream failed, otherwise the new reason `not_run`, which does not fail the run; `excluded` drops the unit. It refuses (PLX105, exit 1) while a `pipeloom run` of the same run id is live (its lock is held and its run record is `running`). It records the run, deletes expired intermediates, emits `run.completed` and rewrites the same result when repeated. Exit 0, 1 or 130; a failed unit wins over a cancelled one.
- `write_json_once` creates the file with an exclusive open when the file system refuses hard links (EPERM, ENOTSUP, EOPNOTSUPP, EXDEV, ENOSYS), such as a network volume.
- `pipeloom-databricks` (ADR-0035, first half), new in this release: a renderer for the `deploy` block (`kind: databricks`) that turns a plan into Databricks Jobs settings JSON, bundle YAML and `runs/submit` bodies. Rendering is pure (no network, no SDK, no credentials) and byte-deterministic. Supports serverless and job-cluster compute, `compute_rules`, several pipelines, `task_grouping` (`none`, `by_tag`, `by_folder`) and a finalize task. Install with `pipeloom[databricks]`.
- PLD001 (more than 1,000 tasks), PLD003 (task groups form a cycle), PLD004 (invalid Databricks deploy options) and the warning PLD005 (a compute rule matches no model). PLD002 stays reserved and is never emitted.
- `tools/versions.py` checks and bumps a member that pins another member (`pipeloom-spark~=X.Y.0` in `pipeloom-databricks`). `import-linter` keeps the core free of `pipeloom_databricks` and `databricks`, and the SDK out of every plugin module except the client ones.
- `pipeloom databricks bundle --target T --out DIR` (command added by `pipeloom-databricks` through `pipeloom.commands`) writes a job file and a stable manifest per pipeline into a Declarative Automation Bundle, with `${workspace.file_path}` paths; `--check` lists stale files and exits 1. Tasks run the new `pipeloom_databricks_task` wrapper, which resolves the run id, rejects unresolved `{{...}}` parameters (PLD013), calls the core in process, sets a size-capped task value (best effort) and exits with the core's code.
- `pipeloom_databricks.testing.jobsim.simulate_job` runs a rendered job locally through the wrapper, with `run_if`, `only` and repairs.
- PLD013: a task parameter still holds a `{{...}}` reference.
- `pipeloom-spark`: with `DATABRICKS_RUNTIME_VERSION` set and no active session, mode `active` attaches with `SparkSession.builder.getOrCreate()` and no conf (a wheel task starts without a session). The session is never configured or stopped. `active_session()` tries the thread's session first, then the default session another thread built.
- `pipeloom databricks deploy` (ADR-0036): uploads the project to the workspace, content addressed, and creates or resets one job per pipeline, found by its `pipeloom_job` tag. `--dry-run` prints the settings and connects to nothing; `--profile` is a group option and no profile is ever picked automatically (`DATABRICKS_CONFIG_PROFILE`, `DATABRICKS_HOST`, the runtime, else PLD010). The client calls the Jobs API 2.2 through the SDK's `api_client.do`; `pipeloom_databricks.api` and `pipeloom_databricks.testing.fake.FakeDatabricks` are new.
- PLD006 (two jobs carry the same `pipeloom_job` tag), PLD009 (Databricks API request failed), PLD010 (no Databricks connection configured) and PLD012 (a project file cannot be uploaded: a notebook, above 10 MB or outside the project).
- `pipeloom databricks run` (start the deployed job, `--ensure`, `--ephemeral`, `--select`, `--wait`) and `pipeloom databricks repair` (rerun failed, cancelled and skipped tasks). The finalize task now reads task states from the Jobs API, so a task that died without an attempt file fails or cancels the run. `pipeloom_databricks.api` gains `run_job`, `find_run`, `repair` and `task_states`; `simulate_job` takes a `FakeDatabricks` to publish task states.
- PLD007 (job or run not found), PLD008 (deployed job out of date), PLD011 (task ended without a unit result), PLD014 (run still active) and the warning PLD015 (a selection covers part of a task group; the whole group runs). `run` and `repair` take `--timeout SECONDS` (default 24 hours) for `--wait`.

## [0.4.0] - 2026-11-05

pipeloom 0.4 adds the `pipeloom-spark` engine (Spark classic and Spark Connect) with Delta writes, idempotent commits, metrics from the commit history, quality checks and a Delta table state store, plus intermediate models on every engine, on plugin API (0, 4).

### Added

- `intermediate` materialization on the memory, polars and pandas engines (ADR-0030, amends ADR-0007): the result is written to `<artifacts.root>/runs/<run_id>/intermediate/<model>` and read by its consumers; `relation_name` in `run_results` is that location. PLP413 rejects `incremental`, a strategy other than overwrite and `partition_by` on an intermediate.
- `artifacts.intermediate_ttl` (default 1d; 0 deletes at the end of a succeeded run). Finalize deletes expired intermediates; a deletion error is reported in the new `run.warnings` of `run_results`, never as a failure.
- `EngineContract` gains the intermediate tests (write then read, repeated token, one folder per run id).
- Plugin API (0, 4), ADR-0030: `RuntimeContext` gains `run_started_at` and `artifacts_root`; `EngineSession` gains `write_intermediate`; `CatalogCapabilities(views=)` and `IntermediateRef` are new types. Plugins built for (0, 3) fail with PLU001.
- `run --run-started-at ISO8601` (environment `PIPELOOM_RUN_STARTED_AT`) sets the start time every engine sees. It needs an offset and `--run-id`; a reused run id keeps its recorded start, and a different value is PLX108 (exit 2).
- The engine session opens before any state-store call and the run lock (so it has no `run_started_at`; engines read `WriteConfig.run_started_at`, new and additive), and `unit_scope` now spans a unit's reads, body, checks and write.
- PLC507 also fires when the catalog declares no views (`CatalogCapabilities(views=False)`).
- `pipeloom-spark` (ADR-0031): a Spark engine, new in this release, with the `delta_path` and `spark_catalog` catalogs. It reads `delta`, `parquet`, `csv`, `json` and `ndjson`, runs SQL models with DataFrame-bound aliases (no session temp views), cancels through Spark job tags, and opens a `local`, `connect` or `active` session; it never sets conf on a session it did not build. pyspark is not a hard dependency: `pipeloom[spark]` pulls `pipeloom-spark[local]` (`pyspark[connect]` 4.0.1 to 4.2, `delta-spark` 4.1 to 4.4).
- CI has a `spark` job (Java 17, three parallel legs: classic contract, classic rest and Connect), not yet a required check, and `typecheck` covers the Spark plugin. `make spark-test` prints the WSL recipe. Spark tests opt in with `PIPELOOM_TEST_SPARK=classic|connect`.
- `tools/versions.py` accepts a root extra that pins a distribution with an extra, as in `pipeloom-spark[local]==X.Y.Z`.
- Spark Delta writes (ADR-0032): `append`, `overwrite`, `merge` and `replace_partitions` (`replaceWhere`), the `view`, `incremental` and `intermediate` materializations, idempotent writes (`txnAppId`, `txnVersion` and a token in `userMetadata`; a MERGE is followed by an empty token commit) and row and byte metrics read from the commit history. No Spark conf is set.
- PLM207: Delta skipped the write because a later transaction version exists for the model (a newer or overlapping run, or clock skew); permanent for retries. `txn_guard: false` turns the guard off.
- `EngineContract`: `test_metrics` and `test_two_threads_materialize_at_once` skip for an engine without the `overwrite` strategy.
- Spark expectations and quarantine: the Spark engine declares `quality` and evaluates every count and aggregate of a model's checks in one Spark job; dropped rows go to the quarantine table with the usual `_pipeloom_*` columns.
- `table` state store in `pipeloom-spark` (ADR-0033): watermarks, runs, units and the run lock in one Delta table, by catalog name (`table`) or by path (`path`). Compare-and-set and the lease lock are single `MERGE` statements, Delta conflicts are retried, and the store reuses the engine's Spark session. It is shared (`shared=True`), so `boundary: task` can use it.
- Spark tests that start a second JVM opt in with `PIPELOOM_TEST_SPARK_HEAVY=1`. CI does not set it yet: on CI the child JVM stalled while resolving the Delta packages.

### Fixed

- Intermediate cleanup no longer keeps a resumed run that crashed again forever: a run whose manifest is newer than its `run_results.json` is now judged like a run without results (deleted once idle for `max(ttl, 1h)`), so an active resumed run is still kept.

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

[Unreleased]: https://github.com/emanuel-luis/pipeloom/compare/v0.9.0...HEAD
[0.9.0]: https://github.com/emanuel-luis/pipeloom/compare/v0.8.0...v0.9.0
[0.8.0]: https://github.com/emanuel-luis/pipeloom/compare/v0.7.0...v0.8.0
[0.7.0]: https://github.com/emanuel-luis/pipeloom/compare/v0.6.1...v0.7.0
[0.6.1]: https://github.com/emanuel-luis/pipeloom/compare/v0.6.0...v0.6.1
[0.6.0]: https://github.com/emanuel-luis/pipeloom/compare/v0.5.0...v0.6.0
[0.5.0]: https://github.com/emanuel-luis/pipeloom/compare/v0.4.0...v0.5.0
[0.4.0]: https://github.com/emanuel-luis/pipeloom/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/emanuel-luis/pipeloom/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/emanuel-luis/pipeloom/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/emanuel-luis/pipeloom/compare/v0.0.2...v0.1.0
[0.0.2]: https://github.com/emanuel-luis/pipeloom/releases/tag/v0.0.2
