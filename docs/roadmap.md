# Roadmap

Status: plan as of 2026-11-06, written against 0.7.0. This page replaces the planned versions that section 16 of
[architecture.md](architecture.md#16-roadmap) used to list; section 16 keeps the record of what each delivered
version contains. Items that change a decision name the ADR they need: ADRs up to 0041 exist, and 0042 to 0049
are proposed.

Terms such as unit, run, boundary and artifacts root are defined in the
[glossary](architecture.md#2-glossary). In short, a unit is one schedulable node of the plan (a persisted model
plus the ephemeral models inlined into it), and the artifacts root is the folder where each run writes its
manifest, results, events and logs.

## 1. Vision and principles

pipeloom should make a pipeline cheap to write, check and run anywhere: one YAML project, one compiled plan, any
engine, any orchestrator, and every run observable without extra code. The roadmap borrows the developer
experience of dbt, the orchestrator integration of astronomer-cosmos and the plan and diff safety of SQLMesh. It
leaves out the parts that would make pipeloom heavier: Jinja, a server process and a portable SQL dialect.

The 13 principles in section 1 of the architecture still apply. These five guide the choices below:

1. Simplicity first. A feature that only needs a flag gets a flag, not a new subsystem. New commands reuse
   `pipeloom.api.Project` and the existing stages.
2. Engine-agnostic. Every new feature goes through the existing protocols (`EngineSession`, `Catalog`,
   `StateStore`, `EventSink`) or declares the capability it needs.
3. Artifacts are the integration contract. Exporters, the docs site, the UI and lineage read `manifest.json`,
   `run_results.json`, the event JSONL and the attempt files, and never reach into the runtime.
4. Observable by default. Every new behavior emits typed events on the bus (`pipeloom.events`), and every consumer
   is a sink.
5. Export over lock-in. pipeloom renders to orchestrators and leaves scheduling, job-level retries and any UI that
   triggers runs to them.

The non-goals that stay through 1.0 are listed in [section 5](#5-non-goals-and-risks). One existing non-goal
changes: the UI.

### The UI non-goal

The architecture lists "a scheduler service or UI" as one non-goal, but the two carry different risks. A scheduler,
or a UI that writes, needs authentication, a database and a daemon, which amounts to a second product. A read-only
viewer over files pipeloom already writes needs none of that, and it makes the observability work visible.
[ADR-0043](adr/0043-docs-site.md) (accepted) replaces the line with "a scheduler service, or any UI that starts, schedules or changes runs".
A read-only viewer over the artifacts root and the state store then becomes allowed. It reads local or mounted
paths, listens on localhost by default and ships as a separate distribution, so the core stays small.

## 2. Where pipeloom stands at 0.7

- Authoring: YAML projects with `_defaults.yml` cascades, SQL or Python models with declared `inputs`, sources, five
  materializations, write strategies (append, overwrite, replace_partitions, merge with deletes; `scd2` is in the
  config literal but no engine implements it), a per-model `catalog`
  ([ADR-0040](adr/0040-per-model-catalog.md)), and `${env}`, `${var}` and `${secret}` interpolation.
- Validation: layers L0 to L6 with file, line and column diagnostics and a generated error catalog. JSON Schemas are
  published under `latest/schema/v1/`, and the `yaml-language-server` modeline gives completion in editors.
- CLI: `validate`, `compile` (with `--stable`), `ls`, `run`, `finalize`, `graph` (text, dot, mermaid, json),
  `schema`, `plugins`, and plugin commands (`pipeloom databricks bundle|deploy|run|repair`). `init` and block-style
  YAML land in 0.8.
- Runtime: a ready-queue scheduler, thread and process executors, retries, timeouts, cancellation, idempotent
  writes, watermarks, and expectations with quarantine. Engines: polars, pandas, and Spark with Delta.
- Observability: the `pipeloom.event/1` stream (run, unit, model and state events) through `EventBus`, with the
  console and jsonl sinks (either can be `buffered: true`) and redaction. Each run writes `runs/<run_id>/` with the
  manifest, `run_results.json` (rows, bytes, watermarks, quality, schema changes, orchestrator ids), per-invocation
  events, attempt files and logs. Run and unit records go to the state store (`record_run`, `get_run`,
  `list_units`); runs cannot be listed.
- Export: `pipeloom-databricks` (jobs, bundle YAML, deploy, repair) and `pipeloom-airflow` (LOCAL, VIRTUALENV,
  KUBERNETES and DATABRICKS modes, `emit_assets`). Selector methods: `name`, `tag`, `path`, `materialized`, `unit`.

### Gaps against dbt, cosmos and SQLMesh

| Feature | pipeloom 0.7 | Gap | Target |
|---|---|---|---|
| Scaffold and first run | `init` in 0.8 | no sample data on Spark targets, no `new model` | 0.8, 0.9 |
| Editor support | JSON Schema and modeline | no workspace setup, no ref navigation, no live diagnostics; dbt ships a language server and a VS Code extension ([dbt Fusion](https://docs.getdbt.com/docs/fusion/about-fusion)) | 0.8 (setup), 1.x (language server) |
| State-based selection and defer | node `checksum` and stable manifests exist | no `state:modified`, no `--defer` | 0.8 |
| Rerun failed units | [ADR-0013](adr/0013-run-identity-and-reruns.md) designs `pipeloom retry` | not built | 0.8 |
| Docs and lineage site | `graph --output mermaid` | no browsable site | 0.8 |
| Unit tests for models | none for users | no tests with fixed input rows and expected output rows | 0.9 |
| Data tests | expectations on the batch in flight | no post-write table tests, no relationships check, no source freshness | 0.9, 1.x |
| Contracts | schema-change policy | no declared column types enforced at write | 0.9 |
| OpenLineage | designed in section 11 | not built | 0.9 |
| Run history | per-run artifacts | no `list_runs`, no history command | 0.9 |
| Orchestrator fast mode | one task per unit | no single-producer mode like the cosmos watcher ([cosmos](https://github.com/astronomer/astronomer-cosmos)) | 0.9 |
| Public manifest spec | `pipeloom.manifest/1` described in prose | no published JSON Schema for artifacts | 1.0 |
| Dagster, Prefect and others | none | no exporter | 1.x |
| Plan, diff and virtual environments | `per_user` naming only | no plan and apply ([SQLMesh](https://sqlmesh.readthedocs.io)) | partial, through `--defer` |
| Column-level lineage | `sqlglot` extra used for PLK603 | not computed | 1.x |
| UI | none | read-only viewer | 0.8 (static site), 1.1 (`pipeloom ui`) |

## 3. Themes

### Authoring simplicity and developer experience

- `pipeloom init` (0.8). Every generated project runs with one command on local engines, with a small CSV under
  `data/`. Templates also write the schema modeline in every YAML file and a `.vscode/settings.json` that maps
  `yaml.schemas` to the published schema URLs, so completion works without an extension.
- `pipeloom new model <name> [--sql|--python] [--folder]` (0.8). Writes the YAML entry and, for Python, a
  `build(ctx, **inputs)` stub whose parameters satisfy PLK602. A few scaffold templates are enough; no generator
  framework.
- `pipeloom explain <CODE>` (0.8). Prints the catalog entry, the hint and the docs URL from
  `pipeloom.diagnostics.codes`. The console failure block links to it.
- Selection by state, and defer (0.8). `--state <dir>` points at a reference `manifest.json`, ideally one from
  `compile --stable`. New selector methods in `graph/selectors.py`: `state:modified` (the node `checksum` differs or
  the node is new), `state:new`, and `result:fail|skip` against a reference `run_results.json`. `--defer` reads
  unselected parents from the reference manifest's relations instead of this target's, so a dev run of one model
  reads the production upstreams. Defer changes only how the runner resolves reads, never the plan hash (the
  fingerprint that ties a run to its compiled plan).
- `pipeloom retry --from <run_id>` (0.8), as [ADR-0013](adr/0013-run-identity-and-reruns.md) designs it: a new run
  with `parent_run_id` that selects the failed, skipped and cancelled units of the parent.
- Sample data on Spark and Databricks (0.9). A source can point at a CSV inside the project. The polars and pandas
  engines already read it, and `pipeloom-spark` needs the same reader on volumes. This covers what dbt seeds do
  without a new materialization.
- SCD2 write strategy (0.9). `scd2` is already in the config literal; the polars and Spark engines implement it, as
  an amendment to [ADR-0008](adr/0008-incremental-and-state.md).
- `pipeloom build` (0.9). Runs the unit tests, then `run` with post-write data tests in DAG order. `run` keeps its
  meaning.
- Dev environments. `per_user` naming already isolates developers. A guide page plus `--defer` covers most of what
  SQLMesh virtual environments give; zero-copy promotion stays out (section 5).
- Language server (1.x). A `pipeloom-lsp` distribution (pygls) reuses the `config.load` marks and the `validate`
  diagnostics for go-to-definition on `ref`, hover with the resolved relation, completion of model and source names,
  and diagnostics on save. A thin VS Code extension wraps it. It is the largest developer experience item, so it
  waits until the config freezes at 1.0.

### Testing and data quality

- Model contracts (0.9). An optional `columns` list on a model, each entry with `name`, a logical `dtype` and
  `nullable`. At write time the core compares it with `EngineSession.schema` before `align`, and a mismatch is a new
  permanent PLM code. Declared columns also feed lineage and the docs site.
- Unit tests (0.9). A `tests` block per model, or files under `tests/`, with `given` (rows per input alias, inline
  or from a CSV) and `expect` (rows, compared without regard to order). `pipeloom test` runs the model body on the
  target's engine with no catalog writes and compares with the semantics in `pipeloom.testing.reference`, the same
  way for SQL and Python models.
- Post-write data tests (0.9). Expectations judge the batch before the write. A model-level `tests` list with
  `unique`, `not_null` and a new `relationships` test (keys `column`, `to` and `field`) runs against the written
  table. It reuses `evaluate` and `quality.judge`, and results go to `run_results.results[].expectations` with
  `scope: table`.
- Source freshness (1.x). A `freshness` block on sources (keys `column`, `warn_after` and `error_after`), checked by
  `pipeloom sources freshness`. It reuses the freshness logic in `quality/judge.py` and emits
  `source.freshness_checked`.

### Export to other platforms

Dagster, Prefect and cosmos all consume dbt the same way: a manifest with nodes, `depends_on` and relation names,
plus run results ([Dagster dbt component](https://docs.dagster.io/integrations/libraries/dbt/dbt-component),
[Prefect dbt runner](https://docs.prefect.io/integrations/prefect-dbt/runner)). pipeloom already writes that shape,
so the first step is to publish it as a contract.

- Public artifact specs and the orchestrator protocol (1.0). Publish JSON Schemas for `pipeloom.manifest/1`,
  `pipeloom.run_results/1`, `pipeloom.event/1` and `pipeloom.unit_states/1` next to the config schemas. One page
  describes the protocol any orchestrator follows: run id rules,
  `pipeloom run --manifest --select unit:<id> --run-id --expect-plan-hash`, exit codes and retry ownership, and
  `pipeloom finalize --unit-states`. [ADR-0034](adr/0034-core-orchestration-hooks.md) already defines the behavior,
  so this is documentation plus golden tests (tests that compare output with committed reference files).
- Unit graph export (1.0). `pipeloom graph --units --output json` emits units, edges, relation names, assets and
  the command line of each unit. Kestra, Argo or GitHub Actions can then be targeted with a short recipe in the docs
  instead of one plugin each.
- Airflow WATCHER mode (0.9), after the cosmos watcher design: one Airflow task runs the whole selection, and one
  lightweight sensor per unit reports that unit's result. The producer task runs `pipeloom run --run-id <rid>` with
  in-process workers and scheduler retries. Each deferrable sensor watches
  `runs/<rid>/units/<unit_id>/attempt-*.json` on the shared artifacts root that
  [ADR-0038](adr/0038-airflow-execution-modes-and-artifact-store.md) already requires. Clearing a failed sensor
  reruns that unit alone, as LOCAL mode does. Assets and finalize stay as they are. Most of it reuses `render.py`,
  `states.py` and the DATABRICKS mirror sensors.
- Databricks. Bundle YAML stays the deploy path
  ([Declarative Automation Bundles](https://docs.databricks.com/aws/en/dev-tools/bundles/resources)). The
  `load_resources` hook for Python bundle resources (section 12) moves to 1.x. Exporting to Lakeflow Spark
  Declarative Pipelines stays research: it would hand expectations, watermarks and writes to Databricks and lose
  pipeloom's events, so it needs its own ADR before any code.
- Dagster and Prefect (1.x). `pipeloom-dagster` builds a `multi_asset` per unit from the manifest, with asset keys
  from `relation`, and each materialization calls the unit command, so Dagster sees one asset per model. Prefect
  first gets a docs recipe over the unit graph JSON.
- Snowflake. It has no place before a Snowflake engine plugin exists; dbt Projects on Snowflake runs dbt itself.
  Recorded as a non-goal for 1.0.

### End-to-end observability

- `pipeloom-openlineage` sink (0.9). Maps unit events to OpenLineage RunEvents (START, COMPLETE, FAIL, ABORT) with
  the job `<project>.<model>` and a parent run facet for the pipeloom run. Input and output datasets come from the
  manifest sources and relations. Facets: schema (from contracts, or a new additive `columns` field on
  `model.written`), output statistics (rows and bytes), data quality assertions (from `model.quality_evaluated`) and
  error message ([OpenLineage facets](https://openlineage.io/docs/spec/facets)). Transport goes through the
  openlineage-python client, and Marquez is the reference for testing. Under Airflow the operators implement
  `get_openlineage_facets_on_complete`, so the Airflow provider emits with the DAG as parent and the sink stays off
  to avoid duplicate events.
- Column-level lineage (1.x). For SQL models with the `sqlglot` extra, computed at compile time and stored as
  `nodes[].column_lineage` in the manifest, outside the plan hash. Python models report none unless contracts
  declare it. The OpenLineage sink and the docs site both read it.
- Run history (0.9). `StateStore.list_runs(project, target, limit, before)` enters the plugin API (the versioned
  protocol plugins implement) at version (0, 5), with a default that raises a clear error on stores that lack it.
  `pipeloom runs ls` and `pipeloom runs show <run_id>` print from the store or, with `--artifacts`, from
  `runs/*/run_results.json`.
- Alerting (0.9). A `webhook` sink posts selected event types (by default `run.completed` with a failed status, and
  warnings above a level) to a URL given as `${secret:...}`. It uses stdlib HTTP, runs with `buffered: true` and has a
  failure budget, which is enough for Slack, Teams or a pager.
- OpenTelemetry (1.x). A `pipeloom-otel` sink: the run as the root span, unit attempts as child spans, events as span
  events, and counters for rows, bytes, retries and durations. The trace id is derived from `run_id`, so every
  invocation of a run (process executor, Airflow tasks, Databricks tasks) joins one trace without propagation.
  Attribute names live under `pipeloom.*` and are marked experimental, since OpenTelemetry has no official semantic
  conventions for data pipelines.
- Cost and volume. Rows and bytes per model are already in `run_results`. With run history, a `row_count` check gains
  a `change_vs_last` option (keys `runs` and `max_ratio`) in 1.x. Databricks cost stays in its system tables, joined
  through `orchestrator_ids`, and the docs show how.

### UI

- Step 1: `pipeloom docs generate` and `pipeloom docs serve` (0.8). A static site in `target/docs/` built from the
  manifest and, when present, the latest `run_results.json`: a model list, model pages (config, SQL or Python body,
  inputs, relation, expectations, contract, last result) and a lineage graph, in one HTML file that works offline.
  Python renders the HTML with the stdlib, the graph is an inline SVG laid out in Python (no JavaScript library, so
  no license or update work and a golden-testable output), and `serve` is the stdlib `http.server`. The interactive
  graph is left to `pipeloom ui`. Decided in [ADR-0043](adr/0043-docs-site.md) (accepted).
- Step 2: `pipeloom ui` (1.1), in a `pipeloom-ui` distribution. A local read-only server (stdlib or one small ASGI
  dependency, chosen in its ADR) over an artifacts root and, optionally, a state store. Views: the DAG colored by the
  last run, the run list, a run timeline (a Gantt chart from `started_at`, `ended_at` and `queue_wait_ms`), unit logs
  from `logs/`, the event stream (tailing the JSONL while a run is live), and quality and schema-change history. Data
  preview is opt-in (`--preview`) and reads at most N rows through the engine reader, on local targets only. The UI
  never starts, retries or cancels a run; it shows the command to copy instead. Kedro-Viz and Elementary work the
  same way: they show metadata and leave runs to the tools that start them.

## 4. Milestones

Each milestone has one headline and is sized for one maintainer. Items in the tables link back to the themes above.

### 0.8: the first hour and fast loops

| Item | ADR | Acceptance |
|---|---|---|
| `init` and block-style YAML | ADR-0041 | `pipeloom init demo --engine polars`, then `pipeloom run` in `demo`, exits 0 in a clean virtualenv in CI on three operating systems |
| Editor setup in templates, `pipeloom new model` | ADR-0041 amendment | a new model validates with no PLK602 and has completion in VS Code with only the YAML extension |
| `pipeloom explain` | none (the catalog exists) | every code in `codes.py` prints, tested by iterating the catalog |
| `state:modified`, `state:new` and `result:` selectors, `--state`, `--defer` | ADR-0042 (proposed) | changing one SQL body selects exactly that node, and `--defer` reads an unselected parent from the reference relation (golden test) |
| `pipeloom retry --from` | [ADR-0013](adr/0013-run-identity-and-reruns.md) | retrying a failed run runs only the failed units and their skipped descendants, with `parent_run_id` set |
| `pipeloom docs generate` and `serve` | [ADR-0043](adr/0043-docs-site.md) | the TPC-H examples produce one HTML file that opens offline and shows the graph and every model |

### 0.9: test it, see it, run it faster

| Item | ADR | Acceptance |
|---|---|---|
| Model contracts (`columns`) | ADR-0044 (proposed) | a type mismatch fails before the write with a new PLM code on polars, pandas and Spark (the shared engine contract suite) |
| `pipeloom test` and `pipeloom build` | ADR-0045 (proposed) | a case whose expected rows match passes, and a case with one wrong row fails with a row diff, on polars, pandas and Spark |
| Post-write data tests with `relationships` | ADR-0045 (proposed) | results appear in `run_results` with `scope: table` |
| Sample data on Spark and Databricks | none | a CSV source in the TPC-H Spark example runs locally and on a Databricks volume |
| SCD2 write strategy | [ADR-0008](adr/0008-incremental-and-state.md) amendment | the SCD2 cells of the engine contract suite pass on polars and Spark |
| `pipeloom-openlineage` sink and Airflow facets | ADR-0046 (proposed) | a TPC-H run in Marquez shows the full graph, the schemas and the quality assertions |
| Run history (`list_runs`, `pipeloom runs`) and the `webhook` sink | ADR-0047 (proposed), plugin API (0, 5) | `pipeloom runs ls` lists the last N runs on the sqlite, json and table stores, and a failed run posts once |
| Airflow WATCHER mode | ADR-0048 (proposed) | the examples' Astro project runs in WATCHER mode with one producer, a sensor and an asset per model, and clearing a failed sensor reruns one unit |

### 1.0: stable contracts

| Item | ADR | Acceptance |
|---|---|---|
| Published JSON Schemas for the manifest, run results, events and unit states | ADR-0049 (proposed) | the docs publish them under `schema/v1/`, and the goldens validate against them in CI |
| Orchestrator protocol page and `graph --units --output json` | ADR-0049 (proposed) | a Kestra or GitHub Actions recipe in the docs runs the TPC-H example end to end |
| `config_version: 1`, plugin API 1.0 and a deprecation policy | [ADR-0016](adr/0016-versioning-and-plugin-api.md) follow-up | a new `tools/versions.py check` step fails when a distribution declares a config version or plugin API other than the released one, and the changelog lists no breaking change after rc1 |
| Hardening and complete docs | none | `mkdocs build --strict` passes and every command and flag has a guide page |

### 1.x, roughly in this order

`pipeloom ui` (1.1, with a short ADR for the server choice), `pipeloom-otel`, column-level lineage from `sqlglot`,
source freshness and volume change checks, `pipeloom-dagster`, the Databricks `load_resources` hook, `pipeloom-lsp`
with the VS Code extension, the Airflow DOCKER mode and the `airflow` secret provider, research on a Lakeflow export,
and a Prefect recipe.

## 5. Non-goals and risks

Non-goals through 1.0: streaming; a scheduler service; any UI that starts, schedules or edits runs; hosted or
multi-user services; a portable SQL dialect; Jinja; user code at compile time; Airflow 2; SQLMesh-style virtual
environments with zero-copy promotion (pipeloom offers `per_user` naming plus `--defer`); a Snowflake target before a
Snowflake engine exists.

Risks:

- Scope. This is a lot for one maintainer. Each milestone keeps its headline (the first hour, testing and
  observability, stable contracts), and items move to 1.x instead of delaying a release.
- Artifact schema churn before 1.0. Contracts, column lineage and the `columns` event field all touch
  `pipeloom.manifest/1` and `pipeloom.event/1`. Changes stay additive, and the changelog records each one.
- Duplicate OpenLineage events under Airflow. The rule (the provider emits inside Airflow, the sink everywhere
  else) needs a test.
- WATCHER mode on shared storage. NFS attribute caching can hide fresh attempt files, so the sensor needs a bounded
  poll and the PLA004 sentinel.
- UI dependency weight. ADR-0043 (accepted) avoids it for the docs site: the graph is an inline SVG laid out in
  Python, with no vendored JavaScript library. `pipeloom ui` (1.1) ships separately, so any library it needs stays
  out of the core wheel; keep exactly one and pin it.
- Defer correctness. Reading production relations from a dev run needs read grants and the same schema. A missing
  relation must fail with a clear PLM204 hint.
