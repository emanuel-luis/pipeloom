# Roadmap

Status: plan as of 2026-11-21, written against 1.2.0. Versions 0.8 to 1.2 are delivered; this page now plans 1.3 to
1.9 and lists the breaking changes the stability policy holds back. Section 16 of [architecture.md](architecture.md#16-roadmap) keeps the record of what each
delivered version contains. The market research behind the plan is in [Market landscape](landscape.md). Items that
change a decision name the ADR they need: ADRs up to 0050 exist and are accepted, and new ones continue from 0051.

Terms such as unit, run, boundary and artifacts root are defined in the
[glossary](architecture.md#2-glossary). In short, a unit is one schedulable node of the plan (a persisted model
plus the ephemeral models inlined into it), and the artifacts root is the folder where each run writes its
manifest, results, events and logs.

The maintainer settled the open choices of this plan on 2026-11-20. Section 6 records each one and the alternative it
replaced. Scope inside a version can still move while its ADR is written.

## 1. Vision and principles

pipeloom should make a pipeline cheap to write, check and run anywhere: one project of Python models, one compiled
plan, any engine, any orchestrator, and every run observable without extra code. The roadmap borrows the developer
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

The non-goals are listed in [section 5](#5-non-goals-and-risks). Several change in this plan, and each change says
what it was before.

### The UI non-goal

Since [ADR-0043](adr/0043-docs-site.md) the non-goal is "a scheduler service, or any UI that starts, schedules or
changes runs". A read-only viewer over the artifacts root and the state store is allowed. It reads local or mounted
paths, listens on localhost by default and ships as a separate distribution, so the core stays small.

## 2. Where pipeloom stands at 1.0

- Authoring: Python models marked with `@model`, YAML for the project and properties files, SQL through `ctx.sql`,
  five materializations, write strategies (append, overwrite, replace_partitions, merge with deletes, scd2), per-model
  catalogs, contracts, unit and data tests, and `${env}`, `${var}` and `${secret}` interpolation.
- Validation: layers L0 to L6 with file, line and column diagnostics, a generated error catalog and published JSON
  Schemas under `schema/v1/`.
- CLI: `init`, `new model`, `validate`, `compile`, `ls`, `run`, `build`, `test`, `retry`, `finalize`, `graph`, `docs`,
  `explain`, `runs`, `schema`, `plugins`, plus plugin commands.
- Runtime: a ready-queue scheduler, thread and process executors, retries, timeouts, cancellation, idempotent
  writes, watermarks, expectations with quarantine, `state:modified` selection and `--defer`.
- Observability: the `pipeloom.event/1` stream, console, jsonl, webhook and OpenLineage sinks, run history through
  `list_runs`, and versioned artifact schemas.
- Export and engines: polars, pandas and Spark with Delta; Databricks jobs and bundles; Airflow in LOCAL, VIRTUALENV,
  KUBERNETES, DATABRICKS and WATCHER modes. The plugin API is (1, 0).

### Gaps against the market

Evidence for each row is in [Market landscape](landscape.md). The pipeloom 1.0 column comes from a code check on 1.0.

| Area | pipeloom 1.0 | Gap | Target |
|---|---|---|---|
| Local SQL engine | polars, pandas, Spark | no DuckDB, which is the usual on-ramp for demos and CI | 1.1 |
| First run | `init` | no demo with sample data and a docs site in one command | 1.1 |
| Change safety | one `checksum` per node, covering config, body, columns and tests | no split checksums, no plan diff, no table diff | 1.2 |
| Backfills | watermark plus lookback | no interval windows, no interval ledger, no backfill command | 1.3 |
| Partitions in orchestrators | assets per unit | no partition keys in artifacts, no partitioned Airflow assets | 1.4 |
| History-based checks | `list_runs`, expectations with warn, drop or fail | no freshness, no volume change, no anomaly score, no reconciliation | 1.5 |
| Column lineage | none | `nodes[].column_lineage` not computed | 1.6 |
| Agent and UI surface | docs site | no generated agent skills, no read layer, no MCP server, no `pipeloom ui` | 1.2, 1.7 |
| Open table formats | Delta through Spark | no Delta without a cluster, no Iceberg | 1.8 |
| Editor support | JSON Schema and modeline | no language server | 1.9 |

## 3. Themes

- Engines and the first hour (1.1, 1.8). DuckDB, a conformance kit that third parties can run, and a one-command
  demo. Delta without a cluster and Iceberg come later.
- Change intelligence (1.2, 1.6). Split checksums, then plan diff and table diff, then column lineage. This delivers
  the main value of SQLMesh plans without virtual environments.
- Intervals and orchestrators (1.3, 1.4). A ledger of processed windows, a backfill command, then partitions
  reaching Airflow and Databricks.
- Trust from run history (1.5). Freshness, volume, anomaly and reconciliation checks, schema contracts, and opt-in
  skipping of unchanged units.
- Agents and people (1.2, 1.7, 1.9). Machine-readable output and generated skills, a shared read layer, MCP, the UI
  and the language server.

Rules that hold for every 1.x release:

- The plugin API gets at most one minor bump per release.
- An optional protocol member lives in its own small `Protocol` and is reached through a core helper with a
  fallback, the way `state.types.list_runs` works today. Nothing is added to `EngineSession` or `StateStore` in 1.x.
- Anything that tracks a moving upstream ships as experimental.
- The 1.0 goldens must validate against the current /1 schemas, and the current output must validate against the 1.0
  schemas. CI guards both from 1.1.
- Every release that changes user config ships a `migrate-1.x` guide page, and every new command or flag gets a guide
  page and a CLI reference entry. Each release adds an Upgrading section to the changelog.
- Every ADR from 0051 on carries a Provenance line: the design comes from public docs and observed behavior, and no
  code was copied.
- Output that can carry row data (diff markdown, CI summaries, MCP, `ui export`) shows counts and schema only by
  default. Samples need an explicit opt-in flag, and columns tagged sensitive in contracts are masked.

Stability lifecycle, to be written into [stability.md](reference/stability.md) with the 1.1 ADR: an experimental
feature is promoted or removed within two minors, only the latest minor gets patches, and security fixes go to the
latest minor.

## 4. Milestones

Each milestone has one headline and is sized for one maintainer. Releases are time-boxed with a cut date, and stretch
items are cut first.

Summary by version:

| Version | Headline | What you can do after it | Status |
|---|---|---|---|
| 0.8 to 1.0 | First hour, Python-first authoring, stable contracts | Author, validate, run and export projects with published schemas | Delivered |
| 1.0.x | Upstream patches | Run on Polars 2.0, the current Databricks CLI and Airflow 3.3 and 3.4 | Delivered in 1.1.0 |
| 1.1 | DuckDB and a one-command demo | Run a project and a demo on a laptop with DuckDB | Delivered |
| 1.2 | Know what a change does before it merges | See the effect of a change with plan diff and table diff before merging | Delivered |
| 1.3 | Intervals and backfills | Backfill a date range and rerun only the missing windows | Planned |
| 1.4 | Orchestrators run partitions | Run one partition from Airflow, Databricks or Dagster | Planned |
| 1.5 | Run history does the work | Catch stale or unusual data from run history | Planned |
| 1.6 | Lineage and telemetry from the manifest | Read column lineage and follow a run as a trace | Planned |
| 1.7 | One read layer for people and agents | Browse runs in a local viewer and query them from an agent (the UI ships as 1.7.x) | Planned |
| 1.8 | Open table formats without a cluster | Write Delta without Spark and Iceberg on Spark | Planned |
| 1.9 | The editor knows your project | Get diagnostics and completion in the editor | Planned |

### Delivered

| Version | What it delivered |
|---|---|
| 0.8 | First hour and fast loops: `init`, `new model`, `explain`, `state:modified` and `--defer`, `retry --from`, `docs generate` and `serve` ([ADR-0042](adr/0042-state-selection-and-defer.md), [ADR-0043](adr/0043-docs-site.md)) |
| 0.9 | Python-first authoring ([ADR-0050](adr/0050-python-first-authoring.md)), contracts, unit and data tests, `build`, SCD2, OpenLineage, run history and webhook, Airflow WATCHER mode ([migration guide](guide/migrate-0.9.md)) |
| 1.0 | Stable contracts: published schemas including the manifest, the [orchestrator protocol](guide/orchestrators.md), `graph --units`, plugin API (1, 0) and the [stability policy](reference/stability.md) ([ADR-0049](adr/0049-artifact-schemas-and-orchestrator-protocol.md)) |
| 1.0.x | Delivered in 1.1.0, with no 1.0.1 release because nothing was broken for users: Databricks CLI version documentation, the Airflow 3.4 matrix and Polars 2 guards |
| 1.1 | Delivered in 1.1.0: DuckDB engine ([ADR-0051](adr/0051-duckdb-engine.md)), conformance kit ([ADR-0067](adr/0067-conformance-kit.md)), portable models guide and cross-engine matrix, `pipeloom demo`, `init --engine duckdb`, experimental tier in the stability policy. The stretch items Sail CI recipe and `show --limit` did not ship and are dropped |
| 1.2 | Delivered in 1.2.0: split checksums and `state:modified.<part>` ([ADR-0052](adr/0052-split-checksums-and-change-categories.md)), `plan --diff`, `pipeloom diff` ([ADR-0053](adr/0053-table-diff.md)), canonical dataset identity ([ADR-0055](adr/0055-canonical-dataset-identity.md)), machine-readable output and generated `AGENTS.md`, `ctx.sql` on pandas through DuckDB ([ADR-0068](adr/0068-pandas-sql-through-duckdb.md)), performance budget, docs overhaul. The stretch items Slim CI recipe on DuckDB, `validate --dry-run`, `llms.txt` and the Coming from dbt guide did not ship and stay stretch for a later minor |

The full list per version is in the CHANGELOG and in section 16 of the architecture.

Stretch (not shipped in 1.2, kept for a later minor): Slim CI recipe on DuckDB with a step summary and Azure DevOps, `validate --dry-run`, `llms.txt`
from `docs generate`, a "Coming from dbt" concept-mapping guide.

### 1.3: Intervals and backfills

- `incremental.interval` with event_time, grain, begin and lookback. Half-open UTC windows follow the rounding rules in the dbt microbatch docs. Batches run first, middle, last inside one unit, so the DAG and the renderers stay unchanged. Each interval gets its own idempotency token (for example `run_id:model:interval_id`) and its own Delta transaction version, as an amendment to ADR-0032. The range replace is an explicit `WriteConfig` field (`replace_range: column, start, end`) under the capability `interval_writes`, so an empty window or a partition coarser than the grain cannot leave stale rows or delete rows outside the window. A missing event_time fails at validation and never becomes a full scan. ADR: 0054 (amends 0032). Done when: Property tests cover the boundary +1 case, month and year truncation, and DST, and run on windows-latest (with a `tzdata` dependency). A 90-day backfill on Spark/Delta and polars writes exactly 90 windows. An empty window and a month partition under a day grain both leave other rows untouched.
- Interval ledger: merged intervals stored in the state store under a reserved key (document type `pipeloom.intervals/1`). The value is a list of ISO-string pairs, because the state codec does not convert datetimes nested in a list. Dev targets never write the prod ledger. The Delta store encodes values through the same codec, so the round trip holds there by code reading. ADR: 0054. Done when: The state contract suite round-trips the ledger on sqlite, json and Delta, and concurrent writers lose no interval.
- `pipeloom backfill --select --start --end [--rerun missing\|all] [--on-failure stop\|continue]`, deterministic interval ids, `restate` for a range, and `--mark-done` to seed the ledger for a model converted from a 1.0 watermark. The `migrate-1.x` guide covers the conversion. ADR: 0054. Done when: A range with one gap reruns only the gap. `--rerun missing` after success writes nothing. A killed run resumes only the missing intervals. Converting a 1.0 watermark model reruns zero windows.
- Outcome fields: documented open `skip_reason` values (`not_selected`, `up_to_date`, `not_due`, `upstream_incomplete`) with their failing class listed next to the existing ones, `failed_upstream`, `blocking`, and test summaries capped at N ids. No new status. The row schema first splits into the frozen 1.0 required set and optional additions, because every row property is required today, and the test of row defaults changes in the same release. ADR: 0056. Done when: A failing blocking test gives its descendants `upstream_failed` and `failed_upstream`. The 1.0 goldens still validate.
- `on_cursor_value_missing: raise\|include\|exclude`. ADR: 0027 (amendment). Done when: A case file for the raise code (PLM210), and a test and an engine contract cell for each of the three modes.

Plugin API: (1, 1): optional `filter_between` in its own `Protocol` with a portable default, capability `interval_writes=False`. Validation rejects interval models on engines without it. Fallback: optional StateStore methods in the same bump.

Stretch: `sequence_by` and ignore-null updates (capability `merge_sequencing` in the same (1, 1) bump; ties are an
error), an EL-then-T recipe with dlt (its `end_value` overlaps intervals), and, on SIGTERM, signaling the whole
process group and printing the retry command and the resume set (Job Objects or `CTRL_BREAK_EVENT` on Windows).

### 1.4: Orchestrators run partitions

- Typed partition spec derived from the interval. `partition_keys` in run_results, events and the OpenLineage run facet. Anchored-regex formats checked at validation. A completeness gate returns (satisfied, unreachable, reason). ADR: 0056. Done when: A bad format fails at validate. A missing upstream interval settles with `upstream_incomplete`. An up_to_date skip still emits the orchestrator signal (or a documented no-op) and counts as non-failing.
- Airflow work: opt-in experimental partitioned assets, templated `emit_assets`, WATCHER with bounded XCom and upstream-failed consumers that settle on attempt 1, select and exclude evaluated on the full graph, task ids derived only from unit ids, a 3.2 to 3.4 matrix, and the stable parse-time budget tests. pipeloom-airflow already requires `apache-airflow>=3.2,<4`, so partitions need no floor change. Done when: The matrix is green. On the Astro example, an asset event with key 2026-11-01 runs exactly that window. A 200-unit WATCHER run with one failure leaves bounded XCom.
- Databricks work: `window_start` and `window_end` job parameters, direct-engine YAML check, CLI range check with a code, the planned `load_resources` hook. Done when: `bundle validate` passes on the minimum and latest CLI in CI. A 7-day window run fills the ledger (workspace demo, not in CI).
- `pipeloom-dagster` (decided 2026-11-20, see section 6): a new distribution that reads `manifest.json` and builds one `multi_asset` per unit, with asset keys from the canonical dataset identity of 1.2. Each materialization runs the unit command of the orchestrator protocol, data tests become asset checks, and interval models map to Dagster time partitions through the partition spec above. Pinned to the public `dagster` API with a version matrix. ADR: 0066. Done when: the TPC-H polars example loads in `dagster dev` with one asset per model, materializing one asset runs exactly that unit, a failing data test shows as a failed asset check, and one partition of an interval model runs exactly that window.

Plugin API: unchanged.

Stretch: DOCKER mode and the `airflow` secret provider (moved out of core to make room for `pipeloom-dagster`), an
orchestrator conformance kit, and a Prefect recipe over `graph --units --output json`. The `airflow` dependency group still does not install on Windows
(openlineage-sql has no Windows wheel); that stays a documented limit and Airflow development happens on Linux,
WSL or CI.

### 1.5: Run history does the work

- `sources freshness`, `row_count.change_vs_last`, and a z-score over comparable runs (window 14, sigma 3, warmup 7). Backfill, full-refresh and retry runs stay out of the baseline, and it resets on a config hash change. Statistics in Python from `list_runs`. ADR: 0057. Done when: With a FakeClock history, only the injected outlier warns. Fewer than 2 points gives no score.
- `reconcile` data test: group_by, filters on both sides, abs or rel tolerance, and groups missing on either side. ADR: 0057. Done when: One missing group and one drifted group are reported on DuckDB and polars.
- `build --skip-unchanged`, opt-in. The fingerprint covers the code checksum, config, upstream fingerprints and data versions. The scheduler change is part of the ADR: an up_to_date skip releases children like a success, and the decision happens before QUEUED with a defined state transition. Data versions come from an optional `Catalog.data_version(target)` in its own `Protocol` (in-tree engines implement it; when it is missing the unit always runs, with a reason). External writes are not detected unless the engine reports a version. ADR: 0058. Done when: A second identical build runs zero units. Bumping one upstream Delta version reruns only it and its descendants.
- `schema_contract` entities (new_table, new_column, type_change) times modes (evolve, fail, drop_value, quarantine_row), with `--schema-contract freeze` for CI. `on_schema_change` maps onto it and is deprecated with a warning code, and it keeps working through 1.x. ADR: 0059. Done when: One case per entity and mode on the shared suite. Schema changes in run_results record the entity and the mode.
- ODCS v3 contract export, sharing one `Threshold(op, value)` type with the anomaly checks. Lossy mappings are a warning code. Done when: An exported contract validates against the ODCS schema, and a lossy field warns.

Plugin API: (1, 1) if the 1.3 bump is not reused.

Stretch: ODCS import, unit-test upgrades (partial rows, subset mode, tolerance, mocks), `pipeloom prune` (dry
run by default, with a retention floor of at least the largest anomaly window), Unity Catalog metadata push as a diff
(an optional `Catalog.apply_metadata` takes the next API bump).

### 1.6: Lineage and telemetry from the manifest

- `nodes[].column_lineage` outside the plan hash, with completeness per column, an indirect pass (filter, join, group, window), a cache keyed by SQL, dialect and schema hash, and `lineage --column`. sqlglot stays in a tested range bounded by major version, with a weekly job against the latest release and golden lineage tests. ADR: 0060. Done when: TPC-H goldens. The plan hash equals 1.5's. Unqualifiable SQL is marked incomplete and compile exits 0. The performance budget from 1.2 is rechecked.
- OpenLineage facet pass: columnLineage, dataQualityAssertions, SQL, job type, parent and root run, naming from the 1.2 identity function, and `lineage export --run`. Done when: Facets validate against the pinned facet schemas. A throwing sink never changes the exit code. Check the spec docs first for whether the 1.53 explicit lineage facets supersede the column facet.
- `pipeloom-otel`: the run is the root span, attempts are child spans, and the trace id is derived from run_id. ADR: 0061 (`pipeloom.*` plus the CI/CD semantic conventions). Done when: A 3-unit process-executor run gives one trace with 3 child spans in an in-memory exporter.

Plugin API: unchanged.

Stretch: UC external lineage push with dry run (verify GA first), Query annotations plus a cost recipe that
joins system tables through orchestrator_ids (the optional hook takes the next API bump).

### 1.7: One read layer for people and agents

The read layer and MCP ship first. The UI follows as 1.7.x with its own cut date, so the largest item cannot hold
the release.

- A read layer over manifest, run_results, events by offset, runs and catalog, placed in `pipeloom.artifacts` (below docs in the layer contract) and re-exported from `pipeloom.api`. Docs, UI and MCP all use it. Done when: `docs generate` output is byte-identical before and after the refactor, and `lint-imports` passes.
- `group_by` in unit_graph (additive /1). File and line already exist on plan nodes. Done when: Goldens updated, and the old goldens still validate.
- `pipeloom-mcp`: read-only tools (models, runs, node, explain, plan diff, lineage). No tool runs SQL. Artifact text is wrapped as data, the threat is described in SECURITY.md, and the distribution is separate with an import-linter rule that keeps it out of core. No row data unless the user opts in. ADR: 0062. Done when: An MCP client test lists the tools and asserts none mutates.
- `pipeloom-ui` (1.7.x, moved from 1.1): GET only, 127.0.0.1, run picker, DAG colored by the last run, event tail, `ui export`, path-traversal guard, never imports user code. Host and Origin allowlist, a random session token in the URL, no CORS. ADR: 0062 (server choice). Done when: A test asserts no non-GET route. A foreign Host is rejected. The traversal fuzz test returns 404, with a Windows corpus (drive letters, backslashes, UNC paths, reserved names, case-insensitive matching). Diffs, partitions, anomalies and lineage for TPC-H are viewable.

Plugin API: unchanged.

Stretch: `profile` and `test scaffold`.

### 1.8: Open table formats without a cluster

- Delta targets and a Delta state store through delta-rs for polars, pandas and DuckDB. ADR: 0063. Done when: The Delta contract cells pass on polars and DuckDB. A table written by polars reads in Spark with the same schema.
- `iceberg` format in pipeloom-spark against UC or REST catalogs, all strategies. ADR: 0064. Done when: Engine contract cells pass on local Spark with a containerized REST catalog.

Plugin API: unchanged.

Stretch: pyiceberg append and overwrite (current release 0.12.0; verify write support in its docs before writing
acceptance criteria), DuckDB attach of DuckLake or Iceberg (DuckLake 1.0 was released on 2026-04-13).

### 1.9: The editor knows your project

- `pipeloom-lsp`: diagnostics on save, go-to-definition from a parameter to its model, hover, completion. Works with Windows drive-letter file URIs. ADR: 0065. Done when: A pygls test client drives all four on `fixtures/shop`.
- VS Code extension that wraps the LSP and registers the JSON Schemas. A separate repository, or deferred in favor of the schema settings `init` already writes, because it brings an npm toolchain and publisher tokens outside the current release path. Done when: The extension test opens a project and gets diagnostics.

Plugin API: unchanged.

Stretch: `@model.parameterize` with literal arguments only, access, groups and deprecation_date.

Parallel work on disjoint files is possible in 1.1 (DuckDB with the kit and the matrix), 1.2 (table diff with the
checksums and plan diff), 1.4 (the Airflow work with the Databricks work), 1.6 (OpenTelemetry with column lineage and
the facet pass) and 1.8 (delta-rs with Iceberg on Spark).

### Breaking changes held back

The [stability policy](reference/stability.md) keeps these out of every 1.x release. They are listed so that 1.x
designs leave room for them. None of them needs a new major today, and no major is planned.

- New run statuses (`cached`, `partial`, `warn`). Adding a value to a closed enum is breaking, so through 1.x the
  open `skip_reason` values carry the same information.
- Removing deprecated items: `on_schema_change` (deprecated in 1.5) and the single legacy `checksum`. Both keep
  working for the whole major.
- `state:modified` switching to the split checksums from 1.2.
- Making optional plugin members required (`filter_between`, `from_rows`, `to_rows`, `apply_metadata`), which would
  remove their fallback helpers.
- The `init` default engine moving from polars to DuckDB.
- Intervals as a value of the closed `strategy` enum.

Research with no version attached: cross-project refs, the Lakeflow Spark Declarative Pipelines export (it needs its
own ADR first), a dbt importer and a community Snowflake engine.

## 5. Non-goals and risks

Non-goals, with what changes (accepted on 2026-11-20, see section 6). Section 1 of the architecture picks up the new
list through the stability ADR amendment in 1.1:

- Streaming: kept. One sentence is added: reading Auto Loader or Lakeflow Connect landing tables through
  `incremental` is supported, and checkpoint-based state is not. Bounded streaming reads (Spark's AvailableNow trigger) are revisited only after
  Delta change data feed, through its own ADR.
- Snowflake: was "a Snowflake target before a Snowflake engine exists". New wording: engines beyond polars, pandas,
  Spark and DuckDB are community plugins tested by the conformance kit.
- UI: was "a scheduler service, or any UI that starts, schedules or changes runs". New wording: "any UI or agent tool
  that starts, schedules or changes runs". MCP write tools fall under it.
- Zero-copy environments: kept. Clone-only dev copies are allowed and promotion is not.
- User code at compile time: kept. Model factories stay static with literal arguments.
- New: a semantic layer, cross-engine data diff, and ML anomaly models beyond fixed statistics.
- Kept as they are: Airflow 2, Jinja, a portable SQL dialect and a scheduler service.

Rejected or deferred, with the reason:

- Cost fields in run_results: contradicts the decision to keep cost in system tables. 1.6 gets a recipe instead.
- Snowflake engine: very large effort and no test account.
- dbt import: L effort and Jinja semantics through the back door. Left as research with no version.
- Bounded streaming reads (Spark's AvailableNow trigger): a second source of state, and per-batch writes under Spark Connect are unverified.
- Clone environments: `--defer` covers most of the value. Revisit on demand.
- YAML Dag export: the upstream format is experimental.
- Lakeflow Connect: source freshness covers it.
- Fabric: no demand.
- Packages, cross-project refs, semantic export and importers: large effort or scope creep, and importers conflict
  with "YAML only configures".
- A unit hook, state sync and a pandera bridge: one consumer each, left to the community.
- The extra check idea folded into the anomaly checks.

Changed from the earlier plan: the Lakeflow export is research with no version, `pipeloom-dagster` is placed in 1.4,
and the two 1.0 follow-ups (parse-time budget tests, the Windows Airflow group) are placed in 1.4.

Risks:

- Single maintainer. Nine minors at 2 to 4 weeks each is 4 to 8 months of full-time work, and every minor also
  releases every plugin in lockstep. Each release has a cut date, stretch items go first, and workspace-only demos
  are not in CI.
- Narwhals may not cover anti join, `is_duplicated` and tz casts on its DuckDB backend. Spike this in week 1 of
  1.1, confirm the supported backends in its docs, and override with SQL.
- Dagster ownership. Prefect announced it is acquiring Dagster Labs ([Market landscape](landscape.md)), so the
  `dagster` API may shift. `pipeloom-dagster` uses only the public API and runs against a version matrix.
- Airflow partition APIs arrived in 3.2 and may keep changing. They stay experimental and covered by the matrix.
- Hash false positives (split checksums) and lineage overclaiming. Version the fingerprint, keep golden tests and
  completeness flags.
- Wrong skips from `--skip-unchanged`. It is opt-in, anything unhashable forces a run, and every skip has a reason.
- Windows. CI runs on windows-latest, and zoneinfo has no system timezone database there, process groups are
  POSIX-only, path guards differ, and DuckDB extension availability needs a check.
- Supply chain. DuckDB extensions download binaries at runtime, the VS Code extension needs a separate release path
  and tokens, and the MCP SDK pulls a web stack. Each is isolated in its own distribution or job.
- Data leaks through samples in PR comments, CI summaries and agent context. Defaults show counts only.
- Prompt injection through artifact text in MCP, and DNS rebinding against the local UI. Both have the mitigations
  listed in 1.7.
- External claims to verify first: the OpenLineage column facet status, UC external lineage GA, and PyIceberg write
  support. Each gets a verify-first task.
- Duplicate OpenLineage events under Airflow. The rule (the provider emits inside Airflow, the sink everywhere
  else) needs a test.
- WATCHER mode on shared storage. NFS attribute caching can hide fresh attempt files, so the sensor needs a bounded
  poll and a sentinel file (code PLA004, see the [error codes](reference/errors.md)).
- Defer correctness. Reading production relations from a dev run needs read grants and the same schema. A missing
  relation must fail with a clear hint (code PLM204, see the [error codes](reference/errors.md)).

## 6. Decisions

Ordered by impact. All were decided on 2026-11-20; each item names the alternative it replaced.

1. Dagster. Decided 2026-11-20: `pipeloom-dagster` ships as a plugin in 1.4, as the 1.0 roadmap promised. The
   alternative was a recipe over `graph --units --output json` and a plugin only with a co-maintainer.
2. `pipeloom ui`. Decided 2026-11-20: it moves from 1.1 to 1.7.x and is built on the read layer. The alternatives
   were 1.2, or keeping 1.1.
3. Version order. Decided 2026-11-20: DuckDB and the kit in 1.1, change intelligence in 1.2, intervals in 1.3. The
   alternatives put intervals or change intelligence first.
4. `on_schema_change`. Decided 2026-11-20: deprecated in 1.5 in favor of `schema_contract`. It keeps working through
   1.x and can be removed only in a new major. The alternative was keeping both.
5. Interval ledger. Decided 2026-11-20: a reserved state key with document type `pipeloom.intervals/1`. The
   alternative was new optional StateStore methods, which stay the fallback named in 1.3.
6. Interval writes. Decided 2026-11-20: an explicit `replace_range` field under the `interval_writes` capability. The
   alternative was reusing `replace_partitions`, which cannot guarantee range replacement.
7. `init` default engine. Decided 2026-11-20: polars stays the default through 1.x, because changing a default is
   breaking under ADR-0016. DuckDB as the default waits for a new major, if one ever comes.
8. Non-goal changes and the experimental tier. Decided 2026-11-20: all accepted as section 5 lists them (the three
   new non-goals, the Snowflake and UI rewording, the streaming note) together with the experimental tier and its
   lifecycle in section 3.
9. Checksum inputs. Decided 2026-11-20: hash after `${var}` resolution and before `${env}` and `${secret}`, so
   secrets never enter a hash. The alternative was hashing the unresolved text.
10. `pipeloom-duckdb`. Decided 2026-11-20: in-tree and released in lockstep. The alternative was a separate
    repository as proof of the kit.
11. OpenTelemetry attributes. Decided 2026-11-20: `pipeloom.*` plus the CI/CD semantic conventions where they fit.
    The alternative was `pipeloom.*` only.
