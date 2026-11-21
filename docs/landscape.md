# Market landscape

Research date: 2026-11-20. This page holds the market evidence behind the [roadmap](roadmap.md). Open-source
projects informed behavior and design ideas only, under the clean-room rule: no code was copied, and DQX, dbt Fusion,
the dbt language server, Soda and Pathway were treated as behavior only, with no source reading.

## Trends

1. The transformation layer is consolidating under Fivetran. Fivetran and dbt Labs completed their merger on
   2026-06-01 ([press release](https://www.fivetran.com/press/fivetran-dbt-labs-complete-merger-to-create-the-data-infrastructure-for-trusted-ai-agents)),
   and Fivetran contributed SQLMesh to the Linux Foundation on 2026-03-25
   ([announcement](https://www.linuxfoundation.org/press/linux-foundation-welcomes-sqlmesh-project)). Open-source users are left unsure what stays neutral.
2. Orchestrators are consolidating too. Prefect announced it is acquiring Dagster Labs (post dated 2026-07-13);
   closing is unconfirmed ([post](https://dagster.io/blog/prefect-is-acquiring-dagster)). This favors a neutral
   manifest over deep coupling to one vendor.
3. dbt v2 is a Rust runtime with a split license. The code that was in dbt-fusion is now in the dbt repository under
   Apache 2.0, the distributed binary is under the dbt Product Licensing Agreement, and dbt Core v1.x stays Apache
   2.0 ([license FAQ](https://www.getdbt.com/licenses-faq)). More in the [dbt blog](https://www.getdbt.com/blog/fivetran-dbt-20-future).
4. Interval and microbatch execution with explicit backfills is the norm: dbt microbatch with event_time
   ([docs](https://docs.getdbt.com/docs/build/incremental-microbatch)), SQLMesh intervals and Bruin partitioned
   backfills ([changelog](https://getbruin.com/changelog/)).
5. Asset scheduling is partition-aware. Airflow 3.2 added asset partitioning under AIP-76
   ([release post](https://airflow.apache.org/blog/airflow-3.2.0/)). Dagster 1.13 added partitioned asset checks
   ([post](https://dagster.io/blog/dagster-1-13-octopuss-garden)).
6. Orchestrators read a manifest. Cosmos WATCHER was hardened in 1.14
   ([post](https://www.astronomer.io/blog/cosmos-1-14-battle-tested-watcher-mode/)).
7. OpenLineage is the common language and releases about monthly (1.55.0 is the latest seen,
   [releases](https://github.com/OpenLineage/OpenLineage/releases)).
8. DuckDB has become a full lakehouse engine. Version 1.4 added MERGE and Iceberg writes
   ([announcement](https://duckdb.org/2025/09/16/announcing-duckdb-140)), and DuckLake 1.0 was released on
   2026-04-13 with DuckDB 1.5.2 ([post](https://ducklake.select/2026/04/13/ducklake-10/)). DuckDB is also the usual local on-ramp for demos and CI.
9. Narwhals is the shared frame layer ([PyPI](https://pypi.org/p/narwhals)). Spark Connect works as a wire protocol (Sail,
   [PyPI](https://pypi.org/project/pysail/)), and Iceberg REST is the neutral catalog contract.
10. Contracts are portable YAML and checks move left into CI. ODCS v3.1 is the interchange format
    ([Bitol](https://bitol.io/bitol-announces-odcs-v3-1-0-stronger-smarter-and-stricter/)), and datacontract-cli has
    breaking-change and changelog checks ([PyPI](https://pypi.org/project/datacontract-cli/)).
11. Anomaly detection means simple statistics over run history. Elementary uses z-scores over stored metrics
    ([PyPI](https://pypi.org/project/elementary-data/)), and Datafold archived the open-source data-diff in 2024
    ([post](https://www.datafold.com/blog/sunsetting-open-source-data-diff)).
12. Declarative pipelines are in open Spark 4.1, without auto CDC in the OSS API; that is Databricks-only for now,
    with a Python AUTO CDC API reported for Spark 4.2
    ([guide](https://spark.apache.org/docs/4.1.2/declarative-pipelines-programming-guide.html)).
13. Agents are first-class users. Vendors build three layers: skills and AGENTS.md for guidance
    ([dagster-io/skills](https://github.com/dagster-io/skills)), MCP for live metadata
    ([dbt-mcp](https://pypi.org/project/dbt-mcp)), and LSP for editing.
14. Databricks bundles change quickly. The direct engine went GA in CLI 1.3.0 (2026-06-10) and Terraform was removed
    in CLI 1.20.0 (2026-10-07) ([release notes](https://docs.databricks.com/aws/en/release-notes/dev-tools/bundles)).
15. EL is now a Python library (dlt, [releases](https://github.com/dlt-hub/dlt/releases)) next to managed lakehouse
    ingestion ([Lakeflow Connect](https://docs.databricks.com/aws/ingestion/lakeflow-connect)).
16. Polars 2.0 is released (post dated 2026-10-06). Streaming is the default `LazyFrame` engine, which can change
    row order ([announcement](https://pola.rs/posts/announcing-polars-2/),
    [upgrade guide](https://docs.pola.rs/releases/upgrade/2/)).

## Tools by segment

### Transformation and authoring

| Tool | What it does | Relevance to pipeloom |
|---|---|---|
| dbt Core and the v2 runtime | SQL models, microbatch, unit tests, state-aware runs | The reference for developer experience. Interval design (1.3) and skip-unchanged (1.5) follow its public behavior. |
| SQLMesh | Plans with change categories, intervals, table diff | Source of plan diff and table diff behavior (1.2) and the interval ledger (1.3). |
| Apache Hamilton | Function-first dataflow, caching | Same authoring idea as `@model`. Informs fingerprints for caching. |
| Bruin | Assets with partitioned backfills | Per-interval run protocol (1.3). |
| Kedro | Pipelines, Kedro-Viz | Read-only viewer design for the UI (1.7). |
| Spark Declarative Pipelines | Declarative pipelines in open Spark | Dry run and full-refresh behavior. Export is research with no version. |

### Orchestrators

| Tool | What it does | Relevance to pipeloom |
|---|---|---|
| Apache Airflow 3.2 to 3.4 | Scheduling, asset partitions, Dag Importer | The main target of the Airflow work (1.4). |
| Astronomer Cosmos | Runs dbt as Airflow tasks, WATCHER mode | Model for the WATCHER hardening and for asset identity. |
| Dagster | Assets, partitions, dbt integration | `pipeloom-dagster` plugin in 1.4. |
| Prefect | Flows and tasks, dbt runner | Recipe over the unit graph. |
| Databricks bundles and jobs | Declarative deploy of jobs | Window parameters and CLI range check (1.0.x patch, 1.4). |

### Engines, formats and catalogs

| Tool | What it does | Relevance to pipeloom |
|---|---|---|
| DuckDB and DuckLake | Local SQL engine, MERGE, Iceberg writes, lakehouse format | `pipeloom-duckdb` and the demo (1.1). |
| Polars 2.0 | DataFrame engine | CI against 2.0 now (1.0.x patch). |
| Narwhals | Frame API over many backends | The portable layer. Cross-engine matrix (1.1). |
| delta-rs | Delta without Spark | Delta targets and state store (1.8). |
| PyIceberg 0.12.0 | Iceberg catalogs and tables | Stretch in 1.8. Write support is unverified. |
| Sail | Spark Connect compatible engine | CI recipe (1.1 stretch). |

### Quality, contracts and lineage

| Tool | What it does | Relevance to pipeloom |
|---|---|---|
| Elementary | Anomaly checks from stored metrics | Z-score design for 1.5. Behavior only. |
| DQX | Checks on Spark | Reconciliation test idea (1.5). Behavior only, no source reading (Databricks License, not OSI). |
| Soda Core | Data quality checks | Behavior only (Elastic License 2.0). |
| Great Expectations (GX Core) | Expectations | Context only. Fivetran is its steward. |
| datacontract-cli and ODCS | Contract import, export, breaking checks | ODCS export (1.5). |
| pandera | DataFrame schema checks | Left to the community (bridge plugin rejected). |
| OpenLineage | Lineage events and facets | Facet pass (1.6). |
| OpenTelemetry CI/CD conventions | Tracing, release-candidate conventions | `pipeloom-otel` (1.6). |
| sqlglot | SQL parsing and column lineage | Column lineage (1.6). Pinned to a tested range. |

### EL, agents and UI

| Tool | What it does | Relevance to pipeloom |
|---|---|---|
| dlt | Python EL library | EL-then-T recipe (1.3 stretch). |
| Lakeflow Connect | Managed lakehouse ingestion | Freshness checks cover it. |
| Sling | EL tool | Subprocess use only, if ever. |
| dbt-mcp, dagster-io/skills | MCP server and agent skills | `pipeloom-mcp` and generated skills (1.2, 1.7). |
| Pathway | Streaming | Out of scope. Behavior only (BSL). |

## What pipeloom has and lacks

Ahead:

- Function-first authoring with sqlglot translation.
- Versioned artifacts with published JSON Schemas and a typed event stream (`pipeloom.event/1`).
- Orchestrator renderers built on a unit protocol with a plan-hash guard.
- Engine-agnostic protocols with conformance suites.
- An Apache license with no hosted tier, neutral between the Fivetran and dbt group and the Prefect and Dagster group.

Behind:

- No local SQL engine, no interval or partition backfill model, no plan diff or data diff, no column lineage.
- No history-based anomaly checks, no agent surface, no dbt migration path.
- No Iceberg, no Delta writes without Spark, and no first-run demo.

Several gaps that reports listed are already covered in 1.0: warn, drop and fail severity on expectations and data
tests, CSV fixtures in unit tests, `--full-refresh` on run and retry, and the capability checks at validation (only a docs matrix
is new). The node checksum does cover config, body, columns and data tests together, which supports the split in 1.2.

## Where each finding lands

This table maps findings to [roadmap](roadmap.md) releases.

| Release | Items |
|---|---|
| 1.0.x | Polars 2.0 CI, Databricks CLI and Airflow upstream checks |
| 1.1 | DuckDB, conformance kit, Narwhals guide and matrix, demo. Stretch: Sail CI recipe, `show --limit` |
| 1.2 | Split checksums, plan diff, table diff, dataset identity, agent surface. Stretch: Slim CI recipe, `validate --dry-run` |
| 1.3 | Intervals, ledger and backfill, outcome fields, cursor options. Stretch: `sequence_by`, EL-then-T recipe, retry resume set |
| 1.4 | Partitions, Airflow and Databricks work, `pipeloom-dagster`. Stretch: orchestrator conformance kit, Prefect recipe |
| 1.5 | Freshness and anomaly checks, reconcile, skip-unchanged, schema contracts, ODCS export. Stretch: ODCS import, unit-test upgrades, prune |
| 1.6 | Column lineage, OpenLineage facets, OpenTelemetry. Stretch: UC external lineage push, query annotations |
| 1.7 | Read layer, grouping, MCP server, UI. Stretch: `profile` |
| 1.8 | Delta without a cluster, Iceberg on Spark |
| 1.9 | Language server and VS Code extension. Stretch: model parameterization, access and deprecation_date |
| Rejected or deferred | See the rejected list in the roadmap |
| Not placed | Delta change data feed |

## Pitfalls taken from the code readings

- Hash false positives: dbt regressed repeatedly on whitespace normalization. Hash normalized ASTs, version the
  algorithm and ship an explainer.
- Deferral computed twice produces disagreeing layers. Compute the map once, in the plan.
- Microbatch traps: silent full scans without event_time, ephemeral upstreams, lookback off-by-one, and timezone and
  DST handling.
- Lineage overconfidence: sqlglot lineage misses indirect dependencies, so record completeness per column.
- Dataset identity drift between assets and lineage: define one canonical function early.
- Observers must never change results: a lineage failure never changes the exit code.
- WATCHER at scale: hanging sensors, stacked retry delays, Variable writes and XCom growth.
- Select and exclude on a narrowed set: evaluate both on the full graph.
- Semantic-layer scope creep: export only.
