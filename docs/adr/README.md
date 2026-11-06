# Architecture decision records

Each significant decision is one file here, written as Context, Options, Decision, Consequences, Status and Date (see [0000-template.md](0000-template.md)). Numbers are never reused. A changed decision gets a new ADR that supersedes the old one. The full design is in [architecture.md](../architecture.md).

| Number | Title | Status | Date |
|---|---|---|---|
| 0001 | [Record architecture decisions](0001-record-architecture-decisions.md) | Accepted | 2026-10-29 |
| 0002 | [Engine abstraction: protocols plus Narwhals helpers](0002-engine-abstraction.md) | Accepted | 2026-10-29 |
| 0003 | [Model authoring: YAML plus Python or SQL, declared inputs](0003-model-authoring.md) | Accepted | 2026-10-29 |
| 0004 | [Plugin discovery via entry points; eight groups](0004-plugin-discovery.md) | Accepted | 2026-10-29 |
| 0005 | [Packaging](0005-packaging.md) | Accepted | 2026-10-29 |
| 0006 | [Ready-queue scheduler](0006-ready-queue-scheduler.md) | Accepted | 2026-10-29 |
| 0007 | [Data handoff: execution units, inlining, intermediates](0007-data-handoff.md) | Accepted | 2026-10-29 |
| 0008 | [Incremental key, watermark state, strategies](0008-incremental-and-state.md) | Accepted | 2026-10-29 |
| 0009 | [Expectations and quarantine](0009-expectations-and-quarantine.md) | Accepted | 2026-10-29 |
| 0010 | [Events, sinks, artifacts; rich for the console](0010-events-sinks-artifacts.md) | Accepted | 2026-10-29 |
| 0011 | [Databricks job rendering](0011-databricks-job-rendering.md) | Accepted | 2026-10-29 |
| 0012 | [Airflow rendering and modes](0012-airflow-rendering-and-modes.md) | Accepted | 2026-10-29 |
| 0013 | [Run identity, idempotency, reruns](0013-run-identity-and-reruns.md) | Accepted. Amended in 0.8 (`pipeloom retry`). | 2026-10-29 |
| 0014 | [Secrets by reference](0014-secrets-by-reference.md) | Accepted | 2026-10-29 |
| 0015 | [Python 3.11 or newer; click CLI; pydantic v2; hatchling; mypy strict](0015-python-version-and-toolchain.md) | Accepted | 2026-10-29 |
| 0016 | [Versioning, release, plugin API version](0016-versioning-and-plugin-api.md) | Accepted | 2026-10-29 |
| 0017 | [Plugin spec, built-in plugins and the exact plugin API gate](0017-plugin-spec-and-builtins.md) | Accepted | 2026-11-04 |
| 0018 | [Events in 0.1: invocation events, nullable run id, jsonl on stderr](0018-events-in-0-1.md) | Accepted | 2026-11-04 |
| 0019 | [YAML dialect: PyYAML with YAML 1.2 core scalars](0019-yaml-dialect.md) | Accepted | 2026-11-04 |
| 0020 | [Execution model in 0.2](0020-execution-model-0-2.md) | Accepted | 2026-11-04 |
| 0021 | [Plugin API (0, 2)](0021-plugin-api-0-2.md) | Accepted | 2026-11-04 |
| 0022 | [Filesystem table format](0022-filesystem-table-format.md) | Accepted | 2026-11-04 |
| 0023 | [Multi-distribution release](0023-multi-distribution-release.md) | Accepted | 2026-11-04 |
| 0024 | [Plugin API (0, 3)](0024-plugin-api-0-3.md) | Accepted | 2026-11-04 |
| 0025 | [Filesystem tables v2](0025-filesystem-tables-v2.md) | Accepted | 2026-11-04 |
| 0026 | [Schema-change policy](0026-schema-change-policy.md) | Accepted | 2026-11-04 |
| 0027 | [Incremental models](0027-incremental-models.md) | Accepted | 2026-11-04 |
| 0028 | [Expectation evaluation](0028-expectation-evaluation.md) | Accepted | 2026-11-04 |
| 0029 | [Retries, timeouts and cancellation](0029-retries-timeouts-cancellation.md) | Accepted | 2026-11-04 |
| 0030 | [Plugin API (0, 4) and intermediates](0030-plugin-api-0-4-and-intermediates.md) | Accepted | 2026-11-04 |
| 0031 | [Spark engine](0031-spark-engine.md) | Accepted | 2026-11-04 |
| 0032 | [Delta writes and idempotency](0032-delta-writes-and-idempotency.md) | Accepted | 2026-11-04 |
| 0033 | [Delta table state store](0033-delta-table-state-store.md) | Accepted | 2026-11-04 |
| 0034 | [Core orchestration hooks](0034-core-orchestration-hooks.md) | Accepted | 2026-11-04 |
| 0035 | [Databricks rendering and job lifecycle](0035-databricks-rendering-and-job-lifecycle.md) | Accepted | 2026-11-05 |
| 0036 | [Databricks client and connection](0036-databricks-client-and-connection.md) | Accepted | 2026-11-05 |
| 0037 | [Airflow packaging and rendering](0037-airflow-packaging-and-rendering.md) | Accepted | 2026-11-05 |
| 0038 | [Airflow execution modes and artifact store](0038-airflow-execution-modes-and-artifact-store.md) | Accepted | 2026-11-05 |
| 0039 | [Airflow DATABRICKS mode](0039-airflow-databricks-mode.md) | Accepted | 2026-11-05 |
| 0040 | [Per-model catalog](0040-per-model-catalog.md) | Accepted | 2026-11-05 |
| 0041 | [pipeloom init](0041-pipeloom-init.md) | Accepted, amended in 0.8 | 2026-11-05 |
| 0042 | [Selection by state and --defer](0042-state-selection-and-defer.md) | Accepted | 2026-11-06 |
| 0043 | [Docs site](0043-docs-site.md) | Accepted | 2026-11-06 |
| 0044 | [Model contracts](0044-model-contracts.md) | Accepted | 2026-11-06 |
| 0047 | [Run history and the webhook sink](0047-run-history-and-webhook.md) | Accepted | 2026-11-06 |
