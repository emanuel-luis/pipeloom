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
| 0013 | [Run identity, idempotency, reruns](0013-run-identity-and-reruns.md) | Accepted | 2026-10-29 |
| 0014 | [Secrets by reference](0014-secrets-by-reference.md) | Accepted | 2026-10-29 |
| 0015 | [Python 3.11 or newer; click CLI; pydantic v2; hatchling; mypy strict](0015-python-version-and-toolchain.md) | Accepted | 2026-10-29 |
| 0016 | [Versioning, release, plugin API version](0016-versioning-and-plugin-api.md) | Accepted | 2026-10-29 |
