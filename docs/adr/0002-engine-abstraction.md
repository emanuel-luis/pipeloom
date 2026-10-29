# ADR-0002: Engine abstraction: protocols plus Narwhals helpers

Architecture: [Section 5, Core protocols](../architecture.md#5-core-protocols)

## Context

Moving from one engine to several couples code in the return type of a model, reads, schema, temporary views, writes, merge, metrics and catalog SQL. The core needs one contract that pandas, polars and PySpark plugins can all meet, without forcing Spark users into a lowest common denominator.

## Options

- **(a) A hand-written protocol per plugin.** Full control over IO, writes and metrics. More code per engine.
- **(b) Narwhals as the core abstraction.** Small and dependency-free, but it has no write, SQL or catalog API, and only supports lazy PySpark.
- **(c) Ibis.** Broad engine coverage, but heavy. Its write support is unverified, and it pushes users into an expression API.
- **(d) A hybrid.** Hand-written protocols for the contract, Narwhals only for portable helpers.

## Decision

Option (d).

- The core contract is the hand-written protocols (`Engine`, `EngineSession`, `Reader`, `Materializer`, `Catalog`, `MetricsProvider` and others). Plugins own IO, writes, merge, catalogs, metrics and cancellation.
- The core depends on Narwhals only for `pipeloom.engine.portable`. That module gives default `count`, `filter_since`, `max_value`, `schema` and `evaluate` that pandas, polars and PySpark plugins may reuse, through `narwhals.stable.v2`.
- Model bodies receive native frames by default. `frame_api: narwhals` on a model passes Narwhals frames, so portable models work on any engine with `capabilities.portable`.

## Consequences

- No lowest-common-denominator API for Spark users.
- Engines can override the portable defaults for speed.
- Narwhals stays a replaceable implementation detail. Pinning to `stable.v2` limits API drift.

## Status

Accepted

## Date

2026-10-29
