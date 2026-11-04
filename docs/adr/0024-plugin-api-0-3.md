# ADR-0024: Plugin API (0, 3)

Architecture: [Section 5, Engine and catalog contracts](../architecture.md#5-engine-and-catalog-contracts)

Amends [ADR-0002](0002-engine-abstraction.md) and [ADR-0021](0021-plugin-api-0-2.md).

## Context

0.3 adds incremental models, merge and replace_partitions, schema-change policies, expectations with quarantine, retries, timeouts and cancellation. The (0, 2) contract has no way to filter by a watermark with a strict bound, to count quality violations, to split dropped rows, to align a frame to a chosen schema, to look up an idempotency token before reading anything, to tag work for cancellation, or to classify an error as retryable. It also cannot say what a write changed beyond row counts.

## Options

- **Let the core do the work on rows.** The core would pull data out of the engine, which defeats lazy and remote engines and breaks the rule that the core never looks inside a relation.
- **Engines count, the core judges.** Engines compute raw counts and aggregates over their own frames; pass or fail, ratios, events and diagnostics stay in the core. One pure-Python reference defines the semantics and the contract suite compares every engine with it.

## Decision

The plugin API version becomes (0, 3). While the major version is 0 a plugin must match it exactly (ADR-0017), so every plugin is rebuilt for it.

`EngineSession` gains:

- `filter_since(rel, column, lower, *, inclusive=True)`: the strict form serves a watermark without lookback.
- `evaluate(rel, checks)` returning `QualityCounts` and `apply_drops(rel, checks, *, quarantine)` returning `DropResult`. Both need the new capability `quality`; an engine without it raises `NotImplementedError` and L5 will reject expectations on it.
- `align(rel, schema)`: select in schema order, add missing columns as typed nulls, cast to the types. The core decides the final table schema; the engine only applies it.
- `applied_token(target, token)`: the stored `WriteResult` with `skipped=True`, or None. The runner can skip read, body and write when a token was already applied.
- `unit_scope(unit_id)`, `is_transient(exc)` and an active `cancel(unit_id)`.
- `max_value` returns plain Python values, never numpy or pandas scalars.

`WriteConfig` gains `partition_by`, `update_condition` (engine dialect, `source.` and `target.` qualifiers, a bare name means source), `deletes` (`DeleteSpec`), `columns` (the final schema chosen by the core) and `commit_meta`. `WriteResult` gains `skipped` and `commit_meta`; its other fields already carried the architecture names. `commit_meta` is stored with the token and echoed on replay, so a replayed write returns the watermark and quality summary of the original attempt.

New types: `ColumnChange`, `SchemaDiff`, `ExpectationSpec`, `QualityCounts`, `QuarantineMeta`, `DropResult`, plus `can_widen` and `diff_schemas`. Errors live in `pipeloom.engine.errors`: `TransientError` (raise it to force a retryable failure), `CommitConflict` (a transient error for table commit races) and `WriteKeyError` (a missing key column or a duplicate merge key, reported as PLM206).

Row-check semantics are fixed for all engines: a check passes on null input except `not_null`; NaN counts as null; `range` bounds are inclusive; `regex` is an unanchored search; `accepted_values` compares exactly; `expr` true passes, false fails, null passes. Aggregates see only the relation passed in. `pipeloom.testing.reference` implements all of it over plain rows, and the contract suite compares each engine with it.

The memory engine implements the whole contract (merge, replace_partitions, quality, cancel, `incremental` materialization). The polars and pandas engines move to (0, 3) and implement `align`, `applied_token`, `unit_scope`, `is_transient` and the new `filter_since`. They declare no `quality`, `merge` or `replace_partitions` yet, so their contract tests for those skip; the filesystem merge and quality work follows in later changes.

## Consequences

- Plugins built for (0, 2) fail with PLU001 until they are updated.
- An engine that cannot cancel keeps `cancel=False` and no-ops.
- Widening rules (`can_widen`) are part of the contract, so the core can decide schema changes without asking the engine.
- Dialect strings (`expr`, `update_condition`) are the only engine-specific text; contract cases keep to a common subset.

## Status

Accepted

## Date

2026-11-05
