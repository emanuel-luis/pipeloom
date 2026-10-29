# ADR-0009: Expectations and quarantine

Architecture: [Section 10, Data quality](../architecture.md#10-data-quality)

## Context

Data quality is a first-class feature (lesson L13). Rules need a clear action when a row or a dataset violates them, and failing rows should not vanish silently.

## Options

- **Fail-only checks.** Simple, but one bad row stops the run.
- **DLT-style actions (warn, drop, fail) plus quarantine.** Lets users choose per rule, and keeps dropped rows for inspection.

## Decision

DLT-style actions plus quarantine, evaluated after the transformation and before the write.

- Row-level checks: `not_null`, `accepted_values`, `range`, `regex`, `expr`. Actions: warn, drop, fail.
- Aggregate checks: `unique`, `row_count`, `freshness`. Actions: warn, fail. `drop` is PLP403.
- `EngineSession.evaluate` computes every row-level predicate in one aggregate pass and returns per-expectation `passed` and `failed` counts. Unlike DLT, counts are kept on `fail` too.
- `drop` filters rows that fail any `drop` predicate. If `max_drop_ratio` is exceeded, the unit fails with PLQ301.
- With `quality.quarantine: true`, dropped rows go to `<target>__quarantine` with `_pipeloom_run_id`, `_pipeloom_failed` and `_pipeloom_ts`.
- Lazy engines may cache the relation before the counting pass. The engine decides, and the decision is recorded in the metadata.

## Consequences

- Results appear in `run_results.json` and in `model.quality_evaluated` events.
- Expressions in `expr` checks use the engine's dialect and are not portable.
- A quarantine table is extra storage that users must manage.

## Status

Accepted

## Date

2026-10-29
