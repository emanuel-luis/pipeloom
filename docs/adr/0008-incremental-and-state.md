# ADR-0008: Incremental key, watermark state, strategies

Architecture: [Section 9, Incremental processing, write strategies, schema change](../architecture.md#9-incremental-processing-write-strategies-schema-change)

## Context

Incremental logic must be declared, not inferred (lesson L12), and must survive crashes and orchestrator retries. Every write option must be applied by each strategy or rejected (L9).

## Options

- **Watermark kept in the target** (`max(column)` of the target). No extra store, but needs the target to exist and read access to it.
- **Watermark in a `StateStore`** with compare-and-swap. Works for targets that cannot be queried cheaply, at the cost of a store.
- **Both,** selected per model with `watermark_source: state | target`.

## Decision

A declared key plus a lookback, with watermarks in a `StateStore`. `watermark_source: target` reads `max(column)` of the target instead.

- Per unit: determine `is_incremental`, read the watermark `w`, filter the input with `filter_since(input, column, w - lookback)`, write, then `cas_watermark(expected=w, new=max_value(filtered input))`. A CAS failure is PLM203.
- State stores: `sqlite` (core, local), `json` (core, atomic rename), `table` (spark plugin). `boundary: task` requires `table` or `json` on a shared path.
- Strategies: append, overwrite, replace_partitions, merge, scd2. Each returns a `WriteResult`.
- Idempotency: each write receives `token = "<run_id>:<model>"`. Engines without idempotent writes declare `idempotent_writes=False`.
- `on_schema_change`: `fail` (default), `ignore`, `append_new_columns`, `sync_all_columns`.
- Maintenance (`optimize`, `vacuum`, clustering) runs only from `maintenance:` blocks, never by default.

## Consequences

- The watermark is committed after the write. A crash between the two reprocesses at most the lookback window plus the batch. That is safe for merge, overwrite and replace_partitions, and for append only when the engine has idempotent writes (otherwise PLC505 forbids retries).
- A schema change under the default policy fails the unit (PLM202). Every detected change emits `model.schema_changed`.

## Status

Accepted

## Date

2026-10-29
