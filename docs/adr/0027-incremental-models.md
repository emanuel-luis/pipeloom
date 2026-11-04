# ADR-0027: Incremental models

Architecture: [Section 9, Incremental processing, write strategies, schema change](../architecture.md#9-incremental-processing-write-strategies-schema-change)

Amends [ADR-0008](0008-incremental-and-state.md).

## Context

ADR-0008 named the watermark and the compare-and-swap but left open how a unit starts without a watermark, what a retry commits, how types are matched, and who owns the lookback arithmetic. Two things can go wrong between the write and the swap: the process can die, and new input can arrive before the retry. A retry that reads the input again would then commit a watermark that covers rows it never wrote.

## Options

- **Recompute on retry.** Simple, but the retried batch can differ from the written one, so the watermark can jump over rows.
- **Store the watermark with the write token.** The write records the watermark it intends to commit in its commit metadata. A retry with the same token skips read, body and write and commits that stored value.

## Decision

The watermark is stored with the token. `pipeloom.runtime.incremental` holds the logic: `resolve` reads the state, `next_watermark` picks the value, `commit` swaps it, and `to_meta` and `from_meta` carry it through `WriteConfig.commit_meta` and `WriteResult.commit_meta`.

The runner checks `EngineSession.applied_token` before it does any work. A replayed token emits `model.written` with `skipped: true`, rebuilds the schema fields and the watermark from the stored metadata, and goes straight to the swap. Otherwise the unit resolves the watermark, reads the input filtered, runs the body, computes the batch maximum on the filtered input, writes with the intended watermark in the commit metadata, and then swaps. Nothing that can be cancelled sits between a finished write and its swap.

Rules:

- A unit is incremental when the materialization is `incremental`, the target exists, the run is not a full refresh, and a watermark exists. `ctx.is_incremental`, `ctx.watermark` and `ctx.lower_bound` are set only then.
- With `watermark_source: state` and no stored watermark but an existing target, the watermark is seeded from `max(column)` of the target and `watermark_seeded: target` goes to `warnings[]`. With `watermark_source: target` the state store is never written.
- Types are compared by family: timestamp with a zone, timestamp without one, date, integer, decimal, float, string. A stored watermark of another family than the input column is PLM205, as is a missing column.
- `lookback: 0` filters with `column > w`. A lookback above zero filters with `column >= w - lookback` for timestamps (seconds) and dates (whole days, rounded up); on any other family it is PLM205. There is no lookback on integers.
- The new watermark is `max(w, m)` when incremental, where `m` is the batch maximum and may be below `w` or missing; otherwise it is `m`, and an empty first batch leaves the state alone. A full refresh commits `m`.
- The swap is `cas_watermark(expected, new)` with `expected` as read at the start (none when seeded). Stores treat a stored value equal to `new` as success, which covers a replay after a crash between the swap and the attempt file. Any other mismatch is PLM203: the data is already written, the unit fails, and the error is permanent. `StateStoreContract` checks the rule.
- Append with a lookback above zero reloads rows it already wrote; PLP410 warns and recommends merge.
- `plan.model.default_strategy` gives the strategy of a model that sets none: append for incremental, overwrite otherwise. A full refresh writes append, merge and replace_partitions as overwrite. A first build keeps its strategy, because into an absent table every strategy writes the same rows and merge still checks its key.
- PLC509 compares the materialization with `EngineCapabilities.materializations`; the memory, polars and pandas engines declare `incremental`.

New event `state.watermark_committed` (column, dtype, before, after, source, seeded). Rows of `run_results` gain `is_incremental`, `watermark_before`, `watermark_after`, `rows_updated` and `rows_deleted` values.

## Consequences

- A retry with the same run id commits exactly the original batch's watermark, even when new rows arrived in between. This also holds in unit mode.
- The commit metadata grows by a small JSON object per incremental write.
- The batch maximum is computed on the filtered input, so a lazy engine scans it twice.
- A first build that fails the merge key check does so on the first run, not the second.

## Status

Accepted.

## Date

2026-11-04
