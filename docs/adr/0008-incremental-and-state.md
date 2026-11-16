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

## Amendment (0.9): SCD2

The scd2 strategy keeps a type 2 history of each `unique_key`.

- No new config keys: `unique_key`, `tracked_columns` and the `deletes` block are reused, so manifests, checksums and plan hashes of projects without scd2 stay byte-identical. `tracked_columns` left unset means every business column except the `unique_key` columns and the `deletes.column`; an explicit empty list is PLP416.
- System columns have fixed names: `valid_from` and `valid_to` (`timestamp[us, UTC]`) and `is_current` (`boolean`). A rename option can come later.
- A change is detected by comparing the tracked columns directly, null-safely, with NaN counted as null. There is no row hash: it would depend on the engine and go stale when `tracked_columns` changes. A change in an untracked column is ignored.
- `valid_from` and `valid_to` come from the run's start time (`WriteConfig.run_started_at`), the same on every retry of a run, so a re-run converges. Timestamps taken from a source column are a non-goal.
- Deletes use the existing block with `mode: hard` only (`soft` is PLP416). A flagged row closes the key's current row; a flagged row for an unknown key is ignored. Keys missing from the batch are not closed. A closed key that comes back gets a new current row.
- A null key, a key repeated in one batch, a batch column named like a system column, or an existing target without the system columns is PLM206 (`WriteKeyError`); the last one asks for `--full-refresh`.
- The first load inserts every non-delete row as current. A full refresh drops the history and reloads as a first load: scd2 keeps its strategy under full refresh. A retry with the same token replays the stored result (ADR-0032); a re-run with an unchanged batch adds no versions.
- Schema policy: scd2 keeps rows. The runner strips the system columns from the target schema before the policy, so the policy acts on business columns only.
- A model contract (ADR-0044) describes the body output, so a system column there is PLP416. PLP416 also covers a `unique_key`, `partition_by`, `cluster_by`, `deletes.column` or tracked column that names a system column, and tracked columns that overlap the `unique_key`.
- Counts reuse the `WriteResult` fields: `rows_inserted` is new current rows, `rows_updated` is rows closed by a change, `rows_deleted` is rows closed by a delete, `rows_written` is their sum.
- `WriteConfig` gains `tracked_columns` (filled by the runner; empty only when the table has no business column besides the key and the delete column) and `full_refresh`, appended with defaults, and the engine types gain `SCD2_COLUMNS`. This is additive and stays within plugin API (0, 5).
- Engines without scd2 get PLC501. The memory engine does not declare it; polars and pandas do (they share the filesystem write code, which rewrites the whole table on each scd2 write, like merge).

## Status

Accepted

## Date

2026-10-29
