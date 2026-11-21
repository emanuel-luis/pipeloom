# ADR-0032: Delta writes and idempotency

Architecture: [Section 9, Write strategies and idempotency](../architecture.md)

Amends [ADR-0008](0008-incremental-and-state.md) and [ADR-0013](0013-run-identity-and-reruns.md).

## Context

The Spark engine writes Delta tables. A write must be safe to repeat with the same token, must report row and byte metrics, and must follow the schema-change policy the core chose, on classic Spark, Spark Connect and Databricks. Two facts shape the design. The `DeltaTable` Python API is not usable on a Connect session without a server plugin. And Delta applies `txnAppId` and `txnVersion` to a MERGE only through session confs, which would leak into concurrent units that share the session.

## Options

- **`DeltaTable` API for MERGE.** Needs Delta Connect on the server; not portable.
- **Session confs for MERGE idempotency.** Leaks to other units of the same session.
- **SQL `MERGE INTO`, then an empty append that carries the token.** Portable, no session state.
- **Dynamic partition overwrite for `replace_partitions`.** Legacy since Delta 4.3 and only fits physical partitions. `replaceWhere` works on partitioned and unpartitioned tables.

## Decision

All Delta statements are SQL text or DataFrameWriter options; no Spark conf is set. `DESCRIBE HISTORY`, `DESCRIBE DETAIL` and `MERGE INTO` run the same everywhere.

Token and version. `txnAppId` is `pipeloom/<project>/<model>`, with the suffix `#quarantine` or `#intermediate` for those writes. `txnVersion` is `WriteConfig.run_started_at` (or `IntermediateRef.run_started_at`) in epoch milliseconds, never read from the session context. `userMetadata` holds `{"pipeloom": {"v": 1, "token", "commit_meta", "result"?}}`. Without a run start time, or with `txn_guard: false`, only the history check applies.

Idempotency. Every write first looks for its token in `DESCRIBE HISTORY ... LIMIT 100` and returns the stored result with `skipped=True`. After a write that produced no commit carrying the token, the scan is repeated (a racing attempt may have won), then the full history; a token found nowhere means Delta refused the write because a later `txnVersion` already exists for this model, and the engine raises PLM207. The limits: a newer run, an overlapping run (`--no-lock`) or a host clock that ran behind trips the guard; `txn_guard: false` turns it off.

Claim commit. A MERGE carries no transaction, and a migration overwrite or a `mergeSchema` commit runs before the guarded write, so on their own they would land before the guard can refuse anything. When the guard applies, a write that needs any of them (every merge into an existing table, and any write that needs a migration) first makes an empty append carrying the real `txnAppId` and `txnVersion` and the `userMetadata` `{"pipeloom": {"v": 1, "claim": token}}`. It has no `token` key, so the token scan and the metrics ignore it. If no claim for this token is then found (the scan window, then the full history), Delta refused the version and the engine raises PLM207 before any data changes; a claim found from an earlier attempt of the same token (a crash retry) lets the write continue, but only when no newer commit carries another token or claim of the same app id (scanning newest first). A later run that wrote the model in between voids the old claim, so the retry gets PLM207 instead of merging over that run's data. After a claim, the write and the token commit use the app id `<app id>@<token>` with version 1, which still keeps two attempts of the same token from both committing.

Strategies.
- append: `mode("append")`, `mergeSchema` when the final columns add columns.
- overwrite: `mode("overwrite")` with `overwriteSchema`, so any schema policy result is accepted.
- replace_partitions: `replaceWhere` over the distinct partition tuples of the batch (above 10,000 tuples it fails and recommends merge). Null partitions use `IS NULL` or `<=>`. An empty batch writes only the token commit.
- merge: the claim commit, SQL `MERGE INTO` (null keys never match, so they insert), then an empty append with the token, the `<app id>@<token>` transaction and the stored result. A crash between the commits is safe: the retry finds its own claim, re-running the MERGE with the same batch converges, and the token commit is the race guard. Soft deletes are counted with one join and moved from updated to deleted. A merge into a missing table is the first load, written as an append of the batch without its delete rows (hard or soft), as on the portable path.

Schema policy. The core decides the final columns. Added columns go in the write itself (`mergeSchema`; for merge, an empty `mergeSchema` append first, with no token). Removed or widened columns run one migration commit first: the table is read, aligned to the final columns and overwritten with `overwriteSchema`, its `userMetadata` marking `migration_for`, and its partitioning kept. Migration commits carry no `txn` options and no token, so they never count in the metrics; with the guard on, the claim commit before them takes the write version.

Metrics come from the commit history. `DeltaMetrics.before_write` returns the latest version and `after_write` sums `operationMetrics` over the versions after it that carry the token, plus the newest MERGE commit for a merge. The mapping is a pure function (`metrics_to_result`). It is plugin-internal: with one implementation there is no core `MetricsProvider`.

Views and intermediates. `create_view` (the `spark_catalog` catalog only) emits `CREATE OR REPLACE VIEW` with one CTE per alias over the physical table or path. `write_intermediate` overwrites a Delta path under the `#intermediate` app id.

PLM207 is a new code and permanent for retries. The core raises it for the new `WriteRefusedError` an engine may raise.

## Consequences

- Every write costs one `DESCRIBE HISTORY` before and one after, plus a schema read.
- The MERGE commit of a write is found by position (the newest MERGE after the starting version): a concurrent MERGE on the same table can be mistaken for it in the metrics only.
- Delta type widening and column mapping are not required.
- A guarded MERGE costs one more commit (the claim), and so does a guarded write that needs a migration commit.
- With the guard on, PLM207 is raised before any data changes, for every strategy.

## Deviations found while testing

- Delta skips an append of an empty DataFrame unless the commit carries a transaction. When the guard does not apply (no run start time, or `txn_guard: false`), an empty token commit gets an app id of its own, `<app id>@<token>` with version 1, so it is still recorded.
- A table that `saveAsTable` creates does not keep the writer's `userMetadata`. The `spark_catalog` catalog therefore creates a missing table empty first, with no token, for append, merge (first load) and replace_partitions, and the tokened write is an ordinary write into it.

## Amendment (1.3): per-window transactions

[ADR-0054](0054-interval-incrementals.md) gives each window of an interval model its own token, `run_id:model:interval_id`, and the Spark engine now declares `interval_writes`.

Transaction identity. Every window of a run carries the same `txnVersion`, the run start in epoch milliseconds. Under the one app id per model, Delta would skip the second window as a replay of the first. A window token therefore gets the app id `pipeloom/<project>/<model>/<interval_id>`: every part of the token after the run id, joined by `/`. Tokens without an interval keep `pipeloom/<project>/<model>`, and the `#quarantine` suffix still applies, so a window's quarantine write has its own app id too. A rerun of window k in the same run finds its token and replays, and window k+1 is a different app id at the same version, so it is never mistaken for a replay. A later run writes the same window with a newer version, so a stale retry of an older run still gets PLM207 for that window. Delta keeps one transaction entry per app id in the table snapshot, so a daily model adds one small entry a day.

Finding a window's token. `applied_result` is unchanged: it matches the exact token in `userMetadata`, scanning `DESCRIBE HISTORY ... LIMIT 100` first. A run of many windows makes more than 100 commits, so the first windows of a retried run can be outside that window; the write then reaches Delta, the per-window transaction makes Delta skip it, and the full history scan after the write returns the stored result. No window is written twice.

The write. `replace_range` with the append strategy is one `mode("overwrite")` write with `replaceWhere` set to `column >= start AND column < end`. Delta deletes the rows that match, whatever the partitioning, and adds the batch in the same commit, so a table partitioned by month loses only the window's day. An empty batch deletes the window's rows. A batch row outside the range or with a null column raises `WriteKeyError` (PLM206) before any commit; Delta would refuse it anyway through its replaceWhere constraint check. Bounds are SQL literals: a zoned timestamp is `timestamp_micros(...)`, so the session time zone never shifts a window. A missing table on the `delta_path` catalog is created by a plain first append of the batch. The same predicate filters the window's input (`SupportsFilterBetween`), so Delta can skip files by their statistics.

Provenance: designed from the public Delta Lake documentation of `replaceWhere` and idempotent writes (`txnAppId`, `txnVersion`). No code from other projects was used.
