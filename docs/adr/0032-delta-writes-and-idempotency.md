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

Strategies.
- append: `mode("append")`, `mergeSchema` when the final columns add columns.
- overwrite: `mode("overwrite")` with `overwriteSchema`, so any schema policy result is accepted.
- replace_partitions: `replaceWhere` over the distinct partition tuples of the batch (above 10,000 tuples it fails and recommends merge). Null partitions use `IS NULL` or `<=>`. An empty batch writes only the token commit.
- merge: SQL `MERGE INTO` (null keys never match, so they insert), then an empty append with the token, `txnAppId`, `txnVersion` and the stored result. A crash between the two commits is safe: re-running the MERGE with the same batch converges, and the token commit is the race guard. Soft deletes are counted with one join and moved from updated to deleted. A merge into a missing table is the first load, written as an append.

Schema policy. The core decides the final columns. Added columns go in the write itself (`mergeSchema`; for merge, an empty `mergeSchema` append first, with no token). Removed or widened columns run one migration commit first: the table is read, aligned to the final columns and overwritten with `overwriteSchema`, its `userMetadata` marking `migration_for`, and its partitioning kept. Migration commits carry no `txn` options and no token, so they never consume the write version or count in the metrics.

Metrics come from the commit history. `DeltaMetrics.before_write` returns the latest version and `after_write` sums `operationMetrics` over the versions after it that carry the token, plus the newest MERGE commit for a merge. The mapping is a pure function (`metrics_to_result`). It is plugin-internal: with one implementation there is no core `MetricsProvider`.

Views and intermediates. `create_view` (the `spark_catalog` catalog only) emits `CREATE OR REPLACE VIEW` with one CTE per alias over the physical table or path. `write_intermediate` overwrites a Delta path under the `#intermediate` app id.

PLM207 is a new code and permanent for retries. The core raises it for the new `WriteRefusedError` an engine may raise.

## Consequences

- Every write costs one `DESCRIBE HISTORY` before and one after, plus a schema read.
- The MERGE commit of a write is found by position (the newest MERGE after the starting version): a concurrent MERGE on the same table can be mistaken for it in the metrics only.
- Delta type widening and column mapping are not required.
- A MERGE carries no transaction, so the guard can only refuse its token commit: when a later run already committed the model, the MERGE has been applied before PLM207 is raised. Append, overwrite and replace_partitions are refused before any data lands.

## Deviations found while testing

- Delta skips an append of an empty DataFrame unless the commit carries a transaction. When the guard does not apply (no run start time, or `txn_guard: false`), an empty token commit gets an app id of its own, `<app id>@<token>` with version 1, so it is still recorded.
- A table that `saveAsTable` creates does not keep the writer's `userMetadata`. The `spark_catalog` catalog therefore creates a missing table empty first, with no token, and the tokened write is an ordinary append into it.
