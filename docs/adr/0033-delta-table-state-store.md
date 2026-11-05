# ADR-0033: Delta table state store

## Context

ADR-0008 keeps run state (watermarks, run and unit records, the run lock) behind the `StateStore` protocol, with a sqlite store for one machine and a JSON directory store for a shared volume. A Spark project often runs where neither fits: several drivers, or a cluster whose local disk is not shared and whose object store gives no atomic create or rename. Such a project already has Delta tables and a Spark session, and Delta commits are atomic with optimistic concurrency.

## Options

- **One Delta table, MERGE for every mutation.** One table holds every record kind; a single `MERGE` gives compare-and-set and lease taking, since Delta rejects a commit that conflicts with a concurrent one. Needs only the Delta SQL extension, so it works the same on classic Spark, Spark Connect and Databricks. Each call is a Spark job, so it is slower than sqlite.
- **One table per record kind.** Mirrors the sqlite schema, but quadruples the tables to create, version and clean up, for no gain in atomicity.
- **The `DeltaTable` Python API.** Routes to Delta Connect on a Connect session, which needs a server plugin (ADR-0032 rejects it for writes for the same reason).
- **An external database.** Strong, but a new dependency and a new service to run.

## Decision

The `pipeloom-spark` plugin adds the `table` state store (`shared=True`): one Delta table, partitioned by `kind` (`watermark`, `run`, `unit`, `lock`), addressed by catalog name (`table`) or by path (`path`), exactly one of the two. A row is found by (`kind`, `key`), where `key` is the canonical JSON array of the record's natural key. The `data` column holds the record in the core codec's JSON, so it reads back exactly; the other columns (`project`, `run_id`, `status`, `owner`, `cas_key`, `expires_us` and the other epoch-microsecond times) serve queries and MERGE conditions. The table property `pipeloom.state.schema` is `1`; a higher value is refused with the same error as sqlite. It is created with `CREATE TABLE IF NOT EXISTS`; a lost creation race is ignored when the table is then readable.

Every insert or replace is one `MERGE INTO <table> AS t USING {src} AS s ON t.kind = '<kind>' AND t.kind = s.kind AND t.key = s.key` whose source is a one-row DataFrame from `createDataFrame`, so no record value is spliced into SQL text. The literal `t.kind = '<kind>'` (one of four fixed names) lets Delta prune to that kind's partition: without it the MERGE reads the whole table, and a unit record written by one worker conflicted with the run record or the lock renewal written at the same time. Writers of different kinds now never conflict. Success is the statement's `num_affected_rows`. The clauses are:

- Upsert (runs, units): update when matched, insert otherwise.
- Watermark compare-and-set: update when the stored `cas_key` equals the expected one (`<=>`), or equals the new one and was written by the same run (a replay); insert only when nothing is expected.
- Lock: update when the stored lease has expired (`expires_us <= now`) or when the stored owner is the taker's own (`t.owner = s.owner`), insert when there is none. The owner is new for every acquisition, so the owner clause only matches a replay: a retry after a dropped Connect channel or Py4J connection whose first MERGE committed unseen finds its own row and succeeds, instead of raising `LockHeld` against itself. Zero affected rows means the lock is held: the row is read and `LockHeld` raised. Renewal is an `UPDATE` and release a `DELETE`, both filtered on the owner, run through the core lease helper. Times come from the client clock, as in the other stores.

A Delta concurrent-modification error is retried up to `CONFLICT_RETRIES = 10` times with 50 to 500 ms of random jitter; the MERGE conditions are re-evaluated on the new snapshot, so a lost race turns into a clean false or `LockHeld`. Reads are filtered selects. The store reuses the Spark session it finds (the engine opens its session before the first state call, ADR-0030) and builds one from its `session` options only when none exists; it stops only a session it built.

## Consequences

- Spark projects get shared state with no new service, on every Spark flavor the engine supports.
- Each state call runs a Spark job (tens to hundreds of milliseconds); fine for a few calls per unit, slow for very wide runs.
- The table grows with every run and unit; Delta's own `OPTIMIZE` and `VACUUM` apply. A retention command is a later concern.
- Lease times rely on the clocks of the writers, as with the other stores.
- The contract's cross-process lock test starts a second JVM, so it runs only with `PIPELOOM_TEST_SPARK_HEAVY=1`. CI does not set it yet: on GitHub runners the child JVM stalled for over 240 s while resolving the Delta packages, before it reached the lock. Lock exclusion between writers is still covered in one process (the eight-thread race), and it rests on the same Delta commit check whether the writers share a JVM or not. Running the test in CI, for example with the Delta jars pre-resolved into a local directory, is a follow-up.

## Status

Accepted. Amends ADR-0008.

## Date

2026-11-04
