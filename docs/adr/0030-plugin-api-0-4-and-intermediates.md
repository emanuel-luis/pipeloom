# ADR-0030: Plugin API (0, 4) and intermediates

## Context

The Spark engine of 0.4 needs three things from the core that plugin API (0, 3) does not give it. Delta makes a write idempotent with an application id and a monotonically increasing transaction version, so every invocation of a run must see the same start time. Spark does its work when the write happens, so cancellation tags must cover the read, the body, the checks and the write, not only the body. A state store that lives in the engine (0.4 adds one on Delta) needs the engine session before the run lock is taken. Large results shared between units also need a place that is not a catalog table. This ADR covers the first half of the change: the API, the run start time and the ordering. The `intermediate` materialization is covered in the second half below.

## Options

- **Derive the start time inside each engine.** Every engine would pick its own clock reading, so two invocations of one run (process boundary, retries after a crash) would disagree and Delta would refuse or double-apply writes.
- **Resolve it once in the core and put it in the runtime context.** One definition, recorded with the run, available to every engine.
- **Open the session after the lock, as in 0.3.** Simple, but a state store that needs a session cannot take the lock.

## Decision

Plugin API becomes (0, 4); during 0.x the match is exact, so the memory, polars and pandas engines move with the core. `RuntimeContext` gains `run_started_at` (timezone-aware UTC) and `artifacts_root` (an absolute POSIX path or URI). `EngineSession` gains `write_intermediate(rel, ref, token)` with `IntermediateRef(run_id, model, location)`. `CatalogCapabilities(views=True)` joins `EngineCapabilities` and `StateStoreCapabilities` as a value of `PluginSpec.capabilities`; PLC507 also fires when the bound catalog declares `views=False`, and the filesystem catalog declares `views=True`.

`run_started_at` resolves in this order: a recorded start for the run id, then `--run-started-at` (environment `PIPELOOM_RUN_STARTED_AT`), then the clock at the start of the owning invocation. A unit invocation with neither has no start. The flag must carry a UTC offset and needs `--run-id`; both are usage errors (exit 2). A flag that disagrees with the recorded start is PLX108 (exit 2) and names both values; an equal value is accepted. The first invocation records the resolved value as the run's `started_at`, so later invocations, unit invocations under the process boundary and reused run ids all read the same instant.

`execute_run` opens the engine session before it takes `state.lock`, and closes it after the lock is released. The runner holds `EngineSession.unit_scope` from the first read of a unit to the end of its write, once per attempt; the scope that wrapped only the model body is gone.

### Intermediates (amends ADR-0007)

An `intermediate` model is a persisted unit. Its result goes to `<artifacts.root>/runs/<run_id>/intermediate/<model>` through `session.write_intermediate(rel, IntermediateRef(run_id, model, location), token="<run_id>:<model>")`, and consumers read it with `ReadSpec("intermediate", location=...)`. The write always overwrites, and a repeated token writes nothing. `run_results` reports the location as the unit's `relation_name`. An intermediate cannot be incremental, use a strategy other than overwrite, or set `partition_by` (PLP413). The memory engine keeps JSON at the location; polars and pandas keep a filesystem table there. A Spark Connect server whose filesystem differs from the client's needs a shared artifacts root; a scratch schema is a later option.

`artifacts.intermediate_ttl` (default 1d, `0` allowed) bounds how long they live. Finalize calls `cleanup_intermediates` after it knows the run status. It deletes the intermediates of runs whose `run_results.json` `ended_at` (folder mtime when absent) is older than the TTL, and the current run's only when the TTL is 0 and the run succeeded, so a failed run can continue within the TTL. A missing folder is fine, and a deletion error becomes an entry in the run's `warnings`, never a failure.

## Consequences

Engines can derive idempotency keys from `run_started_at` without reading the clock. A run that finds the lock held now opens and closes one session first, a small cost the memory, polars and pandas engines do not notice. A flag from an orchestrator that differs from what a retried run recorded stops the run instead of silently using the other value. Plugins built for (0, 3) fail with PLU001.

## Status

Accepted.

## Date

2026-11-04
