# ADR-0010: Events, sinks, artifacts; rich for the console

Architecture: [Section 11, Events, logs and run metadata](../architecture.md#11-events-logs-and-run-metadata)

## Context

Logs, metrics, artifacts and lineage should not each have their own plumbing (lesson L1). Runs must also leave structured metadata that can be queried later.

## Options

- **Separate logging, metrics and lineage paths.** Each is simple alone, but they drift apart.
- **One typed event stream with subscribers.**
- **Console library:** rich gives styled tables, colors and live progress in one dependency. structlog would still need a renderer for those. JSON lines need only the stdlib.

## Decision

One typed stream (`pipeloom.event/1`) and a synchronous in-process bus.

- The envelope carries `id`, `run_id`, `invocation_id`, `seq`, `ts`, `type`, `level`, `unit_id`, `model`, `attempt`, `data` and `context`. Cross-invocation ordering is by (`ts`, `invocation_id`, `seq`).
- The bus fans out to sinks in configured order. Each sink has a `failure_budget` (default 5 consecutive errors). When exhausted the sink is disabled and `sink.disabled` goes to the remaining sinks.
- `buffered: true` wraps a sink in a bounded queue and worker thread, with drop-oldest and a count of drops.
- A redaction filter masks registered secret values before any sink sees an event.
- Built-in sinks: `console`, `jsonl`, `file`, `artifacts` (always on). `pipeloom-openlineage` maps units to OpenLineage jobs.
- The console uses rich. JSON lines use the stdlib.
- Artifacts: `manifest.json` (`pipeloom.manifest/1`) and `run_results.json` (`pipeloom.run_results/1`), with per-model and per-run metadata.

## Consequences

- Any consumer (console, files, lineage) is a sink, and tests can assert ordered events with `RecordingSink`.
- A slow sink on the synchronous bus slows the run unless it is buffered.
- JVM traces are trimmed on the console. The full trace goes to JSON events only.
- The OpenLineage output-statistics facet is used if available (unverified).

## Status

Accepted

## Date

2026-10-29
