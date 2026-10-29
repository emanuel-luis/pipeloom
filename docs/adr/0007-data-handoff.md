# ADR-0007: Data handoff: execution units, inlining, intermediates

Architecture: [Section 8, ADR-0007](../architecture.md#8-execution-and-scheduler)

## Context

With `boundary: task`, each unit runs in its own process or orchestrator task, so no in-memory or temporary-view handoff is possible (lesson L7).

## Options

- **Persisted intermediates everywhere.** Simple, but costs IO.
- **Inlining everywhere.** No extra IO, but recomputes shared work.
- **A hybrid,** decided at compile time.

## Decision

The hybrid.

- `ephemeral` models are inlined. Their bodies run inside each consuming unit. Under `boundary: process` the result is memoized per run, so it is computed once.
- `intermediate` models are persisted to `<artifacts.root>/runs/<run_id>/intermediate/<model>` (or a scratch schema the catalog provides). Finalize deletes them after `intermediate_ttl`.
- `view` creates a catalog view where the engine supports it, never a session view.

## Consequences

- The same plan runs in one process or many.
- PLP406 warns when an ephemeral model fans out to more than `ephemeral_fanout_warn` (default 3) units and suggests `intermediate`.

## Status

Accepted

## Date

2026-10-29
