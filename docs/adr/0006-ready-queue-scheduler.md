# ADR-0006: Ready-queue scheduler

Architecture: [Section 8, Execution and scheduler](../architecture.md#8-execution-and-scheduler)

## Context

Running a graph level by level makes every unit in a level wait for the slowest one (lesson L6). Scheduling must be deterministic so runs are reproducible and testable.

## Options

- **Level barriers.** Simple, but idle workers wait at each level.
- **A ready queue with no level barrier.** Starts a unit as soon as its parents are done.

## Decision

A ready-queue scheduler with no level barrier.

- When a unit reaches a terminal state, each child's count of pending parents goes down by one. A child whose count reaches zero is enqueued.
- Ready units are ordered by descending count of downstream units, then by name, which is deterministic.
- `workers` caps concurrency. `pool` tags (a later milestone) cap per-resource concurrency.
- Policies: retries with exponential backoff and jitter (transient errors only unless `retry_on: any`), per-unit timeouts, cooperative cancellation with `cancel_grace`, skip-downstream by default with `--fail-fast` as an option, and a `StateStore.lock` concurrency guard per project and target.

## Consequences

- Property tests (hypothesis) check the invariants: a unit starts only after all parents succeed, and every unit reaches exactly one terminal state.
- Thread-executor timeouts are cooperative: code that ignores cancellation keeps running until the process ends. The process executor kills the child.
- With `boundary: task` the orchestrator owns retries.

## Status

Accepted

## Date

2026-10-29
