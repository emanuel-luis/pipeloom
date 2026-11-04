# ADR-0020: Execution model in 0.2

Architecture: [Section 8, Execution and scheduler](../architecture.md#8-execution-and-scheduler)

Amends [ADR-0006](0006-ready-queue-scheduler.md), [ADR-0007](0007-data-handoff.md) and [ADR-0010](0010-events-sinks-artifacts.md).

## Context

0.2 is the first release that runs plans. The scheduler, the executors and the unit events need one agreed shape before the model runner, the process executor and the console rendering are built on them.

## Options

- **An `executor` config key.** Flexible, but a second knob that can contradict `boundary`.
- **The boundary picks the executor.** One setting, one meaning.
- **Multiprocessing for the process executor.** Shares state implicitly and behaves differently per platform.
- **A subprocess that runs `pipeloom run --select unit:<id>`.** Only argv crosses the boundary, so the same path works from an orchestrator.

## Decision

- The boundary picks the executor. `boundary: process` uses a thread pool and one engine session shared by the invocation; `boundary: task` starts a subprocess per unit. There is no `executor` key and no multiprocessing.
- The scheduler is a ready-queue loop with no level barrier. Ready units sit in a heap ordered by `(-downstream, unit_id)`, so a given completion order always yields the same start order. Parents outside the selection count as satisfied.
- It talks to an `Executor` protocol: `submit`, a blocking `wait_any`, `cancel` (unused until 0.3) and `shutdown`. The thread executor and a simulated executor for tests implement it.
- Unit states are checked against one transition table. `queued -> skipped` and `queued -> cancelled` exist besides the section 8 transitions, because a unit waits in the queue while a failure or a stop arrives. `retrying`, `timed_out` and `running -> cancelled` are in the table and unused until 0.3.
- A failure skips every pending or queued descendant in topological order with reason `upstream_failed` and the failed unit id. `--fail-fast` skips every other pending or queued unit with reason `fail_fast`; running units finish. A stop request cancels pending and queued units and lets running units finish.
- An internal error (a bug in the runner or executor, not a failed unit) stops dequeuing, drains the running units and is re-raised after the scheduler result is built.
- The scheduler emits `unit.queued`, `unit.skipped` and `unit.cancelled`. The invocation that executes a unit emits `unit.started`, the terminal unit event and the `model.*` events. With `boundary: task` the child emits its own and the owning invocation forwards them, keeping the child's invocation id and sequence. Attempt files are written by the runner, and the child's attempt file is the authoritative result.
- A unit invocation (`--select unit:<id>` only) takes no run lock, emits no `run.*` events and does not finalize.
- The run lock is a lease (60 s, heartbeat every 20 s), not a process liveness check.

## Consequences

- Scheduler events are deterministic for a given completion order; with one worker or the simulator the whole stream is.
- Property tests run the scheduler on random graphs with the simulator and check the invariants.
- The thread executor cannot stop a running unit. Cooperative cancellation, retries and timeouts come in 0.3 and change `wait_any` and `cancel` in an additive way.
- Starting a subprocess per unit costs about a second per unit.

## Status

Accepted

## Date

2026-11-04
