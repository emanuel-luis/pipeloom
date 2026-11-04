# ADR-0029: Retries, timeouts and cancellation

Architecture: [Section 8, Execution](../architecture.md)

Amends [ADR-0006](0006-ready-queue-scheduler.md) and [ADR-0020](0020-execution-model-0-2.md).

## Context

A unit can fail for reasons that pass on their own (a dropped connection, a table lock), run for too long, or be stopped with the run. 0.2 had the states for this but no behavior: a failed unit stayed failed, a hung unit hung the run, and an executor could not stop anything.

## Options

- **Retry inside the runner.** Simple, but a process executor would then retry inside the child, where the parent cannot see it, and an external orchestrator would retry twice.
- **Retry in the owner of the schedule.** The scheduler (the parent for the process executor) retries; a unit invocation under an orchestrator never does.

## Decision

Retries belong to whoever schedules the unit. `Scheduler` keeps a retry heap and drives time from `clock.monotonic()` and `Executor.wait_any(timeout)`; it never sleeps. A failed attempt with retries left moves RUNNING to RETRYING, emits `unit.retrying` and holds no worker slot while it waits. `--select unit:...` runs with `retries=0`; the child of a process executor is such an invocation.

Classification (`pipeloom.runtime.retry.classify`), in order: `TransientError` anywhere in the cause chain (depth 10); the engine's `is_transient` opinion; `ConnectionError` or `TimeoutError` in the chain; in the read, write and state phases any `OSError` except FileNotFoundError, PermissionError (but a Windows sharing violation, `winerror` 32 or 33, is transient), IsADirectoryError, NotADirectoryError and FileExistsError; PLX106 (a child that died without a result) is transient. Everything else is permanent. PLQ301, PLQ302, PLM202, PLM203, PLM205 and PLM206 are always permanent. `retry_on: transient` (the default) retries transient failures only; `retry_on: any` retries every failed attempt, quality and schema failures included, which repeats deterministic failures and is documented as such.

Backoff: `d = min(max(base, 300), base * 2**(n - 1))` and the wait is `d * (0.5 + 0.5 * u)` with `u` from `random.Random(f"{run_id}:{unit_id}:{n}")`, so delays are in [d/2, d], capped, and reproducible.

Timeouts are per attempt and are never retried: a timed-out unit is TIMED_OUT (PLX102), its descendants are skipped, and the run fails. The thread executor runs one daemon thread per attempt so a timed-out thread can be abandoned. It sets the attempt's `CancelToken`, calls `EngineSession.cancel(unit_id)` once, writes the timed_out attempt file itself (the thread may never return) and drops whatever the thread reports later. The runner checks the token at checkpoints (before reading, after the body, before the write and around the quarantine write) and never between the start of the table write and the end of the watermark swap. Cancellation is cooperative, so an abandoned polars or pandas thread can still commit after the verdict; this is the one place where the attempt file can say timed_out while data moved. The first terminal attempt file wins (`write_json_once`: a temp file linked, or renamed on Windows, to the target, which fails when it exists), and `run.completed` reports `abandoned_threads`. Finalize lets a scheduler TIMED_OUT or CANCELLED verdict win over a result written later and records `late_result: <status>` in `warnings[]`.

The process executor starts each child in its own process group (`CREATE_NEW_PROCESS_GROUP` on Windows, `start_new_session` elsewhere) so a terminal Ctrl+C reaches only the owner. The child enforces the timeout itself through its thread executor; the parent's own deadline is `timeout + cancel_grace`, after which it kills the child and everything it started (`killpg` on POSIX, `taskkill /T /F` on Windows, both only while the child has not been reaped, so a reused pid is never signalled) and writes the timed_out attempt. After a child exits on POSIX the executor sweeps its process group, so a grandchild cannot keep the pipes open. Reader threads and `shutdown` joins are bounded, so a stuck child cannot hang the run. Windows has no job objects in 0.3: a grandchild of a child that exits on its own survives. A graceful stop is SIGTERM on POSIX and CTRL_BREAK_EVENT on Windows (CTRL_C_EVENT cannot target a process group), followed by a kill after `cancel_grace`. The executor records every `Popen`, so a test can check that none outlived the run. The parent emits `unit.timed_out` or `unit.cancelled` only when the child did not already forward one for that attempt.

A stop request (`RunControl.request_stop`) cancels pending, queued and retrying units, asks running units to stop, lets those that finish within `cancel_grace` keep their real result, and then marks the rest CANCELLED (PLX104). `RETRYING -> CANCELLED` is added to the transition table, and fail-fast cancels retrying units. The run emits one `run.cancel_requested{signal, grace_s}`, finishes with status `cancelled` and exits 130.

`pipeloom run` wires signals to a `RunControl` (`cli/_signals.py`, main thread only, previous handlers restored): SIGINT and SIGTERM on POSIX, SIGINT and SIGBREAK (Ctrl+Break) on Windows. The first signal requests a graceful stop, the second a hard stop that skips the grace, the third raises KeyboardInterrupt. Handlers only set flags; `RunControl` calls registered wakers (the scheduler registers `executor.wake`), so the install function takes no executor. A hard stop, or the end of the grace, abandons the thread attempts that are still alive (their attempt file is written as cancelled and their late result is dropped). A unit invocation (child process) handles the same signals, stops its unit and exits 130. `cancel_grace` is a setting (`Duration`, default 30s) and the flag `--cancel-grace` overrides it.

## Consequences

- A flaky read or write no longer fails a run; the cost is visible as `unit.retrying` events, one attempt file per attempt and `retries_used` in the result row.
- A hung thread cannot be killed. It is abandoned and counted, and it ends with the process; a hung polars or pandas call can therefore outlive its unit.
- Timeouts and delays are whole seconds in the config (`Duration`); the scheduler accepts fractions, so tests and API users can use shorter ones.
- `Executor` changes shape: `wait_any(timeout)`, `cancel(handle, reason)`, `wake()` and `has_terminal_event(handle)`. `UnitHandle` gains `seq` and `attempt`.

## Status

Accepted

## Date

2026-11-04
