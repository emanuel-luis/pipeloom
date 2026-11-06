# Retries, timeouts and cancellation

A unit can fail for a reason that goes away on its own, run for too long, or be stopped together
with the run. pipeloom has a setting for each case.

## Retries

```yaml
sources:
  - name: raw_orders
    format: csv
    location: raw/orders.csv
models:
  - name: orders
    python: shop.flaky:load
    inputs:
      raw_orders:
        source: raw_orders
    retries: 2          # extra attempts after the first; the default is 0
    retry_delay: 1s     # base delay; the default is 1s
    retry_on: transient # or "any"
    timeout: 5m
```

The model above fails once with a `TransientError` and then succeeds:

```console
$ pipeloom run
... 1/1 FAIL   orders ........ PLX101 pipeloom.engine.errors.TransientError
... RETRY  orders attempt 1 failed PLX101 (transient), next in 0.7s
... 1/1 OK     orders ........ table/overwrite   2 rows
```

Each attempt writes its own file (`units/orders/attempt-1.json`, `attempt-2.json`), and the result
row has `attempt: 2` and `retries_used: 1`. While a unit waits for its next attempt it holds no
worker slot, and every attempt emits `unit.retrying` with the attempt that failed, the next one and
the delay.

`retries`, `retry_delay`, `retry_on` and `timeout` can be set in `defaults` and in a model.

### What is retried

With `retry_on: transient` (the default) only transient failures are retried. A failure is
transient when:

- the model or an engine raised `pipeloom.engine.errors.TransientError`, directly or as the cause
  of another exception;
- the engine says so (for example a table commit conflict);
- a `ConnectionError` or `TimeoutError` is in the cause chain;
- an `OSError` happened while reading, writing or touching state, except file not found, permission
  denied and the directory errors (a Windows sharing violation counts as transient);
- a unit process exited without a result (PLX106).

Everything else is permanent: an exception from your code, a validation error and a failed
expectation. A rejected schema change, a lost watermark race, a bad watermark and a bad write key
are always permanent. With `retry_on: any` every failed attempt is retried, including
deterministic ones, so use it only when you know that a rerun can change the outcome.

An `append` model on an engine without idempotent writes cannot have retries (PLC505), because a
retry could write the same rows twice. The polars and pandas engines have idempotent writes.

### Delay between attempts

For retry number `n` the delay is a random value between half and all of
`min(max(retry_delay, 300 s), retry_delay * 2^(n-1))`. With the default of one second that is up to 1 s,
2 s, 4 s and so on, capped at five minutes. The random part is seeded by the run id, the unit and
`n`, so the same run waits the same time.

### Who retries

Whoever schedules the unit. With `boundary: process` or `task` under `pipeloom run`, the parent
scheduler retries. A single unit run with `--select unit:<id>`, which is what an orchestrator
task does, never retries on its own: the orchestrator owns the retry.

## Timeouts

`timeout` is a limit per attempt. A unit that exceeds it ends as `timed_out` with PLX102, its
downstream units are skipped and the run fails. A timeout is never retried.

```console
... 2/2 TIMEOUT slow_orders ... PLX102 after 2s (attempt 1)
```

pipeloom cannot kill a Python thread. On the thread executor (`boundary: process`) the attempt is
asked to stop and then abandoned: the unit is marked `timed_out` at once and whatever the thread
reports later is dropped. The engine's cancel is called once. The model stops at the checkpoints
before reading, after the body and before the write; a long call in the middle of your code is not
interrupted. If the thread is still working when the run ends, `run.completed` reports it in
`abandoned_threads`, and the thread ends with the process. In the rare case where an abandoned
thread still commits its write, the attempt file says `timed_out` and the unit gets a
`late_result: <status>` warning. With `boundary: task` the child process is killed after
`timeout` plus the grace period, together with everything it started.

## Cancellation

Press Ctrl+C (or send SIGTERM; on Windows, Ctrl+C or Ctrl+Break) to stop a run:

1. The first signal is a graceful stop. Nothing new starts, pending, queued and retrying units are
   cancelled, and running units are asked to stop. They get `cancel_grace` seconds to finish, and
   a unit that finishes in time keeps its real result. The rest end as `cancelled` (PLX104). The
   run emits `run.cancel_requested`, ends with status `cancelled` and exits with 130.
2. A second signal is a hard stop: no more grace; running units are cancelled at once.
3. A third signal raises `KeyboardInterrupt`.

`cancel_grace` is 30 seconds by default. Set it per target, or override it for one run:

```yaml
targets:
  dev:
    engine:
      kind: polars
    catalog:
      kind: filesystem
      root: target/warehouse
    state:
      kind: json
      path: .pipeloom/state
    cancel_grace: 10s
```

```console
$ pipeloom run --cancel-grace 5s
```

From Python, pass a `RunControl` to `Project.run(..., control=control)` and call
`control.request_stop()`.

With `boundary: task` each child runs in its own process group, so a Ctrl+C in your terminal
reaches only the parent, which then stops the children. A graceful stop is SIGTERM on POSIX and
Ctrl+Break on Windows, followed by a kill after the grace. Windows has no job objects in 0.3: a
grandchild started by a child that exits on its own can outlive the run.

Run IDs are reusable after a stop. Rerun with the same `--run-id` and the units that already
committed are skipped through their write tokens.
