# Incremental models

An incremental model processes only the input rows that are new since its last run. pipeloom
keeps a watermark, the largest value of one input column seen so far, and filters the input with
it. The watermark is saved after the write succeeds, so a failed run never skips rows.

<figure markdown="span">
  ![The stored watermark filters the input table; the model body runs on the new rows; the batch is merged or appended into the target table; then the watermark is committed to the state store as the batch maximum.](../diagrams/incremental.light.svg#only-light)
  ![The stored watermark filters the input table; the model body runs on the new rows; the batch is merged or appended into the target table; then the watermark is committed to the state store as the batch maximum.](../diagrams/incremental.dark.svg#only-dark)
  <figcaption>An incremental run: the watermark is committed only after the write. Source: <a href="https://github.com/emanuel-luis/pipeloom/blob/main/docs/diagrams/incremental.d2">incremental.d2</a>.</figcaption>
</figure>

## A first incremental model

Project `pipeloom.yml`:

```yaml
config_version: 1
name: shop
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
sources:
  raw:
    format: csv
    location: raw
    options:
      try_parse_dates: true   # read updated_at as a timestamp, not text
    tables:
      raw_orders:
        location: orders.csv
```

`raw/orders.csv`:

```text
id,customer_id,amount,status,updated_at
1,10,5.0,paid,2026-01-01T10:00:00
2,10,2.5,new,2026-01-01T11:00:00
3,11,7.0,paid,2026-01-02T09:00:00
```

`models/orders.py`:

```python
from pipeloom import model


@model(
    materialized="incremental",
    incremental={"input": "raw_orders", "column": "updated_at", "lookback": "2h"},
    write={"strategy": "merge", "unique_key": ["id"]},
)
def orders(ctx, raw_orders):
    return ctx.sql("select id, customer_id, amount, status, updated_at from raw_orders")
```

The `incremental` block names the input to filter (it must be one of the model's inputs), the
column that moves forward, an optional `lookback` and where the watermark lives. Run it twice,
adding rows to the CSV in between:

```console
$ pipeloom run
... state.watermark_committed {'column': 'updated_at', 'dtype': 'timestamp[us]', 'before': None, 'after': '2026-01-02T09:00:00', ...}
... 1/1 OK     orders ........ incremental/merge   3 rows
$ printf '4,12,9.0,new,2026-01-03T09:00:00\n' >> raw/orders.csv
$ pipeloom run
... 1/1 OK     orders ........ incremental/merge   2 rows
```

The second run read only rows at or after `2026-01-02T07:00:00` (the watermark minus two hours),
so it wrote 2 rows: the new order 4 and a re-merge of order 3 that falls inside the window.

## How a unit decides what to read

A unit runs incrementally when the materialization is `incremental`, the target table exists,
the run is not a `--full-refresh` and a watermark is known. Otherwise it reads the whole input
and writes the full result (the first build, or a rebuild).

- **Strict bound.** With no `lookback` the filter is `column > watermark`. A row equal to the
  watermark is not read again.
- **Lookback.** With `lookback` above zero the filter is `column >= watermark - lookback`. The
  value takes a duration (`2h`, `1d`, or seconds). On a date column it is rounded up to whole
  days. Use it for data that arrives late. A lookback needs a timestamp or date column; on any
  other type the unit fails with PLM205.
- **Types.** The column's type family (timestamp with a zone, timestamp without one, date,
  integer, decimal, float, string) must match the stored watermark, or the unit fails with
  PLM205. A column missing from the input is PLM205 too.
- **New watermark.** After a successful write the watermark becomes the larger of the old one and
  the largest value in the batch. It never moves back, even when the lookback window returns
  older rows than the watermark.

A Python model sees the decision on its context:

```python
def build(ctx, raw_orders):
    if ctx.is_incremental:
        ctx.log("incremental run", watermark=str(ctx.watermark), lower_bound=str(ctx.lower_bound))
    return raw_orders
```

`ctx.is_incremental` is true only on an incremental run. `ctx.watermark` and `ctx.lower_bound`
are `None` otherwise. The input it receives is already filtered.

## Where the watermark lives

`watermark_source` has two values.

| Value | Behavior |
|---|---|
| `state` (default) | The watermark is stored in the state store under the project, target and model. If it is missing but the target table exists, pipeloom reads the maximum of the column from the target, uses it and records `watermark_seeded: target` in the unit's `warnings`. |
| `target` | The watermark is the maximum of the column in the target table, read on every run. The state store is never written. |

Use `target` when the table itself is the truth, for example after restoring it from a backup,
and you do not want a state store entry to disagree with it.

## Appending with a lookback

With `strategy: append` and a lookback above zero, rows inside the window are written again on
every run. `pipeloom validate` warns with PLP410. Use `merge` with a `unique_key` instead (see
[Merge, replace_partitions and schema changes](write-strategies.md)). The default strategy of an
incremental model that sets none is `append`.

## Full refresh and the first build

`pipeloom run --full-refresh` ignores the watermark, reads the whole input and writes the table
as an overwrite, whatever the strategy. The new watermark is the batch maximum. The first build
(the target does not exist) also reads everything, and it keeps the declared strategy: into an
absent table every strategy writes the same rows, and merge still checks its key.

## Retries, reruns and the watermark

The write stores the watermark it intends to commit, together with its idempotency token. If a
run dies after the write and before the watermark is saved, rerunning with the same `--run-id`
finds the token, skips the read, the body and the write, and commits that stored watermark. New
rows that arrived in between are not skipped, because the retry never looked at them.

If the stored watermark changed while the unit ran, which usually means a second run on the same
target, the unit fails with PLM203. The data is already written. Do not use `--no-lock` to run two
runs of one target at once.

## What the run records

- Event `state.watermark_committed` with the column, type, before and after values, the source
  and whether the value was seeded.
- In `run_results.json`: `is_incremental`, `watermark_before` and `watermark_after`, plus
  `rows_inserted`, `rows_updated` and `rows_deleted` for merge. See
  [Run artifacts](../reference/artifacts.md).
- Engines that can run incremental models: polars and pandas (and the in-memory test engine).

## Interval models

An interval model processes its input in time windows instead of after a watermark. pipeloom
remembers which windows it has processed, so a later run fills a gap or reprocesses one window
without duplicates ([ADR-0054](../adr/0054-interval-incrementals.md), since 1.3).

```python
@model(
    inputs={"raw": source("raw", "events")},
    materialized="incremental",
    incremental={
        "input": "raw",
        "interval": {"event_time": "ts", "grain": "day", "begin": "2026-01-01", "lookback": 1},
    },
)
def daily_events(ctx, raw):
    start, end = ctx.interval  # the window this call runs, in UTC
    return raw
```

- `event_time` is a column of the input and of the output. It must be a timestamp or a date;
  `grain: hour` needs a timestamp.
- `grain` is `hour`, `day`, `week`, `month` or `year`. Windows are half-open UTC ranges
  `[start, end)`. They start at the start of the hour, at midnight, on Monday, on the first of
  the month or on January 1, all in UTC. An offset in `begin` is converted to UTC, so a daylight
  saving change never makes a window shorter or longer.
- `begin` is an ISO date or date-time. The first window is the one that holds it. A value without
  an offset is UTC.
- `lookback` is a number of windows. They run again before the first missing window.

Each window reads the incremental input filtered to `start <= event_time < end`, runs the
function once, and replaces the window in the target: rows of the target in the window are
deleted and the new rows are inserted, in one commit. An empty window removes the window's old
rows. Rows outside the window are never touched, even in a table partitioned by month while the
grain is a day. A row the function returns outside its window fails the write with PLM206.

Which windows run:

- The first run, or a run with `--full-refresh`, runs every window from `begin` up to the one
  that holds the run's start time. The current window is partial and runs too. A full refresh
  overwrites the table with its first window.
- A later run runs the windows the ledger does not cover yet, plus `lookback` windows before the
  first of them. The partial window of the previous run counts as covered only up to that run's
  start, so the next run processes it again in full.
- When nothing is missing, nothing runs and the row reports zero rows written.

The windows run first to last inside one execution unit, so the DAG and the orchestrator
renderers do not change. If a window fails, the unit stops there and the earlier windows stay
written and recorded. A retry with the same `--run-id` skips the windows it already wrote,
because each window has its own idempotency token, `<run_id>:<model>:<interval_id>`.

The ledger lives in the state store under the reserved key `<model>@intervals` of the target, as
a list of `[start, end]` pairs. A dev target never reads or writes the ledger of prod.

Rules checked before a run: an interval model is materialized incremental, writes with append,
does not set `incremental.column`, and has the event_time column in its contract when it declares
one (PLP417). The engine must declare `interval_writes`: polars, pandas and DuckDB do; Spark does
not yet (PLC514). A missing or unusable event_time column at run time is PLM209.

In `run_results.json`, an interval model's row has an `intervals` array with one entry per window:
`interval_id`, `start`, `end`, `idempotency_token`, `skipped` and `rows_written`. The row's counts
are the sums over its windows.
Each entry of a failed window also has `failed: true`.

## Backfilling a range

`pipeloom backfill` runs interval models over a range you name instead of from `begin` to now
(since 1.3):

```bash
pipeloom backfill --select daily_events --start 2026-01-01 --end 2026-02-01
```

- `--start` and `--end` are ISO dates or date-times; a value without an offset is UTC. The range
  is `[start, end)`. Each model runs the windows of its own grain that overlap it, so a bound that
  is not on a window edge rounds outward: the whole window that holds it runs. The range is
  clipped to the model's `begin` and to the run's start. `lookback` does not apply.
- `--rerun missing` (the default) runs only the windows the ledger does not cover. Run the same
  backfill again after a failure, or after you stopped it, and it picks up the missing windows
  only. After a backfill that succeeded it writes nothing.
- `--rerun all` restates the range: it removes the range from the ledger, then runs every window
  in it. Use it when the windows are wrong, for example after a fix to the model or to late data
  upstream. If it stops halfway, `--rerun missing` over the same range (or `pipeloom retry`)
  finishes it.
- `--on-failure stop` (the default) ends a model at its first failing window; the earlier windows
  stay written and recorded. `--on-failure continue` runs the rest of the windows and then fails
  the unit with a message that lists every failed window.
- `--mark-done` records the range in the ledger exactly as given, without rounding, and runs
  nothing. It is for a model converted from a watermark: see
  [Migrating within 1.x](migrate-1.x.md). Repeating it changes nothing.

Each window keeps its token and its ledger entry, exactly as in `pipeloom run`. A selected model
without `incremental.interval` runs as it would in `pipeloom run`, so `--select daily_events+`
also rebuilds what reads it. A selection with no interval model is PLX132, and so is
`--mark-done` with a selected model that has no interval. `backfill` takes the flags of `run`
except `--full-refresh`, `--manifest`, `--state` and `--defer`. The `args` of
`run_results.json` record the range under `backfill`.
