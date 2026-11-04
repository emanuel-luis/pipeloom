# Incremental models

An incremental model processes only the input rows that are new since its last run. pipeloom
keeps a watermark, the largest value of one input column seen so far, and filters the input with
it. The watermark is saved after the write succeeds, so a failed run never skips rows.

## A first incremental model

Project `pipeloom.yml`:

```yaml
config_version: 1
name: shop
targets:
  dev:
    engine: {kind: polars}
    catalog: {kind: filesystem, root: target/warehouse}
    state: {kind: json, path: .pipeloom/state}
```

`raw/orders.csv`:

```text
id,customer_id,amount,status,updated_at
1,10,5.0,paid,2026-01-01T10:00:00
2,10,2.5,new,2026-01-01T11:00:00
3,11,7.0,paid,2026-01-02T09:00:00
```

`models/orders.yml`:

```yaml
sources:
  - name: raw_orders
    format: csv
    location: raw/orders.csv
    options: {try_parse_dates: true}   # read updated_at as a timestamp, not text
models:
  - name: orders
    sql: select id, customer_id, amount, status, updated_at from raw_orders
    inputs: {raw_orders: {source: raw_orders}}
    materialized: incremental
    incremental: {input: raw_orders, column: updated_at, lookback: 2h}
    write:
      strategy: merge
      unique_key: [id]
```

The `incremental` block names the input to filter (it must be one of the model's `inputs`), the
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
