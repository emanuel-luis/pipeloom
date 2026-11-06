# Merge, replace_partitions, scd2 and schema changes

`write.strategy` decides how a batch meets the rows already in the target table. 0.3 adds
`merge` and `replace_partitions` next to `append` and `overwrite`, and a policy for batches whose
columns differ from the table. The polars and pandas engines support all of it on filesystem
tables. 0.9 adds `scd2` on polars, pandas and Spark.

## merge

Rows are matched on `unique_key`. Matched rows are updated, unmatched rows are inserted.

```yaml
models:
  - name: orders
    python: shop.orders:build
    inputs:
      raw_orders:
        source: raw_orders
    materialized: incremental
    incremental:
      input: raw_orders
      column: updated_at
      lookback: 2h
    write:
      strategy: merge
      unique_key: [id]
      update_condition: "source.updated_at > target.updated_at"
      deletes:
        column: status
        value: cancelled
        mode: hard
```

- `update_condition` is optional and written in the engine's dialect (a SQL expression on
  polars, a restricted Python expression on pandas). `source.x` is the batch, `target.x` the table,
  and a bare name means the source. A matched row whose condition is false or null is left
  unchanged.
- `deletes` marks batch rows as deletions: a row where `column` equals `value` removes the
  matched target row with `mode: hard`, or writes the batch row with `mode: soft` so you can keep
  a flag. A delete for a key that is not in the table is ignored. The `update_condition` applies
  to deletes too.
- A batch with a repeated key, or without the key column, fails with PLM206 before anything is
  written. A row with a null key never matches and is inserted.
- `run_results.json` reports `rows_inserted`, `rows_updated` and `rows_deleted`; `rows_written`
  counts inserts, updates and soft deletes.

In 0.3 a merge reads the whole table, merges in memory and rewrites it, so its cost grows with the
table. Row order is not preserved.

## replace_partitions

`replace_partitions` swaps the partitions present in the batch and leaves the others alone.
It needs `partition_by`; without it `pipeloom validate` reports PLP407.

```yaml
models:
  - name: daily_clicks
    sql: select event_date, user_id, clicks from raw_events
    inputs:
      raw_events:
        source: raw_events
    partition_by: [event_date]
    write:
      strategy: replace_partitions
      on_schema_change: append_new_columns
```

With `partition_by` every strategy writes one part file per distinct partition value. A
`replace_partitions` write drops the files of the partitions in the batch and adds the new ones
without reading the other files. `rows_deleted` reports how many old rows went away. A table
written by 0.2 has no partition values in its files, so the first replace reads and rewrites
those files once. An empty batch changes no files. A null is a partition value like any other.

## scd2

`scd2` keeps the history of each key as a type 2 slowly changing dimension. The table holds the
model's columns followed by three system columns: `valid_from` and `valid_to`
(`timestamp[us, UTC]`) and `is_current` (`boolean`).

```yaml
models:
  - name: customers_history
    sql: select id, name, tier, status from raw_customers
    inputs:
      raw_customers:
        source: raw_customers
    write:
      strategy: scd2
      unique_key: [id]
      tracked_columns: [name, tier]
      deletes:
        column: status
        value: closed
        mode: hard
```

- The first load, and every `--full-refresh`, writes each batch row as the current version and
  drops any earlier history.
- On later runs a batch row for a new key opens a version. A row whose tracked columns differ
  from the key's current version closes that version and opens a new one. Tracked columns are
  compared null-safely, and NaN counts as null. A change in any other column is ignored.
- `tracked_columns` defaults to every column except `unique_key` and the delete column. An empty
  list or a key column in the list is PLP416, and so is a system column named in `unique_key`,
  `tracked_columns`, `partition_by`, `cluster_by`, `deletes.column` or the model's `columns`.
- Versions open and close at the run's start time, the same on every retry of the run, so a
  rerun with an unchanged batch adds no version.
- `deletes` supports `mode: hard` only (`soft` is PLP416). A delete row closes the key's current
  version without opening a new one; a delete for an unknown key is ignored, and a key that
  comes back later opens a new version. Keys missing from the batch stay current.
- A null or repeated key in the batch, or a batch column named like a system column, fails with
  PLM206 before anything is written.
- `run_results.json` reports `rows_inserted` (versions opened), `rows_updated` (versions closed by
  a change) and `rows_deleted` (versions closed by a delete); `rows_written` is their sum.

The schema policy applies to the model's columns only; old versions read a new column as null.
A target without the system columns fails with PLM206 until you run with `--full-refresh`. On
filesystem tables an scd2 write rewrites the whole table like a merge; on Spark it is one Delta
`MERGE INTO`. The memory engine does not support `scd2` (PLC501).

## Schema changes

`write.on_schema_change` says what happens when the batch's columns differ from the target's.
The default is `fail`.

| Policy | Result |
|---|---|
| `fail` | Any added, removed or retyped column is PLM202. |
| `ignore` | The table keeps its columns. Extra batch columns are dropped, missing ones are filled with nulls, values are cast to the table's types (a failed cast is PLM201). |
| `append_new_columns` | The table's columns plus the new ones, which are nullable. Removed columns stay. A type change is PLM202. |
| `sync_all_columns` | The batch's columns. A type change is accepted only if it widens. |

Widening means a wider integer, an integer to float64, float32 to float64, a decimal that keeps
every digit, a finer timestamp unit in the same zone, or a null column to any type. A batch that
differs from the table only by such a widening of the table's type is cast to the table's type
under every policy.

The policy applies to `append`, `merge`, `replace_partitions` and `scd2` on an existing table. `overwrite`,
`--full-refresh` and the first build use the batch's schema, and report the difference with the
action `replaced`.

With `fail`, adding a column to the model fails like this:

```text
FAIL   daily_clicks ... PLM202
  schema change rejected for model 'daily_clicks' (on_schema_change: fail): the batch schema
  differs from the target: added extra (int32)
  hint: change the model, or set write.on_schema_change to ignore, append_new_columns or sync_all_columns
```

With `append_new_columns` the same run succeeds and prints one line:

```text
daily_clicks: schema changed on main.daily_clicks (policy append_new_columns, applied): +device string
```

Each change is an event `model.schema_changed` (relation, policy, action and the list of changes),
and `schema_changes[]` of the unit in `run_results.json` carries the same diff. The filesystem
catalog reads every old file through the table's current schema, so adding a column or widening a
type never rewrites existing files. A key column (`unique_key` or `partition_by`) missing from the
batch is PLM206, whatever the policy.

## Safe concurrent writes

A filesystem table commit takes a lock file with a 30 second lease and checks that nobody
committed since the table was read. If another writer got in first, the second fails with a
transient error and can be rerun from the new version. Do not run two writers on one table on
purpose; the run lock exists to prevent it.
