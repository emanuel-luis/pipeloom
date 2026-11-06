# Data quality

Expectations check a model's output before it is written. Each one has an action: warn, drop or
fail. Dropped rows can go to a quarantine table so you can look at them later. The polars and
pandas engines support all of it.

## Declare expectations

Project and source as in the [quickstart](../quickstart.md), with this `raw/orders.csv`:

```text
id,amount,status,updated_at
1,5.0,paid,2026-01-01T10:00:00
2,-2.5,paid,2026-01-01T11:00:00
3,7.0,refunded,2026-01-02T09:00:00
,4.0,new,2026-01-02T10:00:00
```

`models/orders.yml`:

```yaml
sources:
  - name: raw_orders
    format: csv
    location: raw/orders.csv
    options:
      try_parse_dates: true
models:
  - name: orders
    sql: select id, amount, status, updated_at from raw_orders
    inputs:
      raw_orders:
        source: raw_orders
    expectations:
      - name: id_present
        check:
          not_null: [id]
        on_violation: drop
      - name: status_known
        check:
          accepted_values:
            column: status
            values: [new, paid, shipped]
        on_violation: drop
      - name: amount_non_negative
        check:
          range:
            column: amount
            min: 0
        on_violation: warn
      - name: unique_id
        check:
          unique: [id]
        on_violation: fail
      - name: not_empty
        check:
          row_count:
            min: 1
        on_violation: fail
    quality:
      quarantine: true
      max_drop_ratio: 0.5
```

```console
$ pipeloom run
... WARN   orders: id_present (not_null) dropped 1 of 4 rows
... WARN   orders: status_known (accepted_values) dropped 1 of 4 rows
... WARN   orders: amount_non_negative (range) failed on 1 of 4 rows
... 1/1 OK     orders ........ table/overwrite   2 rows
```

Two rows were written, two went to `orders__quarantine`, and the negative amount only produced a
warning. The unit still succeeded. `on_violation` defaults to `fail`.

## Checks

Row checks look at each row. `check` takes one of these (or use `expr` instead of `check`):

| Check | Passes when |
|---|---|
| `not_null: [a, b]` | the columns are not null |
| `accepted_values` with `column` and `values` | the value is one of the list (exact comparison) |
| `range` with `column`, `min` and `max` | the value is within the bounds, inclusive; either bound may be left out |
| `regex` with `column` and `pattern` | the pattern is found anywhere in the string (an unanchored search); the column must be a string |
| `expr: "amount >= 0"` | the expression is true |

A null passes every check except `not_null`, and NaN counts as null. A row check can warn, drop
or fail.

Aggregate checks look at the whole batch and can only warn or fail (a `drop` is PLP403):

| Check | Fails when |
|---|---|
| `unique: [a, b]` | a key appears more than once (rows with a null key part are not counted) |
| `row_count` with `min` and `max` | the number of rows is outside the bounds |
| `freshness` with `column` and `max_age` | the newest value is older than `max_age`; an all-null column fails |

Aggregates see the rows that are left after drops. For an incremental model that means the new
rows of this run, not the whole table.

### The expr dialect

An `expr` is written in the dialect of the engine and never runs code. On polars it is a SQL
expression (`amount >= 0 and amount < 1e6`). On pandas it is a restricted Python expression:
column names, literals, comparisons, `and`, `or`, `not`, arithmetic and `in` with a list.
Function calls, attributes and `@name` are rejected with PLQ302. Prefer the named checks when a
model has to run on several engines.

## Drops, quarantine and the drop ratio

A row that fails any `drop` check is removed from the output. With `quality.quarantine: true`
the removed rows are appended to `<target>__quarantine`: the model's columns plus

| Column | Content |
|---|---|
| `_pipeloom_run_id` | the run that dropped the row |
| `_pipeloom_failed` | the names of the failed drop checks (a list; JSON text on a csv catalog) |
| `_pipeloom_ts` | when it happened, a UTC timestamp |

`quality.max_drop_ratio` (0 to 1) fails the unit when the share of dropped rows is larger. In the
example, 0.25 fails the unit because half of the rows are dropped:

```text
FAIL   orders ... PLQ301
  model 'orders' failed expectation(s): max_drop_ratio
  hint: fix the data, or relax on_violation or quality.max_drop_ratio
```

A failed `fail` check or an exceeded ratio stops the unit before it writes: the target and its
quarantine are left as they were. The counts are still recorded. Quarantine rows are written once
per run even if the unit is retried. If the model output changes, the quarantine table follows
(new columns are added); an incompatible type change fails with PLM202 naming the quarantine
table.

A view or an ephemeral model cannot carry expectations (PLP411), and no other model may be called
`<model>__quarantine` in the same catalog and schema (PLP412). An engine that does not declare the `quality`
capability is rejected with PLC510.

## Model contracts

A model can declare its exact output columns with `columns`. Each entry has a `name`, a logical
`dtype` (the type names engines report, such as `int64`, `string`, `decimal(18,2)` or
`timestamp[us, UTC]`) and `nullable` (default true).

```yaml
models:
  - name: fct_orders
    sql: select order_id, amount from stg_orders
    columns:
      - name: order_id
        dtype: int64
        nullable: false
      - name: amount
        dtype: decimal(18,2)
```

Before every write, pipeloom compares the batch with the contract:

- the column names must match exactly (case-sensitive, any order), with no column missing and none
  extra;
- each logical type must equal the declared `dtype`. Nothing is widened or cast, so cast in the
  body. Engines report fixed forms: Spark timestamps are always `timestamp[us, UTC]`. On pandas, an
  integer column with a null is `float64` unless it uses the nullable `Int64` dtype, and a column
  of Python `Decimal` objects is `unknown`; use `pd.ArrowDtype(pa.decimal128(18, 2))` to get
  `decimal(18,2)`. To see what an engine reports, run once: PLM208 names the reported type of
  every column whose type differs;
- a column with `nullable: false` must hold no null (NaN counts as null), counted with one pass
  over the batch.

The check runs after the expectations, on the rows they keep, and before the schema-change policy,
the quarantine write and the table write. A `drop` expectation can therefore remove null rows
before the contract sees them. A mismatch fails the unit with PLM208, writes nothing and is never
retried under `retry_on: transient`. `on_schema_change` still compares the batch with the existing
table as before.

`columns` is a model key; defaults cannot set it. An unknown `dtype` or a repeated name is PLS102
at validation, and a contract on a view or an ephemeral model is PLP414. Declared columns appear as
`nodes[].columns` in `manifest.json` and on the docs site model page. See
[ADR-0044](../adr/0044-model-contracts.md).

## What the run records

- `model.quality_evaluated` event: total rows, one result per expectation, dropped and
  quarantined counts, the drop ratio and the names of the failures.
- `run_results.json`: `expectations[]` (name, kind, action, passed, failed and, for aggregates, a
  value), `rows_dropped`, `rows_quarantined` and `warnings[]`. See
  [Run artifacts](../reference/artifacts.md).
- The console prints a WARN line for every check that warned, dropped or failed rows.

Counts are kept for `fail` checks too, so you can see how bad a failing batch was.
