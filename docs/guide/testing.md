# Testing models

A unit test runs a model body on rows you give it and compares the output with rows you expect.
It needs no warehouse and writes nothing: `pipeloom test` builds the inputs in memory on the
target's engine, runs the SQL or Python body, and reads the result back. The design is in
[ADR-0045](../adr/0045-unit-and-data-tests.md).

## A first case

Put `unit_tests` on the model. Every input alias needs `given` rows, and `expect` lists the rows
the body should return.

```yaml
models:
  - name: orders_enriched
    sql: select o.id, o.amount * 2 as doubled from orders o
    inputs:
      orders:
        ref: stg_orders
    unit_tests:
      - name: doubles_the_amount
        given:
          orders:
            rows:
              - id: 1
                amount: 10
              - id: 2
                amount: null
        expect:
          rows:
            - id: 2
              doubled: null
            - id: 1
              doubled: 20
```

```console
$ pipeloom test
12:00:01 PASS   orders_enriched: doubles_the_amount
12:00:01 unit tests: 1 passed, 0 failed
```

## How rows compare

- Only the columns that `expect` names are compared. Extra output columns are ignored; a missing
  one fails the case.
- Order does not matter, but duplicates do: two expected copies of a row need two in the output.
- Expected values are cast to the type of the output column, so `amount: 3` matches a
  `float64` 3.0.
- Null equals NaN, floats compare to 12 significant digits, a timestamp without a time zone is
  read as UTC, and decimals compare exactly.

When rows differ, the case fails with PLX114 and a row diff: `-` lines are expected rows the
output lacks, `+` lines are output rows nobody expected.

```console
12:00:01 FAIL   orders_enriched: doubles_the_amount PLX114 1 missing, 1 unexpected row(s)
         - {"id": 1, "doubled": 21}
         + {"id": 1, "doubled": 20}
```

The console shows 10 rows of each side, the `unit_test.failed` event holds up to 50.

## Rows from CSV files

`csv` replaces `rows` in a `given` or in `expect`. The path is relative to the project dir. The
file needs a header row; an empty field is null.

```yaml
given:
  orders:
    csv: tests/data/orders.csv
```

## Column types

A given column takes its type from, in order:

1. the `columns` of the `given` block,
2. the [contract](quality.md#model-contracts) `columns` of the upstream model,
3. inference.

Inline values infer `boolean`, `int64`, `float64`, `string`, `date` and timestamps from their YAML
type. CSV values infer `int64`, then `float64`, then `string`, so declare dates, timestamps and
string ids:

```yaml
given:
  orders:
    csv: tests/data/orders.csv
    columns:
      - name: id
        dtype: string
      - name: ordered_at
        dtype: timestamp[us, UTC]
```

A column that holds only nulls and declares no type cannot be typed; the case fails with PLX115.

## Keeping cases in their own files

Any YAML file under `model_paths` may hold a top-level `unit_tests` list. Each entry names its
model:

```yaml
unit_tests:
  - name: doubles_the_amount
    model: orders_enriched
    given:
      orders:
        csv: tests/data/orders.csv
    expect:
      csv: tests/data/doubled.csv
```

To keep such files in a `tests` folder, add it to `model_paths` in `pipeloom.yml`.

## Running them

`pipeloom test` takes `--select`, `--exclude`, `--state`, `--target`, `--vars`, `--plugin`,
`--project-dir` and `--log-format`, like `compile`. It takes no lock, records no run and writes no
artifacts. Exit codes: 0 when every case passed, 1 when one failed or the project does not
validate, 2 for a usage error, 130 when interrupted.

Unit tests never change the manifest or the plan hash, so adding one selects nothing under
`state:modified`.

## Data tests

Expectations judge each batch before it is written. Data tests judge the whole written table
afterwards, which is where a foreign key check belongs:

```yaml
models:
  - name: orders
    sql: select * from raw_orders
    inputs:
      raw_orders:
        source: raw_orders
    data_tests:
      - name: customer_exists
        check:
          relationships:
            column: customer_id
            to:
              ref: customers
            field: id
      - name: order_id_unique
        check:
          unique:
            - order_id
        on_violation: warn
```

A check is one of `not_null` (a list of columns), `unique` (a list of columns) or
`relationships`. A relationships violation is a row whose non-null `column` matches no `field`
value of the parent named in `to` (a `ref` or a `source`); NaN counts as null, and parent nulls
never match. `on_violation` is `fail` (the default) or `warn`.

Data tests run under `pipeloom build`, inside each unit after its write and its watermark commit,
and again when a retry replays a write. They read the whole table, incremental models included,
so set `on_violation: warn` where a full scan on every build is too slow. The parent is read like
an input, so `--defer` reads it from the reference. Each result is an entry of
`results[].expectations` in `run_results.json` with `scope: table` (expectations carry
`scope: batch`). A failed `warn` test adds a warning; a failed `fail` test is PLQ303: the unit
fails, its descendants are skipped and the run exits 1, but the data stays written.

Tables, incremental, intermediate and view models take data tests. On an ephemeral model they are
PLP415, as is a relationships parent that is the model itself, one of its descendants, an
ephemeral model, an intermediate model, or a model that closes a cycle among input and
relationship edges.

## build

`pipeloom build` takes the flags of `run`. It runs the unit tests of the selected models first;
if one fails it stops with exit 1 before a run id exists and before anything is written. Then it
runs the selection with data tests on, ordering each model after its relationship parents.
`run_results.json` records `args.data_tests: true`, and `pipeloom retry --from` keeps it.

## Problems

| Code | Meaning |
| --- | --- |
| PLP415 | Data tests on an ephemeral model, or a relationships parent that is the model itself, a descendant, an ephemeral model, an intermediate model, or closes a cycle among input and relationship edges |
| PLQ303 | A data test with `on_violation: fail` found violations; the data stays written |
| PLR208 | A case names an unknown model, gives an alias that is not an input, leaves an input without rows, or reuses a case name |
| PLX114 | The output differs from `expect` |
| PLX115 | The case cannot run: a CSV is missing, a value does not cast, an untyped all-null column, the body raised, or the engine has no `from_rows` or `to_rows` |
