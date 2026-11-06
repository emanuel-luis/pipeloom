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

## Problems

| Code | Meaning |
| --- | --- |
| PLR208 | A case names an unknown model, gives an alias that is not an input, leaves an input without rows, or reuses a case name |
| PLX114 | The output differs from `expect` |
| PLX115 | The case cannot run: a CSV is missing, a value does not cast, an untyped all-null column, the body raised, or the engine has no `from_rows` or `to_rows` |
