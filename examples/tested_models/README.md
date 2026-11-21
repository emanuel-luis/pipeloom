# tested_models

A small revenue pipeline whose logic is tested before it touches data: orders, refunds and
exchange rates become net revenue in US dollars per day. It shows unit tests (`given` rows inline
and from CSV, `expect` rows), data tests after the write, `pipeloom test`, `pipeloom build`, and a
plain pytest that calls a model function directly.

Walkthrough: [docs/examples/tested-models.md](../../docs/examples/tested-models.md).

## Run it

```console
$ uv sync --all-packages
$ uv run pipeloom build --project-dir examples/tested_models
```

The default target is `duckdb`. Add `--target polars` for polars. Run only the unit tests with
`pipeloom test`, and the plain pytest with `uv run pytest examples/tested_models/tests`.

## What to look at

- `pipeloom test` prints three PASS lines and writes nothing.
- `pipeloom build` runs those tests first, then the three models, then the data tests. They are
  the entries with `scope: table` in `target/run_results.json`.
- Break the multiplication in `orders_usd` and `pipeloom test` fails with PLX114 and a row diff.

## Layout

| Path | What |
|---|---|
| `models/revenue.py` | `orders_usd`, `net_revenue`, `daily_revenue` (portable narwhals models) |
| `models/_properties.yml` | unit tests and data tests of each model |
| `data/` | the three source CSVs (10 orders, 4 refunds, 9 rates) |
| `tests/data/` | CSV rows for the unit tests |
| `tests/test_revenue.py` | plain pytest on the model functions |
