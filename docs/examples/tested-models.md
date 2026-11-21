# Tested models

Check what a transformation does before it writes anything. This example is a small revenue
pipeline: orders in several currencies, refunds and daily exchange rates become net revenue in US
dollars per day. Three models carry logic worth testing: a currency conversion with a join, a
refund netting with an aggregate and a join, and a daily total with a window.

The project is `examples/tested_models`. It runs on DuckDB (the default target) and on polars.

## The models

Each model is a portable function (`frame_api="narwhals"`), so the same code runs on both
engines. See [Portable models](../guide/portable-models.md).

```python
--8<-- "examples/tested_models/models/revenue.py:orders_usd"
```

```python
--8<-- "examples/tested_models/models/revenue.py:net_revenue"
```

```python
--8<-- "examples/tested_models/models/revenue.py:daily_revenue"
```

## Unit tests

A unit test gives a model rows and says which rows it must return. It builds the inputs in memory
and writes nothing. The cases live in the properties file, next to the model.

```yaml
--8<-- "examples/tested_models/models/_properties.yml:orders_usd"
```

`net_revenue` reads its refunds from a CSV file, and `daily_revenue` reads both its input and its
expected rows from CSV. CSV dates must be declared, so those cases list `columns`.

```yaml
--8<-- "examples/tested_models/models/_properties.yml:net_revenue"
```

```yaml
--8<-- "examples/tested_models/models/_properties.yml:daily_revenue"
```

## Run the tests

```console
$ uv run pipeloom test --project-dir examples/tested_models
PASS   daily_revenue: running_total_follows_the_date
PASS   net_revenue: refunds_are_netted
PASS   orders_usd: converts_with_the_rate_of_the_day
unit tests: 3 passed, 0 failed
```

Exit code 0. Nothing was written and no run was recorded. Add `--target polars` to run the same
cases on polars.

## Build

`pipeloom build` runs the unit tests of the selected models first. If one fails it stops with exit
1 before a run id exists. When they pass it runs the models, then the data tests of each model
after its write.

```console
$ uv run pipeloom build --project-dir examples/tested_models
unit tests: 3 passed, 0 failed
1/3 OK     orders_usd ............. table/overwrite        9 rows
2/3 OK     net_revenue ............ table/overwrite        9 rows
3/3 OK     daily_revenue .......... table/overwrite        3 rows
```

The data tests are `unique` on `order_id` and on `order_date`, and a `relationships` check that
every `net_revenue.order_id` exists in `orders_usd`. Their results are in
`target/run_results.json`, as entries of `results[].expectations` with `scope: table`. The model
also has an expectation, `rate_found`, which judges each batch before it is written (`scope:
batch`).

## When the logic is wrong

Suppose someone changes the conversion to divide by the rate instead of multiplying:

```diff
-        .with_columns((nw.col("amount") * nw.col("rate")).round(2).alias("amount_usd"))
+        .with_columns((nw.col("amount") / nw.col("rate")).round(2).alias("amount_usd"))
```

`pipeloom test` now fails with PLX114 and a row diff. Lines starting with `-` are expected rows
the model did not return, lines starting with `+` are rows nobody expected:

```console
FAIL   orders_usd: converts_with_the_rate_of_the_day PLX114 2 missing, 2 unexpected row(s)
       - {"order_id": 1, "amount_usd": 55.0}
       - {"order_id": 2, "amount_usd": 60.0}
       + {"order_id": 1, "amount_usd": 45.45}
       + {"order_id": 2, "amount_usd": 41.67}
unit tests: 2 passed, 1 failed
```

Exit code 1. `pipeloom build` stops at the same point, so the wrong numbers never reach a table.
The example does not commit the broken model: the CI test applies this edit to a copy in a
temporary directory.

## Plain pytest

A decorated function is still a function. A portable model takes frames, so a test can wrap a
polars frame, pass `None` for `ctx` and check the rows. This is the fastest loop and needs no
project run.

```python
--8<-- "examples/tested_models/tests/test_revenue.py:direct"
```

```console
$ uv run pytest examples/tested_models/tests
```

## What to try next

- Add a case for a missing exchange rate and watch the `rate_found` expectation fail.
- Add a refund for an order that does not exist and see the `order_exists` data test report it.
- Move the cases to their own folder, as in [Testing models](../guide/testing.md).
- Run `pipeloom build --select daily_revenue`, which runs only that model's unit tests first.

The CI test for this page is `tests/examples/test_walkthrough_tested_models.py`.
