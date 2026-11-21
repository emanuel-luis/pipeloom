# Orders from an untrusted feed

A partner sends you orders as CSV and you do not control the file. Some rows will be wrong, and
one day a column will appear that nobody announced. This example shows what pipeloom does about
each, on DuckDB and on polars. The project is `examples/orders_quality`.

## The problem

Three things can go wrong, and each needs a different answer.

- A few rows are bad (a negative amount, no customer). Keep the run going, set those rows aside
  and tell someone.
- The schema changes. Stop before anything is written.
- Rows are well formed but point at a customer you do not know. Write them, then flag them.

## The project

Two sources: `customers.csv`, and `orders.csv` from a batch folder (`day1`, `day2`, `day3`)
chosen by the `batch` variable. Two models: `customers`, and `orders`.

The orders model declares a contract on its output columns. It selects everything, so a column
the partner adds reaches the contract:

```python
--8 < --"examples/orders_quality/models/orders.py:orders"
```

Expectations and data tests live in the properties file:

```yaml
--8<-- "examples/orders_quality/models/_properties.yml:quality"
```

Both targets are in `pipeloom.yml`:

```yaml
--8<-- "examples/orders_quality/pipeloom.yml"
```

## Run it

Day 1 is clean. Use `pipeloom build`, because it also runs the data tests.

```console
$ pipeloom build --project-dir examples/orders_quality
1/2 OK     customers .............. table/overwrite        5 rows
2/2 OK     orders ................. table/overwrite        6 rows
build finished | exit 0
```

Day 2 has a negative amount twice, a missing customer, an unknown status and an order for
customer 999:

```console
$ pipeloom build --project-dir examples/orders_quality --vars '{"batch": "day2"}'
WARN   orders: customer_present (not_null) dropped 1 of 8 rows
WARN   orders: amount_non_negative (range) dropped 2 of 8 rows
WARN   orders: status_known (accepted_values) failed on 1 of 8 rows
2/2 OK     orders ................. table/overwrite        5 rows
build finished | exit 0
```

The run continues. Three rows were dropped and quarantined, the unknown status only warned, and
5 rows were written.

Day 3 adds a `coupon` column:

```console
$ pipeloom build --project-dir examples/orders_quality --vars '{"batch": "day3"}'
2/2 FAIL   orders ................. PLM208 ...
         orders  PLM208 model 'orders' violates its contract: ... extra column 'coupon'
build finished | exit 1
```

The contract is checked before the write, so the `orders` table still holds day 2. PLM208 is
permanent and is never retried. Add `coupon` to `columns` and run again to accept the change.

## Read the result

| Where | What you see |
|---|---|
| Console | One WARN line per check that warned or dropped rows, and the PLM208 error with its hint. |
| `target/run_results.json` | In the `orders` result: `rows_dropped`, `rows_quarantined`, and `expectations[]` with name, kind, action, passed and failed. |
| Table `orders__quarantine` | The three dropped rows, plus `_pipeloom_run_id`, `_pipeloom_failed` and `_pipeloom_ts`. |

Expectations have `scope: batch`. The data tests have `scope: table`: on day 2,
`customer_exists` has `failed: 1` (order 2005), and since it is set to warn, the run still
succeeds. Order ids were unique and not null.

To look at the quarantine on DuckDB:

```console
$ duckdb examples/orders_quality/target/duckdb/orders_quality.duckdb "select order_id, _pipeloom_failed from shop.orders__quarantine"
```

On polars the same table is a folder of parquet files under
`target/warehouse/shop/orders__quarantine/`. Use `--target polars` for any command above.

## What to try next

- Change `customer_exists` to `on_violation: fail`, and run day 2 again. The data stays
  written, but the run exits 1 with PLQ303.
- Add `max_drop_ratio: 0.25` under `quality` to fail a batch that loses too many rows (PLQ301).
- Read the [data quality](../guide/quality.md) and [testing](../guide/testing.md) guides.

The walkthrough runs in CI: `tests/examples/test_walkthrough_orders_quality.py`.
