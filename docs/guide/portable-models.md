# Portable models

A portable model is one Python function that runs unchanged on polars, pandas, DuckDB and Spark.
You opt in with `frame_api="narwhals"` on the model. The function then receives
[Narwhals](https://narwhals-dev.github.io/narwhals/) frames, and whatever it returns is converted
back to the engine's own frame. pipeloom uses `narwhals.stable.v2`, the API Narwhals promises not
to break, and imports it in one module only, `pipeloom.engine.portable`.

The engine must declare portable frames. All four do. On an
engine that does not, such as the memory engine, `pipeloom validate` stops with
[PLC508](../reference/errors.md#plc508).

<figure markdown="span">
  ![One @model function written with Narwhals passes the capability check and runs on polars, DuckDB and Spark (lazy) and pandas (eager), giving the same output rows on each.](../diagrams/portable-models.light.svg#only-light)
  ![One @model function written with Narwhals passes the capability check and runs on polars, DuckDB and Spark (lazy) and pandas (eager), giving the same output rows on each.](../diagrams/portable-models.dark.svg#only-dark)
  <figcaption>One model, four engines, the same rows. Source: <a href="https://github.com/emanuel-luis/pipeloom/blob/main/docs/diagrams/portable-models.d2">portable-models.d2</a>.</figcaption>
</figure>

## An example

```python
import narwhals.stable.v2 as nw
from pipeloom import model, ref


@model(
    frame_api="narwhals",
    inputs={"orders": ref("stg_orders"), "customers": ref("stg_customers")},
)
def revenue_by_region(ctx, orders, customers):
    paid = orders.filter(nw.col("status") == "paid")
    joined = paid.join(customers, on="customer_id", how="inner")
    doubled = joined.with_columns((nw.col("amount") * 2).alias("amount_x2"))
    return doubled.group_by("region").agg(
        nw.col("order_id").count().alias("orders"),
        nw.col("amount_x2").sum().alias("total"),
    )
```

Filters, joins, `with_columns`, `select` and `group_by(...).agg(...)` behave the same everywhere.
Return the Narwhals frame as it is; there is nothing to convert. Returning the engine's native
frame (`nw.to_native(frame)`) also works.

The repository tests exactly this shape of model on every engine, with the same input rows, and
requires the same output rows on each (`tests/portable/test_matrix.py`, plus a Spark leg in the
`spark` CI job).

## Lazy and eager frames

What the function receives depends on the engine:

| Engine | Frame | Kind |
| - | - | - |
| polars | `LazyFrame` | lazy |
| DuckDB | relation | lazy |
| Spark | `DataFrame` | lazy |
| pandas | `DataFrame` | eager |

Write for the lazy case and the model works on both. A lazy frame is a query plan: nothing runs
until the engine writes the result. That rules out anything that needs the data in hand, such as
indexing a row by position or `to_list()`. Collecting inside the model defeats the engine, so
leave the result lazy and let pipeloom materialize it.

## Rows have no order

A lazy frame makes no promise about row order. A table has no first row, and a Spark or DuckDB
plan may return the same rows in a different order on every run. A portable model therefore never
relies on the order its input arrived in, and its output is a set of rows.

An operation that needs an order must say which one. Narwhals refuses the rest on a lazy frame:

| Fails on a lazy frame | Works |
| - | - |
| `nw.col("a").cum_sum()` | `nw.col("a").cum_sum().over(order_by="day")` |
| `nw.col("a").shift(1)` or `diff()` | `nw.col("a").shift(1).over(order_by="day")` |
| `nw.col("a").first()` or `last()` | `nw.col("a").first().over(order_by="day")` |
| `frame.unique(keep="first")` | `frame.unique(subset=["id"], keep="any")` |

Add `partition_by` to `over` for a window per group, for example
`.over("customer_id", order_by="day")`. A `sort()` earlier in the chain does not count: on a lazy
frame Narwhals still refuses, so name the order on the operation itself. pandas runs
these without `order_by`, which is how a model can pass on pandas and fail on DuckDB. Test a
portable model on a lazy engine before you trust it.

Fix the order in the output only if a consumer needs one, and then sort at the end. Rows written
to a table have no order either; the sort is for what you return, not a guarantee about storage.

### PLX122

When Narwhals refuses an operation for lack of an order, the unit fails with
[PLX122](../reference/errors.md#plx122) instead of a bare traceback. The message names the model
and carries Narwhals' own explanation, and the hint points here. The exception behind it is
`narwhals.exceptions.InvalidOperationError` with an `order_by` hint. The fix is in the model: name
the order with `over(order_by=...)` or `first(order_by=...)`, or rewrite without it. The failure
is permanent, so retries do not repeat it.

## When to drop to ctx.sql

Not every operation exists in the Narwhals API, and some are cheaper to say in SQL: window
functions with frames, `QUALIFY`, engine-specific functions. Use `ctx.sql` for those. Under
`frame_api="narwhals"` it returns a Narwhals frame, so you can mix the two:

```python
@model(frame_api="narwhals", inputs={"events": ref("stg_events")})
def sessions(ctx, events):
    ranked = ctx.sql(
        "select *, row_number() over (partition by user_id order by ts) as n from events"
    )
    return ranked.filter(nw.col("n") == 1)
```

The query is written in the dialect of the engine, or in the `sql_dialect` the model declares, and
pipeloom translates it when they differ (this needs the sqlglot extra, otherwise
[PLC513](../reference/errors.md#plc513)). pandas has no SQL: a model that calls `ctx.sql` is not
portable to it, and the validation warns with [PLK606](../reference/errors.md#plk606). If you need
all four engines, stay in the frame API; if you need windows over an explicit order, `over` covers
most of it.

## Checklist

- Set `frame_api="narwhals"` and import `narwhals.stable.v2 as nw`.
- Use expressions (`nw.col`), not engine methods. Do not call `.collect()`, `.to_pandas()` or
  `.to_native()` on an input.
- Give every order-dependent operation an `order_by`.
- Never rely on the order of rows going in or out.
- Run the model on a lazy engine (polars, DuckDB or Spark) as well as on pandas.
