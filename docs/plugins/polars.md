# Polars engine

`pipeloom-polars` runs models on [Polars](https://pola.rs) lazy frames.

```console
$ pip install "pipeloom[polars]"
```

```yaml
targets:
  dev:
    engine: {kind: polars}
    catalog: {kind: filesystem, root: warehouse}
    state: {kind: json, path: .pipeloom/state}
```

The engine needs the `filesystem` catalog: tables are directories of part files committed through
`_pipeloom/commit.json` ([ADR-0022](../adr/0022-filesystem-table-format.md)). Any other catalog is
rejected when the session opens.

## Capabilities

| Capability | Value |
| --- | --- |
| Source formats | `parquet`, `csv`, `json`, `ndjson` |
| Write strategies | `append`, `overwrite` |
| Materializations | `table`, `view`, `ephemeral` |
| SQL models and views | yes, through `pl.SQLContext` |
| Idempotent writes | yes, by run token |
| Portable frames (`frame_api: narwhals`) | yes |
| Cancel | no |

`incremental` and `intermediate` materializations are rejected with PLC509. The polars bound is
`polars>=1.30,<3`.

## Frames

Relations are `LazyFrame`s, so `rows_read` is empty in `run_results.json`. A Python model receives
`LazyFrame` inputs and returns a `LazyFrame` or a `DataFrame`. With `frame_api: narwhals` the model
receives Narwhals frames and returns a Narwhals or a native frame.

```python
import narwhals.stable.v2 as nw


def fct_orders(ctx, enriched):
    return enriched.filter(nw.col("amount") > 1)
```

## SQL and views

Each SQL model runs in a fresh `pl.SQLContext` that holds only its declared inputs, so aliases do
not leak between models. A view stores its SQL and inputs in the commit file and is evaluated again
on every read, so it sees later overwrites of its inputs. Rerunning with an unchanged definition
commits nothing.

## Writes

A table is collected and written as one part file in the catalog format (`parquet`, `csv` or
`ndjson`). Repeating a run id with the same token returns the stored result and writes nothing. A
failure leaves the previous commit untouched and removes the unfinished part file. A whole table
must fit in memory in 0.2.

## Reading CSV and NDJSON tables

Part files in `csv` and `ndjson` lose some type information, so a table is read back with the
logical types stored in the commit file when this engine can name them all.
