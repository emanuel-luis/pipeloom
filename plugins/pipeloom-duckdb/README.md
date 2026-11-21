# pipeloom-duckdb

The [DuckDB](https://duckdb.org) engine for [pipeloom](https://github.com/emanuel-luis/pipeloom).

```console
$ pip install "pipeloom[duckdb]"
```

Select it in a target:

```yaml
targets:
  dev:
    engine:
      kind: duckdb
    catalog:
      kind: duckdb
      path: target/warehouse.duckdb
```

- Relations are DuckDB relations. Python models receive `DuckDBPyRelation` inputs, or Narwhals
  frames with `frame_api: narwhals`.
- SQL models and views run in DuckDB's own dialect (`sql_dialect: duckdb`).
- Tables live in a DuckDB database file. `merge`, `scd2` and `replace_partitions` run as SQL
  inside one transaction, and an idempotency token makes a repeated write a no-op.
- Reads `parquet` and `csv` sources, and `delta` tables read-only through the DuckDB `delta`
  extension, installed from a pinned `extension_directory`.

Documentation: https://emanuel-luis.github.io/pipeloom/latest/plugins/duckdb/

License: Apache-2.0.
