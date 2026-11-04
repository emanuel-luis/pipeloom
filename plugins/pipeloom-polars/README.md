# pipeloom-polars

The [Polars](https://pola.rs) engine for [pipeloom](https://github.com/emanuel-luis/pipeloom).

```console
$ pip install "pipeloom[polars]"
```

Select it in a target:

```yaml
targets:
  dev:
    engine: {kind: polars}
    catalog: {kind: filesystem, root: warehouse}
```

- Relations are `LazyFrame`s. Python models receive `LazyFrame` inputs, or Narwhals frames with
  `frame_api: narwhals`.
- SQL models and views run on `pl.SQLContext`.
- Reads `parquet`, `csv`, `json` and `ndjson` sources. Writes tables through the `filesystem`
  catalog with the `append` and `overwrite` strategies, and an idempotency token makes a repeated
  write a no-op.

Documentation: https://emanuel-luis.github.io/pipeloom/latest/plugins/polars/

License: Apache-2.0.
