# pipeloom-pandas

The [pandas](https://pandas.pydata.org) engine for [pipeloom](https://github.com/emanuel-luis/pipeloom).

```console
$ pip install "pipeloom[pandas]"
```

Select it in a target:

```yaml
targets:
  dev:
    engine:
      kind: pandas
    catalog:
      kind: filesystem
      root: warehouse
```

- Relations are eager `DataFrame`s. Python models receive `DataFrame` inputs, or Narwhals frames
  with `frame_api: narwhals`.
- No SQL models and no views: validation rejects them (PLC502, PLC507).
- Reads `parquet`, `csv`, `json` and `ndjson` sources. Writes tables through the `filesystem`
  catalog with the `append`, `overwrite`, `merge` and `replace_partitions` strategies, and an
  idempotency token makes a repeated write a no-op.
- Supports `incremental` models, the schema-change policy and expectations with quarantine.

Documentation: https://emanuel-luis.github.io/pipeloom/latest/plugins/pandas/

License: Apache-2.0.
