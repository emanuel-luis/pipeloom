# pipeloom-pandas

The [pandas](https://pandas.pydata.org) engine for [pipeloom](https://github.com/emanuel-luis/pipeloom).

```console
$ pip install "pipeloom[pandas]"
```

Select it in a target:

```yaml
targets:
  dev:
    engine: {kind: pandas}
    catalog: {kind: filesystem, root: warehouse}
```

- Relations are eager `DataFrame`s. Python models receive `DataFrame` inputs, or Narwhals frames
  with `frame_api: narwhals`.
- No SQL models and no views in 0.2: validation rejects them (PLC502, PLC507).
- Reads `parquet`, `csv`, `json` and `ndjson` sources. Writes tables through the `filesystem`
  catalog with the `append` and `overwrite` strategies, and an idempotency token makes a repeated
  write a no-op.

Documentation: https://emanuel-luis.github.io/pipeloom/latest/plugins/pandas/

License: Apache-2.0.
