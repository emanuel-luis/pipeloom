# pandas engine

`pipeloom-pandas` runs models on [pandas](https://pandas.pydata.org) DataFrames.

```console
$ pip install "pipeloom[pandas]"
```

```yaml
targets:
  dev:
    engine: {kind: pandas}
    catalog: {kind: filesystem, root: warehouse}
    state: {kind: json, path: .pipeloom/state}
```

The engine needs the `filesystem` catalog: tables are directories of part files committed through
`_pipeloom/commit.json` ([ADR-0022](../adr/0022-filesystem-table-format.md)). Any other catalog is
rejected when the session opens.

A model can set `catalog` (and a `_defaults.yml` folder default can set it for a layer). The
catalog becomes a leading directory: `<root>/<catalog>/<schema>/<name>/`. A table with no
catalog keeps `<root>/<schema>/<name>/`.

## Capabilities

| Capability | Value |
| --- | --- |
| Source formats | `parquet`, `csv`, `json`, `ndjson` |
| Write strategies | `append`, `overwrite`, `merge`, `replace_partitions` |
| Materializations | `table`, `incremental`, `ephemeral` |
| Expectations and quarantine | yes (`expr` limited to column names, literals and operators) |
| SQL models and views | no |
| Idempotent writes | yes, by run token |
| Portable frames (`frame_api: narwhals`) | yes |
| Cancel | no |

A SQL model on this engine is rejected with PLC502 and a view with PLC507. An `intermediate`
materialization is supported (files under the artifacts root). `incremental` is supported. SQL on pandas is deferred past 0.2. The supported
versions are `pandas>=2.3.3,<4` and `pyarrow>=22`.

## Frames

Relations are eager `DataFrame`s, so `rows_read` is filled in `run_results.json`. A Python model
receives `DataFrame` inputs and returns a `DataFrame`. With `frame_api: narwhals` the model receives
Narwhals frames and returns a Narwhals or a native frame. Inputs are shared between the units that
read them, so a model must not modify an input in place.

Reads use Arrow-backed columns (`dtype_backend="pyarrow"`): integer columns with nulls stay
integers and the logical types of a table survive a round trip. String columns, object dtype in
pandas 2 or the string dtype in pandas 3, are reported as `string`.

## Writes

A table is written as one part file in the catalog format (`parquet`, `csv` or `ndjson`).
Repeating a run id with the same token returns the stored result and writes nothing. A failure
leaves the previous commit untouched and removes the unfinished part file. Reading a table
concatenates its committed part files; `csv` and `ndjson` parts are cast back to the logical types
stored in the commit file.
