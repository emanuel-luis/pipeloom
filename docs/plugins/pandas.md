# pandas engine

`pipeloom-pandas` runs models on [pandas](https://pandas.pydata.org) DataFrames.

```console
$ pip install "pipeloom[pandas]"
```

```yaml
targets:
  dev:
    engine:
      kind: pandas
    catalog:
      kind: filesystem
      root: warehouse
    state:
      kind: json
      path: .pipeloom/state
```

The engine needs the `filesystem` catalog: tables are directories of part files committed through
`_pipeloom/commit.json` ([ADR-0022](../adr/0022-filesystem-table-format.md)). Any other catalog is
rejected when the session opens.

A model can set `catalog` (and a `+catalog` key in the `models:` tree can set it for a layer). The
catalog becomes a leading directory: `<root>/<catalog>/<schema>/<name>/`. A table with no
catalog keeps `<root>/<schema>/<name>/`.

## Capabilities

| Capability | Value |
| --- | --- |
| Source formats | `parquet`, `csv`, `json`, `ndjson` |
| Write strategies | `append`, `overwrite`, `merge`, `replace_partitions` |
| Materializations | `table`, `incremental`, `ephemeral` |
| Expectations and quarantine | yes (`expr` limited to column names, literals and operators) |
| `ctx.sql` | with DuckDB installed (`pipeloom-pandas[sql]`), in the `duckdb` dialect |
| Views | no |
| Idempotent writes | yes, by run token |
| Portable frames (`frame_api: narwhals`) | yes |
| Cancel | no |

A view is rejected with PLC507. An `intermediate` materialization is supported (files under the artifacts root), and so is
`incremental`. The supported versions are `pandas>=2.3.3,<4` and `pyarrow>=22`.

## SQL

pandas has no SQL engine of its own. Install the `sql` extra and the pandas engine runs `ctx.sql` through DuckDB
([ADR-0068](../adr/0068-pandas-sql-through-duckdb.md)):

```console
$ pip install "pipeloom-pandas[sql]"
```

Each query opens an in-memory DuckDB connection, binds the model's inputs by alias as Arrow tables (no copy for
Arrow-backed columns), and returns an Arrow-backed DataFrame, the same dtypes the pandas reader produces. The
connection uses UTC and never downloads a DuckDB extension. The engine's `sql_dialect` is `duckdb`, so a model
written in another dialect is translated with the `sqlglot` extra, as on the other engines.

Without DuckDB the engine declares no SQL: a model that calls `ctx.sql` gets a PLK606 warning at validate and
fails at run time with PLX101.

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
