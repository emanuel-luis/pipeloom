# DuckDB engine

`pipeloom-duckdb` runs models on [DuckDB](https://duckdb.org): SQL in process, with tables in a
DuckDB database file ([ADR-0051](../adr/0051-duckdb-engine.md)).

```console
$ pip install "pipeloom[duckdb]"
```

```yaml
targets:
  dev:
    engine:
      kind: duckdb
      extension_directory: .duckdb/extensions
    catalog:
      kind: duckdb
      path: target/warehouse.duckdb
    state:
      kind: json
      path: .pipeloom/state
```

The engine needs the `duckdb` catalog. `path` is the database file that holds tables without a
catalog (default `target/pipeloom.duckdb`), or `:memory:`. A model catalog (`+catalog` in the
`models:` tree) is the file `<catalog>.duckdb` next to it, attached under that name, so
`pipeloom_gold.tpch.top_customers` lives in `target/pipeloom_gold.duckdb`. Pair the engine with
the `json` or `sqlite` state store.

One process writes a database file at a time, so units run on threads (`boundary: process`).

## Capabilities

| Capability | Value |
| --- | --- |
| Source formats | `parquet`, `csv`, `delta` (read only) |
| Write strategies | `append`, `overwrite`, `merge`, `replace_partitions`, `scd2` |
| Materializations | `table`, `incremental`, `view`, `ephemeral`, `intermediate` |
| Expectations and quarantine | yes (`expr` in DuckDB SQL) |
| SQL models and views | yes, `sql_dialect: duckdb` |
| Idempotent writes | yes, by run token |
| Portable frames (`frame_api: narwhals`) | yes |
| Cancel | yes |

## Options

Engine:

| Option | Default | Meaning |
| --- | --- | --- |
| `extension_directory` | DuckDB's default | Where extensions are installed and loaded from, relative to the project. |
| `extensions` | `[]` | Extensions to load when the session opens. `delta` loads on the first Delta read. |
| `install_extensions` | `false` | Install a missing extension from the network into `extension_directory`. |
| `config` | `{}` | DuckDB configuration for the connection, for example `threads` or `memory_limit`. |

Catalog: `path`, as above.

## Frames and SQL

Relations are `DuckDBPyRelation`s on the session's connection. A Python model receives relations
and returns one; with `frame_api: narwhals` it receives Narwhals frames. `ctx.sql` runs DuckDB
SQL, where each input is a CTE named after its alias. A query written for another engine
declares `sql_dialect` and is translated (needs `pipeloom[sqlglot]`). A view is a DuckDB view, so
it sees later writes of its inputs.

## Writes

A write stages the batch in a temporary table, then runs one transaction that changes the table
and records the run token, so a failure leaves the table as it was and a repeated token writes
nothing. `merge` is a `MERGE INTO` keyed on `unique_key` (a null key never matches);
`update_condition` is DuckDB SQL over `source` and `target`. `scd2` closes changed and deleted
current rows at the run start and inserts the new versions. `replace_partitions` deletes every
partition the batch holds, null included, then inserts the batch. Bytes written are not reported.

## Extensions offline

The engine never downloads an extension on its own: DuckDB's automatic install is off, and
`install_extensions` defaults to `false`. Pin `extension_directory` in the project and fill it
once on a machine with network access, for example with a run that sets
`install_extensions: true`, or in CI:

```console
$ python -c "import duckdb; duckdb.connect(config={'extension_directory': '.duckdb/extensions'}).install_extension('delta')"
```

A missing extension fails the read or write that needs it with
[PLX121](../reference/errors.md#plx121) in the error message.

## Delta

`format: delta` reads a Delta table through `delta_scan`. Writes to Delta are not supported. CI
runs a Delta read test on Linux, macOS and Windows with the extension preinstalled.
