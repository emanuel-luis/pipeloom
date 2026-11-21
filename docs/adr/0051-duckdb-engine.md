# ADR-0051: DuckDB engine

## Context

pipeloom 1.1 adds `pipeloom-duckdb`, an in-tree engine distribution released in lockstep with the
others. DuckDB runs SQL in process, reads Parquet and CSV natively and Delta through its `delta`
extension, and since 1.4 supports `MERGE INTO`
([announcement](https://duckdb.org/2025/09/16/announcing-duckdb-140)). The engine needs a place
for its tables, a mapping of pipeloom relations onto it, an extension policy that works offline in
CI, and a state store. The plugin API stays at (1, 0) in 1.1. Every design input here comes from
the public DuckDB documentation.

## Options

- **A DuckDB database file as the catalog.** Tables and views are DuckDB objects; writes are SQL
  inside DuckDB transactions, so a write and its idempotency token commit together. Only one
  process may open a file for writing.
- **An attached lakehouse (Delta or Iceberg files).** Tables readable by other engines, but DuckDB
  writes neither format with transactions across files, and merge or scd2 would leave DuckDB.
- **The filesystem catalog, as polars uses.** Reuses `fs_write`, but every strategy runs in Python
  over frames instead of pushing down to SQL.

## Decision

1. **Target.** The `duckdb` catalog holds tables in one DuckDB database file (`path`, default
   `target/pipeloom.duckdb`, relative to the project), or `:memory:` for tests. The engine works
   only with this catalog; any other catalog is rejected when the session opens.
2. **Relations.** A target without a catalog is `"<database>"."<schema>"."<name>"` in that file.
   A target catalog `<name>` (`+catalog` in the `models:` tree) is the database file
   `<name>.duckdb` next to `path`, attached as `<name>` on first use and created by
   `ensure_namespace`; with `:memory:` it is an in-memory database. Existing sibling files are
   attached when a session opens, so a view that reads another catalog binds. Schemas are DuckDB
   schemas, created by `ensure_namespace`.
3. **Sessions.** A session opens one connection with `TimeZone=UTC`. The unit threads share it
   under a lock: DuckDB parallelizes each query, and a connection must not run two statements at
   once. Process boundaries (`boundary: task`) open the file from several processes and DuckDB
   refuses a second writer, so they are not supported.
4. **Sources.** `parquet` and `csv` read through `read_parquet` and `read_csv`, with the source
   options passed as named parameters; a directory reads every file of that format in it.
   `delta` is read only, through `delta_scan`. A missing source fails at read (PLM204).
5. **Extensions.** `autoinstall_known_extensions` is always off. Extensions load from the engine's
   `extension_directory` (pinned in the project). `extensions` lists the ones to load at session
   open; `delta` loads on the first Delta read. When an extension is missing and
   `install_extensions` is off (the default), the engine raises an error that carries
   **PLX121**; the read, write or model that needed it fails with that text in its message
   (PLM204, PLM201 or PLX101), permanent. The plugin API has no channel for plugin codes in 1.x,
   so the code travels in the message. A session still opens: a listed missing extension fails
   the first data operation, not the run start. With `install_extensions: true`, the engine
   installs from the network into that directory.
6. **Writes.** The batch is staged in a temporary table first, outside any transaction, so a
   failing relation never touches the target and a model can read its own table. Then one
   transaction runs the strategy and inserts the token into `<database>._pipeloom.commits`:
   `overwrite` is `CREATE OR REPLACE TABLE`, `append` an `INSERT ... BY NAME`,
   `replace_partitions` a `DELETE` of every partition in the batch (null is a partition value)
   then an insert, `merge` one `MERGE INTO` (hard deletes as `WHEN MATCHED ... THEN DELETE`,
   `update_condition` in DuckDB SQL over `source` and `target`), and `scd2` a `MERGE INTO` that
   closes changed or deleted current rows at the run start, then an insert of the new versions
   (hard deletes only; NaN compares as null). Counts come from a query over the staged batch and
   the target before the change. A repeated token returns the stored result and writes nothing.
   A schema change the core allowed rebuilds the table with `CREATE OR REPLACE TABLE ... AS
   SELECT` from itself, so column order follows the decided schema.
7. **Capabilities.** Formats `parquet`, `csv`, `delta`; strategies `append`, `overwrite`, `merge`,
   `replace_partitions`, `scd2`; materializations `table`, `view`, `ephemeral`, `incremental`,
   `intermediate`; SQL models and views; idempotent writes; cancel (through the connection's
   interrupt); portable (Narwhals accepts DuckDB relations); quality; `sql_dialect="duckdb"`;
   no bytes metrics. Views are DuckDB views over CTEs of their inputs. Intermediates are one
   Parquet file per model under the run's artifacts, with a token file.
8. **State.** The engine pairs with the core `sqlite` or `json` state stores. There is no DuckDB
   state store.

## Consequences

- Merge, scd2 and replace_partitions run where the data is, in one transaction with the token.
- Tables are DuckDB-only: another engine reads them by attaching the file, not as open table
  formats. Writing Delta or Iceberg from DuckDB is out of scope, as are remote databases
  (MotherDuck, attached Postgres) and a DuckDB state store.
- One writer process per database file; units run on threads.
- CI preinstalls `delta` into a directory and runs the plugin tests without network on Linux;
  macOS and Windows run the same Delta read test with installs off. Revisit PLX121 as a first-class
  diagnostic when a plugin API minor adds a way for engines to raise coded errors.

## Status

Accepted

## Date

2026-11-20
