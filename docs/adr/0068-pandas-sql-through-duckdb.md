# ADR-0068: pandas runs ctx.sql through DuckDB

## Context

pandas has no SQL engine. Since 0.9 the pandas engine declares `sql_dialect=None`: a model that calls `ctx.sql` gets
PLK606 at validation and fails with PLX101 at run time. A project that runs on DuckDB, polars and Spark therefore
cannot move to pandas without rewriting its SQL models as frame code, and the conformance kit skips every SQL cell
for pandas.

The pandas engine reads files with `dtype_backend="pyarrow"`, so its frames are Arrow-backed. DuckDB can scan Arrow
tables without copying them and returns Arrow tables. pipeloom already ships a DuckDB engine (ADR-0051) and pins
`duckdb>=1.4,<3` there.

## Options

- **DuckDB as the SQL executor.** A query opens an in-memory DuckDB connection, the input frames are registered as Arrow
  tables under their aliases, and the result comes back as an Arrow-backed DataFrame. Full SQL, no copy for
  Arrow-backed columns, a dependency pipeloom already uses. DuckDB's own dialect becomes the engine's.
- **polars `SQLContext`.** Convert pandas to polars and back. Two conversions, a smaller SQL surface, and a second
  dataframe library in a pandas project.
- **pandasql (sqlite).** Copies frames into sqlite and loses types; unmaintained. Rejected.
- **sqlglot's Python executor.** Pure Python and far too slow for real data. Rejected.
- **Ibis with its pandas backend.** Ibis removed that backend. Rejected.

## Decision

The pandas engine runs `ctx.sql` through DuckDB when DuckDB is installed. It is an optional extra,
`pipeloom-pandas[sql]` (`duckdb>=1.4,<3`, the DuckDB engine's range), so a pandas project without SQL stays as light as
before.

- The spec module checks for DuckDB with `importlib.util.find_spec`, which imports nothing. With DuckDB installed the
  capabilities declare `sql_models=True` and `sql_dialect="duckdb"`; without it they stay `False` and `None`, and the
  0.9 behavior (PLK606, then PLX101) is unchanged.
- `EngineSession.sql` opens a fresh in-memory connection per query, registers each input frame under its alias with
  `pyarrow.Table.from_pandas(frame, preserve_index=False)`, runs the query, and converts the Arrow result with
  `types_mapper=pd.ArrowDtype`. The connection is closed after the query, so an alias never leaks into another query
  and no connection is shared between unit threads.
- The connection sets `TimeZone=UTC`, because DuckDB otherwise returns `TIMESTAMPTZ` values in the machine's local zone,
  and `autoinstall_known_extensions=false`, the extension policy of ADR-0051: `ctx.sql` never downloads anything.
- When DuckDB returns no result relation for the query, the call fails with a clear error instead of an attribute error.
- `sql_dialect` is `duckdb`, so a model written in another dialect is translated with the `sqlglot` extra exactly as
  on the other engines.
- Views stay unsupported on pandas (PLC507): a pandas table is files, and a stored view needs a database.

Provenance: designed from the public DuckDB Python API documentation and pandas' Arrow dtype documentation; no code was
copied from other projects.

## Consequences

- The same `ctx.sql` model runs on DuckDB, polars, pandas and Spark, and the conformance kit runs its SQL cells on
  pandas.
- The engine's capabilities depend on whether the extra is installed. A project validated where DuckDB is installed
  and run where it is not fails as before, with PLK606 and PLX101 naming the cause.
- Plugin API, config and defaults are unchanged: this is an additive capability under the stability policy.
- Each query pays a small cost to open a connection and wrap the frames; it is negligible next to the query on any
  real input. Revisit with a per-session connection if profiling ever says otherwise.

## Status

Accepted

## Date

2026-11-21
