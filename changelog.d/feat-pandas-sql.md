### Added

- The pandas engine runs `ctx.sql` through DuckDB with the new extra `pipeloom-pandas[sql]` ([ADR-0068](docs/adr/0068-pandas-sql-through-duckdb.md)): the inputs are bound by alias as Arrow tables, the result stays Arrow-backed, timestamps stay in UTC and no DuckDB extension is downloaded. The engine's `sql_dialect` is `duckdb` when DuckDB is installed; without it nothing changes (PLK606, then PLX101).
