# Troubleshooting and FAQ

## Reading an error

Every problem pipeloom reports has a code such as `PLM204`. Run `pipeloom explain <CODE>` for the explanation, the
usual fix and a link, or look it up in the [error codes](reference/errors.md). With `--log-format json`, each
diagnostic event also carries an `explain_url`.

## Installing and running

**Spark fails on Windows with `Connection reset`.**
PySpark workers do not run in Windows Python. Run Spark targets on Linux, macOS or WSL, with Java 17 or 21. The
polars, pandas and DuckDB engines run natively on Windows.

**The `airflow` extras do not install on Windows.**
One of Airflow's OpenLineage dependencies publishes no Windows wheel. Develop and test the Airflow plugin on Linux, WSL
or in CI.

**A timezone-aware column fails with `ZoneInfoNotFoundError` on Windows.**
Windows has no system time zone database. pipeloom-polars depends on `tzdata` on Windows from 1.1; on older versions,
`pip install tzdata`.

**DuckDB stops with PLX121 when reading a Delta source.**
The DuckDB engine never downloads extensions on its own. Install the `delta` extension into the engine's
`extension_directory` on a machine with network access, or set `install_extensions: true` on the target. See the
[DuckDB plugin](plugins/duckdb.md).

## Models and sources

**A model and a source have the same name, and the source is not found (PLM204).**
A model never reads itself, so a parameter with the model's own name binds the source table. Give that source table
an explicit `location` in `pipeloom.yml`.

**A CSV date column is a string on polars and a date on DuckDB.**
CSV readers infer types differently. Cast in the model (`cast(order_date as date)` in `ctx.sql`) or declare the column
in a contract so a difference fails early, instead of relying on inference.

**`ctx.sql` fails on DuckDB with "Ambiguous reference".**
A schema with the same name as one of the model's input parameters makes the name ambiguous in DuckDB. Rename the
schema or the parameter.

**An order-dependent operation fails with PLX122.**
Lazy engines have no row order. Give the expression an order with `.over(order_by=...)` or sort first. See
[portable models](guide/portable-models.md).

## Runs and state

**How do I rerun only what failed?**
`pipeloom retry --from <run_id>` starts a new run with the failed, skipped and cancelled units of that run. See
[running a pipeline](guide/running.md).

**How do I build only what changed in a pull request?**
Compile the main branch with `pipeloom compile --stable` and keep its `target/` as a reference, then select
`state:modified+` with `--state <dir>` and read unchanged parents from the reference with `--defer`.

**Where are the results of a run?**
Under `target/runs/<run_id>/`: the manifest, `run_results.json`, the events and one attempt file per unit. The latest
`run_results.json` is also copied to `target/`. `pipeloom runs ls` and `pipeloom runs show` read the run history. See
[run artifacts](reference/artifacts.md).

## Still stuck?

Open an issue on [GitHub](https://github.com/emanuel-luis/pipeloom/issues) with the command, the diagnostic code and the
output of `pipeloom plugins list`.
