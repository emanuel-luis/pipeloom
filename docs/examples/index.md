# Examples

Each example is a small pipeloom project in [`examples/`](https://github.com/emanuel-luis/pipeloom/tree/main/examples)
that runs on a laptop with one command and ships its own data. Every example is run end to end in CI, so the pages
below never drift from the code.

## Walkthroughs

<div class="grid cards" markdown>

-   **Late-arriving events**

    ---

    Clickstream events land in daily files. An incremental model with a watermark, a lookback and a merge on the
    event id picks up late events, reruns idempotently and recovers the rest with a full refresh.

    DuckDB, polars. [Walkthrough](events_incremental.md)

-   **Customer dimension with history**

    ---

    Three CRM snapshots build a type 2 slowly changing dimension with `valid_from`, `valid_to`, `is_current` and
    hard deletes, then answer "as of" questions.

    DuckDB, polars, pandas. [Walkthrough](customers_scd2.md)

-   **An untrusted partner feed**

    ---

    Expectations drop and quarantine bad rows, a relationships test flags orphans, and a model contract stops a
    schema break before anything is written.

    DuckDB, polars. [Walkthrough](orders_quality.md)

-   **Testing transformation logic**

    ---

    Unit tests with given and expected rows, data tests after the write, `pipeloom test`, `pipeloom build`, and the
    row diff a wrong formula produces.

    DuckDB, polars. [Walkthrough](tested-models.md)

</div>

## Reference projects

<div class="grid cards" markdown>

-   **One pipeline, four engines**

    ---

    The same TPC-H bronze, silver and gold layers on polars, pandas, Spark and Databricks, and on DuckDB through
    the `duckdb` target of `tpch_polars`.

    [examples/README.md](https://github.com/emanuel-luis/pipeloom/blob/main/examples/README.md)

-   **Airflow, one task or one watcher per model**

    ---

    An Astro project that renders the TPC-H example as an Airflow DAG in LOCAL, DATABRICKS and WATCHER modes.

    [Airflow plugin](../plugins/airflow.md),
    [examples/airflow](https://github.com/emanuel-luis/pipeloom/tree/main/examples/airflow)

-   **Lineage in Marquez**

    ---

    Runs the TPC-H example with the OpenLineage sink and shows the graph, the schemas and the quality assertions
    in Marquez.

    [OpenLineage guide](../guide/openlineage.md),
    [examples/marquez](https://github.com/emanuel-luis/pipeloom/tree/main/examples/marquez)

-   **A Databricks job**

    ---

    The TPC-H example as one serverless Databricks job per pipeline, deployed from a bundle or directly.

    [Databricks plugin](../plugins/databricks.md),
    [examples/tpch_databricks](https://github.com/emanuel-luis/pipeloom/tree/main/examples/tpch_databricks)

</div>

## Try one

```console
$ git clone https://github.com/emanuel-luis/pipeloom
$ cd pipeloom
$ uv sync --all-packages
$ uv run pipeloom build --project-dir examples/orders_quality --vars '{"batch": "day1"}'
```

Each walkthrough lists the exact commands for every step. For a project of your own, start with
`pipeloom demo` or `pipeloom init` ([Quickstart](../quickstart.md)).
