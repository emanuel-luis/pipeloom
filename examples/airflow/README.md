# pipeloom examples on Airflow

An [Astro](https://www.astronomer.io/docs/astro/cli/overview) project (Airflow 3.2, Python 3.13) that
runs the four TPC-H example projects as DAGs with `pipeloom-airflow`: `tpch_polars`, `tpch_pandas`
and `tpch_spark` run inside the Airflow containers, `tpch_databricks` runs a Databricks job.

## Prerequisites

- Docker and the [Astro CLI](https://www.astronomer.io/docs/astro/cli/install-cli).
- For `tpch_databricks`: a workspace set up as in [../README.md](../README.md#run-on-databricks).

## Run it

1. Generate the data on the host, from the repository root:

   ```console
   $ uv run examples/tpch_data.py
   ```

2. For the Databricks DAG, write `examples/airflow/.env` from `.env.example` with your workspace URL
   and token. The file is git-ignored; never commit it.

3. Deploy the Databricks job first. The DAG does not deploy at run time:

   ```console
   $ pipeloom databricks --profile <your-profile> deploy --target dev --project-dir examples/tpch_databricks
   ```

4. Start Airflow from this folder:

   ```console
   $ cd examples/airflow
   $ astro dev start
   ```

5. Open http://localhost:8085, unpause a DAG and trigger it. DAGs have no schedule.

Stop with `astro dev stop`, or remove the containers and volumes with `astro dev kill`.

## How it works

`docker-compose.override.yml` mounts the whole `examples/` folder at
`/usr/local/airflow/examples` in the scheduler, DAG processor, triggerer and API server. The DAGs
compile each project at parse time (`LoadMode.PROJECT`), so no manifest is needed. The project
variable `tpch_dir` (`../data/tpch`) resolves to `examples/data/tpch`.

## DAGs and output

| DAG | Mode | Output |
|---|---|---|
| `tpch_polars` | local | `examples/tpch_polars/target/warehouse/pipeloom_<layer>/tpch/<table>/` |
| `tpch_pandas` | local | `examples/tpch_pandas/target/warehouse/pipeloom_<layer>/tpch/<table>/` |
| `tpch_spark` | local, max 2 tasks at once | `examples/tpch_spark/target/warehouse/pipeloom_<layer>/tpch/<table>/` |
| `tpch_databricks` | Databricks job `main`, target `dev`, connection `databricks_default` | Unity Catalog tables `pipeloom_<layer>.tpch.<table>` |

`<layer>` is `bronze`, `silver` or `gold`. Local outputs are written by the containers into the
project folders on your host.
