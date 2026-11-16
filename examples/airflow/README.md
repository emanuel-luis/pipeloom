# pipeloom examples on Airflow

An [Astro](https://www.astronomer.io/docs/astro/cli/overview) project (Airflow 3.2, Python 3.13) that
runs the four TPC-H example projects as DAGs with `pipeloom-airflow`: `tpch_polars`, `tpch_pandas`
and `tpch_spark` run inside the Airflow containers, `tpch_databricks` runs a Databricks job, and
`tpch_polars_watcher` runs the polars project in WATCHER mode.

## Prerequisites

- Docker and the [Astro CLI](https://www.astronomer.io/docs/astro/cli/install-cli).
- For `tpch_databricks`: a workspace set up as in [../README.md](../README.md#run-on-databricks).

## Run it

1. Generate the data on the host, from the repository root:

   ```console
   $ uv run examples/tpch_data.py
   ```

2. Write the stable manifest that `tpch_polars_watcher` loads, from the repository root:

   ```console
   $ uv run pipeloom compile --project-dir examples/tpch_polars --target dev --stable
   ```

3. `tpch_polars_watcher` needs the development wheels until a release with the WATCHER mode is on
   PyPI, because `requirements.txt` pins an older release. Build them before you start Airflow (the
   Dockerfile installs them, see [Development wheels](#development-wheels)):

   ```console
   $ uv build --all-packages --out-dir examples/airflow/include/wheels
   ```

4. For the Databricks DAG, write `examples/airflow/.env` from `.env.example` with your workspace URL
   and token. The file is git-ignored; never commit it.

5. Deploy the Databricks job first. The DAG does not deploy at run time:

   ```console
   $ pipeloom databricks --profile <your-profile> deploy --target dev --project-dir examples/tpch_databricks
   ```

6. Start Airflow from this folder:

   ```console
   $ cd examples/airflow
   $ astro dev start
   ```

7. Open http://localhost:8085, unpause a DAG and trigger it. DAGs have no schedule.

Stop with `astro dev stop`, or remove the containers and volumes with `astro dev kill`.

## How it works

`docker-compose.override.yml` mounts the whole `examples/` folder at
`/usr/local/airflow/examples` in the scheduler, DAG processor, triggerer and API server. The DAGs
compile each project at parse time (`LoadMode.PROJECT`), so no manifest is needed. Every
`@model` decorator in the examples has only literal arguments, so compiling reads the model files
without importing them; a unit task imports only the module of the model it runs. The project
variable `tpch_dir` (`../data/tpch`) resolves to `examples/data/tpch`.

`tpch_polars_watcher` loads the stable manifest from step 2 of "Run it" (`LoadMode.MANIFEST`, relative
source locations so it works inside the containers) and uses `emit_assets=True`. The DAG has a producer task `pipeloom_run` that runs every model in one process, one deferrable
watcher and one asset per model, and `pipeloom_finalize`. The watchers wait on the triggerer, which
mounts the same `examples/` folder, so it sees the artifacts root at the same path. If a model fails,
fix the cause and clear its watcher with Downstream: the watcher reruns that one unit.

### Development wheels

By default the containers install pipeloom from PyPI, as pinned in `requirements.txt`. To try
unreleased code, build wheels from this checkout into `include/wheels/` (git-ignored) and restart:

```console
$ uv build --all-packages --out-dir examples/airflow/include/wheels
$ cd examples/airflow
$ astro dev restart
```

The `Dockerfile` installs any `include/wheels/*.whl` after the requirements. With no wheels the step
does nothing.

### Java for Spark

`packages.txt` installs Java 21 for the Spark DAG: the runtime 3.2 image is based on Debian 13, which
has no Java 17 package, and Spark 4 supports both.

## DAGs and output

| DAG | Mode | Output |
|---|---|---|
| `tpch_polars` | local | `examples/tpch_polars/target/warehouse/pipeloom_<layer>/tpch/<table>/` |
| `tpch_pandas` | local | `examples/tpch_pandas/target/warehouse/pipeloom_<layer>/tpch/<table>/` |
| `tpch_spark` | local, max 2 tasks at once | `examples/tpch_spark/target/warehouse/pipeloom_<layer>/tpch/<table>/` |
| `tpch_polars_watcher` | watcher: one producer, one watcher per model | `examples/tpch_polars/target/warehouse/pipeloom_<layer>/tpch/<table>/` |
| `tpch_databricks` | Databricks job `main`, target `dev`, connection `databricks_default` | Unity Catalog tables `pipeloom_<layer>.tpch.<table>` |

`<layer>` is `bronze`, `silver` or `gold`. Local outputs are written by the containers into the
project folders on your host. Docker Desktop (Windows, macOS) maps file ownership for you; on a
native Linux Docker host the containers run as uid 50000, so make `examples/` writable for that
user first.
