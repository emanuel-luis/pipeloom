# Command line

## Where each command is explained

The generated reference at the end of this page lists every flag. These pages explain what the
commands are for. A test keeps this table in step with the commands.

| Command | Explained in |
|---|---|
| `pipeloom init` | [pipeloom init](#pipeloom-init), [Quickstart](../quickstart.md) |
| `pipeloom demo` | [pipeloom demo](#pipeloom-demo), [Quickstart](../quickstart.md#try-it-in-one-command) |
| `pipeloom new` | [pipeloom new model](#pipeloom-new-model) |
| `pipeloom new model` | [pipeloom new model](#pipeloom-new-model), [Quickstart](../quickstart.md#add-a-model) |
| `pipeloom validate` | [Inspecting a project](../guide/running.md#inspecting-a-project), [Quickstart](../quickstart.md#check-it) |
| `pipeloom compile` | [Inspecting a project](../guide/running.md#inspecting-a-project), [Selection by state](../guide/running.md#selection-by-state-and-defer) |
| `pipeloom ls` | [Inspecting a project](../guide/running.md#inspecting-a-project) |
| `pipeloom graph` | [Inspecting a project](../guide/running.md#inspecting-a-project), [Orchestrator protocol](../guide/orchestrators.md) |
| `pipeloom run` | [Running a pipeline](../guide/running.md) |
| `pipeloom retry` | [Retrying a run](../guide/running.md#retrying-a-run) |
| `pipeloom test` | [Testing models](../guide/testing.md#running-them) |
| `pipeloom build` | [Testing models](../guide/testing.md#build) |
| `pipeloom runs` | [Run history](../guide/run-history.md) |
| `pipeloom runs ls` | [Run history](../guide/run-history.md#list-runs) |
| `pipeloom runs show` | [Run history](../guide/run-history.md#show-one-run) |
| `pipeloom finalize` | [Closing the run](../guide/running.md#closing-the-run-pipeloom-finalize), [Orchestrator protocol](../guide/orchestrators.md#9-finalize) |
| `pipeloom docs` | [Docs site](../guide/docs-site.md) |
| `pipeloom docs generate` | [Docs site](../guide/docs-site.md#generate) |
| `pipeloom docs serve` | [Docs site](../guide/docs-site.md#serve) |
| `pipeloom schema` | [JSON Schema](schema.md) |
| `pipeloom schema export` | [JSON Schema](schema.md) |
| `pipeloom explain` | [Running a pipeline](../guide/running.md#output) |
| `pipeloom plugins` | [Inspecting a project](../guide/running.md#inspecting-a-project) |
| `pipeloom plugins list` | [Inspecting a project](../guide/running.md#inspecting-a-project) |
| `pipeloom databricks` | [Databricks plugin](../plugins/databricks.md), [Databricks commands](databricks-cli.md) |
| `pipeloom databricks bundle` | [Bundle and direct deploy](../plugins/databricks.md#bundle-and-direct-deploy) |
| `pipeloom databricks deploy` | [Bundle and direct deploy](../plugins/databricks.md#bundle-and-direct-deploy) |
| `pipeloom databricks run` | [Run](../plugins/databricks.md#run) |
| `pipeloom databricks repair` | [Repair](../plugins/databricks.md#repair) |

## pipeloom init

`pipeloom init [DIRECTORY]` creates a project, like `dbt init`. DIRECTORY defaults to a new
folder named after the project in the current directory.

- When stdin is a terminal it asks for what the flags leave out: the project name (default: the
  directory name), the engine (`polars`, `pandas`, `spark`, `databricks` or `duckdb`, default `polars`; the default stays polars)
  and, on databricks, the Unity Catalog catalog (default `main`). Otherwise, or with
  `--no-input`, it uses the flags and those defaults.
- Each layer folder, `models/bronze`, `models/silver` and `models/gold`, gets one `.py` file with
  a decorated model. polars, spark and databricks models return `ctx.sql(...)`; pandas models
  are frame code, since pandas has no SQL. `pipeloom.yml` holds the source group `raw` and a
  `models:` tree that sets each layer's schema and tags; `models/silver/_properties.yml` holds a
  description. `.vscode/settings.json` maps `project.json` to `pipeloom.yml` and
  `properties.json` to `models/**/*.yml`. polars, pandas and spark read a small CSV in `data/`,
  so `pipeloom run` works right away on polars and pandas. databricks reads
  `samples.tpch.orders` and comes with a state table, an artifacts
  volume and a serverless `deploy` block; the comments in `pipeloom.yml` say what to create
  before `pipeloom databricks deploy`.
- A directory that is not empty needs `--force`. Existing files are never overwritten: they are
  kept and listed as skipped. A directory that already holds `pipeloom.yml` is refused.
- It also writes `AGENTS.md` and three skills under `.agents/skills/`, generated from the code
  (the command list and the diagnostic catalog); see
  [Working with coding agents](../guide/running.md#working-with-coding-agents).
  `--update-agents` rewrites just those files in the project at DIRECTORY (default: the current
  directory) and takes no other option except `--log-format`.
- Exit codes: 0 on success, 2 on a usage error (bad name or engine, `--catalog` without
  databricks, an existing project, a non-empty directory without `--force`).

## pipeloom demo

`pipeloom demo [DIRECTORY]` is the quickest way to see pipeloom work. It writes the `init`
project for the duckdb engine into DIRECTORY (default `pipeloom_demo` in the current directory),
runs `pipeloom build` on it (a unit test, three models and their data tests), writes the docs
site and prints where it is and what to try next.

- It needs the DuckDB engine (`pip install "pipeloom[duckdb]"`). Without it the command stops
  with PLU004 and the install command as the hint. It never falls back to
  another engine, so the demo is the same on every machine.
- DIRECTORY must be missing or empty. The site is `DIRECTORY/target/docs/index.html`.
- Exit codes: 0 on success, 1 when the engine is missing or the build fails, 2 for a directory
  that is not empty.

## pipeloom new model

`pipeloom new model NAME` adds one model file to a project and prints its path.

- The file is `models/<folder>/<NAME>.py`, with one function `NAME` decorated with `@model`.
  `--folder` is relative to the first model path and cannot leave it.
- `--sql` writes a body that returns `ctx.sql("select 1 as id")`. `--python` writes a frame stub
  that raises `NotImplementedError` until you write the body. Without a flag, pandas default
  targets get `--python` and the rest `--sql`. Add one parameter per input, named like the model
  or source table it reads.
- Exit codes: 0 on success, 1 when the project does not load, 2 on a usage error (bad name, a
  Python keyword or `ctx`, `model` or `ModelContext` as the name, a model or file that exists,
  `--sql` with `--python`, a folder outside the model path, a file outside every `python_paths`
  root, or a name that is already a folder where the file would go, such as `bronze` in an `init`
  project).

## Machine-readable output

Every command takes `--log-format json` (or `PIPELOOM_LOG_FORMAT=json`): stderr then carries one
`pipeloom.event/1` object per line. A `diagnostic` event has an optional `explain_url`, the link to
the code's entry on the error-code page. `pipeloom compile --output json` prints a
`pipeloom.compile_result/1` object (manifest path and hash, plan counts) instead of the bare path.
`pipeloom init` writes `AGENTS.md` and skills under `.agents/skills/`, and
`pipeloom init --update-agents [DIRECTORY]` regenerates only those files in an existing project.

## Reference

::: mkdocs-click
    :module: pipeloom.cli
    :command: main
    :prog_name: pipeloom
    :depth: 2
