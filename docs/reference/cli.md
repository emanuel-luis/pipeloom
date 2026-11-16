# Command line

## pipeloom init

`pipeloom init [DIRECTORY]` creates a project, like `dbt init`. DIRECTORY defaults to a new
folder named after the project in the current directory.

- When stdin is a terminal it asks for what the flags leave out: the project name (default: the
  directory name), the engine (`polars`, `pandas`, `spark` or `databricks`, default `polars`)
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
- Exit codes: 0 on success, 2 on a usage error (bad name or engine, `--catalog` without
  databricks, an existing project, a non-empty directory without `--force`).

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
  `--sql` with `--python`, a folder outside the model path).

## Reference

::: mkdocs-click
    :module: pipeloom.cli
    :command: main
    :prog_name: pipeloom
    :depth: 2
