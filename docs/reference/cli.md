# Command line

## pipeloom init

`pipeloom init [DIRECTORY]` creates a project, like `dbt init`. DIRECTORY defaults to a new
folder named after the project in the current directory.

- When stdin is a terminal it asks for what the flags leave out: the project name (default: the
  directory name), the engine (`polars`, `pandas`, `spark` or `databricks`, default `polars`)
  and, on databricks, the Unity Catalog catalog (default `main`). Otherwise, or with
  `--no-input`, it uses the flags and those defaults.
- polars, spark and databricks get SQL models; pandas gets Python models under `src/`. polars,
  pandas and spark read a small CSV in `data/`, so `pipeloom run` works right away on polars and
  pandas. databricks reads `samples.tpch.orders` and comes with a state table, an artifacts
  volume and a serverless `deploy` block; the comments in `pipeloom.yml` say what to create
  before `pipeloom databricks deploy`.
- A directory that is not empty needs `--force`. Existing files are never overwritten: they are
  kept and listed as skipped. A directory that already holds `pipeloom.yml` is refused.
- Exit codes: 0 on success, 2 on a usage error (bad name or engine, `--catalog` without
  databricks, an existing project, a non-empty directory without `--force`).

## Reference

::: mkdocs-click
    :module: pipeloom.cli
    :command: main
    :prog_name: pipeloom
    :depth: 2
