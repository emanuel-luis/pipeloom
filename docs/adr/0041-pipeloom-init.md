# ADR-0041: `pipeloom init`

Architecture: [Section 3, Architecture overview](../architecture.md)

## Context

A new user starts from an empty folder and has to write `pipeloom.yml`, a target, a catalog, a state store and the model files by hand, copying from the quickstart or the TPC-H examples. dbt solves the same problem with `dbt init`, which asks a few questions and writes a working project. pipeloom has no such command, and the CLI list in the architecture does not include one.

## Options

- **Where the templates live.**
  - Inside the core package, `pipeloom/scaffold/templates/`, shipped in the wheel. Nothing to download, and the version of the templates always matches the installed core.
  - In each engine plugin, through a new entry-point group. Each plugin owns its template, but `init` would have to import a plugin before the user picked an engine, and a new group changes the plugin API.
  - A remote template repository (cookiecutter style). Flexible, but it needs network access and a new dependency.
- **How the user picks values.**
  - Flags plus click prompts for what the flags leave out, prompting only when stdin is a terminal.
  - Flags only. Scriptable, but a first-time user has to read `--help` first.
- **What `--force` does with existing files.**
  - Keep every existing file and report it as skipped.
  - Overwrite. Fast to retry, but it can destroy user work without a warning.

## Decision

- `pipeloom init [DIRECTORY]` writes a project from templates inside the core package. The templates are plain text files with `{{key}}` placeholders, rendered with string replacement. The core imports no engine to do it.
- Each engine is a stack of template folders (`common`, `local`, `sql`, then the engine's own folder); a later folder's file replaces an earlier one with the same path. polars, spark and databricks get SQL models; pandas gets Python models, because pandas has no SQL (PLC502).
- polars, pandas and spark read a six-row CSV written into `data/`, so the project runs with no download. databricks reads `samples.tpch.orders`, which every workspace has, and gets a `unity` catalog, a state table, an artifacts root on a volume and a serverless `deploy` block, with comments that say what must exist before the first deploy.
- Every layer writes to its own schema (`bronze`, `silver`, `gold`) through the folder `_defaults.yml`, not its own catalog, so local Spark needs no extra catalog configuration.
- The generated YAML is block style; only short scalar lists such as `tags: [bronze]` stay inline.
- Flags `--name`, `--engine`, `--catalog`, `--no-input` and `--force`. Prompts run only when stdin is a terminal and `--no-input` is not given, and only for values the flags leave out.
- `--force` allows a non-empty directory and never overwrites: an existing file is kept and listed on stdout as skipped. A directory that already holds `pipeloom.yml` is refused even with `--force`.
- A bad name, a bad engine, `--catalog` with an engine other than databricks, an existing project and a non-empty directory without `--force` are click usage errors (exit 2). No diagnostic code is added: `init` reads no project.
- `init` writes no event stream: there is no project, target or sink configuration to send events to.

## Consequences

- Templates ship with the core and change with it. A template that stops validating is caught by the tests, which validate and run (polars, pandas) or validate and compile (spark, databricks) every generated project.
- The databricks template pins the `deploy` dependencies to the installed pipeloom version.
- A plugin cannot add its own template yet. When one needs to, an entry-point group can be added then.

## Status

Accepted.
