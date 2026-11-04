# pipeloom

Declarative, engine-agnostic data pipelines in YAML. 0.2 validates, compiles and runs projects on polars or pandas.

## Rules

- Clean room: design inputs are public references only. Never import code from other codebases.
- `docs/architecture.md` and `docs/adr/` are the source of truth. A deviation needs a new ADR in the same PR.
- The core never imports a plugin. Discovery reads entry-point metadata; a plugin is imported only when a config names its `kind`.
- Every diagnostic code lives in `pipeloom.diagnostics.codes` and has a case file `tests/cases/<CODE>.yml` with goldens (`pytest --update-goldens`). Codes are never reused.
- Every CLI flag and environment variable has a CliRunner test. Command results go to stdout; diagnostics and logs go through the event bus to stderr.
- Paths in diagnostics and artifacts are POSIX and relative to the project dir; goldens use LF.
- Prose in English, no em dashes.

## Layout

- `src/pipeloom/`: `diagnostics/` (catalog), `config/` (YAML loader, models, bind, resolve, schema), `secrets/`, `plugins/` and `builtins/` (memory, filesystem catalog, fs_table), `engine/` (protocols, types, portable), `graph/` (DAG, selectors), `validate/` (L2, L4-L6; PLK603 and PLK605 need sqlglot), `plan/` (units, manifest), `runtime/` (scheduler, executors, runner, run), `state/` (sqlite and json stores, lock lease), `artifacts/` (layout, run_results, finalize), `events/` (bus, sinks), `cli/`, `testing/` (MemoryEngine, RecordingSink, FakeClock, contract suites), `api.py`, `platform.py`
- `plugins/pipeloom-polars/` and `plugins/pipeloom-pandas/`: engine distributions (uv workspace members, sources under `src/`, tests under `tests/`)
- `tests/`: pytest; `cases/` and `goldens/`; `fixtures/shop` is the reference project
- `tools/docs_hooks.py`: generates the error-code page and the JSON Schemas for the docs site; `tools/versions.py`: `check [--tag vX.Y.Z]` and `bump X.Y.Z` keep every distribution on one version
- `docs/`: mkdocs site, `architecture.md`, `adr/`, `diagrams/` (D2 sources and committed SVGs)
- `.github/workflows/`: ci, security, release, docs, dependabot-auto-merge

## Commands

- `uv sync --locked --all-packages` (the workspace has one lockfile), `uv run pytest -q` (also collects the plugin tests)
- `uv run ruff check`, `uv run ruff format --check`, `uv run mypy --strict src plugins/pipeloom-polars/src plugins/pipeloom-pandas/src`
- `uv run lint-imports` (layers, `pipeloom.testing` isolation, no polars, pandas, pyarrow or sqlglot in the core except `validate.static -> sqlglot`)
- `uv run --group docs mkdocs build --strict`
- `uv build --all-packages`
- `make help` (diagram targets need Linux or WSL)
- Commits: Conventional Commits, signed off (`git commit -s`).
