# pipeloom

Declarative, engine-agnostic data pipelines in YAML. 0.1 validates and compiles projects (no execution yet; that lands in 0.2).

## Rules

- Clean room: design inputs are public references only. Never import code from other codebases.
- `docs/architecture.md` and `docs/adr/` are the source of truth. A deviation needs a new ADR in the same PR.
- The core never imports a plugin. Discovery reads entry-point metadata; a plugin is imported only when a config names its `kind`.
- Every diagnostic code lives in `pipeloom.diagnostics.codes` and has a case file `tests/cases/<CODE>.yml` with goldens (`pytest --update-goldens`). Codes are never reused.
- Every CLI flag and environment variable has a CliRunner test. Command results go to stdout; diagnostics and logs go through the event bus to stderr.
- Paths in diagnostics and artifacts are POSIX and relative to the project dir; goldens use LF.
- Prose in English, no em dashes.

## Layout

- `src/pipeloom/`: `diagnostics/` (catalog), `config/` (YAML loader, models, bind, resolve, schema), `secrets/`, `plugins/` and `builtins/`, `engine/` (protocols and types), `graph/` (DAG, selectors), `validate/` (L2, L4-L6), `plan/` (units, manifest), `events/` (bus, sinks), `cli/`, `testing/` (MemoryEngine, RecordingSink, FakeClock), `api.py`, `platform.py`
- `tests/`: pytest; `cases/` and `goldens/`; `fixtures/shop` is the reference project
- `tools/docs_hooks.py`: generates the error-code page and the JSON Schemas for the docs site
- `docs/`: mkdocs site, `architecture.md`, `adr/`, `diagrams/` (D2 sources and committed SVGs)
- `.github/workflows/`: ci, security, release, docs, dependabot-auto-merge

## Commands

- `uv sync`, `uv run pytest -q`
- `uv run ruff check`, `uv run ruff format --check`, `uv run mypy --strict src`
- `uv run --group docs mkdocs build --strict`
- `uv build`
- `make help` (diagram targets need Linux or WSL)
- Commits: Conventional Commits, signed off (`git commit -s`).
