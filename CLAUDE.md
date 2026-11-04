# pipeloom

Declarative, engine-agnostic data pipelines in YAML. Version 0.0.2 is a name-reserving placeholder.

## Rules

- No functionality exists before 0.1. The package only exposes `__version__` and `pipeloom --version`.
- Clean room: design inputs are public references only. Never import code from other codebases.
- Prose in English, no em dashes. Every CLI flag has a CliRunner test.

## Layout

- `src/pipeloom/`: package (`cli/`, `config/`, `diagnostics/`, `secrets/`, `api.py`, `platform.py`, `__main__.py`, `py.typed`)
- `tests/`: pytest
- `docs/`: mkdocs site, `architecture.md`, `adr/`, `diagrams/` (D2 sources and committed SVGs)
- `.github/workflows/`: ci, security, release, docs

## Commands

- `uv sync`, `uv run pytest -q`
- `uv run ruff check`, `uv run ruff format --check`, `uv run mypy --strict src`
- `uv build`
- `make help` (diagram targets need Linux or WSL)
- Commits: Conventional Commits, signed off (`git commit -s`).
