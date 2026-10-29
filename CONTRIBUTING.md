# Contributing

Thanks for helping. pipeloom is in its design phase, so the most useful contributions right now
are review comments on the [architecture](docs/architecture.md) and the
[ADRs](docs/adr/README.md).

## Setup

You need [uv](https://docs.astral.sh/uv/).

```bash
uv sync
uv run pytest -q
```

## Make targets

Run `make help` for the full list. The usual ones:

- `make lint`: ruff check and format check
- `make test`: pytest
- `make diagrams`: render `docs/diagrams/*.d2` to SVG (run `make d2-install` once; Linux or WSL)

Before a pull request, run `uv run ruff check`, `uv run ruff format --check`,
`uv run mypy --strict src` and `uv run pytest -q`.

## Commits

- Use [Conventional Commits](https://www.conventionalcommits.org/) (`feat:`, `fix:`, `docs:`,
  `chore:`).
- Sign off every commit with the Developer Certificate of Origin: `git commit -s`.
- Use only public references as design input. Do not paste code from other codebases.

## Tests

Every CLI flag has a test that uses `click.testing.CliRunner`. A behavior change comes with a test.

## Architecture decisions

A change to the architecture needs an ADR. Copy `docs/adr/0000-template.md`, number it next in
sequence, fill in Context, Options, Choice and Consequences, and add it to `docs/adr/README.md`.
Discuss it in the pull request.

## Releases

CHANGELOG.md keeps `[X.Y.Z] - unreleased` until release day. Set the date (`YYYY-MM-DD`) in a commit just before tagging; the release workflow fails on any other heading.
