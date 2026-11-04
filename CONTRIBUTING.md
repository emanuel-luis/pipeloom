# Contributing

Thanks for helping. pipeloom is in its design phase, so the most useful contributions right now
are review comments on the [architecture](docs/architecture.md) and the
[ADRs](docs/adr/README.md).

## Setup

You need [uv](https://docs.astral.sh/uv/).

```bash
uv sync --all-packages
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

### Release checklist

Every distribution (`pipeloom` and each plugin under `plugins/`) is released at one version.

1. Bump the versions: `uv run python tools/versions.py bump X.Y.Z`. It rewrites each `pyproject.toml`, checks them and runs `uv lock`.
2. Set the date of the CHANGELOG.md heading, then merge to main with CI green.
3. For each distribution with no PyPI project yet, create a pending publisher on PyPI right before tagging (it does not reserve the name): project name as in its `pyproject.toml`, Owner `emanuel-luis`, Repository `pipeloom`, Workflow `release.yml`, and Environment `pypi` for the core or `pypi-<plugin>` for a plugin (for example `pypi-polars`). PyPI accepts one pending publisher per owner, repository, workflow and environment, which is why every distribution has its own GitHub environment.
4. Tag `vX.Y.Z` on main and push the tag. If main already holds work for the next version, cut `release/X.Y` from the release commit instead, cherry-pick only the fixes the release needs (for example to the release workflow, since a tag runs the workflow file of its own commit), run CI on that branch (`gh workflow run ci --ref release/X.Y`) and tag there. The branch can be deleted after the release; the tag keeps the commit.
5. Each distribution publishes from its own job, the core first. If one fails with `invalid-pending-publisher`, fix the publisher and use "Re-run failed jobs". Jobs that already published stay published, and `skip-existing` makes the rerun safe. A plugin job is skipped when that plugin is not in the release yet.
