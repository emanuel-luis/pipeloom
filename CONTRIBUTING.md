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

## Live Databricks checks

Facts that only a real workspace can confirm are checked by the workflow `databricks-live`
(`.github/workflows/databricks-live.yml`, driver `tools/databricks_live.py`, project
`tests/fixtures/databricks_live`). It runs only when a maintainer dispatches it by hand in the
upstream repository, never on a pull request, and it prints `CHECK <fact> ok|failed` per fact.
Regular CI and the test suite never reach a workspace.

To set it up once, in the repository settings:

1. Create the environment `databricks-live`. Add required reviewers, and restrict deployment to
   the `main` branch.
2. Add these environment secrets. Use a service principal, never a personal token. Either
   `DATABRICKS_HOST` with `DATABRICKS_CLIENT_ID` and `DATABRICKS_CLIENT_SECRET` (OAuth), or
   `DATABRICKS_HOST` with `DATABRICKS_TOKEN`.
3. In the workspace, give the principal `USE CATALOG` and `CREATE SCHEMA` on the catalog you pass
   as the `catalog` input, and `READ VOLUME` and `WRITE VOLUME` on the volume you pass as
   `volume` (`/Volumes/<catalog>/<schema>/<volume>`). It also needs permission to create jobs and
   to use serverless compute.

The driver refuses to start unless `DATABRICKS_HOST` is set and `DATABRICKS_CONFIG_FILE` points
at a file that does not exist, so no profile file can ever be read. Each wait is bounded to 20
minutes so the cleanup always gets time inside the 60-minute limit.

Dispatch it from the Actions tab (or `gh workflow run databricks-live`) with the `catalog` and
`volume` inputs; `environment_version` defaults to 5. The run deploys two jobs, runs and repairs
them, then deletes the jobs, the uploaded project folders and a scratch schema. Set `keep_job` to
keep them for inspection. The wheels it uploads stay under `<volume>/wheels/<sha>/`.

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
