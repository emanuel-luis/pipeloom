# ADR-0023: Multi-distribution release

Architecture: [Section 17, ADR-0005 text](../architecture.md#17-adr-list-and-diagrams)

Amends [ADR-0005](0005-packaging.md) and [ADR-0016](0016-versioning-and-plugin-api.md).

## Context

0.2 adds the first plugin distribution, `pipeloom-polars`, to the uv workspace, and `pipeloom-pandas` follows. One tag must publish all of them at the same version. PyPI creates a project from a pending trusted publisher on the first upload, and a pending publisher does not reserve the name. Whether one pending publisher can serve several projects in one workflow run is unconfirmed.

## Options

- **One publish step for a flat `dist/`.** Simple, but one missing publisher fails the whole upload and gives no control over order.
- **A separate workflow or job per distribution.** Clear failures, but the tag fans out into jobs that each need the environment approval.
- **One job with one publish step per distribution, core first, each with `skip-existing`.** A failure stops at the broken step, earlier steps stay published and a rerun skips what is already on PyPI.

## Decision

- Every distribution has the same version (lockstep, ADR-0016), each plugin depends on `pipeloom~=X.Y.0`, and the root extras pin `pipeloom-<name>==X.Y.Z`. `tools/versions.py check [--tag vX.Y.Z]` verifies this, the release workflow runs it before building, and `bump X.Y.Z` rewrites the lines and runs `uv lock`. The root extra and the plugin's dependency on the root form a cycle; `uv lock` accepts it.
- The release builds each distribution with `uv build --package <name> -o dist/<name>/`. The publish job has one `pypa/gh-action-pypi-publish` step per distribution, core first, with `packages-dir: dist/<name>/`, `attestations: true` and `skip-existing: true`. The GitHub release attaches every wheel and sdist.
- A pending publisher is created for each new distribution right before tagging (checklist in CONTRIBUTING.md): project name, Owner `emanuel-luis`, Repository `pipeloom`, Workflow `release.yml`, Environment `pypi`. A missing one fails its own step with `invalid-pending-publisher`; fix it and re-run the failed jobs.
- PEP 639 forbids `..` in `license-files`, so each plugin directory holds its own copy of `LICENSE` and `NOTICE`.
- CI builds every distribution with `uv build --all-packages`, installs the core wheel alone in a clean venv, then all wheels in a second one and runs `pipeloom plugins list --check` and a smoke project on polars.

## Consequences

- A plugin name stays unclaimed until its pending publisher is used, so someone else could register it first; the checklist narrows that window to the release itself.
- If PyPI accepts only one pending publisher per token, the per-step layout limits the damage to the steps after it, and a rerun finishes the job.
- The LICENSE and NOTICE copies can drift; the release review compares them.

## Status

Accepted

## Date

2026-11-04
