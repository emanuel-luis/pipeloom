# ADR-0016: Versioning, release, plugin API version

Architecture: [Section 15 (Release) and section 6 (Plugin API version)](../architecture.md#15-testing-ci-release-docs)

## Context

Several distributions ship from one repository, and several contracts evolve at different speeds: the package, the plugin protocol, the config format and the manifest, event and run_results schemas.

## Options

- **Lockstep versions** for first-party distributions during 0.x, with one tag. Simple releases. Plugin-only fixes still bump every distribution.
- **Independent versions per distribution.** Precise, but more release work and a larger compatibility matrix while the API is moving.

## Decision

- SemVer per distribution, with lockstep versions for first-party distributions during 0.x and one tag `vX.Y.Z`.
- The tag workflow checks that the tag equals the version, builds, then publishes to PyPI by trusted publishing, then creates the GitHub release with notes from `CHANGELOG.md` (Keep a Changelog).
- Each distribution publishes from its own job and its own GitHub environment: `pypi` for the core and `pypi-<plugin>` for each plugin (`pypi-polars`, `pypi-pandas`, `pypi-spark`, `pypi-databricks`, `pypi-airflow`). PyPI accepts only one pending trusted publisher per owner, repository, workflow and environment, so new plugin projects cannot share one environment. Amended on 2026-11-04 after PyPI rejected a second pending publisher for `pypi`; ADR-0023 has the job layout.
- `config_version`, the manifest and event schema versions, and the plugin API version are versioned separately.
- `pipeloom.plugins.API_VERSION = (MAJOR, MINOR)`, independent of the package version. MINOR covers additive changes. MAJOR covers breaking protocol changes. The core accepts plugins with the same MAJOR and MINOR not above its own, otherwise it raises PLU001.
- Before 1.0 the plugin API is `0.x` and any minor may break. Plugin API 1.0 ships with pipeloom 1.0.

## Consequences

- Plugin authors can target a plugin API version instead of a package version.
- Lockstep versioning means plugin-only fixes bump every distribution during 0.x.
- At 1.0, `config_version: 1`, the plugin API 1.0, and the manifest, event and run_results schemas v1 are declared stable.

## Amendment (1.0): stability and deprecation policy

From 1.0.0 pipeloom follows Semantic Versioning for the surfaces below. The user-facing summary is [Stability and deprecations](../reference/stability.md).

### What is stable

- CLI command names, flag names and their meanings, and the exit codes 0, 1, 2 and 130.
- Every JSON output that carries a version tag.
- `config_version: 1`. It is the only value accepted through 1.x.
- The artifact schemas `pipeloom.manifest/1`, `pipeloom.run_results/1`, `pipeloom.event/1`, `pipeloom.unit_states/1` and `pipeloom.unit_graph/1`, and the event types, as defined in ADR-0049.
- Plugin API 1.0: the protocols, `PluginSpec` and the capabilities. `pipeloom.testing` is part of the plugin API.
- The meaning of each diagnostic code. Messages and hints are not stable.
- The documented Python API: the `pipeloom` top-level exports and `pipeloom.api`.

Not stable: console text (including log lines and table layout) and anything undocumented, such as private modules and names that start with an underscore.

### What counts as breaking

A breaking change needs a new major version. These are breaking:

- Removing or renaming anything listed above.
- Changing a type, a default or an exit code.
- Adding a value to a closed enum.
- Adding a required method to a plugin protocol.
- Raising a warning to an error.
- Rejecting configuration that was valid before. The exception is a rule that was letting wrong data through: that fix ships in a minor or patch release and is listed under Security or Fixed in the CHANGELOG.

Additive changes ship in a minor release: new commands, flags, optional fields, event types, diagnostic codes and optional protocol members.

### How deprecation works

- The CHANGELOG lists the item under "Deprecated", and the Deprecations table on the stability page gets a row with the item, the version that deprecated it, the replacement and the earliest removal.
- At use time pipeloom emits a warning diagnostic with a code in the new reserved prefix `PLV` (layer "Deprecations"). Python and plugin API members also raise a `DeprecationWarning`.
- A deprecated item keeps working for the rest of the major. It is removed only in the next major, and only after at least one minor release that warned.
- The `PLV` prefix is reserved by this amendment. No code is added now: the first `PLV` code and its case file (`tests/cases/<CODE>.yml`) arrive with the first real deprecation.

### Plugin API and config version

The plugin API becomes `(1, 0)` with pipeloom 1.0. The core accepts a plugin with the same major and a minor not above its own, otherwise it raises PLU001. The exact match that ADR-0017 requires while the major is 0 ends with 1.0. `config_version: 1` is the only accepted value through 1.x.

### Version check

`tools/versions.py check` runs these checks in the CI lint job and in the release workflow:

- The root `[tool.pipeloom]` keys `config-version` and `plugin-api` equal `CONFIG_VERSION` (`pipeloom.config.types`) and `API_VERSION` (`pipeloom.plugins`) in the source (read with `ast`, no import).
- Every member distribution declares the same `plugin-api` and `config-version` as the root.
- Every `PluginSpec` passes `API_VERSION`, not a literal.

### Prereleases

Prereleases such as `1.0.0rc1` are published to PyPI. Version pins between distributions must accept them (`pipeloom~=1.0.0` does not match `1.0.0rc1`), so for a prerelease the members depend on `pipeloom~=1.0.0rc1`, which accepts the rc and the final `1.0.0`. `tools/versions.py` accepts `X.Y.Z` and `X.Y.Z(a|b|rc)N` in `check`, `bump` and `--tag`, and `bump 1.0.0` after `bump 1.0.0rc1` restores the `~=X.Y.0` pins. The release workflow marks a prerelease tag as a GitHub prerelease.

## Amendment (1.1): experimental tier, supported releases, optional protocol members

Decided on 2026-11-20 (roadmap section 6, decision 8). The user-facing summary is [Stability and deprecations](../reference/stability.md).

### Experimental tier

- A feature, flag, config field or artifact field marked experimental is outside the stability guarantee of the 1.0 amendment. Changing or removing it is not breaking.
- The mark appears in the docs, in the CLI help and in the JSON Schema `description` of the field.
- An experimental item is promoted to stable or removed within two minor releases after the one that added it. The CHANGELOG says which.
- At use time pipeloom emits a warning diagnostic with a code in the new reserved prefix `PLW` (layer "Experimental"). A warning never fails a run.
- Same precedent as `PLV`: this amendment reserves the prefix and adds no code, because a code with no emitter would be dead. The first `PLW` code and its case file (`tests/cases/<CODE>.yml`) arrive with the first experimental feature.

### Supported releases

Only the latest minor release gets patch releases. Security fixes go to the latest minor. Older minors are not patched, so users upgrade to the latest minor.

### Optional protocol members

- Nothing is added to `EngineSession` or `StateStore` in 1.x. A new method on those protocols breaks third-party plugins under mypy, and the 1.0 amendment lists a new required method as breaking.
- An optional member lives in its own small `typing.Protocol`, for example `SupportsFilterBetween`. Plugins implement it structurally.
- The core reaches it through a helper that checks for the member and falls back when it is missing, as `pipeloom.state.types.list_runs` does today. The helper is where the default lives.

## Status

Accepted. Amended by [ADR-0017](0017-plugin-spec-and-builtins.md): while the plugin API major is 0 the versions must match exactly. Amended in 1.0: stability and deprecation policy. Amended in 1.1: experimental tier, supported releases, optional protocol members (see above).

## Date

2026-10-29
