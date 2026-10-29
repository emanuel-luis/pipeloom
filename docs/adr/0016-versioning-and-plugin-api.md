# ADR-0016: Versioning, release, plugin API version

Architecture: [Section 15 (Release) and section 6 (Plugin API version)](../architecture.md#15-testing-ci-release-docs)

## Context

Several distributions ship from one repository, and several contracts evolve at different speeds: the package, the plugin protocol, the config format and the manifest, event and run_results schemas.

## Options

- **Lockstep versions** for first-party distributions during 0.x, with one tag. Simple releases. Plugin-only fixes still bump every distribution.
- **Independent versions per distribution.** Precise, but more release work and a larger compatibility matrix while the API is moving.

## Decision

- SemVer per distribution, with lockstep versions for first-party distributions during 0.x and one tag `vX.Y.Z`.
- The tag workflow checks that the tag equals the version, builds, then publishes to PyPI by trusted publishing through the protected `pypi` environment, then creates the GitHub release with notes from `CHANGELOG.md` (Keep a Changelog).
- `config_version`, the manifest and event schema versions, and the plugin API version are versioned separately.
- `pipeloom.plugins.API_VERSION = (MAJOR, MINOR)`, independent of the package version. MINOR covers additive changes. MAJOR covers breaking protocol changes. The core accepts plugins with the same MAJOR and MINOR not above its own, otherwise it raises PLU001.
- Before 1.0 the plugin API is `0.x` and any minor may break. Plugin API 1.0 ships with pipeloom 1.0.

## Consequences

- Plugin authors can target a plugin API version instead of a package version.
- Lockstep versioning means plugin-only fixes bump every distribution during 0.x.
- At 1.0, `config_version: 1`, the plugin API 1.0, and the manifest, event and run_results schemas v1 are declared stable.

## Status

Accepted

## Date

2026-10-29
