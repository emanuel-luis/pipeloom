# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses
[Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

- Diagnostics package with the full error-code catalog (active and reserved codes), text and JSON rendering.
- YAML loader with YAML 1.2 core scalars, source marks, duplicate-key detection, merge keys and an alias expansion limit (ADR-0019).
- `${env:...}` and `${var:...}` interpolation, and a secret-like field check (PLS104).
- Pydantic configuration models for `pipeloom.yml`, model files and `_defaults.yml`, with schema errors mapped to file locations and did-you-mean hints.
- `pipeloom validate` with `--project-dir` (`PIPELOOM_PROJECT_DIR`), `--vars` and `--warn-error`; exit codes 0, 1, 2 and 130.
- Python API: `pipeloom.api.Project` and `ProjectOptions`; `pipeloom.platform` clock and identity seams.

### Changed

- The CLI needs `click>=8.2`; runtime dependencies now include pydantic and PyYAML.
- CI tests Python 3.11 to 3.14 on Ubuntu, Windows and macOS now that the repository is public.

## [0.0.2] - 2026-10-29

0.0.1 was never published: PyPI rejects that file name because an unrelated, deleted project
once used it.

### Added

- Package skeleton with `pipeloom --version` and `__version__`.
- CI, release and docs workflows.
- Apache-2.0 license, contributing guide, code of conduct, security policy and governance.

[Unreleased]: https://github.com/emanuel-luis/pipeloom/compare/v0.0.2...HEAD
[0.0.2]: https://github.com/emanuel-luis/pipeloom/releases/tag/v0.0.2
