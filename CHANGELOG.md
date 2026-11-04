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
- Plugin system: `pipeloom.plugins` with `PluginSpec`, `PluginRegistry`, `API_VERSION` and `check_api`; entry-point discovery that reads metadata only, `--plugin module:attr`, `PIPELOOM_DISABLE_PLUGIN_AUTOLOAD`, and codes PLU001 to PLU003 (ADR-0017).
- Built-in plugins from a lazy in-code table: `memory` engine and catalog, `none`, `prefix` and `per_user` naming, `env` secret provider, and the `filesystem` catalog spec (implementation in 0.2).
- Engine contracts: `pipeloom.engine` types and protocols, `RuntimeContext`, and `pipeloom.testing.memory` (MemoryEngine and MemoryCatalog on sqlite3).
- Target binding: `--target` (`PIPELOOM_TARGET`), `default_target`, runtime merge and plugin option validation, with codes PLR204 and PLR207. `Project.bind()` in the API.
- `pipeloom schema export` writes JSON Schemas for `pipeloom.yml`, model files and `_defaults.yml`, with plugin options as `oneOf` branches.
- `pipeloom plugins list` with `--group`, `--output`, `--check` and `--plugin`.
- Model settings resolution: `_defaults.yml` cascades from the model's folder up to its model_paths root, then `defaults.models`; mappings merge deeply, lists replace and `tags` is a deduplicated union. Unique ids `model.<project>.<name>` and `source.<project>.<name>`.
- L2 reference checks (PLR201, PLR202, PLR203, PLR205) with did-you-mean hints.
- Dependency graph with deterministic topological order, depth-limited ancestors and descendants, and cycle detection (PLG301, PLG302).
- Selector engine: space union, comma intersection, `N+` and `+N` graph operators, and the `name`, `tag`, `path` and `materialized` methods (PLG303 to PLG305). No CLI flags yet.
- `Project.resolve()` in the API runs load, bind, resolve, L2 and graph.
- Typed events (`pipeloom.event/1`) and a synchronous event bus with failure budgets, redaction and context-manager close; `console`, `jsonl` and buffered sinks; code PLE401 (ADR-0018).
- `--log-format text|json` (`PIPELOOM_LOG_FORMAT`) on every command; `console` and `jsonl` sinks configurable under `events.sinks`.
- `pipeloom.testing.RecordingSink` and `FakeClock`.

### Changed

- `pipeloom validate` binds the selected target and checks plugin options, and accepts `--target` and `--plugin`.
- Diagnostics and the run summary go to stderr through the event bus; the summary line now reads `<command> finished in N ms | exit C | E error(s), W warning(s)`. `rich>=13.7` is a new dependency.
- PLS104 matches secret words in key segments (`max_tokens` is no longer flagged) and ignores booleans.
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
