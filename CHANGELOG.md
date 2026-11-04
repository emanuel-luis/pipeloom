# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses
[Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

- Plugin API (0, 3), ADR-0024: `EngineSession` gains `filter_since(..., inclusive=)`, `evaluate`, `apply_drops`, `align`, `applied_token`, `unit_scope` and `is_transient`; `cancel` becomes active. `WriteConfig` gains `partition_by`, `update_condition`, `deletes`, `columns` and `commit_meta`; `WriteResult` gains `skipped` and `commit_meta`. `EngineCapabilities.quality` marks engines that implement expectations. Plugins built for (0, 2) fail with PLU001.
- `pipeloom.engine.types`: `DeleteSpec`, `ColumnChange`, `SchemaDiff`, `ExpectationSpec`, `QualityCounts`, `QuarantineMeta`, `DropResult`, `can_widen` and `diff_schemas`. `pipeloom.engine.errors`: `TransientError`, `CommitConflict` and `WriteKeyError`.
- `pipeloom.testing.reference`: a pure-Python reference for quality counts, drops, merge, replace_partitions and align. The memory engine now implements merge, replace_partitions, quality, cancel and the `incremental` materialization on top of it.
- `EngineContract` gains the strategy matrix, idempotent replay per strategy, `align`, `filter_since` bounds, `max_value` types, quality counts and drops against the reference, and cancel. `EngineHarness` gains `expr_cases`, `update_condition` and `slow_relation`.
- Diagnostic PLM206 (write key invalid) is emitted by the runner; PLM205, PLQ302, PLP410, PLP411, PLP412 and PLC510 are reserved in the catalog for 0.3.
- `pipeloom.engine.portable.rewrite_qualified` and an `inclusive` option on `portable.filter_since`.
- `pipeloom-polars`, the first plugin distribution (`plugins/pipeloom-polars/`, install with `pipeloom[polars]`): the polars engine on lazy frames, SQL and views through `pl.SQLContext`, tables on the filesystem catalog with exact token replay. It passes `EngineContract`; see `docs/plugins/polars.md`.
- `pipeloom-pandas` (`plugins/pipeloom-pandas/`, install with `pipeloom[pandas]`): the pandas engine on eager DataFrames with Arrow-backed reads, tables on the filesystem catalog with exact token replay. No SQL models and no views in 0.2 (PLC502, PLC507). It passes `EngineContract`; see `docs/plugins/pandas.md`.
- `tests/fixtures/smoke-pandas`: the smoke project on the pandas target (Python models only); CI runs it from the built wheels.
- uv workspace (`members = ["plugins/*"]`) with one lockfile; the `polars` extra of `pipeloom` pins `pipeloom-polars` to the same version.
- `frame_api: narwhals` is wired in the runner: models receive Narwhals frames and a Narwhals result is converted back. PLC508 still rejects it on a non-portable engine.
- `tools/versions.py` (`check [--tag vX.Y.Z]` and `bump X.Y.Z`) keeps every distribution on one version.
- `tests/fixtures/smoke`: a small project on the polars engine that CI runs from the built wheels.
- CI builds every distribution, smoke tests the core wheel alone and all wheels together, and covers the plugin in typecheck and coverage. The release workflow builds each distribution into `dist/<name>/` and publishes each with its own step. ADR-0023.
- `pipeloom run`: runs the selected models with `-s/--select`, `--exclude`, `--manifest`, `--expect-plan-hash`, `--run-id`, `--full-refresh [true|false]`, `--fail-fast`, `--no-lock`, `--workers` (`PIPELOOM_WORKERS`), `--boundary` and `--warn-error`. Exit codes: 0, 1, 2 and 130.
- Process executor for `boundary: task`: each unit runs as `python -m pipeloom run --select unit:<id>`, its events are relayed to the parent with their original invocation id and sequence, and its logs go to `runs/<run_id>/logs/`.
- Unit mode (`--select unit:<id>`): no lock, no `run.*` events and no results file. The `unit:` selector method and `apply_to_plan` select over a manifest.
- Run console output with one line per unit, a summary, the failure block and the artifacts line.
- Diagnostics PLX103 (plan hash mismatch), PLX105 (run lock held), PLX106 (unit process died without a result), PLX107 (manifest invalid) and PLG306 (unknown unit).
- `pipeloom.testing.contract.engine`: `EngineHarness`, `requires` and `EngineContract`, gated by the capabilities an engine declares. `EventBus.forward` relays events from another process, and a per-invocation run file sink writes `runs/<run_id>/events/<invocation_id>.jsonl`.
- Filesystem table format (`pipeloom.builtins.fs_table`): part files plus an atomically replaced `_pipeloom/commit.json` with a format version, an idempotency token log (last 100), overwrite garbage collection and view definitions. ADR-0022.
- The `filesystem` catalog is implemented, with options `root` and `format` (`parquet`, `csv` or `ndjson`); it works with the polars and pandas engines.
- `pipeloom.engine.portable`: Narwhals helpers (`to_frame_api`, `from_frame_api`, `schema`, `count`, `max_value`, `filter_since`, `logical_dtype`). `narwhals>=2.0` is a core dependency, and an import-linter contract keeps it confined to that module.
- State stores in `pipeloom.state`: `StateKey`, `Watermark`, `RunRecord`, `UnitRecord`, `LockInfo`, `LockHeld` and the `StateStore` protocol, with a JSON codec and a run lock that is a 60 s lease renewed by a heartbeat thread every 20 s.
- Built-in `sqlite` state store (WAL, one connection per call, schema version 1) and `json` directory state store (atomic file replace, `O_EXCL` lock files, stale lock takeover). `sqlite` is local to one machine; `json` is shared.
- The `state` block of a target is bound and validated; without one, `sqlite` at `.pipeloom/state.db` is used. Relative state paths resolve against the project directory.
- PLC503: `boundary: task` with a state store that is not shared.
- `pipeloom.testing.contract.state.StateStoreContract`, a pytest suite state store plugins subclass.
- Import contracts (layers, `pipeloom.testing` isolation, no dataframe or SQL parsing library in the core) checked by `uv run lint-imports` in the `lint` CI job and `make lint`.
- Unit state machine with a checked transition table, and a ready-queue scheduler with skip-downstream, `--fail-fast` and stop requests (not wired to the CLI yet).
- Thread executor and a simulated executor for tests; `unit.queued`, `unit.skipped` and `unit.cancelled` event payloads.
- ADR-0020, the execution model for 0.2.
- Model runner: `pipeloom.runtime.runner.ModelRunner` runs one unit (reads inputs, runs a SQL or Python body, writes a table or creates a view, records the attempt). `ModelContext`, `CallableLoader` (puts `python_paths` on `sys.path` for the run only), `EphemeralMemo` (an ephemeral model shared by several units is computed once) and `ErrorInfo` with a trace excerpt and a transient or permanent category.
- `Project.run(RunOptions, bus)` and `pipeloom.runtime.run.execute_run` run the selected units on the thread executor, take the run lock, record the run in the state store and finalize. Reusing a run id continues the run.
- Run artifacts in `pipeloom.artifacts`: `RunPaths`, `write_json_atomic`, `run_results.json` (`pipeloom.run_results/1`, one row per persisted model) and finalize, which copies `manifest.json` and `run_results.json` to the artifacts root (`target` by default).
- Events `run.started`, `run.compiled`, `run.completed`, `unit.started`, `unit.succeeded`, `unit.failed`, `model.input_read`, `model.written` and `model.log`.
- Codes PLX101 (model raised), PLM201 (write failed), PLM204 (input read failed), PLC509 (materialization unsupported by the engine) and PLP409 (a view must be plain SQL).
- `MemoryCatalog(root=...)` and the memory engine option `root` keep tables as JSON files, so tests can share them between processes. The memory engine supports views.
- ADR-0021, the plugin API (0, 2).

### Changed

- Plugin API version is (0, 2): `Engine.version`, `Engine.session(ctx, *, catalog)`, and `EngineSession.relation`, `persist` and `create_view`. Plugins built for (0, 1) fail with PLU001.
- A model with a materialization the engine does not declare (`incremental`, `intermediate`) is rejected with PLC509.
- The import-linter layers include `pipeloom.artifacts` next to `pipeloom.state`, and imports under `TYPE_CHECKING` are ignored.

## [0.1.0] - 2026-11-04

First functional release: configuration, layered validation, the dependency graph, selectors,
the plan and manifest, the event bus and the CLI. Running plans comes in 0.2.

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
- Selector engine: space union, comma intersection, `N+` and `+N` graph operators, and the `name`, `tag`, `path` and `materialized` methods (PLG303 to PLG305).
- `Project.resolve()` in the API runs load, bind, resolve, L2 and graph.
- `pipeloom ls` (`--output text|json`) and `pipeloom graph` (`--output text|dot|mermaid|json`), both with repeatable `-s/--select` and `--exclude`. A malformed selector (PLG304, PLG305) exits 2; a selector that matches nothing (PLG303) is a warning.
- Typed events (`pipeloom.event/1`) and a synchronous event bus with failure budgets, redaction and context-manager close; `console`, `jsonl` and buffered sinks; code PLE401 (ADR-0018).
- `--log-format text|json` (`PIPELOOM_LOG_FORMAT`) on every command; `console` and `jsonl` sinks configurable under `events.sinks`.
- `pipeloom.testing.RecordingSink` and `FakeClock`.
- Plan model (`pipeloom.plan`): frozen `Plan`, `PlanNode`, `ExecutionUnit`; one unit per non-ephemeral model with its inlined ephemeral ancestors, identical for both boundaries; target relations from the bound naming strategy.
- Manifest `pipeloom.manifest/1` with `plan_hash` and per-node checksums (CRLF read as LF), `write_manifest`, `read_manifest` and `compute_plan_hash`.
- L4 plan semantics (PLP401 to PLP408), L5 capability checks read from the engine spec without importing it (PLC501, PLC502, PLC504 to PLC508) and L6 static checks of Python callables (PLK601, PLK602, PLK604).
- `Project.validate()` and `Project.compile(selection, boundary)` in the API; `pipeloom validate` now runs L0 to L6.
- `pipeloom compile` with the selection options, `--boundary`, `--out` and `--warn-error`; it emits `plan.compiled` and `artifact.written`.
- Generated docs: error codes, JSON Schemas under `schema/v1/`, the command-line reference, a quickstart and the schema guide.

### Changed

- `pipeloom validate` binds the selected target and checks plugin options, and accepts `--target` and `--plugin`.
- Diagnostics and the run summary go to stderr through the event bus; the summary line now reads `<command> finished in N ms | exit C | E error(s), W warning(s)`. `rich>=13.7` is a new dependency.
- PLS104 matches secret words in key segments (`max_tokens` is no longer flagged) and ignores booleans.
- The CLI needs `click>=8.2`; runtime dependencies now include pydantic and PyYAML.
- CI tests Python 3.11 to 3.14 on Ubuntu, Windows and macOS now that the repository is public.
- PLE401 names the exception type and the sink's configured path instead of the OS error text.

## [0.0.2] - 2026-10-29

0.0.1 was never published: PyPI rejects that file name because an unrelated, deleted project
once used it.

### Added

- Package skeleton with `pipeloom --version` and `__version__`.
- CI, release and docs workflows.
- Apache-2.0 license, contributing guide, code of conduct, security policy and governance.

[Unreleased]: https://github.com/emanuel-luis/pipeloom/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/emanuel-luis/pipeloom/compare/v0.0.2...v0.1.0
[0.0.2]: https://github.com/emanuel-luis/pipeloom/releases/tag/v0.0.2
