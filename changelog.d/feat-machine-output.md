### Added

- Machine-readable diagnostics ([ADR-0018](docs/adr/0018-events-in-0-1.md) amendment 1.2): `--log-format` on `init` and `explain`, so every command has it; an optional `explain_url` on the `diagnostic` event (additive in `pipeloom.event/1`; older events still validate); and `compile --output json`, a new `pipeloom.compile_result/1` document with its published schema.
- `pipeloom init` writes `AGENTS.md` and three skills under `.agents/skills/`, generated from the CLI and the diagnostics catalog, and `pipeloom init --update-agents` regenerates them in an existing project ([ADR-0041](docs/adr/0041-pipeloom-init.md) amendment 1.2). A test fails when a catalog code is missing from the skills.
