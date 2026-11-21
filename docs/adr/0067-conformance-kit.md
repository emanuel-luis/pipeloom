# ADR-0067: Conformance kit as an opt-in pytest plugin

Architecture: [Section 3, Architecture overview](../architecture.md)

## Context

`pipeloom.testing` ships the engine and state store contract suites, and the in-tree plugins subclass them in their own tests. A third-party engine author could do the same, but nothing named the supported entry point, nothing printed which version of the suite ran, and the capability declarations were only visible in code. The 1.1 roadmap asks for a kit that an outside project can run in CI.

## Options

- **A separate distribution** (`pipeloom-conformance`). Keeps pytest out of the core, but pytest is already imported by `pipeloom.testing.contract`, which nothing outside tests imports, and a second distribution has to be released and kept in lockstep.
- **A `pytest11` entry point in the core.** No flag, but the plugin would load in every pytest run of every user that has pipeloom installed.
- **An opt-in plugin module in the core**, loaded with `-p`.

## Decision

- `pipeloom.testing.pytest_plugin` is a pytest plugin loaded only with `-p`. pipeloom declares no `pytest11` entry point and does not depend on pytest: the module is imported only by a project that already runs pytest. It sits inside `pipeloom.testing`, which the import-linter contracts already isolate, so no layer or isolation rule changes.
- `--pipeloom-harness MODULE:CALLABLE` names a function that takes a `tmp_path` and returns an `EngineHarness`, the value the in-tree `engine_factory` fixtures already return. When it is given, the plugin replaces the collection arguments with `pipeloom.testing.kit`, which binds `EngineContract` to that function. A bad value is a pytest usage error.
- `KIT_VERSION` (`pipeloom.testing.contract`) is printed in the report header with the pipeloom version. The major number changes when a cell changes meaning or `EngineHarness` gains a required field; the minor number changes when cells are added.
- Subclassing `EngineContract` and `StateStoreContract` stays supported unchanged. The plugin and the classes are on the stability list.
- The capability matrix is generated at docs-build time by `tools/docs_hooks.py` from the specs of the built-ins and the installed entry points. The core gains no code for it and imports no plugin.
- CI builds the wheels, installs only pipeloom and pipeloom-polars in a fresh venv and runs the kit from a directory outside the repository.

## Consequences

- An engine author needs a harness function and pytest, and nothing else from this repository.
- Adding cells to the contract is a kit minor and can turn a green third-party build red, which is why the version is in the header and the stability page says to pin it.
- Only the engine suite has a plugin option. A state store option can follow the same shape.
