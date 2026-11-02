# ADR-0015: Python 3.11 or newer; click CLI; pydantic v2; hatchling; mypy strict

Architecture: [Section 17, ADR-0015 text, and section 18](../architecture.md#17-adr-list-and-diagrams)

## Context

A new public project started in late 2026 must pick a Python floor and a base toolchain. Python 3.10 reaches end of life in October 2026.

## Options

- **Python floor:** 3.10 (reaches end of life in October 2026) or 3.11.
- **CLI:** click (the 0.0.2 skeleton depends on `click>=8.1`).
- **Config models:** pydantic v2, which also exports the JSON Schema (section 4).
- **Build backend:** hatchling.
- **Types:** mypy in strict mode.

## Decision

Python 3.11 or newer, click for the CLI, pydantic v2 for config models, hatchling as the build backend, and mypy `--strict` in CI. CI runs Python 3.11 to 3.14 on Ubuntu, Windows and macOS.

## Consequences

- Code may use 3.11 features, and users on 3.10 cannot install pipeloom.
- The package has one runtime dependency (`click`) at 0.0.2.
- `pyproject.toml` and CI follow this ADR.
- While the repo was private, CI ran one Windows and one macOS job on 3.12 only, to save macOS minutes. The full matrix returned when the repo went public on 2026-11-02.

## Status

Accepted

## Date

2026-10-29
