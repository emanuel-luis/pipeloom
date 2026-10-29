# ADR-0003: Model authoring: YAML plus Python or SQL, declared inputs

Architecture: [Section 4, Configuration](../architecture.md#4-configuration)

## Context

Declared dependencies (lesson L2) and YAML validation (L14) are core principles. Users still need a real transformation language.

## Options

- **YAML only.** Easy to validate, but too weak for transformations.
- **YAML plus Python.** Full power, but SQL users must wrap queries in code.
- **YAML plus SQL.** Familiar for SQL users, but no escape for logic SQL cannot express.
- **YAML plus both.** Covers both audiences. Needs a rule for how inputs reach the body.

## Decision

YAML plus both. YAML declares every model. The body is either a Python callable or a SQL body. The `inputs` map binds local aliases to `ref` or `source`.

- A Python callable is called as `build(ctx, **inputs)`. Each alias becomes a keyword argument holding a relation.
- SQL refers to aliases as table names. The engine registers them as named relations for that query only.
- At compile time pipeloom imports nothing from the project. Validation layer L6 parses the callable's file with `ast` to check that the function exists and that its parameters equal `ctx` plus the input aliases.
- Opt-in SQL table-reference checking through the sqlglot extra warns on mismatch.

## Consequences

- Graphs are cheap to build, so Airflow can parse DAG files safely.
- No Jinja templating.
- Reads of undeclared tables through `ctx.engine.native` are a documented escape hatch outside lineage.

## Status

Accepted

## Date

2026-10-29
