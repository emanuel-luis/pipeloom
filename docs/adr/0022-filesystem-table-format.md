# ADR-0022: Filesystem table format

Architecture: [Section 5, Engine abstraction](../architecture.md)

Amends [ADR-0008](0008-incremental-and-state.md) and [ADR-0013](0013-run-identity-and-reruns.md).

## Context

The polars and pandas engines write files, and a rerun of the same run id must not write twice (ADR-0013). Plain files give no atomic multi-file publish and no place to remember which idempotency tokens were applied. Views also need a home on a catalog that has no SQL engine of its own.

## Options

- **Glob the table directory.** Simple, but a half-written part file or a crash between two writes is visible to readers.
- **Adopt Delta or Iceberg.** Solves it, but adds a heavy dependency for a default catalog.
- **A small commit file next to the parts.** One atomic rename publishes a new file set, and the same file holds the token log.

## Decision

- A table is a directory `<root>/<schema>/<name>/` with part files `part-<uuid12>.<ext>` and `_pipeloom/commit.json`. The catalog name of a target is ignored.
- The commit file is written to `commit.json.<uuid>.tmp` and moved with `os.replace` (retried on Windows). Readers use the files the commit lists, never a directory listing. A part file no commit lists is invisible.
- The commit carries a format version (`"schema": "pipeloom.fs_table/1"`; a reader rejects any other value), a commit counter, the file names, the logical columns, row and byte totals and the last 100 applied idempotency tokens with their write results. The stored result is the writer's result with `target_version` set to the commit version (and `bytes_written` filled from the part files when the writer left it empty), so a replay is identical to the first write. A write with a token already in the log returns the stored result and writes nothing. This is the only catalog that offers idempotent writes; polars and pandas declare `idempotent_writes=True` because they write through it.
- Overwrite commits the new files and then deletes `part-*` files not in the commit, best effort. Append lists the old files plus the new ones. An overwrite with no rows still writes one file, so the schema survives in `columns`.
- A view is a commit of kind `view` with no files and a `ViewDefinition` (the SQL and its inputs). Catalog inputs store the `TargetRef`, never a path. Reading a view runs the SQL again. Only SQL models become views (PLP409).
- There is no commit lock: one model writes a target at a time. Merge and replace_partitions can add a lock file and per-partition files under a new format version without breaking readers of version 1.
- Narwhals enters the core as a dependency used by `pipeloom.engine.portable` alone, through `narwhals.stable.v2`. An import-linter contract keeps every other module from importing it.

## Consequences

- External tools that glob a table directory may see part files of an unfinished write; they should read through the commit file.
- `os.replace` is weak on network file systems, and an orphan part file from a crash survives until the next overwrite.
- A schema or token change that older readers cannot parse needs a new format version.

## Status

Accepted. Amended by [ADR-0040](0040-per-model-catalog.md): a target's catalog is a leading directory, `<root>/<catalog>/<schema>/<name>/`.

## Date

2026-11-04
