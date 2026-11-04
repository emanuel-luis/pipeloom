# ADR-0025: Filesystem tables v2

Architecture: [Section 5, Engine abstraction](../architecture.md)

Amends [ADR-0022](0022-filesystem-table-format.md).

## Context

Merge and replace_partitions need to read the committed table, compute a new one and publish it, and two writers must not both succeed from the same starting point. Format /1 has no lock, does not know which rows a part file holds and keeps no partition values, so replacing a partition would mean reading every file. ADR-0022 left room for a lock file and per-partition files under a new format version.

## Options

- **Keep /1 and rewrite the whole table for every strategy.** No format change, but replace_partitions reads and rewrites data it does not touch.
- **Delta or Iceberg.** Solves it, but is the heavy dependency ADR-0022 rejected for the default catalog.
- **Format /2: per-file row counts and partition values, a commit lock and an expected version.** Small, and /1 tables stay readable.

## Decision

- The commit file declares `"schema": "pipeloom.fs_table/2"`. A reader accepts /1 and /2 and rejects anything else. A /1 table is read as a table whose files have unknown rows and no partition values, and its next commit writes /2. Nothing rewrites old part files.
- `file_info` maps each part file to `FsFile(rows, bytes, partition)`. Partition keys are computed natively, never from a Python round trip: a timestamp is its epoch in nanoseconds, a decimal its text, a float its value with NaN as null, a date or time its text, and rows are grouped by those exact columns. The stored JSON value is a normalized text of the same key: ISO text for timestamps (UTC, nine fractional digits), a decimal without trailing zeros, null for null and NaN. A key therefore keeps matching after a lossless widening (decimal scale, timestamp unit, int to float); float32 to float64 is the one widening that does not. The write guards that the parts add up to the frame's row count. `partition_by` on the commit names the columns the values belong to. When a write changes `partition_by`, the old files keep no partition values and are treated as unknown.
- With `partition_by` set, every strategy writes one part file per distinct partition tuple (null is a value). An empty batch writes one empty file without a partition.
- A commit takes `_pipeloom/commit.lock`, created with `O_CREAT|O_EXCL`, with a 30 second lease. An expired lock is taken over by renaming it away first, the same way the JSON state store does. Under the lock the commit compares the committed version with `expected_version` (0 for a table that does not exist) and publishes with one atomic replace of `commit.json`. A held lock or a version mismatch raises `CommitConflict`, a `TransientError`. There is no wait or retry in the table; the scheduler owns retries.
- Merge and replace_partitions are read-modify-write. The engine reads the committed table through the commit schema (`FsTable.read_aligned`), aligns it and the batch to the final schema, and computes the result with the portable Narwhals code (`merge_frames`, `replace_partition_frames`). Merge writes the result as an overwrite. In 0.3 a merge rewrites the whole table; a per-partition rewrite is a later optimization. Replace_partitions drops the files whose partition is in the batch and adds the batch files in one append commit, without reading data. Files with no partition values are read, filtered and rewritten. `rows_deleted` comes from `file_info.rows`.
- An empty merge batch commits a new version with the same files. An empty replace_partitions batch changes no files. Both record the token. A merge or replace_partitions into a table that does not exist writes the batch as a new table.
- The shared write logic lives in `pipeloom.builtins.fs_write`; each engine supplies a small `FrameOps` object (collect, read a file, align, concat, write, evaluate the update condition in its own dialect).

## Consequences

- Two writers on one table no longer corrupt it: the second one fails with a transient conflict and reruns from the new version.
- Merge cost grows with table size. Overwrite still deletes every `part-*` file the new commit does not list, so a concurrent writer's uncommitted file can be removed; its commit then fails or is discarded.
- A stale lock from a crashed writer blocks commits for up to 30 seconds.
- Partition pruning on read is not implemented; the values are recorded so it can be.
- Splitting a frame filters once per partition (O(partitions x rows)) because Narwhals has no single-pass split; each part is written and released in turn.

## Status

Accepted

## Date

2026-11-05
