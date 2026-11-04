"""Write strategies on filesystem tables, shared by the dataframe engines.

An engine supplies `FrameOps` (how to read, align, concatenate, write and evaluate a condition on
its own eager frames); this module does the rest: per-partition part files, merge and
replace_partitions as read-modify-write, and the commit with its expected version.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import replace
from pathlib import Path
from typing import Any, Protocol

from pipeloom.builtins.fs_table import FsCommit, FsFile, FsTable
from pipeloom.engine import portable
from pipeloom.engine.errors import WriteKeyError
from pipeloom.engine.types import Schema, WriteConfig, WriteResult


class FrameOps(Protocol):
    """The engine's side. Frames are native (polars or pandas); `collect` makes one eager."""

    def collect(self, native: Any) -> Any: ...

    def read_file(self, path: Path, fmt: str, schema: Schema) -> Any: ...

    def align(self, native: Any, schema: Schema) -> Any: ...

    def concat(self, frames: Sequence[Any]) -> Any: ...

    def write(self, native: Any, fmt: str, path: Path) -> None: ...

    def condition(self, native: Any, condition: str) -> Any:
        """A boolean native series: the rewritten `update_condition` on `native`."""
        ...


def read_table(table: FsTable, ops: FrameOps, commit: FsCommit | None = None) -> Any:
    """The committed data of a table, every file aligned to the commit schema."""
    return table.read_aligned(ops.read_file, ops.align, ops.concat, commit=commit)


def _union(first: Schema, second: Schema) -> Schema:
    names = {c.name for c in first}
    return (*first, *(c for c in second if c.name not in names))


class _Writer:
    def __init__(self, table: FsTable, ops: FrameOps, fmt: str, ext: str) -> None:
        self.table, self.ops, self.fmt, self.ext = table, ops, fmt, ext
        self.files: list[Path] = []
        self.info: dict[str, FsFile] = {}

    def put(self, frame: Any, partition: tuple[Any, ...] | None) -> None:
        path = self.table.new_file(self.ext)
        self.files.append(path)
        self.ops.write(frame, self.fmt, path)
        self.info[path.name] = FsFile(portable.count(frame), None, partition)

    def put_partitioned(self, frame: Any, partition_by: Sequence[str]) -> None:
        """One file per distinct partition tuple; one (empty) file when there are no rows."""
        parts = portable.split_partitions(frame, partition_by) if partition_by else []
        if not parts:
            self.put(frame, None)
        for key, part in parts:
            self.put(part, key)


def materialize(
    table: FsTable, ops: FrameOps, fmt: str, ext: str, native: Any, cfg: WriteConfig, token: str
) -> WriteResult:
    """Write `native` into `table` with `cfg.strategy` and record `token`. A replay is skipped."""
    replay = table.applied(token)
    if replay is not None:
        return replace(replay, skipped=True)
    writer = _Writer(table, ops, fmt, ext)
    try:
        previous = table.current()
        if previous is not None and previous.kind == "view":
            raise ValueError(f"{table.path.name} is a view; cannot write {cfg.strategy} to it")
        batch = ops.collect(native)
        if cfg.columns is not None:
            batch = ops.align(batch, cfg.columns)
        if cfg.partition_by:
            portable.check_columns(batch, cfg.partition_by, "partition_by")
        if cfg.strategy in ("append", "overwrite"):
            return _plain(writer, batch, cfg, token)
        if cfg.strategy == "merge":
            return _merge(writer, previous, batch, cfg, token)
        if cfg.strategy == "replace_partitions":
            return _replace_partitions(writer, previous, batch, cfg, token)
        raise ValueError(f"filesystem tables do not support strategy '{cfg.strategy}'")
    except BaseException:
        table.discard(writer.files)
        raise


def _plain(w: _Writer, batch: Any, cfg: WriteConfig, token: str) -> WriteResult:
    n = portable.count(batch)
    w.put_partitioned(batch, cfg.partition_by)
    commit = w.table.commit(
        mode="append" if cfg.strategy == "append" else "overwrite",
        files=w.files,
        columns=portable.schema(batch),
        rows=n,
        token=token,
        result=WriteResult(rows_inserted=n, rows_written=n, commit_meta=dict(cfg.commit_meta)),
        file_info=w.info,
        partition_by=cfg.partition_by,
    )
    return commit.result


def _final_schema(previous: FsCommit, batch: Any, cfg: WriteConfig) -> Schema:
    return cfg.columns or _union(previous.columns, portable.schema(batch))


def _merge(
    w: _Writer, previous: FsCommit | None, batch: Any, cfg: WriteConfig, token: str
) -> WriteResult:
    if not cfg.unique_key:
        raise WriteKeyError("merge needs a unique_key")
    portable.check_columns(batch, cfg.unique_key, "unique_key")
    n = portable.count(batch)
    meta = dict(cfg.commit_meta)
    if previous is None:
        columns = portable.schema(batch)
        current = portable.empty_like(batch)
    else:
        columns = _final_schema(previous, batch, cfg)
        if n == 0:
            commit = w.table.commit(
                mode="append",
                files=[],
                columns=columns,
                rows=0,
                token=token,
                result=WriteResult(0, 0, 0, 0, commit_meta=meta),
                partition_by=cfg.partition_by or previous.partition_by,
                expected_version=previous.version,
            )
            return commit.result
        current = w.ops.align(w.ops.collect(read_table(w.table, w.ops, previous)), columns)
        batch = w.ops.align(batch, columns)
    portable.check_columns(current, cfg.unique_key, "unique_key")
    outcome = portable.merge_frames(current, batch, cfg, condition_hook=w.ops.condition)
    w.put_partitioned(outcome.frame, cfg.partition_by)
    soft = outcome.deleted if cfg.deletes is not None and cfg.deletes.mode == "soft" else 0
    rows = portable.count(outcome.frame)
    commit = w.table.commit(
        mode="overwrite",
        files=w.files,
        columns=portable.schema(outcome.frame),
        rows=rows,
        token=token,
        result=WriteResult(
            rows_written=outcome.inserted + outcome.updated + soft,
            rows_inserted=outcome.inserted,
            rows_updated=outcome.updated,
            rows_deleted=outcome.deleted,
            commit_meta=meta,
        ),
        file_info=w.info,
        partition_by=cfg.partition_by,
        expected_version=0 if previous is None else previous.version,
    )
    return commit.result


def _replace_partitions(
    w: _Writer, previous: FsCommit | None, batch: Any, cfg: WriteConfig, token: str
) -> WriteResult:
    if not cfg.partition_by:
        raise WriteKeyError("replace_partitions needs partition_by")
    n = portable.count(batch)
    meta = dict(cfg.commit_meta)
    if previous is None:
        w.put_partitioned(batch, cfg.partition_by)
        commit = w.table.commit(
            mode="overwrite",
            files=w.files,
            columns=portable.schema(batch),
            rows=n,
            token=token,
            result=WriteResult(n, n, 0, 0, commit_meta=meta),
            file_info=w.info,
            partition_by=cfg.partition_by,
            expected_version=0,
        )
        return commit.result
    columns = _final_schema(previous, batch, cfg)
    if n == 0:  # nothing replaces anything: record the token only
        commit = w.table.commit(
            mode="append",
            files=[],
            columns=columns,
            rows=0,
            token=token,
            result=WriteResult(0, 0, 0, 0, commit_meta=meta),
            partition_by=cfg.partition_by,
            expected_version=previous.version,
        )
        return commit.result
    batch = w.ops.align(batch, columns)
    wanted = portable.distinct_partitions(batch, cfg.partition_by)
    keys = {portable.partition_key(v) for v in wanted}
    same = previous.partition_by == cfg.partition_by
    drop: list[str] = []
    deleted = removed = added = 0
    for name in previous.files:
        info = previous.info(name)
        if same and info.partition is not None and info.rows is not None:
            if info.partition in keys:
                drop.append(name)
                deleted += info.rows
                removed += info.rows
            continue
        # No partition info (a /1 table, or other partition columns): read, filter, rewrite.
        frame = w.ops.collect(
            w.ops.align(
                w.ops.read_file(w.table.path / name, previous.format or "", previous.columns),
                previous.columns,
            )
        )
        kept, gone = portable.drop_partitions(frame, wanted, cfg.partition_by)
        if not gone:
            continue
        drop.append(name)
        total = portable.count(frame)
        deleted += gone
        removed += total
        if total - gone:
            w.put_partitioned(w.ops.align(kept, columns), cfg.partition_by)
            added += total - gone
    w.put_partitioned(batch, cfg.partition_by)
    commit = w.table.commit(
        mode="append",
        files=w.files,
        columns=columns,
        rows=n + added,
        token=token,
        result=WriteResult(n, n, 0, deleted, commit_meta=meta),
        file_info=w.info,
        drop=drop,
        removed_rows=removed,
        partition_by=cfg.partition_by,
        expected_version=previous.version,
    )
    return commit.result


__all__ = ["FrameOps", "materialize", "read_table"]
