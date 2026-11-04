"""Filesystem table format: part files plus an atomically replaced commit file.

Layout: `<table>/part-<uuid12>.<ext>`, `<table>/_pipeloom/commit.json` and, during a commit,
`<table>/_pipeloom/commit.lock`. A part file is invisible until a commit lists it; readers go
through the commit, never a directory glob. Format /2 adds per-file row counts and partition
values; /1 tables stay readable and become /2 on their next commit.
"""

from __future__ import annotations

import json
import os
import time
import uuid
from collections.abc import Callable, Collection, Iterator, Mapping, Sequence
from contextlib import contextmanager, suppress
from dataclasses import asdict, dataclass, field, replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Final, Literal, TypeVar

from pipeloom.diagnostics import JsonValue
from pipeloom.engine.errors import CommitConflict
from pipeloom.engine.types import Column, ReadSpec, Schema, TargetRef, WriteResult
from pipeloom.platform import Clock

FS_TABLE_SCHEMA: Final = "pipeloom.fs_table/2"
"""Format version written in every commit file."""
READABLE_SCHEMAS: Final = frozenset({"pipeloom.fs_table/1", FS_TABLE_SCHEMA})
"""Format versions a reader accepts; any other value is rejected."""
TOKEN_LOG_SIZE: Final = 100
COMMIT_PATH: Final = Path("_pipeloom") / "commit.json"
LOCK_PATH: Final = Path("_pipeloom") / "commit.lock"
LOCK_LEASE_SECONDS: Final = 30

F = TypeVar("F")


@dataclass(frozen=True)
class FsFile:
    """What a commit knows about one part file. None means unknown (a /1 table)."""

    rows: int | None = None
    bytes: int | None = None
    partition: tuple[JsonValue, ...] | None = None
    """The encoded partition values, in `FsCommit.partition_by` order."""


@dataclass(frozen=True)
class TokenRecord:
    token: str
    result: WriteResult
    committed_at: str


@dataclass(frozen=True)
class ViewDefinition:
    query: str
    inputs: Mapping[str, ReadSpec] = field(default_factory=dict)
    """Catalog inputs carry the TargetRef, never a path."""


@dataclass(frozen=True)
class FsCommit:
    version: int
    kind: Literal["table", "view"]
    format: str | None
    files: tuple[str, ...]
    """Names relative to the table directory."""
    columns: Schema
    rows: int | None
    bytes: int | None
    view: ViewDefinition | None
    tokens: tuple[TokenRecord, ...]
    committed_at: str
    file_info: Mapping[str, FsFile] = field(default_factory=dict)
    partition_by: tuple[str, ...] = ()

    def info(self, name: str) -> FsFile:
        return self.file_info.get(name, FsFile())

    @property
    def result(self) -> WriteResult:
        """The write result this commit stored for its token (the last record)."""
        return self.tokens[-1].result


def _read_spec_to_json(spec: ReadSpec) -> dict[str, Any]:
    return asdict(spec)


def _read_spec_from_json(d: Mapping[str, Any]) -> ReadSpec:
    table = d.get("table")
    return ReadSpec(
        d["format"],
        d.get("location"),
        TargetRef(**table) if table else None,
        dict(d.get("options") or {}),
    )


def _to_json(c: FsCommit) -> dict[str, Any]:
    return {
        "schema": FS_TABLE_SCHEMA,
        "version": c.version,
        "kind": c.kind,
        "format": c.format,
        "files": list(c.files),
        "columns": [asdict(col) for col in c.columns],
        "rows": c.rows,
        "bytes": c.bytes,
        "view": None
        if c.view is None
        else {
            "query": c.view.query,
            "inputs": {k: _read_spec_to_json(v) for k, v in c.view.inputs.items()},
        },
        "tokens": [
            {"token": t.token, "result": asdict(t.result), "committed_at": t.committed_at}
            for t in c.tokens
        ],
        "committed_at": c.committed_at,
        "partition_by": list(c.partition_by),
        "file_info": {
            n: {"rows": i.rows, "bytes": i.bytes, "partition": i.partition}
            for n, i in c.file_info.items()
        },
    }


def _from_json(d: Mapping[str, Any]) -> FsCommit:
    if d.get("schema") not in READABLE_SCHEMAS:
        raise ValueError(f"unsupported filesystem table format {d.get('schema')!r}")
    view = d.get("view")
    return FsCommit(
        version=d["version"],
        kind=d["kind"],
        format=d["format"],
        files=tuple(d["files"]),
        columns=tuple(Column(**c) for c in d["columns"]),
        rows=d["rows"],
        bytes=d["bytes"],
        view=None
        if view is None
        else ViewDefinition(
            view["query"], {k: _read_spec_from_json(v) for k, v in view["inputs"].items()}
        ),
        tokens=tuple(
            TokenRecord(t["token"], WriteResult(**t["result"]), t["committed_at"])
            for t in d["tokens"]
        ),
        committed_at=d["committed_at"],
        file_info={
            n: FsFile(
                i["rows"], i["bytes"], None if i["partition"] is None else tuple(i["partition"])
            )
            for n, i in (d.get("file_info") or {}).items()
        },
        partition_by=tuple(d.get("partition_by") or ()),
    )


def _replace(src: Path, dst: Path) -> None:
    """`os.replace`, retried: Windows refuses while a reader holds the target open."""
    for attempt in range(5):
        try:
            os.replace(src, dst)
            return
        except PermissionError:
            if attempt == 4:
                raise
            time.sleep(0.05 * (attempt + 1))


def _lock_text(clock: Clock) -> str:
    now = clock.now()
    body = {
        "owner": uuid.uuid4().hex,
        "acquired_at": now.isoformat(),
        "expires_at": (now + timedelta(seconds=LOCK_LEASE_SECONDS)).isoformat(),
    }
    return json.dumps(body, sort_keys=True) + "\n"


def _read_lock(path: Path) -> dict[str, Any] | None:
    """The lock record; None when the file is gone. A file its writer never filled ages by mtime."""
    for _ in range(5):
        try:
            text = path.read_text(encoding="utf-8")
        except FileNotFoundError:
            return None
        try:
            data = json.loads(text)
        except ValueError:
            time.sleep(0.02)
            continue
        if isinstance(data, dict):
            return data
    try:
        mtime = datetime.fromtimestamp(path.stat().st_mtime, UTC)
    except FileNotFoundError:
        return None
    return {
        "owner": "unknown",
        "expires_at": (mtime + timedelta(seconds=LOCK_LEASE_SECONDS)).isoformat(),
    }


class FsTable:
    """One table directory.

    A commit takes `_pipeloom/commit.lock` (30 s lease, taken over once expired), compares the
    table version with `expected_version` and publishes with one atomic replace of `commit.json`.
    A held lock or a version mismatch raises `CommitConflict` (transient: the caller reruns the
    unit). ponytail: no wait or retry here; the retry policy belongs to the scheduler.
    """

    def __init__(self, path: Path, *, clock: Clock) -> None:
        self.path = path
        self._clock = clock

    def current(self) -> FsCommit | None:
        try:
            text = (self.path / COMMIT_PATH).read_text(encoding="utf-8")
        except FileNotFoundError:
            return None
        return _from_json(json.loads(text))

    def files(self) -> list[Path]:
        """Committed part files only."""
        commit = self.current()
        return [] if commit is None else [self.path / name for name in commit.files]

    def read_aligned(
        self,
        read: Callable[[Path, str, Schema], F],
        align: Callable[[F, Schema], F],
        concat: Callable[[Sequence[F]], F],
        *,
        commit: FsCommit | None = None,
    ) -> F:
        """Every committed part file, read, aligned to the commit schema and concatenated.

        Old files never get rewritten for a schema change: a column they lack reads as null.
        """
        commit = commit or self.current()
        if commit is None or commit.kind != "table" or commit.format is None or not commit.files:
            raise FileNotFoundError(f"{self.path} has no committed data files")
        fmt = commit.format
        return concat(
            [align(read(self.path / n, fmt, commit.columns), commit.columns) for n in commit.files]
        )

    def new_file(self, ext: str) -> Path:
        """A path for a part file. It stays invisible until a commit lists it."""
        self.path.mkdir(parents=True, exist_ok=True)
        return self.path / f"part-{uuid.uuid4().hex[:12]}.{ext.lstrip('.')}"

    def applied(self, token: str) -> WriteResult | None:
        commit = self.current()
        if commit is not None:
            for record in commit.tokens:
                if record.token == token:
                    return record.result
        return None

    @contextmanager
    def _locked(self) -> Iterator[None]:
        path = self.path / LOCK_PATH
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = _lock_text(self._clock)
        owner = json.loads(payload)["owner"]
        while True:
            try:
                fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            except FileExistsError:
                pass
            else:
                with os.fdopen(fd, "wb") as fh:
                    fh.write(payload.encode("utf-8"))
                break
            held = _read_lock(path)
            if held is None:
                continue  # released between our create and our read
            if datetime.fromisoformat(held["expires_at"]) > self._clock.now():
                raise CommitConflict(f"{self.path.name} is being committed by another writer")
            stale = path.with_name(f"{path.name}.stale-{uuid.uuid4().hex}")
            try:
                os.rename(path, stale)
            except FileNotFoundError:
                continue  # another contender took it over first
            moved = _read_lock(stale)
            if moved is not None and moved.get("owner") != held.get("owner"):
                # Raced: the lease we moved was renewed or retaken. Put it back, never delete it.
                with suppress(OSError):
                    os.link(stale, path)
                    stale.unlink()
                raise CommitConflict(f"{self.path.name} is being committed by another writer")
            with suppress(OSError):
                stale.unlink()
        try:
            yield
        finally:
            current = _read_lock(path)
            if current is not None and current.get("owner") == owner:
                with suppress(OSError):
                    path.unlink()

    def commit(
        self,
        *,
        mode: Literal["append", "overwrite", "view"],
        files: Sequence[Path],
        columns: Schema,
        rows: int | None,
        token: str,
        result: WriteResult,
        view: ViewDefinition | None = None,
        file_info: Mapping[str, FsFile] | None = None,
        drop: Collection[str] = (),
        removed_rows: int | None = 0,
        partition_by: Sequence[str] = (),
        expected_version: int | None = None,
    ) -> FsCommit:
        """Publish `files` atomically. For append, `rows` counts the rows added.

        `drop` (append only) removes committed part files in the same commit and `removed_rows`
        is how many rows they held (None when unknown). `file_info` describes the new files by
        name; a lone file without an entry gets `rows`. `expected_version` (0 for a table that
        does not exist) must equal the committed version or `CommitConflict` is raised.

        The result stored for `token` is `result` with `target_version` set to the commit version
        and, when unset, `bytes_written` set to the size of `files`. A replay returns that stored
        result, and the caller gets the same one as `FsCommit.result`.
        """
        if (mode == "view") != (view is not None):
            raise ValueError("a view commit needs a ViewDefinition and nothing else does")
        if mode == "view" and files:
            raise ValueError("a view has no files")
        if drop and mode != "append":
            raise ValueError("only an append commit can drop files")
        for f in files:
            if f.parent != self.path or not f.name.startswith("part-"):
                raise ValueError(f"{f} is not a part file of {self.path}")
        with self._locked():
            return self._commit_locked(
                mode, files, columns, rows, token, result, view, file_info or {},
                set(drop), removed_rows, tuple(partition_by), expected_version,
            )  # fmt: skip

    def _commit_locked(
        self,
        mode: Literal["append", "overwrite", "view"],
        files: Sequence[Path],
        columns: Schema,
        rows: int | None,
        token: str,
        result: WriteResult,
        view: ViewDefinition | None,
        file_info: Mapping[str, FsFile],
        drop: set[str],
        removed_rows: int | None,
        partition_by: tuple[str, ...],
        expected_version: int | None,
    ) -> FsCommit:
        previous = self.current()
        have = previous.version if previous else 0
        if expected_version is not None and expected_version != have:
            raise CommitConflict(
                f"{self.path.name} is at version {have}, the writer read version {expected_version}"
            )
        new_names = tuple(f.name for f in files)
        fmt = Path(new_names[0]).suffix.lstrip(".") if new_names else None
        total_rows = rows
        names = new_names
        info: dict[str, FsFile] = {}
        kept_partition_by = partition_by
        if mode == "append" and previous is not None:
            if previous.kind == "view":
                raise ValueError("cannot append to a view")
            if fmt is not None and previous.format not in (None, fmt):
                raise ValueError(f"cannot append {fmt} to a {previous.format} table")
            missing = drop - set(previous.files)
            if missing:
                raise ValueError(f"cannot drop uncommitted files {sorted(missing)}")
            old = tuple(n for n in previous.files if n not in drop)
            same = previous.partition_by == partition_by
            for n in old:
                i = previous.info(n)
                info[n] = i if same else replace(i, partition=None)
            names = old + new_names
            fmt = fmt or previous.format
            if rows is None or previous.rows is None or removed_rows is None:
                total_rows = None
            else:
                total_rows = previous.rows - removed_rows + rows
        elif mode == "append" and drop:
            raise ValueError("cannot drop files from a table that does not exist")
        for f in files:
            given = file_info.get(f.name)
            if given is None:
                given = FsFile(rows if len(files) == 1 else None, None, None)
            info[f.name] = replace(
                given, bytes=given.bytes if given.bytes is not None else f.stat().st_size
            )
        size = None if mode == "view" else sum((self.path / n).stat().st_size for n in names)
        now = self._clock.now().isoformat()
        version = have + 1
        if result.bytes_written is None and mode != "view":
            result = replace(result, bytes_written=sum(f.stat().st_size for f in files))
        result = replace(result, target_version=version)
        tokens = (*(previous.tokens if previous else ()), TokenRecord(token, result, now))
        commit = FsCommit(
            version=version,
            kind="view" if mode == "view" else "table",
            format=fmt,
            files=names,
            columns=columns,
            rows=total_rows,
            bytes=size,
            view=view,
            tokens=tokens[-TOKEN_LOG_SIZE:],
            committed_at=now,
            file_info=info,
            partition_by=kept_partition_by,
        )
        target = self.path / COMMIT_PATH
        tmp = target.with_name(f"{target.name}.{uuid.uuid4().hex}.tmp")
        try:
            tmp.write_text(json.dumps(_to_json(commit), indent=2) + "\n", encoding="utf-8")
            _replace(tmp, target)
        finally:
            tmp.unlink(missing_ok=True)
        if mode != "append":
            self._collect_garbage(set(names))
        else:
            self.discard([self.path / n for n in drop])
        return commit

    def _collect_garbage(self, keep: set[str]) -> None:
        """Best effort: delete part files the new commit does not list."""
        for old in self.path.glob("part-*"):
            if old.name not in keep:
                try:
                    old.unlink()
                except OSError:
                    pass

    def discard(self, files: Sequence[Path]) -> None:
        """Remove uncommitted part files after a failed write."""
        for f in files:
            try:
                f.unlink(missing_ok=True)
            except OSError:
                pass
