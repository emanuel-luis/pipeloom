"""Filesystem table format: part files plus an atomically replaced commit file.

Layout: `<table>/part-<uuid12>.<ext>` and `<table>/_pipeloom/commit.json`. A part file is
invisible until a commit lists it; readers go through the commit, never a directory glob.
"""

from __future__ import annotations

import json
import os
import time
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Final, Literal

from pipeloom.engine.types import Column, ReadSpec, Schema, TargetRef, WriteResult
from pipeloom.platform import Clock

FS_TABLE_SCHEMA: Final = "pipeloom.fs_table/1"
"""Format version stored in every commit file; a reader rejects any other value."""
TOKEN_LOG_SIZE: Final = 100
COMMIT_PATH: Final = Path("_pipeloom") / "commit.json"


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
    }


def _from_json(d: Mapping[str, Any]) -> FsCommit:
    if d.get("schema") != FS_TABLE_SCHEMA:
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


class FsTable:
    """One table directory. One writer at a time.

    ponytail: no commit lock; the scheduler runs one model per target. A lock file arrives with
    merge and replace_partitions (the commit format version is the hook for it).
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
    ) -> FsCommit:
        """Publish `files` atomically. For append, `rows` counts the rows added."""
        previous = self.current()
        if (mode == "view") != (view is not None):
            raise ValueError("a view commit needs a ViewDefinition and nothing else does")
        if mode == "view" and files:
            raise ValueError("a view has no files")
        for f in files:
            if f.parent != self.path or not f.name.startswith("part-"):
                raise ValueError(f"{f} is not a part file of {self.path}")
        names = tuple(f.name for f in files)
        fmt = Path(names[0]).suffix.lstrip(".") if names else None
        total_rows = rows
        if mode == "append" and previous is not None:
            if previous.kind == "view":
                raise ValueError("cannot append to a view")
            if fmt is not None and previous.format not in (None, fmt):
                raise ValueError(f"cannot append {fmt} to a {previous.format} table")
            names = previous.files + names
            fmt = fmt or previous.format
            total_rows = None if rows is None or previous.rows is None else previous.rows + rows
        size = None if mode == "view" else sum((self.path / n).stat().st_size for n in names)
        now = self._clock.now().isoformat()
        tokens = (*(previous.tokens if previous else ()), TokenRecord(token, result, now))
        commit = FsCommit(
            version=previous.version + 1 if previous else 1,
            kind="view" if mode == "view" else "table",
            format=fmt,
            files=names,
            columns=columns,
            rows=total_rows,
            bytes=size,
            view=view,
            tokens=tokens[-TOKEN_LOG_SIZE:],
            committed_at=now,
        )
        target = self.path / COMMIT_PATH
        target.parent.mkdir(parents=True, exist_ok=True)
        tmp = target.with_name(f"{target.name}.{uuid.uuid4().hex}.tmp")
        try:
            tmp.write_text(json.dumps(_to_json(commit), indent=2) + "\n", encoding="utf-8")
            _replace(tmp, target)
        finally:
            tmp.unlink(missing_ok=True)
        if mode != "append":
            self._collect_garbage(set(names))
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
