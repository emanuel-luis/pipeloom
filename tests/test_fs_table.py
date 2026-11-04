from __future__ import annotations

from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest

from pipeloom.builtins.filesystem import SPEC, FilesystemCatalog
from pipeloom.builtins.fs_table import TOKEN_LOG_SIZE, FsCommit, FsTable, ViewDefinition
from pipeloom.engine.types import Column, Namespace, ReadSpec, TargetRef, WriteResult
from pipeloom.testing.clock import FakeClock

COLS = (Column("id", "int64"),)


def table(tmp_path: Path) -> FsTable:
    return FsTable(tmp_path / "t", clock=FakeClock(step=timedelta(seconds=1)))


def part(t: FsTable, text: str, ext: str = "txt") -> Path:
    p = t.new_file(ext)
    p.write_text(text, encoding="utf-8")
    return p


def commit(t: FsTable, mode: Any, files: list[Path], token: str, rows: int = 1) -> FsCommit:
    return t.commit(
        mode=mode,
        files=files,
        columns=COLS,
        rows=rows,
        token=token,
        result=WriteResult(rows_written=rows),
    )


def current(t: FsTable) -> FsCommit:
    c = t.current()
    assert c is not None
    return c


def test_new_file_is_invisible_until_commit(tmp_path: Path) -> None:
    t = table(tmp_path)
    p = part(t, "a")
    assert t.current() is None and t.files() == []
    c = commit(t, "overwrite", [p], "k1")
    assert t.files() == [p] and c.version == 1 and c.format == "txt" and c.bytes == 1
    assert (t.path / "_pipeloom" / "commit.json").is_file()
    assert not list((t.path / "_pipeloom").glob("*.tmp"))


def test_overwrite_collects_old_parts_and_append_keeps_them(tmp_path: Path) -> None:
    t = table(tmp_path)
    a = part(t, "a")
    commit(t, "overwrite", [a], "k1", rows=1)
    b = part(t, "b")
    commit(t, "append", [b], "k2", rows=2)
    assert t.files() == [a, b] and current(t).rows == 3
    c = part(t, "c")
    commit(t, "overwrite", [c], "k3")
    assert t.files() == [c] and not a.exists() and not b.exists() and current(t).version == 3


def test_empty_overwrite_still_has_a_file(tmp_path: Path) -> None:
    t = table(tmp_path)
    p = part(t, "id\n")
    c = commit(t, "overwrite", [p], "k1", rows=0)
    assert c.files == (p.name,) and c.rows == 0 and c.columns == COLS


def test_token_replay(tmp_path: Path) -> None:
    t = table(tmp_path)
    assert t.applied("k1") is None
    commit(t, "overwrite", [part(t, "a")], "k1", rows=5)
    assert t.applied("k1") == WriteResult(rows_written=5)
    assert t.applied("other") is None


def test_token_log_is_capped(tmp_path: Path) -> None:
    t = table(tmp_path)
    commit(t, "overwrite", [part(t, "a")], "t0")
    for i in range(1, TOKEN_LOG_SIZE + 5):
        commit(t, "append", [part(t, "x")], f"t{i}")
    tokens = [r.token for r in current(t).tokens]
    assert len(tokens) == TOKEN_LOG_SIZE and tokens[-1] == f"t{TOKEN_LOG_SIZE + 4}"
    assert t.applied("t0") is None


def test_crash_between_part_write_and_commit(tmp_path: Path) -> None:
    t = table(tmp_path)
    a = part(t, "a")
    commit(t, "overwrite", [a], "k1")
    orphan = part(t, "lost")  # the writer died here: no commit
    assert t.files() == [a] and t.applied("k2") is None
    # a retry with the same token writes again and commits; the orphan is collected
    b = part(t, "b")
    commit(t, "overwrite", [b], "k2")
    assert t.files() == [b] and not orphan.exists()


def test_rejected_commit_leaves_previous_commit(tmp_path: Path) -> None:
    t = table(tmp_path)
    a = part(t, "a")
    commit(t, "overwrite", [a], "k1")
    with pytest.raises(ValueError):
        commit(t, "overwrite", [tmp_path / "elsewhere.txt"], "k2")
    t.discard([t.path / "part-missing.txt"])
    assert t.files() == [a]


def test_discard_removes_uncommitted_part(tmp_path: Path) -> None:
    t = table(tmp_path)
    p = part(t, "a")
    t.discard([p])
    assert not p.exists()


def test_view_round_trip(tmp_path: Path) -> None:
    t = table(tmp_path)
    ref = TargetRef(None, "raw", "orders")
    view = ViewDefinition("select * from o", {"o": ReadSpec("table", table=ref)})
    t.commit(
        mode="view", files=[], columns=COLS, rows=None, token="v", result=WriteResult(), view=view
    )
    c = current(t)
    assert c.kind == "view" and c.view == view and c.files == ()
    with pytest.raises(ValueError):
        commit(t, "append", [part(t, "a")], "k")


def test_unknown_format_version_is_rejected(tmp_path: Path) -> None:
    t = table(tmp_path)
    commit(t, "overwrite", [part(t, "a")], "k1")
    f = t.path / "_pipeloom" / "commit.json"
    f.write_text(
        f.read_text(encoding="utf-8").replace("fs_table/1", "fs_table/9"), encoding="utf-8"
    )
    with pytest.raises(ValueError, match="unsupported"):
        t.current()


def test_catalog(tmp_path: Path) -> None:
    cat = FilesystemCatalog(tmp_path, clock=FakeClock())
    ref = TargetRef(None, "silver", "orders")
    assert not cat.exists(ref) and cat.table_schema(ref) is None
    cat.ensure_namespace(Namespace(None, "silver"))
    cat.ensure_namespace(Namespace(None, "silver"))
    t = cat.table(ref)
    assert t.path == tmp_path / "silver" / "orders"
    commit(t, "overwrite", [part(t, "a")], "k")
    assert cat.exists(ref) and cat.table_schema(ref) == COLS
    with pytest.raises(ValueError):
        cat.table(TargetRef(None, "..", "x"))


def test_spec_builds_catalog(tmp_path: Path) -> None:
    opts = SPEC.config_model.model_validate({"format": "csv"}, context={"project_dir": tmp_path})
    cat = SPEC.factory(opts)
    assert isinstance(cat, FilesystemCatalog) and cat.format == "csv"
    assert cat.root == Path(tmp_path / ".")
    assert SPEC.compatible_engines == {"polars", "pandas"}
