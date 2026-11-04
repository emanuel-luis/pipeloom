from __future__ import annotations

import json
from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest

from pipeloom.builtins.filesystem import SPEC, FilesystemCatalog
from pipeloom.builtins.fs_table import (
    FS_TABLE_SCHEMA,
    TOKEN_LOG_SIZE,
    FsCommit,
    FsFile,
    FsTable,
    ViewDefinition,
)
from pipeloom.engine.errors import CommitConflict, TransientError
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
    assert t.applied("k1") == WriteResult(rows_written=5, bytes_written=1, target_version=1)
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
        f.read_text(encoding="utf-8").replace("fs_table/2", "fs_table/9"), encoding="utf-8"
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


def test_commit_returns_the_stored_result(tmp_path: Path) -> None:
    t = table(tmp_path)
    c = commit(t, "overwrite", [part(t, "abc")], "k1", rows=3)
    assert c.result == WriteResult(rows_written=3, bytes_written=3, target_version=1)
    assert t.applied("k1") == c.result
    c2 = commit(t, "append", [part(t, "de")], "k2")
    assert c2.result.target_version == 2 and c2.result.bytes_written == 2


def test_failed_replace_keeps_previous_commit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    t = table(tmp_path)
    a = part(t, "a")
    commit(t, "overwrite", [a], "k1")

    def boom(src: object, dst: object) -> None:
        raise OSError("disk gone")

    monkeypatch.setattr("pipeloom.builtins.fs_table.os.replace", boom)
    with pytest.raises(OSError, match="disk gone"):
        commit(t, "overwrite", [part(t, "b")], "k2")
    assert t.files() == [a] and t.applied("k2") is None
    assert not list((t.path / "_pipeloom").glob("*.tmp"))


def test_replace_retries_on_permission_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import os

    t = table(tmp_path)
    calls: list[int] = []
    real = os.replace

    def flaky(src: Any, dst: Any) -> None:
        calls.append(1)
        if len(calls) < 3:
            raise PermissionError("in use")
        real(src, dst)

    monkeypatch.setattr("pipeloom.builtins.fs_table.os.replace", flaky)
    monkeypatch.setattr("pipeloom.builtins.fs_table.time.sleep", lambda _s: None)
    commit(t, "overwrite", [part(t, "a")], "k1")
    assert len(calls) == 3 and t.applied("k1") is not None


BAD = ["C:evil", "a/b", "a" + chr(92) + "b", "..", "", "x" + chr(0) + "y", "D:", "a" + chr(8)]


@pytest.mark.parametrize("bad", BAD)
def test_catalog_rejects_escaping_segments(tmp_path: Path, bad: str) -> None:
    cat = FilesystemCatalog(tmp_path, clock=FakeClock())
    with pytest.raises(ValueError):
        cat.table(TargetRef(None, bad, "x"))
    with pytest.raises(ValueError):
        cat.table(TargetRef(None, "s", bad))


def _lock(t: FsTable, expires: str) -> Path:
    path = t.path / "_pipeloom" / "commit.lock"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"owner": "dead", "expires_at": expires}), encoding="utf-8")
    return path


def test_commit_takes_over_an_expired_lock(tmp_path: Path) -> None:
    t = table(tmp_path)  # FakeClock starts at 2026-01-01 12:00 UTC
    lock = _lock(t, "2026-01-01T11:59:00+00:00")
    assert commit(t, "overwrite", [part(t, "a")], "k1").version == 1
    assert not lock.exists()


def test_commit_refuses_a_live_lock(tmp_path: Path) -> None:
    t = table(tmp_path)
    lock = _lock(t, "2026-01-01T12:10:00+00:00")
    p = part(t, "a")
    with pytest.raises(CommitConflict):
        commit(t, "overwrite", [p], "k1")
    assert lock.exists() and t.current() is None


def test_expected_version_mismatch_is_a_transient_conflict(tmp_path: Path) -> None:
    t = table(tmp_path)
    kw: dict[str, Any] = {"columns": COLS, "rows": 1, "result": WriteResult()}
    t.commit(mode="overwrite", files=[part(t, "a")], token="k1", expected_version=0, **kw)
    t.commit(mode="overwrite", files=[part(t, "b")], token="k2", expected_version=1, **kw)
    with pytest.raises(CommitConflict) as err:
        t.commit(mode="overwrite", files=[part(t, "c")], token="k3", expected_version=1, **kw)
    assert isinstance(err.value, TransientError) and current(t).version == 2
    assert not (t.path / "_pipeloom" / "commit.lock").exists()


def test_v1_commit_is_readable_and_upgrades_on_the_next_commit(tmp_path: Path) -> None:
    t = table(tmp_path)
    a = part(t, "a")
    commit(t, "overwrite", [a], "k1")
    f = t.path / "_pipeloom" / "commit.json"
    doc = json.loads(f.read_text(encoding="utf-8"))
    doc["schema"] = "pipeloom.fs_table/1"
    del doc["file_info"], doc["partition_by"]
    f.write_text(json.dumps(doc), encoding="utf-8")
    old = current(t)
    assert old.info(a.name) == FsFile() and old.partition_by == ()
    commit(t, "append", [part(t, "b")], "k2")
    assert json.loads(f.read_text(encoding="utf-8"))["schema"] == FS_TABLE_SCHEMA


def test_append_can_drop_files_and_tracks_partitions(tmp_path: Path) -> None:
    t = table(tmp_path)
    a, b = part(t, "aa"), part(t, "b")
    t.commit(
        mode="overwrite",
        files=[a, b],
        columns=COLS,
        rows=5,
        token="k1",
        result=WriteResult(),
        file_info={a.name: FsFile(3, None, ("x",)), b.name: FsFile(2, None, ("y",))},
        partition_by=("p",),
    )
    c = part(t, "c")
    out = t.commit(
        mode="append",
        files=[c],
        columns=COLS,
        rows=4,
        token="k2",
        result=WriteResult(),
        file_info={c.name: FsFile(4, None, ("x",))},
        drop=[a.name],
        removed_rows=3,
        partition_by=("p",),
    )
    assert out.files == (b.name, c.name) and out.rows == 6 and not a.exists()
    assert out.info(b.name).partition == ("y",) and out.info(c.name) == FsFile(4, 1, ("x",))
    with pytest.raises(ValueError, match="uncommitted"):
        t.commit(
            mode="append", files=[], columns=COLS, rows=0, token="k3", result=WriteResult(),
            drop=["part-nope.txt"],
        )  # fmt: skip


def test_read_aligned_reads_every_file_through_the_commit_schema(tmp_path: Path) -> None:
    t = table(tmp_path)
    commit(t, "overwrite", [part(t, "1")], "k1")
    commit(t, "append", [part(t, "2")], "k2")
    got = t.read_aligned(
        lambda p, fmt, schema: [p.read_text(encoding="utf-8")],
        lambda frame, schema: [*frame, len(schema)],
        lambda frames: [x for f in frames for x in f],
    )
    assert got == ["1", 1, "2", 1]
