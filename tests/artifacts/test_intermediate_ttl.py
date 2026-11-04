"""`cleanup_intermediates`: keep inside the TTL, delete outside, ttl 0 only on success."""

from __future__ import annotations

import json
from datetime import timedelta
from pathlib import Path

import pytest

from pipeloom.artifacts.finalize import cleanup_intermediates
from pipeloom.testing.clock import FakeClock

HOUR = 3600


def make_run(root: Path, run_id: str, ended: str | None) -> Path:
    folder = root / "runs" / run_id
    (folder / "intermediate" / "m").mkdir(parents=True)
    (folder / "intermediate" / "m" / "rows.json").write_bytes(b"[]")
    if ended is not None:
        doc = {"run": {"ended_at": ended}}
        (folder / "run_results.json").write_bytes(json.dumps(doc).encode("utf-8"))
    return folder / "intermediate"


def test_keeps_runs_inside_the_ttl_and_deletes_the_rest(tmp_path: Path) -> None:
    clock = FakeClock(step=timedelta(0))
    now = clock.now()
    fresh = make_run(tmp_path, "fresh", (now - timedelta(hours=1)).isoformat())
    stale = make_run(tmp_path, "stale", (now - timedelta(hours=3)).isoformat())
    warnings = cleanup_intermediates(
        tmp_path, now=now, ttl=2 * HOUR, run_id="current", status="succeeded"
    )
    assert warnings == []
    assert fresh.is_dir() and not stale.exists()
    assert (stale.parent / "run_results.json").is_file()  # only the intermediates go


def test_a_run_without_results_ages_by_its_folder_mtime(tmp_path: Path) -> None:
    folder = make_run(tmp_path, "crashed", None)
    mtime = folder.parent.stat().st_mtime
    from datetime import UTC, datetime

    then = datetime.fromtimestamp(mtime, UTC)
    assert (
        cleanup_intermediates(
            tmp_path, now=then + timedelta(hours=1), ttl=2 * HOUR, run_id="x", status="failed"
        )
        == []
    )
    assert folder.is_dir()
    cleanup_intermediates(
        tmp_path, now=then + timedelta(hours=3), ttl=2 * HOUR, run_id="x", status="failed"
    )
    assert not folder.exists()


@pytest.mark.parametrize(
    ("status", "ttl", "kept"),
    [
        ("succeeded", 0, False),
        ("failed", 0, True),
        ("cancelled", 0, True),
        ("succeeded", HOUR, True),
    ],
)
def test_the_current_run_goes_only_at_ttl_zero_on_success(
    tmp_path: Path, status: str, ttl: int, kept: bool
) -> None:
    now = FakeClock(step=timedelta(0)).now()
    mine = make_run(tmp_path, "mine", None)
    cleanup_intermediates(tmp_path, now=now, ttl=ttl, run_id="mine", status=status)
    assert mine.is_dir() is kept


def test_a_missing_folder_is_fine(tmp_path: Path) -> None:
    now = FakeClock().now()
    assert cleanup_intermediates(tmp_path, now=now, ttl=0, run_id="r", status="succeeded") == []
    (tmp_path / "runs" / "r").mkdir(parents=True)
    assert cleanup_intermediates(tmp_path, now=now, ttl=0, run_id="r", status="succeeded") == []


def test_a_deletion_error_becomes_a_warning(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    now = FakeClock(step=timedelta(0)).now()
    stale = make_run(tmp_path, "stale", (now - timedelta(days=2)).isoformat())

    def boom(path: object) -> None:
        raise PermissionError("locked")

    monkeypatch.setattr("pipeloom.artifacts.finalize.shutil.rmtree", boom)
    warnings = cleanup_intermediates(
        tmp_path, now=now, ttl=HOUR, run_id="other", status="succeeded"
    )
    assert len(warnings) == 1 and "stale" in warnings[0] and "locked" in warnings[0]
    assert stale.is_dir()
