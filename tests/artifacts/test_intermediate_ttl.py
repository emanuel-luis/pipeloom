"""`cleanup_intermediates`: judged by each run's own TTL and last activity, never by the caller's."""

from __future__ import annotations

import json
import os
from datetime import timedelta
from pathlib import Path

import pytest

from pipeloom.artifacts.finalize import cleanup_intermediates
from pipeloom.artifacts.results import RunStatusName
from pipeloom.testing.clock import FakeClock

HOUR = 3600
NOW = FakeClock(step=timedelta(0)).now()


def touch_tree(folder: Path, age: timedelta) -> None:
    stamp = (NOW - age).timestamp()
    for p in (*folder.rglob("*"), folder):
        os.utime(p, (stamp, stamp))


def make_run(
    root: Path,
    run_id: str,
    *,
    ended: timedelta | None,
    files: timedelta,
    ttl: int | None = None,
) -> Path:
    """A run that ended `ended` ago (None: no run_results.json) with files last written `files` ago."""
    run_dir = root / "runs" / run_id
    (run_dir / "intermediate" / "m").mkdir(parents=True)
    (run_dir / "intermediate" / "m" / "rows.json").write_bytes(b"[]")
    if ended is not None:
        run = {"ended_at": (NOW - ended).isoformat(), "intermediate_ttl": ttl}
        (run_dir / "run_results.json").write_bytes(json.dumps({"run": run}).encode("utf-8"))
    touch_tree(run_dir, files)
    return run_dir / "intermediate"


def clean(
    root: Path, ttl: float = HOUR, run_id: str = "current", status: RunStatusName = "succeeded"
) -> list[str]:
    return cleanup_intermediates(root, now=NOW, ttl=ttl, run_id=run_id, status=status)


def test_keeps_runs_inside_their_own_ttl_and_deletes_the_rest(tmp_path: Path) -> None:
    day = timedelta(days=1)
    fresh = make_run(
        tmp_path, "fresh", ended=timedelta(hours=1), files=timedelta(hours=1), ttl=2 * HOUR
    )
    stale = make_run(
        tmp_path, "stale", ended=timedelta(hours=3), files=timedelta(hours=3), ttl=2 * HOUR
    )
    assert clean(tmp_path, ttl=10 * day.total_seconds()) == []
    assert fresh.is_dir() and not stale.exists()
    assert (stale.parent / "run_results.json").is_file()  # only the intermediates go


def test_a_run_is_judged_by_its_recorded_ttl_not_the_callers(tmp_path: Path) -> None:
    long_lived = make_run(
        tmp_path, "long", ended=timedelta(hours=5), files=timedelta(hours=5), ttl=10 * HOUR
    )
    clean(tmp_path, ttl=HOUR)  # a target with a short TTL shares the root
    assert long_lived.is_dir()


def test_ttl_zero_deletes_only_the_current_run_on_success(tmp_path: Path) -> None:
    mine = make_run(tmp_path, "current", ended=None, files=timedelta(0))
    other = make_run(
        tmp_path, "other", ended=timedelta(minutes=5), files=timedelta(minutes=5), ttl=HOUR
    )
    old_failed = make_run(
        tmp_path, "failed0", ended=timedelta(minutes=5), files=timedelta(minutes=5), ttl=0
    )
    assert clean(tmp_path, ttl=0) == []
    assert not mine.exists() and other.is_dir() and old_failed.is_dir()


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
    tmp_path: Path, status: RunStatusName, ttl: int, kept: bool
) -> None:
    mine = make_run(tmp_path, "current", ended=None, files=timedelta(0))
    clean(tmp_path, ttl=ttl, status=status)
    assert mine.is_dir() is kept


def test_a_resumed_run_with_fresh_files_is_kept_despite_an_old_ended_at(tmp_path: Path) -> None:
    resumed = make_run(
        tmp_path, "resumed", ended=timedelta(days=3), files=timedelta(minutes=1), ttl=HOUR
    )
    clean(tmp_path)
    assert resumed.is_dir()


def test_a_run_without_results_is_kept_until_idle_for_the_longer_of_ttl_and_an_hour(
    tmp_path: Path,
) -> None:
    running = make_run(tmp_path, "running", ended=None, files=timedelta(minutes=30))
    idle = make_run(tmp_path, "idle", ended=None, files=timedelta(hours=3))
    clean(tmp_path, ttl=60)
    assert running.is_dir() and not idle.exists()
    longer = make_run(tmp_path, "longer", ended=None, files=timedelta(hours=3))
    clean(tmp_path, ttl=10 * HOUR)
    assert longer.is_dir()


def test_a_missing_folder_is_fine(tmp_path: Path) -> None:
    assert clean(tmp_path, ttl=0) == []
    (tmp_path / "runs" / "current").mkdir(parents=True)
    assert clean(tmp_path, ttl=0) == []


def test_a_deletion_error_becomes_a_warning(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    stale = make_run(tmp_path, "stale", ended=timedelta(days=2), files=timedelta(days=2), ttl=HOUR)

    def boom(path: object) -> None:
        raise PermissionError("locked")

    monkeypatch.setattr("pipeloom.artifacts.finalize.shutil.rmtree", boom)
    warnings = clean(tmp_path)
    assert len(warnings) == 1 and "stale" in warnings[0] and "locked" in warnings[0]
    assert stale.is_dir()


def test_replayed_writes_still_count_as_activity_through_the_rest_of_the_run_folder(
    tmp_path: Path,
) -> None:
    """r1 failed at ttl 0 (counted as 1h); it resumed 10 minutes ago and replayed its writes."""
    resumed = make_run(tmp_path, "r1", ended=timedelta(hours=2), files=timedelta(hours=2), ttl=0)
    run_dir = resumed.parent
    (run_dir / "events").mkdir()
    events = run_dir / "events" / "inv2.jsonl"
    events.write_bytes(b"{}")
    stamp = (NOW - timedelta(minutes=10)).timestamp()
    os.utime(events, (stamp, stamp))
    clean(tmp_path)
    assert resumed.is_dir()


def resumed_run(root: Path, run_id: str, idle: timedelta) -> Path:
    """A run with old results and a manifest newer than them; everything idle for `idle`."""
    kept = make_run(root, run_id, ended=timedelta(days=3), files=idle, ttl=HOUR)
    run_dir = kept.parent
    manifest = run_dir / "manifest.json"
    manifest.write_bytes(b"{}")
    stamp = (NOW - idle).timestamp()
    os.utime(run_dir / "run_results.json", (stamp - 10, stamp - 10))
    os.utime(manifest, (stamp, stamp))
    os.utime(run_dir, (stamp, stamp))
    return kept


def test_a_resumed_and_active_run_is_kept(tmp_path: Path) -> None:
    active = resumed_run(tmp_path, "r1", timedelta(minutes=5))
    clean(tmp_path)
    assert active.is_dir()


def test_a_resumed_run_that_crashed_again_expires_once_idle(tmp_path: Path) -> None:
    crashed = resumed_run(tmp_path, "r1", timedelta(hours=3))
    clean(tmp_path, ttl=10 * HOUR)
    assert crashed.is_dir()  # idle, but inside max(ttl, 1h)
    clean(tmp_path, ttl=HOUR)
    assert not crashed.exists()
