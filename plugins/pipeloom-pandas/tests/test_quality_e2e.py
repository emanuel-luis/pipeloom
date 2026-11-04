"""Expectations and quarantine end to end on pandas: the shared run_results golden, quarantine
contents on parquet and csv catalogs, and token replay."""

from __future__ import annotations

import json
import shutil
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow.parquet as pq
import pytest

from pipeloom.api import Project, ProjectOptions
from pipeloom.builtins.fs_table import FsTable
from pipeloom.events.bus import EventBus
from pipeloom.runtime.run import RunOptions
from pipeloom.testing.clock import FakeClock

ROOT = Path(__file__).resolve().parents[3] / "tests"
KEYS = (
    "unit_id",
    "status",
    "rows_written",
    "rows_dropped",
    "rows_quarantined",
    "expectations",
    "warnings",
)


def project(tmp_path: Path, fmt: str) -> Path:
    shutil.copytree(
        ROOT / "fixtures" / "quality",
        tmp_path,
        ignore=shutil.ignore_patterns("target", ".pipeloom"),
        dirs_exist_ok=True,
    )
    yml = tmp_path / "pipeloom.yml"
    text = yml.read_text(encoding="utf-8").replace("format: parquet", f"format: {fmt}")
    yml.write_text(text, encoding="utf-8", newline="\n")
    return tmp_path


def run(root: Path, run_id: str) -> int:
    clock = FakeClock(datetime(2026, 1, 1, tzinfo=UTC), step=timedelta(0))
    proj = Project(
        ProjectOptions(project_dir=root, target="pandas", env={"PIPELOOM_USER": "tester"}),
        clock=clock,
        invocation_id="inv",
    )
    with EventBus([], clock=clock, invocation_id="inv") as bus:
        return proj.run(RunOptions(run_id=run_id, workers=1), bus).exit_code


def table(root: Path, name: str, fmt: str) -> list[dict[str, Any]]:
    t = FsTable(root / "target" / "warehouse" / "main" / name, clock=FakeClock())
    rows: list[dict[str, Any]] = []
    for f in t.files():
        if fmt == "parquet":
            rows += pq.read_table(str(f)).to_pylist()
        else:
            rows += pd.read_csv(f).to_dict("records")
    return sorted(rows, key=lambda r: int(r["id"]))


def reduced(root: Path, run_id: str) -> list[dict[str, Any]]:
    doc = json.loads((root / "target" / "runs" / run_id / "run_results.json").read_bytes())
    out = []
    for r in doc["results"]:
        item = {k: r[k] for k in KEYS}
        item["error"] = None if r["error"] is None else r["error"]["code"]
        out.append(item)
    return out


@pytest.mark.parametrize("fmt", ["parquet", "csv"])
def test_quality_matches_the_golden_and_quarantines(tmp_path: Path, fmt: str) -> None:
    root = project(tmp_path, fmt)
    assert run(root, "r1") == 1  # orders_strict fails on purpose
    golden = json.loads((ROOT / "goldens" / "run_results" / "quality.json").read_bytes())
    assert reduced(root, "r1") == golden
    assert [int(r["id"]) for r in table(root, "orders", fmt)] == [1, 2, 3, 5, 6, 7]
    (q,) = table(root, "orders__quarantine", fmt)
    assert q["id"] == 4 and q["_pipeloom_run_id"] == "r1"
    failed = q["_pipeloom_failed"]
    assert (json.loads(failed) if fmt == "csv" else failed) == ["amount_not_negative"]
    assert not (root / "target" / "warehouse" / "main" / "orders_strict").exists()


def test_quarantine_replay_and_append(tmp_path: Path) -> None:
    root = project(tmp_path, "parquet")
    run(root, "r1")
    run(root, "r1")  # same token: nothing is written twice
    assert len(table(root, "orders__quarantine", "parquet")) == 1
    run(root, "r2")
    rows = table(root, "orders__quarantine", "parquet")
    assert [r["_pipeloom_run_id"] for r in rows] == ["r1", "r2"]
    assert pd.Timestamp(rows[0]["_pipeloom_ts"]) == pd.Timestamp("2026-01-01", tz="UTC")
