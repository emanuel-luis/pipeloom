"""An `intermediate` model end to end on polars: Parquet under the artifacts root."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import polars as pl

from pipeloom.api import Project, ProjectOptions
from pipeloom.builtins.fs_table import FsTable
from pipeloom.events.bus import EventBus
from pipeloom.runtime.run import RunOptions
from pipeloom.testing.clock import FakeClock


def project(tmp_path: Path, ttl: str) -> Path:
    files = {
        "pipeloom.yml": (
            "config_version: 1\nname: shop\npython_paths: [src]\n"
            "targets:\n  polars:\n    engine: {kind: polars}\n"
            "    catalog: {kind: filesystem, root: target/warehouse}\n"
            f"    artifacts: {{intermediate_ttl: {ttl}}}\n"
            "    state: {kind: json, path: .pipeloom/state}\n    naming: {kind: none}\n"
        ),
        "models/sources.yml": (
            "sources:\n  - {name: raw_orders, format: csv, location: raw/orders.csv}\n"
        ),
        "models/m.yml": (
            "models:\n"
            "  - name: stg\n    materialized: intermediate\n"
            "    sql: select id, v from raw where v > 0\n"
            "    inputs: {raw: {source: raw_orders}}\n"
            "  - name: rpt\n    materialized: table\n"
            "    sql: select count(*) as n, sum(v) as total from stg\n"
            "    inputs: {stg: {ref: stg}}\n"
        ),
        "raw/orders.csv": "id,v\n1,5\n2,-1\n3,7\n",
    }
    for rel, text in files.items():
        (tmp_path / rel).parent.mkdir(parents=True, exist_ok=True)
        (tmp_path / rel).write_bytes(text.encode("utf-8"))
    return tmp_path


def run(root: Path) -> dict[str, Any]:
    clock = FakeClock(datetime(2026, 1, 1, tzinfo=UTC), step=timedelta(0))
    proj = Project(ProjectOptions(project_dir=root, target="polars", env={}), clock=clock)
    with EventBus([], clock=clock, invocation_id="inv") as bus:
        outcome = proj.run(RunOptions(run_id="r1", workers=2), bus)
    assert outcome.exit_code == 0, outcome.diagnostics
    doc: dict[str, Any] = json.loads(
        (root / "target" / "runs" / "r1" / "run_results.json").read_bytes()
    )
    return doc


def test_intermediate_is_kept_inside_the_ttl(tmp_path: Path) -> None:
    root = project(tmp_path, "1d")
    doc = run(root)
    assert doc["run"]["warnings"] == []
    place = root / "target" / "runs" / "r1" / "intermediate" / "stg"
    stg = FsTable(place, clock=FakeClock())
    frame = pl.scan_parquet([str(f) for f in stg.files()]).collect().sort("id")
    assert frame.to_dicts() == [{"id": 1, "v": 5}, {"id": 3, "v": 7}]
    wh = FsTable(root / "target" / "warehouse" / "main" / "rpt", clock=FakeClock())
    assert pl.scan_parquet([str(f) for f in wh.files()]).collect().to_dicts() == [
        {"n": 2, "total": 12}
    ]


def test_ttl_zero_deletes_it_after_a_successful_run(tmp_path: Path) -> None:
    root = project(tmp_path, "0")
    run(root)
    assert not (root / "target" / "runs" / "r1" / "intermediate").exists()
