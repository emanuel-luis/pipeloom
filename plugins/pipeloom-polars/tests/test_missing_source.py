"""A missing source file fails the unit at read with PLM204, not at compute with PLX101."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from tests.conftest import make_project

from pipeloom.api import Project, ProjectOptions
from pipeloom.runtime.run import RunOptions
from pipeloom.testing.clock import FakeClock


@pytest.mark.parametrize("fmt", ["parquet", "csv", "ndjson"])
def test_a_missing_source_is_plm204(tmp_path: Path, fmt: str) -> None:
    root = make_project(
        tmp_path,
        {
            "pipeloom.yml": (
                "config_version: 1\nname: shop\n"
                "targets:\n  polars:\n    engine: {kind: polars}\n"
                "    catalog: {kind: filesystem, root: target/warehouse}\n"
                "    state: {kind: json, path: .pipeloom/state}\n    naming: {kind: none}\n"
            ),
            "models/sources.yml": (
                f"sources:\n  - {{name: raw, format: {fmt}, location: raw/nation.{fmt}}}\n"
            ),
            "models/m.yml": (
                "models:\n  - name: nation\n    sql: select * from raw\n"
                "    inputs: {raw: {source: raw}}\n"
            ),
        },
    )
    project = Project(ProjectOptions(project_dir=root, target="polars", env={}), clock=FakeClock())
    outcome = project.run(RunOptions(run_id="r1", workers=1))
    assert outcome.exit_code == 1
    path = root / "target" / "runs" / "r1" / "run_results.json"
    (row,) = json.loads(path.read_bytes())["results"]
    assert row["status"] == "failed"
    assert row["error"]["code"] == "PLM204"
    assert "cannot read input" in row["error"]["message"]
