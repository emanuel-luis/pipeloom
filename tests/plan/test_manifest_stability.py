"""An additive config setting must leave plans that do not use it byte-identical: the plan hash,
node checksums and `state:modified` of every 1.0 project hold across a 1.x upgrade."""

from __future__ import annotations

import json
from pathlib import Path

from pipeloom.api import Project, ProjectOptions
from pipeloom.plan.manifest import manifest_bytes
from pipeloom.plan.model import Plan
from tests.conftest import make_project
from tests.plan.test_plan import compile_shop

FROZEN = Path(__file__).parent.parent / "frozen" / "v1.0.0" / "documents" / "manifest_shop-ci.json"


def test_shop_plan_hash_and_checksums_equal_the_frozen_1_0_manifest() -> None:
    """Shop compiles to the 1.0 document exactly: any difference is a hash break."""
    frozen = json.loads(FROZEN.read_bytes().decode("utf-8"))
    plan = compile_shop()
    assert plan.metadata.plan_hash == frozen["metadata"]["plan_hash"]
    got = {uid: node.checksum for uid, node in plan.nodes.items()}
    assert got == {uid: node["checksum"] for uid, node in frozen["nodes"].items()}


def incremental_plan(tmp_path: Path, extra: str = "") -> Plan:
    root = make_project(
        tmp_path,
        {
            "pipeloom.yml": (
                "config_version: 1\nname: inc\npython_paths: [src]\nmodel_paths: [src]\n"
                "sources:\n  raw:\n    tables:\n      raw: {table: main.raw}\n"
                "targets:\n  dev:\n    engine: {kind: memory}\n    catalog: {kind: memory}\n"
            ),
            "src/m.py": (
                "from pipeloom import model, source\n\n"
                "@model(\n    inputs={'raw': source('raw', 'raw')},\n"
                "    materialized='incremental',\n"
                f"    incremental={{'input': 'raw', 'column': 'ts'{extra}}},\n)\n"
                "def m(ctx, raw):\n    return raw\n"
            ),
        },
    )
    outcome = Project(ProjectOptions(project_dir=root, target="dev", env={})).compile()
    assert outcome.value is not None, outcome.diagnostics
    return outcome.value


def test_unset_incremental_settings_are_absent_from_the_manifest(tmp_path: Path) -> None:
    plan = incremental_plan(tmp_path)
    inc = json.loads(manifest_bytes(plan))["nodes"]["model.inc.m"]["config"]["incremental"]
    assert sorted(inc) == ["column", "input", "lookback", "watermark_source"]


def test_a_set_setting_is_in_the_manifest_and_moves_the_hash(tmp_path: Path) -> None:
    base = incremental_plan(tmp_path / "a")
    plan = incremental_plan(tmp_path / "b", ", 'on_cursor_value_missing': 'include'")
    inc = json.loads(manifest_bytes(plan))["nodes"]["model.inc.m"]["config"]["incremental"]
    assert inc["on_cursor_value_missing"] == "include"
    assert plan.metadata.plan_hash != base.metadata.plan_hash
