"""`pipeloom runs ls|show`: every flag, on the sqlite and json stores and the artifacts root."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from click.testing import CliRunner, Result

from pipeloom.cli import main
from tests.conftest import make_project

ENV = {"PIPELOOM_TARGET": "", "PIPELOOM_DISABLE_PLUGIN_AUTOLOAD": "", "PIPELOOM_LOG_FORMAT": ""}
YML = """config_version: 1
name: shop
targets:
  dev:
    engine:
      kind: memory
    catalog:
      kind: memory
    state:
      kind: STATE
  prod:
    engine:
      kind: memory
    catalog:
      kind: memory
    state:
      kind: STATE
"""
MODELS = """models:
  - name: a
    sql: select 1 as x
"""


def cli(root: Path, *args: str) -> Result:
    argv = [*args, "--project-dir", str(root), "--log-format", "json"]
    return CliRunner().invoke(main, argv, env=ENV)


@pytest.fixture(params=["sqlite", "json"])
def root(request: pytest.FixtureRequest, tmp_path: Path) -> Path:
    yml = YML.replace("STATE", request.param)
    root = make_project(tmp_path, {"pipeloom.yml": yml, "models/m.yml": MODELS})
    for run_id in ("r1", "r2", "r3"):
        done = cli(root, "run", "--run-id", run_id, "--target", "dev")
        assert done.exit_code == 0, done.stderr
    assert cli(root, "run", "--run-id", "p1", "--target", "prod").exit_code == 0
    return root


def ids(result: Result) -> list[str]:
    assert result.exit_code == 0, result.stderr
    return [line.split("\t")[0] for line in result.stdout.splitlines()]


@pytest.mark.parametrize("source", [(), ("--artifacts",)])
def test_ls_lists_the_last_runs_newest_first(root: Path, source: tuple[str, ...]) -> None:
    dev = ("--target", "dev", *source)
    assert ids(cli(root, "runs", "ls", *dev)) == ["r3", "r2", "r1"]
    assert ids(cli(root, "runs", "ls", *dev, "--limit", "2")) == ["r3", "r2"]
    assert ids(cli(root, "runs", "ls", *dev, "--limit", "2", "--before", "r2")) == ["r1"]
    assert ids(cli(root, "runs", "ls", "--target", "prod", *source)) == ["p1"]
    line = cli(root, "runs", "ls", *dev, "--limit", "1").stdout.strip().split("\t")
    assert line[1] == "succeeded" and line[3].endswith("s") and line[4] == "succeeded=1 total=1"


def test_ls_output_json(root: Path) -> None:
    done = cli(root, "runs", "ls", "--target", "dev", "--output", "json", "--limit", "1")
    assert done.exit_code == 0, done.stderr
    (row,) = json.loads(done.stdout)
    assert row["run_id"] == "r3" and row["project"] == "shop" and row["status"] == "succeeded"


@pytest.mark.parametrize("source", [(), ("--artifacts",)])
def test_show_prints_the_run_and_its_units(root: Path, source: tuple[str, ...]) -> None:
    done = cli(root, "runs", "show", "r2", "--target", "dev", *source)
    assert done.exit_code == 0, done.stderr
    lines = done.stdout.splitlines()
    assert lines[0] == "run r2 succeeded" and lines[-1] == "a\t1\tsucceeded"
    as_json = cli(root, "runs", "show", "r2", "--target", "dev", "--output", "json", *source)
    doc = json.loads(as_json.stdout)
    assert doc["run"]["run_id"] == "r2" and [u["unit_id"] for u in doc["units"]] == ["a"]


@pytest.mark.parametrize("source", [(), ("--artifacts",)])
def test_unknown_run_is_plx118(root: Path, source: tuple[str, ...]) -> None:
    shown = cli(root, "runs", "show", "p1", "--target", "dev", *source)  # a run of another target
    assert shown.exit_code == 1 and "PLX118" in shown.stderr and shown.stdout == ""
    paged = cli(root, "runs", "ls", "--target", "dev", "--before", "nope", *source)
    assert paged.exit_code == 1 and "PLX118" in paged.stderr


def test_limit_must_be_positive(root: Path) -> None:
    assert cli(root, "runs", "ls", "--target", "dev", "--limit", "0").exit_code == 2


def test_a_store_without_list_runs_is_plx117(tmp_path: Path) -> None:
    yml = YML.replace("STATE", "nolist")
    root = make_project(tmp_path, {"pipeloom.yml": yml, "models/m.yml": MODELS})
    done = cli(root, "runs", "ls", "--target", "dev", "--plugin", "tests.plugins.fakes:NOLIST")
    assert done.exit_code == 1 and "PLX117" in done.stderr
    assert "--artifacts" in done.stderr


def test_artifacts_skip_unreadable_results(root: Path) -> None:
    broken = root / "target" / "runs" / "zz" / "run_results.json"
    broken.parent.mkdir(parents=True)
    broken.write_text("{not json", encoding="utf-8")
    assert ids(cli(root, "runs", "ls", "--target", "dev", "--artifacts")) == ["r3", "r2", "r1"]
