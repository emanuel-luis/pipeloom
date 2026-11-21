"""The tested_models walkthrough: unit tests pass, a broken model fails them, build gates the run.

Runs on DuckDB and polars with no gate (docs/examples/tested-models.md)."""

from __future__ import annotations

import json
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest
from click.testing import CliRunner

from pipeloom.cli import main

EXAMPLES = Path(__file__).resolve().parents[2] / "examples"
SOURCE = EXAMPLES / "tested_models"
if not SOURCE.is_dir():  # the sdist ships tests/ without examples/
    pytest.skip("examples/ is not in this source tree", allow_module_level=True)

ENGINES = ["duckdb", "polars"]


def copy_project(dest: Path) -> Path:
    shutil.copytree(
        SOURCE, dest, ignore=shutil.ignore_patterns("target", ".pipeloom", "__pycache__")
    )
    return dest


def cli(*args: str) -> Any:
    return CliRunner().invoke(main, list(args), env={})


def events(result: Any) -> list[dict[str, Any]]:
    lines = {ln for ln in (result.output + result.stderr).splitlines() if ln.startswith("{")}
    return sorted((json.loads(ln) for ln in lines), key=lambda e: e["seq"])


@pytest.mark.parametrize("engine", ENGINES)
def test_unit_tests_pass(engine: str, tmp_path: Path) -> None:
    project = copy_project(tmp_path / "p")
    result = cli("test", "--project-dir", str(project), "--target", engine, "--log-format", "json")
    assert result.exit_code == 0, result.stderr
    done = [e for e in events(result) if e["type"] == "unit_tests.completed"]
    assert done[0]["data"] == {"passed": 3, "failed": 0}
    assert not (project / "target" / "run_results.json").exists()


@pytest.mark.parametrize("engine", ENGINES)
def test_a_wrong_model_fails_with_a_row_diff(engine: str, tmp_path: Path) -> None:
    project = copy_project(tmp_path / "broken")
    model_file = project / "models" / "revenue.py"
    text = model_file.read_text(encoding="utf-8")
    # The bug: divide by the rate instead of multiplying.
    broken = text.replace('nw.col("amount") * nw.col("rate")', 'nw.col("amount") / nw.col("rate")')
    assert broken != text
    model_file.write_text(broken, encoding="utf-8")

    args = ["--project-dir", str(project), "--target", engine, "--log-format", "json"]
    result = cli("test", *args)
    assert result.exit_code == 1
    failed = [e for e in events(result) if e["type"] == "unit_test.failed"]
    assert len(failed) == 1
    data = failed[0]["data"]
    assert data["code"] == "PLX114"
    assert data["model"] == "orders_usd"
    assert sorted(data["missing"], key=lambda r: r["order_id"]) == [
        {"order_id": 1, "amount_usd": 55.0},
        {"order_id": 2, "amount_usd": 60.0},
    ]
    assert sorted(data["unexpected"], key=lambda r: r["order_id"]) == [
        {"order_id": 1, "amount_usd": 45.45},
        {"order_id": 2, "amount_usd": 41.67},
    ]

    # build stops before a run id exists and before anything is written.
    result = cli("build", *args)
    assert result.exit_code == 1
    assert not (project / "target" / "run_results.json").exists()


@pytest.mark.parametrize("engine", ENGINES)
def test_build_runs_models_and_data_tests(engine: str, tmp_path: Path) -> None:
    project = copy_project(tmp_path / "p")
    result = cli("build", "--project-dir", str(project), "--target", engine)
    assert result.exit_code == 0, result.stderr
    doc = json.loads((project / "target" / "run_results.json").read_text(encoding="utf-8"))
    assert doc["args"]["data_tests"] is True
    rows = {r["unit_id"].split(".")[-1]: r for r in doc["results"]}
    assert {n: r["rows_written"] for n, r in rows.items()} == {
        "orders_usd": 9,
        "net_revenue": 9,
        "daily_revenue": 3,
    }
    tests = {
        e["name"]: e for r in doc["results"] for e in r["expectations"] if e["scope"] == "table"
    }
    assert set(tests) == {"order_id_unique", "order_exists", "one_row_per_day"}
    assert all(e["failed"] == 0 for e in tests.values())


def test_plain_pytest_calls_the_model_functions(tmp_path: Path) -> None:
    project = copy_project(tmp_path / "p")
    proc = subprocess.run(
        [sys.executable, "-I", "-m", "pytest", "-q", "-p", "no:cacheprovider", "tests"],
        cwd=project,
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert "2 passed" in proc.stdout
