"""The customers_scd2 walkthrough: three CRM snapshots into one dimension with history.

Each run gets a fixed --run-started-at, so valid_from and valid_to are the same on every engine
and every day, and the dimension can be compared with the committed rows in expected/.
"""

from __future__ import annotations

import importlib.util
import json
import shutil
from pathlib import Path
from typing import Any

import polars as pl
import pytest
from click.testing import CliRunner

from pipeloom.cli import main

PROJECT = Path(__file__).resolve().parents[2] / "examples" / "customers_scd2"
if not PROJECT.is_dir():  # the sdist ships tests/ without examples/
    pytest.skip("examples/ is not in this source tree", allow_module_level=True)

_spec = importlib.util.spec_from_file_location("customers_scd2_show", PROJECT / "show.py")
assert _spec is not None and _spec.loader is not None
_show = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_show)

# day, rows inserted, rows updated (closed by a change), rows deleted (closed by a delete)
DAYS = [(1, 5, 0, 0), (2, 2, 2, 0), (3, 1, 0, 1)]


def run(root: Path, target: str, day: int, run_id: str | None = None) -> Any:
    result = CliRunner().invoke(
        main,
        [
            "run",
            "--project-dir",
            str(root),
            "--target",
            target,
            "--vars",
            json.dumps({"day": f"day{day}"}),
            "--run-id",
            run_id or f"{target}-day{day}",
            "--run-started-at",
            f"2026-01-0{day}T00:00:00Z",
        ],
        env={},
    )
    assert result.exit_code == 0, result.stderr
    return result


def counts(root: Path, run_id: str) -> dict[str, int]:
    doc = json.loads((root / "target" / "runs" / run_id / "run_results.json").read_text("utf-8"))
    (unit,) = doc["results"]
    assert unit["status"] == "succeeded" and unit["strategy"] == "scd2"
    return {k: unit[k] for k in ("rows_inserted", "rows_updated", "rows_deleted")}


@pytest.mark.parametrize("target", ["duckdb", "polars", "pandas"])
def test_dimension_after_each_snapshot(target: str, tmp_path: Path) -> None:
    root = tmp_path / "customers_scd2"
    shutil.copytree(
        PROJECT, root, ignore=shutil.ignore_patterns("target", ".pipeloom", "__pycache__")
    )
    assert CliRunner().invoke(main, ["validate", "--project-dir", str(root)], env={}).exit_code == 0

    for day, inserted, updated, deleted in DAYS:
        run(root, target, day)
        assert counts(root, f"{target}-day{day}") == {
            "rows_inserted": inserted,
            "rows_updated": updated,
            "rows_deleted": deleted,
        }
        expected = pl.read_csv(PROJECT / "expected" / f"dim_customers_day{day}.csv")
        actual = _show.read_dimension(root, target)
        assert actual.select(expected.columns).equals(expected, null_equal=True), actual

    # A rerun of day 3 with the same start time opens and closes nothing.
    run(root, target, 3, run_id=f"{target}-again")
    assert counts(root, f"{target}-again") == {
        "rows_inserted": 0,
        "rows_updated": 0,
        "rows_deleted": 0,
    }

    # "As of" query: who was current on 2026-01-01 and on 2026-01-02 at noon.
    dim = _show.read_dimension(root, target)
    for moment, cities in [
        ("2026-01-01T12:00:00Z", {"1": "Recife", "4": "Manaus"}),
        ("2026-01-02T12:00:00Z", {"1": "Olinda", "4": "Manaus"}),
        ("2026-01-03T12:00:00Z", {"1": "Olinda"}),
    ]:
        live = dim.filter(
            (pl.col("valid_from") <= moment)
            & (pl.col("valid_to").is_null() | (pl.col("valid_to") > moment))
        )
        found = {str(r["id"]): r["city"] for r in live.iter_rows(named=True)}
        assert {k: v for k, v in found.items() if k in cities} == cities, moment
        if moment.startswith("2026-01-03"):
            assert "4" not in found
