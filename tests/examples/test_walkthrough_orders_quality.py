"""The orders_quality walkthrough (docs/examples/orders_quality.md), end to end on DuckDB and polars.

Day 1 is clean, day 2 has rows for the quarantine, day 3 adds a column and breaks the contract.
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import Any

import polars as pl
import pytest
from click.testing import CliRunner

from pipeloom.cli import main

SOURCE = Path(__file__).resolve().parents[2] / "examples" / "orders_quality"
if not SOURCE.is_dir():  # the sdist ships tests/ without examples/
    pytest.skip("examples/ is not in this source tree", allow_module_level=True)


def build(project: Path, target: str, batch: str) -> Any:
    args = ["build", "--project-dir", str(project), "--target", target]
    return CliRunner().invoke(main, [*args, "--vars", json.dumps({"batch": batch})], env={})


def orders_result(project: Path) -> dict[str, Any]:
    doc = json.loads((project / "target" / "run_results.json").read_text(encoding="utf-8"))
    return next(r for r in doc["results"] if r["unit_id"] == "orders")


def read_table(project: Path, target: str, name: str) -> pl.DataFrame:
    if target == "polars":
        folder = project / "target" / "warehouse" / "shop" / name
        return pl.read_parquet(sorted(folder.glob("*.parquet")))
    import duckdb

    con = duckdb.connect(
        str(project / "target" / "duckdb" / "orders_quality.duckdb"), read_only=True
    )
    try:
        return con.sql(f"select * from shop.{name}").pl()
    finally:
        con.close()


@pytest.mark.parametrize("target", ["duckdb", "polars"])
def test_walkthrough(target: str, tmp_path: Path) -> None:
    project = tmp_path / "orders_quality"
    shutil.copytree(SOURCE, project, ignore=shutil.ignore_patterns("target", ".pipeloom"))

    # Day 1: clean. Nothing is dropped or quarantined, and every check passes.
    result = build(project, target, "day1")
    assert result.exit_code == 0, result.stderr
    row = orders_result(project)
    assert (row["status"], row["rows_written"], row["rows_dropped"], row["rows_quarantined"]) == (
        "succeeded",
        6,
        0,
        0,
    )
    assert all(e["failed"] == 0 for e in row["expectations"])

    # Day 2: three rows go to quarantine, one warns, one orphan is flagged after the write.
    result = build(project, target, "day2")
    assert result.exit_code == 0, result.stderr
    assert "customer_present (not_null) dropped 1 of 8 rows" in result.stderr
    assert "amount_non_negative (range) dropped 2 of 8 rows" in result.stderr
    assert "status_known (accepted_values) failed on 1 of 8 rows" in result.stderr
    row = orders_result(project)
    assert row["status"] == "succeeded"
    assert (row["rows_written"], row["rows_dropped"], row["rows_quarantined"]) == (5, 3, 3)
    checks = {e["name"]: e for e in row["expectations"]}
    assert (checks["customer_present"]["action"], checks["customer_present"]["failed"]) == (
        "drop",
        1,
    )
    assert (checks["amount_non_negative"]["action"], checks["amount_non_negative"]["failed"]) == (
        "drop",
        2,
    )
    assert (checks["status_known"]["action"], checks["status_known"]["failed"]) == ("warn", 1)
    for name in ("order_id_not_null", "order_id_unique", "customer_exists"):
        assert checks[name]["scope"] == "table"
    assert checks["order_id_not_null"]["failed"] == 0
    assert checks["order_id_unique"]["failed"] == 0
    assert checks["customer_exists"]["failed"] == 1
    assert read_table(project, target, "orders")["order_id"].sort().to_list() == [
        2001,
        2004,
        2005,
        2007,
        2008,
    ]
    quarantine = read_table(project, target, "orders__quarantine")
    assert quarantine["order_id"].sort().to_list() == [2002, 2003, 2006]
    assert "_pipeloom_failed" in quarantine.columns

    # Day 3: a new column breaks the contract. The run fails before the write.
    result = build(project, target, "day3")
    assert result.exit_code == 1
    row = orders_result(project)
    assert row["status"] == "failed"
    assert row["error"]["code"] == "PLM208"
    assert "extra column 'coupon'" in row["error"]["message"]
    assert row["rows_written"] is None
    assert read_table(project, target, "orders")["order_id"].sort().to_list() == [
        2001,
        2004,
        2005,
        2007,
        2008,
    ]
    assert read_table(project, target, "orders__quarantine").height == 3
