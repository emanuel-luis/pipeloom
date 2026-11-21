"""The tpch_polars example's SQL models run on DuckDB through its `duckdb` target: the queries
declare `sql_dialect: postgres` (the polars dialect) and are translated to DuckDB's."""

from __future__ import annotations

import json
import shutil
from pathlib import Path

import duckdb
import pytest
from click.testing import CliRunner

from pipeloom.cli import main

EXAMPLE = Path(__file__).resolve().parents[3] / "examples" / "tpch_polars"
if not EXAMPLE.is_dir():  # the sdist ships tests/ without examples/
    pytest.skip("examples/ is not in this source tree", allow_module_level=True)

# A few rows per table, enough for every join and both gold reports to be non-empty.
TABLES = {
    "region": "SELECT 0 AS r_regionkey, 'AMERICA' AS r_name",
    "nation": "SELECT * FROM (VALUES (0, 'BRAZIL', 0), (1, 'CANADA', 0)) "
    "AS t(n_nationkey, n_name, n_regionkey)",
    "customer": "SELECT * FROM (VALUES (1, 'Customer#1', 'BUILDING', 0), "
    "(2, 'Customer#2', 'MACHINERY', 1)) AS t(c_custkey, c_name, c_mktsegment, c_nationkey)",
    "orders": "SELECT * FROM (VALUES (10, 1, DATE '1995-01-15', 'F', 100.0), "
    "(11, 2, DATE '1995-02-01', 'O', 50.0), (12, 1, DATE '1995-02-20', 'F', 75.0)) "
    "AS t(o_orderkey, o_custkey, o_orderdate, o_orderstatus, o_totalprice)",
    "lineitem": "SELECT * FROM (VALUES (10, 1, 2.0, 60.0, 0.1, DATE '1995-01-20'), "
    "(10, 2, 1.0, 40.0, 0.0, DATE '1995-01-21'), (11, 1, 5.0, 50.0, 0.5, DATE '1995-02-03'), "
    "(12, 1, 3.0, 75.0, 0.2, DATE '1995-02-25')) AS t(l_orderkey, l_linenumber, l_quantity, "
    "l_extendedprice, l_discount, l_shipdate)",
}


def test_tpch_polars_sql_models_run_on_duckdb(tmp_path: Path) -> None:
    pytest.importorskip("sqlglot")
    data = tmp_path / "data"
    data.mkdir()
    con = duckdb.connect()
    for name, query in TABLES.items():
        con.execute(f"COPY ({query}) TO '{(data / name).as_posix()}.parquet' (FORMAT parquet)")
    con.close()
    project = tmp_path / "tpch_polars"
    shutil.copytree(
        EXAMPLE, project, ignore=shutil.ignore_patterns("target", ".pipeloom", "__pycache__")
    )
    result = CliRunner().invoke(
        main,
        [
            "run",
            "--project-dir",
            str(project),
            "--target",
            "duckdb",
            "--vars",
            json.dumps({"tpch_dir": data.as_posix()}),
        ],
        env={},
    )
    assert result.exit_code == 0, result.stderr
    gold = project / "target" / "duckdb" / "pipeloom_gold.duckdb"
    con = duckdb.connect(gold.as_posix(), read_only=True)
    try:
        revenue = con.execute(
            "SELECT region, nation, order_year, order_month, orders, net_revenue "
            "FROM tpch.revenue_by_region_month ORDER BY nation, order_month"
        ).fetchall()
        top = con.execute(
            "SELECT customer_key, orders, net_revenue FROM tpch.top_customers "
            "ORDER BY customer_key"
        ).fetchall()
    finally:
        con.close()
    assert revenue == [
        ("AMERICA", "BRAZIL", 1995, 1, 1, 94.0),
        ("AMERICA", "BRAZIL", 1995, 2, 1, 60.0),
        ("AMERICA", "CANADA", 1995, 2, 1, 25.0),
    ]
    assert top == [(1, 2, 154.0), (2, 1, 25.0)]
