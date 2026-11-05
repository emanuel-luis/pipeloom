# /// script
# requires-python = ">=3.11"
# dependencies = ["duckdb==2.0.0"]
# ///
"""Generate the TPC-H tables as parquet files for the pipeloom examples.

    uv run examples/tpch_data.py [--sf 0.01] [--out examples/data/tpch]

DuckDB lives only in this script's inline environment. DECIMAL columns become DOUBLE so polars and
pandas read the files without extra care. The output is overwritten on every run.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import duckdb

TABLES = ("orders", "lineitem", "customer", "nation", "region")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--sf", type=float, default=0.01, help="TPC-H scale factor")
    parser.add_argument("--out", type=Path, default=Path("examples/data/tpch"))
    args = parser.parse_args()
    args.out.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()
    con.execute("INSTALL tpch")
    con.execute("LOAD tpch")
    con.execute(f"CALL dbgen(sf = {args.sf})")
    for table in TABLES:
        columns = con.execute(f"DESCRIBE {table}").fetchall()
        select = ", ".join(
            f"CAST({name} AS DOUBLE) AS {name}" if kind.startswith("DECIMAL") else name
            for name, kind, *_ in columns
        )
        # ORDER BY the first column keeps the file stable between runs.
        path = (args.out / f"{table}.parquet").as_posix()
        con.execute(f"COPY (SELECT {select} FROM {table} ORDER BY 1) TO '{path}' (FORMAT parquet)")
        count = con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]  # type: ignore[index]
        print(f"{path}: {count} rows")


if __name__ == "__main__":
    main()
