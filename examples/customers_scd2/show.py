"""Print the dim_customers table of a target: `uv run examples/customers_scd2/show.py [target]`.

Run it from the repository root, or pass --project-dir. Not part of the pipeline.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import polars as pl

HERE = Path(__file__).resolve().parent


def read_dimension(project_dir: Path, target: str) -> pl.DataFrame:
    """The table as a frame, sorted by id and valid_from, timestamps as UTC text."""
    if target == "duckdb":
        import duckdb

        con = duckdb.connect(str(project_dir / "target/duckdb/crm.duckdb"), read_only=True)
        frame = con.sql("select * from crm.crm.dim_customers").pl()
        con.close()
    else:
        table = project_dir / f"target/warehouse-{target}/crm/dim_customers"
        frame = pl.read_parquet(sorted(table.glob("*.parquet")))
    # Show the run clock in UTC, whatever zone the engine returns.
    stamps = [
        pl.col(c).dt.convert_time_zone("UTC").dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        for c in ("valid_from", "valid_to")
    ]
    return frame.sort("id", "valid_from").with_columns(stamps)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("target", nargs="?", default="duckdb")
    parser.add_argument("--project-dir", type=Path, default=HERE)
    args = parser.parse_args()
    print(read_dimension(args.project_dir, args.target).write_csv(), end="")
