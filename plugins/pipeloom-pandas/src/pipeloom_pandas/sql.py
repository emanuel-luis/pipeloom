"""`ctx.sql` on pandas: DuckDB runs the query over the input frames (ADR-0068).

pandas has no SQL of its own. When DuckDB is installed (`pipeloom-pandas[sql]`), each query gets a
fresh in-memory DuckDB connection: the input frames are registered under their aliases as Arrow
tables (no copy for Arrow-backed columns), the query runs, and the result comes back as an
Arrow-backed DataFrame, the same dtypes the pandas reader produces.
"""

from __future__ import annotations

from collections.abc import Mapping

import duckdb
import pandas as pd
import pyarrow as pa

# UTC so timestamps with a time zone never come back in the machine's local zone, and no
# extension is ever downloaded on its own (the same policy as the DuckDB engine).
_CONFIG: dict[str, str | bool | int | float | list[str]] = {
    "TimeZone": "UTC",
    "autoinstall_known_extensions": False,
}


def run(query: str, frames: Mapping[str, pd.DataFrame]) -> pd.DataFrame:
    """The rows of `query`, with each alias in `frames` bound to its DataFrame."""
    con = duckdb.connect(config=_CONFIG)
    try:
        for alias, frame in frames.items():
            con.register(alias, pa.Table.from_pandas(frame, preserve_index=False))
        rel = con.sql(query)
        if rel is None:
            raise ValueError("ctx.sql on pandas takes a query that returns rows")
        # to_arrow_table is the current name; fetch_arrow_table is the one older DuckDB has.
        fetch = getattr(rel, "to_arrow_table", None) or rel.fetch_arrow_table
        table: pa.Table = fetch()
        return table.to_pandas(types_mapper=pd.ArrowDtype)
    finally:
        con.close()
