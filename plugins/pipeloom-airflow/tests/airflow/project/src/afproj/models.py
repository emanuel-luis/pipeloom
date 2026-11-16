"""Models of the Airflow test project. AFPROJ_MODE makes `other` fail or hang."""

import os
import time
from typing import Any

import polars as pl

from pipeloom import model


@model
def base(ctx: Any) -> Any:
    return pl.DataFrame({"id": [1, 2, 3]})


@model
def child(ctx: Any, base: Any) -> Any:
    return base.filter(pl.col("id") > 1)


@model
def other(ctx: Any) -> Any:
    mode = os.environ.get("AFPROJ_MODE", "")
    if mode == "permanent":
        raise ValueError("bad data")
    if mode == "transient":
        raise ConnectionError("network blip")
    if mode == "sleep":
        time.sleep(120)
    return pl.DataFrame({"x": [1]})
