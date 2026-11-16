"""Reads the sample CSV that tools/databricks_live.py uploads to the volume (`sample_csv` var)."""

from typing import Any

from pipeloom import ModelContext, model


@model(tags=["stable"])
def r(ctx: ModelContext, region: Any) -> Any:
    return ctx.sql("select * from region")
