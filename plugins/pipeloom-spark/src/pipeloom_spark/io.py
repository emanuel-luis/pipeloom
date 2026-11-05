"""Reading source files into DataFrames."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Final

FORMATS: Final = frozenset({"delta", "parquet", "csv", "json", "ndjson"})


def string_options(options: Mapping[str, Any]) -> dict[str, str]:
    """Reader options as Spark takes them: strings, with booleans in lower case."""
    return {k: str(v).lower() if isinstance(v, bool) else str(v) for k, v in options.items()}


def read(spark: Any, fmt: str, location: str, options: Mapping[str, Any]) -> Any:
    """A DataFrame over a source path.

    csv has a header and infers types, and `json` is a multi-line document, unless the options
    say otherwise; `ndjson` is Spark's line-delimited json.
    """
    opts = string_options(options)
    if fmt == "delta":
        return spark.read.format("delta").options(**opts).load(location)
    if fmt == "parquet":
        return spark.read.options(**opts).parquet(location)
    if fmt == "csv":
        return spark.read.options(**{"header": "true", "inferSchema": "true", **opts}).csv(location)
    if fmt == "json":
        return spark.read.options(**{"multiLine": "true", **opts}).json(location)
    if fmt == "ndjson":
        return spark.read.options(**opts).json(location)
    raise ValueError(f"the spark engine cannot read format '{fmt}'")
