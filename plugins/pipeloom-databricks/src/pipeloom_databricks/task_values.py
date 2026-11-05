"""The task value a unit task leaves for the job: a small summary of what its units did.

Task values are documented for notebooks; wheel tasks are unconfirmed, so everything here is
best effort. This is one of the three modules that import `databricks.sdk`, and it does so
lazily, only on a Databricks runtime.
"""

from __future__ import annotations

import json
import os
from collections.abc import Mapping, Sequence
from typing import Final, Protocol

from pydantic import JsonValue

TASK_VALUE_KEY: Final = "pipeloom"
TASK_VALUE_MAX_BYTES: Final = 32_768
"""Well under the 48 KiB Databricks allows for one value."""


class TaskValues(Protocol):
    def set(self, key: str, value: JsonValue) -> None: ...


def _size(doc: Mapping[str, JsonValue]) -> int:
    return len(json.dumps(doc, separators=(",", ":"), ensure_ascii=True).encode("ascii"))


def summary(
    run_id: str, status: str, rows: Sequence[Mapping[str, JsonValue]]
) -> dict[str, JsonValue]:
    """`{v, run_id, status, units}` from the newest attempt row of each unit.

    Above `TASK_VALUE_MAX_BYTES` the units are dropped from the end and `truncated` is true.
    """
    units: dict[str, JsonValue] = {}
    for row in sorted(rows, key=lambda r: str(r["unit_id"])):
        error = row.get("error")
        warnings = row.get("warnings")
        units[str(row["unit_id"])] = {
            "status": row.get("status"),
            "attempt": row.get("attempt"),
            "rows_written": row.get("rows_written"),
            "duration_ms": row.get("duration_ms"),
            "target_version": row.get("target_version"),
            "error_code": error.get("code") if isinstance(error, dict) else None,
            "warnings": len(warnings) if isinstance(warnings, list) else 0,
        }
    doc: dict[str, JsonValue] = {"v": 1, "run_id": run_id, "status": status, "units": units}
    if _size(doc) <= TASK_VALUE_MAX_BYTES:
        return doc
    doc["truncated"] = True
    while units and _size(doc) > TASK_VALUE_MAX_BYTES:
        units.popitem()
    return doc


def runtime_task_values() -> TaskValues | None:
    """`dbutils.jobs.taskValues` on a Databricks runtime, else None."""
    if not os.environ.get("DATABRICKS_RUNTIME_VERSION"):
        return None
    try:
        from databricks.sdk.runtime import dbutils

        values: TaskValues = dbutils.jobs.taskValues
    except Exception:  # noqa: BLE001 - no runtime utilities: skip the task value
        return None
    return values
