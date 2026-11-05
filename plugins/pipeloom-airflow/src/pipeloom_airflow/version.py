"""The Airflow versions the plugin supports."""

from __future__ import annotations

import re
from typing import Final

from pipeloom.diagnostics import Report

MIN_AIRFLOW: Final = (3, 2)
_VERSION = re.compile(r"(\d+)\.(\d+)")


def check_airflow_version(version: str, report: Report) -> bool:
    """True for 3.2 up to (not including) 4.0; otherwise report PLA005 and return False."""
    m = _VERSION.match(version)
    if m is not None and MIN_AIRFLOW <= (int(m[1]), int(m[2])) < (4, 0):
        return True
    report.add(
        "PLA005",
        f"Airflow {version} is not supported",
        hint="install apache-airflow>=3.2,<4",
        context={"airflow": version},
    )
    return False
