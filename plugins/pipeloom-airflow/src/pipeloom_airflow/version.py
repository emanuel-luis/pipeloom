"""The Airflow versions the plugin supports."""

from __future__ import annotations

import importlib.metadata
import re
from typing import Final

from pipeloom.diagnostics import Report
from pipeloom_airflow.errors import PipeloomAirflowError

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


def require_airflow() -> None:
    """Raise `PipeloomAirflowError` (PLA005) unless a supported Airflow is installed. Reads the
    distribution metadata only, so it runs before anything imports `airflow`."""
    report = Report()
    version = None
    # `apache-airflow` is a wrapper over `apache-airflow-core` in Airflow 3; either may be alone
    for dist in ("apache-airflow-core", "apache-airflow"):
        try:
            version = importlib.metadata.version(dist)
            break
        except importlib.metadata.PackageNotFoundError:
            continue
    if version is None:
        report.add(
            "PLA005",
            "Airflow is not installed",
            hint="install pipeloom-airflow[airflow] (apache-airflow>=3.2,<4)",
        )
    else:
        check_airflow_version(version, report)
    if report.has_errors:
        raise PipeloomAirflowError(report.sorted())
