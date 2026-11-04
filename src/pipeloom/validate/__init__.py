"""Validation layers above L1. L0 to L3 run while the project loads; L4 to L6 live here."""

from __future__ import annotations

from pipeloom.diagnostics import Outcome, Report
from pipeloom.graph.dag import ProjectGraph
from pipeloom.validate.capabilities import check_capabilities
from pipeloom.validate.semantics import check_semantics
from pipeloom.validate.static import check_static

__all__ = ["validate_project"]


def validate_project(resolved: Outcome[ProjectGraph]) -> Outcome[ProjectGraph]:
    """Run L4 to L6 on a project that passed L0 to L3.

    A failure in L0 to L3 (a None value) is returned as it is. L4, L5 and L6 run together, so
    one pass reports every finding. The value is None when any error was found.
    """
    if resolved.value is None:
        return resolved
    report = Report()
    report.extend(resolved.diagnostics)
    for check in (check_semantics, check_capabilities, check_static):
        check(resolved.value, report)
    return Outcome(None if report.has_errors else resolved.value, report.sorted())
