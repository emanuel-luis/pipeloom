"""Expectations: build the checks a model declares, judge what an engine counted, and name the
quarantine table. Engines count; this package decides."""

from __future__ import annotations

from pipeloom.quality.judge import ExpectationResult, QualityReport, judge
from pipeloom.quality.quarantine import (
    QUARANTINE_SUFFIX,
    quarantine_target,
    quarantine_token,
)
from pipeloom.quality.specs import build_specs, check_columns

__all__ = [
    "QUARANTINE_SUFFIX",
    "ExpectationResult",
    "QualityReport",
    "build_specs",
    "check_columns",
    "judge",
    "quarantine_target",
    "quarantine_token",
]
