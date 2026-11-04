"""Judge the counts an engine returned: pass or fail per expectation, drop ratio, failures."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Any

from pipeloom.diagnostics import JsonValue
from pipeloom.engine.types import ROW_CHECKS, ExpectationSpec, QualityCounts

MAX_DROP_RATIO = "max_drop_ratio"


@dataclass(frozen=True)
class ExpectationResult:
    name: str
    kind: str
    action: str
    passed: int | None
    """Rows that passed a row check; None for an aggregate check."""
    failed: int
    """Violating rows for a row check or `unique`; 1 or 0 for the other aggregate checks."""
    value: JsonValue | None
    """The measured value of an aggregate check: duplicates, rows, or the age in seconds."""

    def to_json(self) -> dict[str, JsonValue]:
        return {
            "name": self.name,
            "kind": self.kind,
            "action": self.action,
            "passed": self.passed,
            "failed": self.failed,
            "value": self.value,
        }


@dataclass(frozen=True)
class QualityReport:
    total_rows: int
    results: tuple[ExpectationResult, ...]
    rows_dropped: int
    rows_quarantined: int
    drop_ratio: float
    cached: bool
    failures: tuple[str, ...]
    """Names with action fail and failed > 0, plus `max_drop_ratio` when exceeded."""

    @property
    def warnings(self) -> tuple[ExpectationResult, ...]:
        return tuple(r for r in self.results if r.action == "warn" and r.failed > 0)


def _as_utc(value: Any) -> datetime | None:
    if isinstance(value, str):
        value = datetime.fromisoformat(value)
    if isinstance(value, datetime):
        return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)
    if isinstance(value, date):
        return datetime(value.year, value.month, value.day, tzinfo=UTC)
    return None


def _aggregate(spec: ExpectationSpec, value: Any, now: datetime) -> ExpectationResult:
    failed = 0
    shown: JsonValue | None = None
    if spec.kind == "unique":
        failed = shown = int(value)
    elif spec.kind == "row_count":
        low, high = spec.params.get("min"), spec.params.get("max")
        shown = int(value)
        bad = (isinstance(low, int | float) and shown < low) or (
            isinstance(high, int | float) and shown > high
        )
        failed = int(bad)
    else:  # freshness: an empty or all-null column fails
        latest = _as_utc(value)
        limit = spec.params.get("max_age")
        if latest is None:
            failed = 1
        else:
            shown = round((now.astimezone(UTC) - latest).total_seconds(), 3)
            failed = int(isinstance(limit, int | float) and shown > limit)
    return ExpectationResult(spec.name, spec.kind, spec.action, None, failed, shown)


def judge(
    specs: Sequence[ExpectationSpec],
    row_counts: QualityCounts,
    agg_counts: QualityCounts | None,
    *,
    now: datetime,
    max_drop_ratio: float | None,
    cached: bool,
    rows_quarantined: int = 0,
) -> QualityReport:
    """`row_counts` come from the full output, `agg_counts` from what is kept after the drops
    (it may be `row_counts` itself when nothing drops)."""
    total = row_counts.total_rows
    aggregates = {} if agg_counts is None else agg_counts.aggregates
    results: list[ExpectationResult] = []
    for spec in specs:
        if spec.kind in ROW_CHECKS:
            failed = row_counts.failed[spec.name]
            results.append(
                ExpectationResult(spec.name, spec.kind, spec.action, total - failed, failed, None)
            )
        else:
            results.append(_aggregate(spec, aggregates[spec.name], now))
    ratio = row_counts.dropped / total if total else 0.0
    failures = [r.name for r in results if r.action == "fail" and r.failed > 0]
    if max_drop_ratio is not None and ratio > max_drop_ratio:
        failures.append(MAX_DROP_RATIO)
    return QualityReport(
        total_rows=total,
        results=tuple(results),
        rows_dropped=row_counts.dropped,
        rows_quarantined=rows_quarantined,
        drop_ratio=ratio,
        cached=cached,
        failures=tuple(failures),
    )
