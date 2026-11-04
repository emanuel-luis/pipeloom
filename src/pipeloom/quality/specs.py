"""From a model's `expectations:` to the engine-facing `ExpectationSpec` list."""

from __future__ import annotations

from collections.abc import Sequence

from pipeloom.config.models import ExpectationConfig, ResolvedModelConfig
from pipeloom.diagnostics import JsonValue
from pipeloom.engine.types import CheckKind, ExpectationSpec, Schema


def _spec(e: ExpectationConfig) -> ExpectationSpec:
    cols: tuple[str, ...] = ()
    params: dict[str, JsonValue] = {}
    kind: CheckKind
    if e.expr is not None:
        kind, params = "expr", {"expr": e.expr}
    else:
        check = e.check
        assert check is not None  # the config requires a check or an expr
        if check.not_null is not None:
            kind, cols = "not_null", tuple(check.not_null)
        elif check.accepted_values is not None:
            kind, cols = "accepted_values", (check.accepted_values.column,)
            params = {"values": list(check.accepted_values.values)}
        elif check.range is not None:
            kind, cols = "range", (check.range.column,)
            params = {"min": check.range.min, "max": check.range.max}
        elif check.regex is not None:
            kind, cols = "regex", (check.regex.column,)
            params = {"pattern": check.regex.pattern}
        elif check.unique is not None:
            kind, cols = "unique", tuple(check.unique)
        elif check.row_count is not None:
            kind = "row_count"
            params = {"min": check.row_count.min, "max": check.row_count.max}
        else:
            assert check.freshness is not None
            kind, cols = "freshness", (check.freshness.column,)
            params = {"max_age": check.freshness.max_age}
    return ExpectationSpec(e.name, kind, e.on_violation, cols, params)


def build_specs(cfg: ResolvedModelConfig) -> tuple[ExpectationSpec, ...]:
    """The model's expectations in declaration order."""
    return tuple(_spec(e) for e in cfg.expectations)


def check_columns(specs: Sequence[ExpectationSpec], schema: Schema) -> list[str]:
    """One message per column an expectation names that the output lacks (PLQ302)."""
    have = {c.name: c.dtype for c in schema}
    problems: list[str] = []
    for s in specs:
        missing = [c for c in s.columns if c not in have]
        if missing:
            problems.append(f"expectation '{s.name}' names unknown column(s): {', '.join(missing)}")
        elif s.kind == "regex" and have[s.columns[0]] != "string":
            problems.append(
                f"expectation '{s.name}': regex needs a string column, "
                f"'{s.columns[0]}' is {have[s.columns[0]]}"
            )
    return problems
