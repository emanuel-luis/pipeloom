"""Run one unit test (ADR-0045): given rows in, the model body, expected rows compared.

Nothing is written: no materializer, namespace, catalog or state-store call.
"""

from __future__ import annotations

import json
import math
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from pipeloom.config.models import ColumnConfig, GivenConfig, UnitTestConfig
from pipeloom.engine import portable
from pipeloom.engine.protocols import EngineSession
from pipeloom.engine.types import Column, Relation, Schema
from pipeloom.plan.model import PlanNode, SqlBody
from pipeloom.quality.rows import coerce, diff_rows, infer_dtype, read_csv_rows
from pipeloom.runtime.context import ModelContext, RuntimeContext
from pipeloom.runtime.runner import call_model
from pipeloom.runtime.sql import SqlUnsupportedError, sql_runner

MESSAGE_ROWS = 10
EVENT_ROWS = 50


@dataclass(frozen=True)
class TestOutcome:
    __test__ = False  # not a pytest class

    code: str | None
    """None when the case passed, PLX114 when rows differ, PLX115 when it could not run."""
    message: str = ""
    missing: list[dict[str, Any]] = field(default_factory=list)
    """Up to 50 expected rows the output lacks, as JSON values."""
    unexpected: list[dict[str, Any]] = field(default_factory=list)
    """Up to 50 output rows that were not expected, as JSON values."""

    @property
    def passed(self) -> bool:
        return self.code is None


class _CannotRun(Exception):
    pass


def _json_row(row: Mapping[str, Any]) -> dict[str, Any]:
    clean = {k: None if isinstance(v, float) and math.isnan(v) else v for k, v in row.items()}
    out: dict[str, Any] = json.loads(json.dumps(clean, default=str))
    return out


def _rows(
    what: str,
    given: GivenConfig,
    project_dir: Path,
    types: Mapping[str, str],
) -> tuple[list[dict[str, Any]], Schema]:
    """Rows of a given or expect block, cast to their logical types. `types` holds types from
    outside the block (the upstream contract, or the output schema); the block's own `columns`
    win over them."""
    if given.csv is not None:
        try:
            header, raw = read_csv_rows(project_dir / given.csv)
        except (OSError, UnicodeDecodeError) as exc:
            reason = getattr(exc, "strerror", None) or type(exc).__name__  # no absolute path
            raise _CannotRun(f"{what}: cannot read CSV '{given.csv}': {reason}") from exc
        from_csv = True
    else:
        raw = [dict(r) for r in given.rows or ()]
        header = list(dict.fromkeys(k for r in raw for k in r))
        from_csv = False
    declared = {c.name: c.dtype for c in given.columns or ()}
    names = list(dict.fromkeys([*header, *declared]))
    schema: list[Column] = []
    for name in names:
        dtype = (
            declared.get(name)
            or types.get(name)
            or infer_dtype([r.get(name) for r in raw], from_csv=from_csv)
        )
        if dtype is None:
            raise _CannotRun(
                f"{what}: column '{name}' holds only nulls and has no type; declare it in columns"
            )
        schema.append(Column(name, dtype))
    rows = []
    for r in raw:
        row = {}
        for col in schema:
            try:
                row[col.name] = coerce(r.get(col.name), col.dtype)
            except (ValueError, TypeError, ArithmeticError) as exc:
                raise _CannotRun(
                    f"{what}: value {r.get(col.name)!r} of column '{col.name}' is not {col.dtype}"
                ) from exc
        rows.append(row)
    return rows, tuple(schema)


def _show(rows: Sequence[Mapping[str, Any]]) -> str:
    shown = ", ".join(json.dumps(r, default=str) for r in rows[:MESSAGE_ROWS])
    more = len(rows) - MESSAGE_ROWS
    return shown + (f" and {more} more" if more > 0 else "")


def _body(
    session: EngineSession,
    node: PlanNode,
    inputs: Mapping[str, Relation],
    load: Callable[[str], Callable[..., Any]],
    rt: RuntimeContext,
    sql_dialect: str | None,
) -> Relation:
    if isinstance(node.body, SqlBody):
        return session.sql(node.body.sql, inputs)
    api = node.config.frame_api
    ctx = ModelContext(
        run_id=rt.run_id,
        invocation_id=rt.invocation_id,
        unit_id=node.name,
        unique_id=node.unique_id,
        model=node.name,
        attempt=1,
        target=node.relation,
        materialized=node.materialized,
        vars=rt.vars,
        full_refresh=False,
        engine=session,
        frame_api=api,
        started_at=rt.clock.now(),
        _run_sql=sql_runner(
            session,
            inputs,
            node_dialect=node.config.sql_dialect,
            engine_dialect=sql_dialect,
            api=api,
        ),
    )
    args = {a: portable.to_frame_api(r.native, api) for a, r in inputs.items()}
    out = call_model(load(node.body.callable), ctx, args)
    return session.relation(portable.from_frame_api(out))


def run_unit_test(
    session: EngineSession,
    node: PlanNode,
    case: UnitTestConfig,
    *,
    load: Callable[[str], Callable[..., Any]],
    rt: RuntimeContext,
    upstream: Mapping[str, Sequence[ColumnConfig] | None] | None = None,
    sql_dialect: str | None = None,
) -> TestOutcome:
    """Run `case` against `node`'s body. `upstream` maps an input alias to the contract columns
    of the model it reads, which type the given rows. `load` turns `module:function` into the
    callable. `sql_dialect` is the engine's (`capabilities.sql_dialect`); None means no SQL."""
    label = f"unit test '{case.name}' of model '{node.name}'"
    if not (hasattr(session, "from_rows") and hasattr(session, "to_rows")):
        return TestOutcome(
            "PLX115", f"{label} cannot run: the engine has no from_rows or to_rows (plugin API 0.5)"
        )
    try:
        inputs: dict[str, Relation] = {}
        for alias in node.inputs:
            contract = {c.name: c.dtype for c in (upstream or {}).get(alias) or ()}
            rows, schema = _rows(f"given '{alias}'", case.given[alias], rt.project_dir, contract)
            inputs[alias] = session.from_rows(rows, schema)
        try:
            out = _body(session, node, inputs, load, rt, sql_dialect)
            actual = session.to_rows(out)
            out_types = {c.name: c.dtype for c in session.schema(out)}
        except SqlUnsupportedError as exc:
            raise _CannotRun("the model called ctx.sql, but the engine has no SQL") from exc
        except Exception as exc:
            raise _CannotRun(f"the model body raised {type(exc).__name__}: {exc}") from exc
        expected, schema = _rows("expect", case.expect, rt.project_dir, out_types)
    except _CannotRun as exc:
        return TestOutcome("PLX115", f"{label} cannot run: {exc}")
    except Exception as exc:  # noqa: BLE001 - from_rows of a plugin
        return TestOutcome("PLX115", f"{label} cannot run: {type(exc).__name__}: {exc}")
    columns = [c.name for c in schema]
    absent = [c for c in columns if c not in out_types]
    if absent:
        return TestOutcome(
            "PLX114", f"{label} failed: the output has no column {', '.join(map(repr, absent))}"
        )
    diff = diff_rows(expected, actual, columns)
    if diff.ok:
        return TestOutcome(None)
    missing = [_json_row(r) for r in diff.missing]
    unexpected = [_json_row(r) for r in diff.unexpected]
    parts = []
    if missing:
        parts.append(f"{len(missing)} expected row(s) missing: {_show(missing)}")
    if unexpected:
        parts.append(f"{len(unexpected)} unexpected row(s): {_show(unexpected)}")
    return TestOutcome(
        "PLX114",
        f"{label} failed: {'; '.join(parts)}",
        missing[:EVENT_ROWS],
        unexpected[:EVENT_ROWS],
    )
