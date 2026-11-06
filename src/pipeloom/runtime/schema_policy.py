"""Decide a table's final schema when a batch differs from the existing target (`on_schema_change`).

Only strategies that keep existing rows (append, merge, replace_partitions) on an existing target,
outside a full refresh, are subject to the policy. Everything else replaces the schema and only
reports the difference.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Final, Literal

from pipeloom.diagnostics import JsonValue
from pipeloom.engine.errors import WriteKeyError
from pipeloom.engine.protocols import EngineSession
from pipeloom.engine.types import (
    Column,
    ColumnChange,
    ExpectationSpec,
    Relation,
    Schema,
    SchemaDiff,
    can_widen,
    diff_schemas,
)

SchemaAction = Literal["applied", "ignored", "rejected", "replaced", "none"]

KEEPS_ROWS: Final = frozenset({"append", "merge", "replace_partitions"})
POLICIES: Final = ("fail", "ignore", "append_new_columns", "sync_all_columns")


class SchemaRejected(Exception):
    """The policy does not allow the change. The runner reports it as PLM202."""

    def __init__(self, policy: str, diff: SchemaDiff, reason: str) -> None:
        super().__init__(f"{reason}: {describe(diff)}")
        self.policy = policy
        self.diff = diff


class ContractViolation(Exception):
    """The batch does not match the model contract. The runner reports it as PLM208."""


def contract_failure(
    model: str, exc: ContractViolation
) -> tuple[str, Literal["write"], ContractViolation, str, str]:
    """The unit failure a violation becomes: code, phase, cause, message and hint. The runner and
    the engine contract suite both use it."""
    return (
        "PLM208",
        "write",
        exc,
        f"model '{model}' violates its contract",
        "change the model body (cast or rename columns), or update its columns",
    )


def contract_problems(declared: Schema, batch: Schema) -> list[str]:
    """What differs between a contract and a batch schema: exact names (order ignored) and
    exact logical types (ADR-0044)."""
    have = {c.name: c.dtype for c in batch}
    want = {c.name: c.dtype for c in declared}
    out = [f"missing column '{n}'" for n in want if n not in have]
    out += [f"extra column '{n}'" for n in have if n not in want]
    out += [
        f"column '{n}' is {have[n]}, declared {t}"
        for n, t in want.items()
        if n in have and have[n] != t
    ]
    return out


def check_contract(session: EngineSession, rel: Relation, declared: Schema) -> Relation:
    """Raise `ContractViolation` unless `rel` matches `declared`; a null in a non-nullable column
    is counted with one `evaluate` call. Returns the relation to write (persisted when lazy and
    counted, so the write does not recompute it)."""
    problems = contract_problems(declared, session.schema(rel))
    if problems:
        raise ContractViolation("; ".join(problems))
    strict = [c.name for c in declared if not c.nullable]
    if not strict:
        return rel
    if rel.lazy:
        rel = session.persist(rel)
    specs = [
        ExpectationSpec(f"contract_not_null_{i}", "not_null", "fail", (name,), {})
        for i, name in enumerate(strict)
    ]
    failed = session.evaluate(rel, specs).failed
    nulls = [n for s, n in zip(specs, strict, strict=True) if failed.get(s.name, 0)]
    if nulls:
        raise ContractViolation(
            "; ".join(f"column '{n}' is declared nullable: false and holds nulls" for n in nulls)
        )
    return rel


def describe(diff: SchemaDiff) -> str:
    """The changes in one line: `added x (string), removed y, v: int64 -> string`."""
    parts: list[str] = []
    for c in diff.changes:
        if c.kind == "added":
            parts.append(f"added {c.column} ({c.new})")
        elif c.kind == "removed":
            parts.append(f"removed {c.column}")
        else:
            parts.append(f"{c.column}: {c.old} -> {c.new}")
    return ", ".join(parts)


def _kinds(diff: SchemaDiff, kind: str) -> list[ColumnChange]:
    return [c for c in diff.changes if c.kind == kind]


def change_action(policy: str, change: ColumnChange, overall: SchemaAction) -> SchemaAction:
    """What happened to one change. Only `append_new_columns` mixes: it adds columns and leaves
    removed ones in place."""
    if policy == "append_new_columns" and overall in ("applied", "ignored"):
        return "applied" if change.kind == "added" else "ignored"
    return overall


def changes_json(
    policy: str, diff: SchemaDiff, overall: SchemaAction
) -> list[dict[str, JsonValue]]:
    """The diff for `run_results`: each change with its own action."""
    return [
        {
            "kind": c.kind,
            "column": c.column,
            "old": c.old,
            "new": c.new,
            "action": change_action(policy, c, overall),
        }
        for c in diff.changes
    ]


def resolve_columns(
    policy: str,
    target: Schema | None,
    batch: Schema,
    *,
    strategy: str,
    full_refresh: bool,
    keys: Sequence[str] = (),
) -> tuple[Schema, SchemaDiff, SchemaAction]:
    """The final table schema, the diff from the target to the batch, and what was done.

    Raises `SchemaRejected` when the policy forbids the change, and `WriteKeyError` when a row
    keeping write lacks one of `keys` (unique_key and partition_by columns) in the batch: no
    policy may fill a key with nulls or drop it.
    """
    if policy not in POLICIES:
        raise ValueError(f"unknown on_schema_change policy '{policy}'")
    if target is None:
        return batch, SchemaDiff(()), "none"
    diff = diff_schemas(target, batch)
    if full_refresh or strategy not in KEEPS_ROWS:
        return batch, diff, "none" if diff.empty else "replaced"
    missing = [k for k in keys if k not in {c.name for c in batch}]
    if missing:
        raise WriteKeyError(f"the batch lacks key column(s) {', '.join(missing)}")
    if diff.empty:
        return target, diff, "none"  # a narrower batch type is cast up to the target's
    if policy == "fail":
        raise SchemaRejected(policy, diff, "the batch schema differs from the target")
    if policy == "ignore":
        return target, diff, "ignored"
    typed = _kinds(diff, "type_changed")
    if policy == "append_new_columns":
        if typed:
            raise SchemaRejected(policy, diff, "a column changed type")
        added = tuple(Column(c.column, c.new or "string") for c in _kinds(diff, "added"))
        return (*target, *added), diff, "applied" if added else "ignored"
    # sync_all_columns: the batch's columns, widened where the target's type is narrower
    target_by = {c.name: c for c in target}
    out: list[Column] = []
    for col in batch:
        old = target_by.get(col.name)
        if old is None:
            out.append(Column(col.name, col.dtype))
        elif can_widen(col.dtype, old.dtype):
            out.append(old)
        elif can_widen(old.dtype, col.dtype):
            out.append(Column(old.name, col.dtype, old.nullable))
        else:
            raise SchemaRejected(
                policy, diff, f"column '{col.name}' cannot widen from {old.dtype} to {col.dtype}"
            )
    return tuple(out), diff, "applied"
