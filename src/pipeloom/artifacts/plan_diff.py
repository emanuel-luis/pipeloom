"""`plan --diff` (ADR-0052): sort each model's change against a reference manifest into a
category, and write it as text or as a `pipeloom.plan_diff/1` document."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Final

from pipeloom.diagnostics import Report
from pipeloom.graph.selectors import CHECKSUM_VERSION, COMPONENTS
from pipeloom.plan.manifest import drop_unset
from pipeloom.plan.model import Plan, PlanNode
from pipeloom.validate.static import sql_change

PLAN_DIFF_SCHEMA: Final = "pipeloom.plan_diff/1"
CATEGORIES: Final = (
    "new",
    "removed",
    "breaking",
    "indirect-breaking",
    "non-breaking",
    "indirect-non-breaking",
    "metadata",
)
BREAKING: Final = frozenset({"removed", "breaking", "indirect-breaking"})
"""The categories `--fail-on breaking` fails on."""

_RANK: Final = {"metadata": 0, "non-breaking": 1, "breaking": 2}
# Config keys that do not change the data a model writes. Every other key, including one a
# later version adds, counts as breaking.
_METADATA_KEYS: Final = frozenset({"tags", "meta", "retries", "retry_delay", "retry_on", "timeout"})
_NON_BREAKING_KEYS: Final = frozenset({"cluster_by"})


@dataclass(frozen=True)
class Change:
    unique_id: str
    name: str
    category: str
    parts: tuple[str, ...]
    """The split checksums that differ, `new`, `removed` or `legacy`."""
    reason: str


def _contract(old: PlanNode, new: PlanNode) -> list[tuple[str, str]]:
    if old.columns is None or new.columns is None:
        return [("breaking", "contract added" if old.columns is None else "contract removed")]
    before = {c.name: c for c in old.columns}
    after = {c.name: c for c in new.columns}
    out: list[tuple[str, str]] = []
    for name, col in before.items():
        if name not in after:
            out.append(("breaking", f"column '{name}' dropped"))
        elif after[name].dtype != col.dtype:
            out.append(("breaking", f"column '{name}' retyped"))
        elif after[name].nullable != col.nullable:
            out.append(("breaking", f"column '{name}' nullability changed"))
    for name, col in after.items():
        if name not in before:
            level = "non-breaking" if col.nullable else "breaking"
            nullable = "nullable" if col.nullable else "not nullable"
            out.append((level, f"{nullable} column '{name}' added"))
    return out or [("non-breaking", "contract column order changed")]


def _config(old: PlanNode, new: PlanNode) -> list[tuple[str, str]]:
    a = drop_unset(old.config.model_dump(mode="json", by_alias=True))
    b = drop_unset(new.config.model_dump(mode="json", by_alias=True))
    out: list[tuple[str, str]] = []
    for key in sorted(a.keys() | b.keys()):
        if a.get(key) == b.get(key):
            continue
        if key in _METADATA_KEYS:
            out.append(("metadata", f"config '{key}' changed"))
        elif key in _NON_BREAKING_KEYS:
            out.append(("non-breaking", f"config '{key}' changed"))
        elif key == "expectations":
            changed = [e for e in a.get(key, []) if e not in b.get(key, [])]
            changed += [e for e in b.get(key, []) if e not in a.get(key, [])]
            drops = any(e.get("on_violation") == "drop" for e in changed)
            out.append(
                ("breaking", "an expectation that drops rows changed")
                if drops
                else ("non-breaking", "expectations changed, none drops rows")
            )
        else:
            out.append(("breaking", f"config '{key}' changed"))
    # The digest also sees `${env:...}` text the resolved config does not show.
    return out or [("breaking", "config changed in a way the manifest does not show")]


def _code(old: PlanNode, new: PlanNode, engine_dialect: str | None) -> list[tuple[str, str]]:
    if new.sql is None:
        return [("breaking", "Python code changed")]
    if old.sql is None:
        return [("breaking", "code changed and the reference has no SQL record")]
    if old.sql.shell != new.sql.shell:
        return [("breaking", "Python code around the SQL changed")]
    if len(old.sql.queries) != len(new.sql.queries):
        return [("breaking", "the number of SQL queries changed")]
    out: list[tuple[str, str]] = []
    for before, after in zip(old.sql.queries, new.sql.queries, strict=True):
        dialect = after.dialect or new.config.sql_dialect or engine_dialect
        level, why = sql_change(before.sql, after.sql, dialect)
        if level != "same":
            out.append((level, why))
    return out


def _own(old: PlanNode, new: PlanNode, engine_dialect: str | None) -> Change | None:
    """The node's own change, before its parents are taken into account."""
    found: list[tuple[str, str]] = []
    if old.checksums is None or old.checksum_version != CHECKSUM_VERSION or not new.checksums:
        if (old.checksum, old.inputs) == (new.checksum, new.inputs):
            return None
        parts: tuple[str, ...] = ("legacy",)
        found.append(("breaking", "the reference has no split checksums, so the change is unknown"))
    else:
        parts = tuple(c for c in COMPONENTS if old.checksums.get(c) != new.checksums.get(c))
        if old.inputs != new.inputs:
            found.append(("breaking", "inputs changed"))
        if "code" in parts:
            found += _code(old, new, engine_dialect)
        if "config" in parts:
            found += _config(old, new)
        if "contract" in parts:
            found += _contract(old, new)
        if "tests" in parts:
            found.append(("metadata", "data tests changed"))
    if not found:
        return None
    level = max(found, key=lambda f: _RANK[f[0]])[0]
    reason = "; ".join(dict.fromkeys(why for lv, why in found if lv == level))
    return Change(new.unique_id, new.name, level, parts, reason)


def diff_plans(ref: Plan, cur: Plan, engine_dialect: str | None, report: Report) -> list[Change]:
    """Every model that differs from the reference, sorted by name. Descendants of a breaking
    change are indirect-breaking, of a non-breaking one indirect-non-breaking, unless their own
    change ranks higher. Reports PLG309 when reference nodes have no split checksums."""
    own: dict[str, Change] = {}
    for uid, node in cur.nodes.items():
        old = ref.nodes.get(uid)
        if old is None:
            own[uid] = Change(uid, node.name, "new", ("new",), "not in the reference")
        elif (change := _own(old, node, engine_dialect)) is not None:
            own[uid] = change
    legacy = sum(
        uid in cur.nodes and (n.checksums is None or n.checksum_version != CHECKSUM_VERSION)
        for uid, n in ref.nodes.items()
    )
    if legacy:
        report.add(
            "PLG309",
            f"plan --diff compares the legacy checksum for {legacy} model(s): the reference "
            "manifest has no split checksums of this version for them, so any change is breaking",
            hint="compile the reference again with this pipeloom version",
            context={"command": "plan --diff", "models": legacy},
        )
    below = {level: _descendants(cur, own, level) for level in ("breaking", "non-breaking")}
    out: list[Change] = []
    for uid, node in cur.nodes.items():
        mine = own.get(uid)
        parts = mine.parts if mine else ("upstream",)
        if mine and mine.category in ("new", "breaking"):
            out.append(mine)
        elif uid in below["breaking"]:
            out.append(Change(uid, node.name, "indirect-breaking", parts, below["breaking"][uid]))
        elif mine and mine.category == "non-breaking":
            out.append(mine)
        elif uid in below["non-breaking"]:
            reason = below["non-breaking"][uid]
            out.append(Change(uid, node.name, "indirect-non-breaking", parts, reason))
        elif mine:
            out.append(mine)
    out += [
        Change(uid, n.name, "removed", ("removed",), "not in the project")
        for uid, n in ref.nodes.items()
        if uid not in cur.nodes
    ]
    return sorted(out, key=lambda c: (c.name, c.unique_id))


def _descendants(cur: Plan, own: Mapping[str, Change], level: str) -> dict[str, str]:
    """Models below a change of `level`, with the reason naming those changes."""
    roots: dict[str, set[str]] = {}
    for uid, change in own.items():
        if change.category != level:
            continue
        stack, seen = list(cur.child_map.get(uid, ())), set()
        while stack:
            child = stack.pop()
            if child in seen or child not in cur.nodes:
                continue
            seen.add(child)
            roots.setdefault(child, set()).add(change.name)
            stack += cur.child_map.get(child, ())
    return {uid: "downstream of " + ", ".join(sorted(names)) for uid, names in roots.items()}


def failed(changes: list[Change], fail_on: str) -> bool:
    if fail_on == "none":
        return False
    return any(fail_on == "any" or c.category in BREAKING for c in changes)


def document(plan: Plan, changes: list[Change], fail_on: str) -> dict[str, Any]:
    counts = {cat: sum(c.category == cat for c in changes) for cat in CATEGORIES}
    return {
        "schema": PLAN_DIFF_SCHEMA,
        "project": plan.metadata.project,
        "target": plan.metadata.target,
        "fail_on": fail_on,
        "failed": failed(changes, fail_on),
        "counts": counts,
        "changes": [
            {
                "unique_id": c.unique_id,
                "name": c.name,
                "category": c.category,
                "parts": list(c.parts),
                "reason": c.reason,
            }
            for c in changes
        ],
    }


def render_text(changes: list[Change]) -> str:
    """One row per model: name, category, changed parts and reason, in aligned columns."""
    if not changes:
        return "no changes"
    rows = [("model", "category", "parts", "reason")]
    rows += [(c.name, c.category, ",".join(c.parts), c.reason) for c in changes]
    widths = [max(len(r[i]) for r in rows) for i in range(3)]
    return "\n".join(
        "  ".join(cell.ljust(w) for cell, w in zip(r[:3], widths, strict=True)) + "  " + r[3]
        for r in rows
    )
