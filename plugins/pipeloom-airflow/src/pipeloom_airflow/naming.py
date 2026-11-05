"""Ids: the pipeloom run id, Airflow task ids and asset URIs. Pure and deterministic."""

from __future__ import annotations

import hashlib
import re
from collections import Counter
from collections.abc import Collection, Sequence
from typing import Final
from urllib.parse import quote

TASK_ID_MAX: Final = 200
FINALIZE_ID: Final = "pipeloom_finalize"
LAUNCH_ID: Final = "pipeloom_launch"
_UNSAFE = re.compile(r"[^A-Za-z0-9_.-]")


def _hash8(raw: str) -> str:
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:8]


def airflow_run_id(dag_id: str, run_id: str, group_id: str | None = None) -> str:
    """`af-` and 16 hex of sha256 over the parts, NUL separated so no two inputs concatenate
    alike. The group id keeps two task groups of one DAG run apart."""
    parts = [dag_id, run_id] if group_id is None else [dag_id, run_id, group_id]
    return "af-" + hashlib.sha256("\0".join(parts).encode("utf-8")).hexdigest()[:16]


def task_ids(raw: Sequence[str], *, reserved: Collection[str]) -> dict[str, str]:
    """Raw name -> Airflow task id. Unsafe characters become `_`; an id above 200 characters,
    one that collides with another, or one equal to a reserved id ends in `_` and 8 hex of the
    sha256 of the raw name."""
    unique = list(dict.fromkeys(raw))
    base = {r: _UNSAFE.sub("_", r) for r in unique}
    counts = Counter(base.values())
    out: dict[str, str] = {}
    for r, b in base.items():
        if len(b) > TASK_ID_MAX or counts[b] > 1 or b in reserved:
            b = f"{b[: TASK_ID_MAX - 9]}_{_hash8(r)}"
        out[r] = b
    return out


PLACEHOLDERS: Final = ("project", "target", "model", "relation", "unique_id")


def asset_uri(
    template: str, *, project: str, target: str, model: str, relation: str, unique_id: str
) -> str:
    """Fill the placeholders; each value is percent-encoded so the URI stays one string."""
    values = {
        "project": project,
        "target": target,
        "model": model,
        "relation": relation,
        "unique_id": unique_id,
    }
    return template.format_map({k: quote(v, safe="._-~") for k, v in values.items()})
