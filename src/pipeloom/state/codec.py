"""JSON encoding of state records, shared by the stores.

A watermark value is stored as `{"dtype": ..., "value": ...}`. Dates and timestamps are ISO
text and decimals are strings; the dtype says how to read them back.
"""

from __future__ import annotations

import json
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from pipeloom.diagnostics import JsonValue
from pipeloom.state.types import LockInfo, RunRecord, UnitRecord, Watermark


def dumps(obj: Any) -> str:
    """Canonical JSON text: sorted keys, ASCII only."""
    return json.dumps(obj, sort_keys=True, ensure_ascii=True, separators=(",", ":"))


def encode_value(value: Any) -> JsonValue:
    if isinstance(value, datetime | date):  # datetime first: it is a date subclass
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    return value  # type: ignore[no-any-return]


def decode_value(dtype: str, raw: JsonValue) -> Any:
    if raw is None:
        return None
    kind = dtype.lower()
    if kind.startswith(("timestamp", "datetime")):
        return datetime.fromisoformat(str(raw))
    if kind == "date":
        return date.fromisoformat(str(raw))
    if kind.startswith(("decimal", "numeric")):
        return Decimal(str(raw))
    return raw


def value_key(w: Watermark) -> str:
    """What CAS compares: the encoded value and the dtype."""
    return dumps({"dtype": w.dtype, "value": encode_value(w.value)})


def dt_text(value: datetime | None) -> str | None:
    return None if value is None else value.isoformat()


def dt_load(text: str | None) -> datetime | None:
    return None if text is None else datetime.fromisoformat(text)


def watermark_to_json(w: Watermark) -> dict[str, JsonValue]:
    return {
        "dtype": w.dtype,
        "value": encode_value(w.value),
        "run_id": w.run_id,
        "updated_at": w.updated_at.isoformat(),
    }


def watermark_from_json(d: dict[str, Any]) -> Watermark:
    return Watermark(
        decode_value(d["dtype"], d["value"]),
        d["dtype"],
        d["run_id"],
        datetime.fromisoformat(d["updated_at"]),
    )


def run_to_json(r: RunRecord) -> dict[str, JsonValue]:
    return {
        "run_id": r.run_id,
        "project": r.project,
        "target": r.target,
        "plan_hash": r.plan_hash,
        "status": r.status,
        "started_at": r.started_at.isoformat(),
        "ended_at": dt_text(r.ended_at),
        "parent_run_id": r.parent_run_id,
        "invocation_ids": list(r.invocation_ids),
        "counts": dict(r.counts),
    }


def run_from_json(d: dict[str, Any]) -> RunRecord:
    return RunRecord(
        run_id=d["run_id"],
        project=d["project"],
        target=d["target"],
        plan_hash=d["plan_hash"],
        status=d["status"],
        started_at=datetime.fromisoformat(d["started_at"]),
        ended_at=dt_load(d["ended_at"]),
        parent_run_id=d["parent_run_id"],
        invocation_ids=tuple(d["invocation_ids"]),
        counts=dict(d["counts"]),
    )


def unit_to_json(u: UnitRecord) -> dict[str, JsonValue]:
    return {
        "run_id": u.run_id,
        "unit_id": u.unit_id,
        "attempt": u.attempt,
        "status": u.status,
        "invocation_id": u.invocation_id,
        "started_at": dt_text(u.started_at),
        "ended_at": dt_text(u.ended_at),
        "data": dict(u.data),
    }


def unit_from_json(d: dict[str, Any]) -> UnitRecord:
    return UnitRecord(
        run_id=d["run_id"],
        unit_id=d["unit_id"],
        attempt=d["attempt"],
        status=d["status"],
        invocation_id=d["invocation_id"],
        started_at=dt_load(d["started_at"]),
        ended_at=dt_load(d["ended_at"]),
        data=dict(d["data"]),
    )


def lock_to_json(i: LockInfo) -> dict[str, JsonValue]:
    return {
        "scope": i.scope,
        "run_id": i.run_id,
        "owner": i.owner,
        "acquired_at": i.acquired_at.isoformat(),
        "expires_at": i.expires_at.isoformat(),
    }


def lock_from_json(d: dict[str, Any]) -> LockInfo:
    return LockInfo(
        d["scope"],
        d["run_id"],
        d["owner"],
        datetime.fromisoformat(d["acquired_at"]),
        datetime.fromisoformat(d["expires_at"]),
    )
