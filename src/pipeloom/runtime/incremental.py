"""Incremental models: resolve the watermark, filter the input, advance the watermark.

The watermark is committed after the write, with a compare-and-swap against the value read at the
start of the unit (architecture section 9). The write stores the intended watermark in its
commit metadata, so a replayed token commits exactly the original batch's watermark.
"""

from __future__ import annotations

import math
import re
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Final, Literal

from pipeloom.diagnostics import JsonValue
from pipeloom.engine.protocols import Catalog, EngineSession
from pipeloom.engine.types import ReadSpec, Schema
from pipeloom.plan.model import PlanNode
from pipeloom.platform import Clock
from pipeloom.state import codec
from pipeloom.state.types import StateKey, StateStore, Watermark

_TEMPORAL: Final = ("timestamp", "timestamp_tz", "date")
_UNITS: Final = ("s", "ms", "us", "ns")
_EPOCH: Final = datetime(1970, 1, 1, tzinfo=UTC)


class IncrementalError(Exception):
    """The watermark cannot be used (PLM205)."""


class WatermarkConflict(Exception):
    """The stored watermark moved since the unit read it (PLM203)."""


@dataclass(frozen=True)
class IncrementalState:
    is_incremental: bool
    watermark: Any
    """The watermark the batch starts from; None when the unit is not incremental."""
    lower_bound: Any
    inclusive: bool
    dtype: str
    """The logical type of the incremental column in the input."""
    expected: Watermark | None
    """What the state store held at the start; None when absent or when `source` is target."""
    seeded: bool
    """The watermark came from the target because the state store had none."""
    source: Literal["state", "target"]


def family(dtype: str) -> str:
    """The comparison family of a logical type; two types of one family compare and convert."""
    d = dtype.lower()
    if d.startswith(("timestamp", "datetime")):
        inner = d[d.find("[") + 1 : -1] if "[" in d else ""
        zones = [p.strip() for p in inner.split(",") if p.strip() and p.strip() not in _UNITS]
        return "timestamp_tz" if zones else "timestamp"
    if d.startswith("date"):
        return "date"
    if re.fullmatch(r"u?int\d*", d):
        return "integer"
    if d.startswith(("decimal", "numeric")):
        return "decimal"
    if d.startswith("float") or d == "double":
        return "float"
    if d in ("string", "utf8", "large_string", "text"):
        return "string"
    return d


def _column(schema: Schema | None, name: str) -> str | None:
    return next((c.dtype for c in schema or () if c.name == name), None)


def resolve(
    node: PlanNode,
    *,
    session: EngineSession,
    catalog: Catalog,
    state: StateStore,
    input_schema: Schema,
    full_refresh: bool,
    key: StateKey,
) -> IncrementalState:
    cfg = node.config.incremental
    target = node.relation
    assert cfg is not None and cfg.column is not None and target is not None
    column = cfg.column
    dtype = _column(input_schema, column)
    if dtype is None:
        raise IncrementalError(f"incremental column '{column}' is not in input '{cfg.input}'")
    fam = family(dtype)
    if cfg.lookback > 0 and fam not in _TEMPORAL:
        raise IncrementalError(
            f"lookback needs a timestamp or date column, but '{column}' is {dtype}"
        )
    source = cfg.watermark_source
    expected = state.get_watermark(key) if source == "state" else None
    w: Any = None
    seeded = False
    if not full_refresh and catalog.exists(target):
        if expected is not None:
            if family(expected.dtype) != fam:
                raise IncrementalError(
                    f"stored watermark is {expected.dtype} but column '{column}' is {dtype}"
                )
            w = expected.value
        else:
            target_dtype = _column(catalog.table_schema(target), column)
            if target_dtype is not None:
                if family(target_dtype) != fam:
                    raise IncrementalError(
                        f"target column '{column}' is {target_dtype} but the input has {dtype}"
                    )
                rel = session.reader("table").read(ReadSpec("table", table=target))
                w = session.max_value(rel, column)
                seeded = source == "state" and w is not None
    if w is None:
        return IncrementalState(False, None, None, True, dtype, expected, False, source)
    if cfg.lookback == 0:
        return IncrementalState(True, w, w, False, dtype, expected, seeded, source)
    if fam == "date":
        lower = w - timedelta(days=math.ceil(cfg.lookback / 86400))
    else:
        lower = w - timedelta(seconds=cfg.lookback)
    return IncrementalState(True, w, lower, True, dtype, expected, seeded, source)


def next_watermark(st: IncrementalState, batch_max: Any) -> Any:
    """The batch may be empty or sit below the watermark (lookback window): never move back."""
    if not st.is_incremental:
        return batch_max
    return st.watermark if batch_max is None else max(st.watermark, batch_max)


def commit(
    st: IncrementalState,
    value: Any,
    *,
    state: StateStore,
    key: StateKey,
    run_id: str,
    clock: Clock,
) -> bool:
    """CAS the new watermark. False means the stored value moved (PLM203).

    Nothing is written for `source: target` (the target is the watermark) or an empty first batch.
    """
    if st.source == "target" or value is None:
        return True
    return state.cas_watermark(key, st.expected, Watermark(value, st.dtype, run_id, clock.now()))


def encode(value: Any) -> JsonValue:
    return codec.encode_value(value)


def to_meta(st: IncrementalState, after: Any, column: str) -> dict[str, JsonValue]:
    """What a write stores with its token, so a replay can finish the watermark."""
    expected: JsonValue = (
        None
        if st.expected is None
        else {"value": encode(st.expected.value), "dtype": st.expected.dtype}
    )
    return {
        "column": column,
        "dtype": st.dtype,
        "source": st.source,
        "seeded": st.seeded,
        "is_incremental": st.is_incremental,
        "before": encode(st.watermark),
        "after": encode(after),
        "expected": expected,
    }


def from_meta(meta: dict[str, Any]) -> tuple[IncrementalState, Any, str]:
    """The inverse of `to_meta`: the state, the watermark to commit and the column."""
    dtype = str(meta["dtype"])
    raw = meta.get("expected")
    expected = (
        None
        if raw is None
        else Watermark(codec.decode_value(raw["dtype"], raw["value"]), raw["dtype"], "", _EPOCH)
    )
    st = IncrementalState(
        bool(meta["is_incremental"]),
        codec.decode_value(dtype, meta.get("before")),
        None,
        True,
        dtype,
        expected,
        bool(meta["seeded"]),
        meta["source"],
    )
    return st, codec.decode_value(dtype, meta.get("after")), str(meta["column"])


__all__ = [
    "IncrementalError",
    "IncrementalState",
    "WatermarkConflict",
    "commit",
    "encode",
    "family",
    "from_meta",
    "next_watermark",
    "resolve",
    "to_meta",
]
