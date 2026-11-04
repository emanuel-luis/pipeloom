"""Where dropped rows go: `<model>__quarantine`, written append-only under its own token."""

from __future__ import annotations

from typing import Final

from pipeloom.engine.types import TargetRef

QUARANTINE_SUFFIX: Final = "__quarantine"
RUN_ID_COLUMN: Final = "_pipeloom_run_id"
FAILED_COLUMN: Final = "_pipeloom_failed"
TS_COLUMN: Final = "_pipeloom_ts"
"""`_pipeloom_failed` is `list<string>`, or JSON text in a `string` column on csv catalogs."""


def quarantine_target(target: TargetRef) -> TargetRef:
    return TargetRef(target.catalog, target.schema, target.name + QUARANTINE_SUFFIX)


def quarantine_token(token: str) -> str:
    """The idempotency token of the quarantine write for the model write `token`."""
    return f"{token}#quarantine"
