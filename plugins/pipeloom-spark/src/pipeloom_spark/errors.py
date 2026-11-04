"""Error classification for the retry policy. Reads exceptions by attribute, no Spark import."""

from __future__ import annotations

from contextlib import suppress
from typing import Final

_DEPTH: Final = 6
_TRANSIENT_STATUS: Final = frozenset({"UNAVAILABLE", "DEADLINE_EXCEEDED"})
_TRANSIENT_CONDITIONS: Final = frozenset({"DELTA_METADATA_CHANGED", "DELTA_PROTOCOL_CHANGED"})
_PERMANENT_CLASSES: Final = frozenset({"AnalysisException", "ParseException"})


def condition(exc: BaseException) -> str | None:
    """The Spark error condition (`getCondition`, else `getErrorClass`), if the error has one."""
    for method in ("getCondition", "getErrorClass"):
        getter = getattr(exc, method, None)
        if callable(getter):
            value = None
            with suppress(Exception):  # a getter of a foreign exception type
                value = getter()
            if isinstance(value, str) and value:
                return value
    return None


def _status(exc: BaseException) -> str | None:
    code = getattr(exc, "code", None)
    if not callable(code):
        return None
    try:
        name = getattr(code(), "name", None)
    except Exception:  # noqa: BLE001 - not a gRPC error after all
        return None
    return name if isinstance(name, str) else None


def _verdict(exc: BaseException) -> bool | None:
    cond = condition(exc)
    if cond is not None:
        if cond.startswith("DELTA_CONCURRENT_") or cond in _TRANSIENT_CONDITIONS:
            return True
        if cond == "PARSE_SYNTAX_ERROR":
            return False
    if _status(exc) in _TRANSIENT_STATUS or type(exc).__name__ == "Py4JNetworkError":
        return True
    if type(exc).__name__ in _PERMANENT_CLASSES:
        return False
    return None


def is_transient(exc: BaseException) -> bool | None:
    """True for a retryable error, False for a permanent one, None for no opinion.

    Delta concurrent-modification conditions and a dropped or slow Connect channel are transient;
    an analysis or syntax error is not. The cause chain is searched and the first opinion wins.
    """
    seen: BaseException | None = exc
    for _ in range(_DEPTH):
        if seen is None:
            return None
        verdict = _verdict(seen)
        if verdict is not None:
            return verdict
        seen = seen.__cause__ or seen.__context__
    return None
