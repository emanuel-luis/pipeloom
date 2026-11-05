"""is_transient on fake exceptions (no pyspark, no grpc)."""

from __future__ import annotations

from typing import Any

import pytest

from pipeloom_spark.errors import is_transient


class _Code:
    def __init__(self, name: str) -> None:
        self.name = name


def make(cls: str, *, condition: str | None = None, status: str | None = None) -> Exception:
    attrs: dict[str, Any] = {}
    if condition is not None:
        attrs["getCondition"] = lambda self: condition
    if status is not None:
        attrs["code"] = lambda self: _Code(status)
    exc: Exception = type(cls, (Exception,), attrs)("boom")
    return exc


@pytest.mark.parametrize(
    ("exc", "want"),
    [
        (make("E", condition="DELTA_CONCURRENT_APPEND"), True),
        (make("E", condition="DELTA_CONCURRENT_DELETE_READ"), True),
        (make("E", condition="DELTA_METADATA_CHANGED"), True),
        (make("E", condition="DELTA_PROTOCOL_CHANGED"), True),
        (make("E", status="UNAVAILABLE"), True),
        (make("E", status="DEADLINE_EXCEEDED"), True),
        (make("Py4JNetworkError"), True),
        (make("AnalysisException", condition="TABLE_OR_VIEW_NOT_FOUND"), False),
        (make("E", condition="PARSE_SYNTAX_ERROR"), False),
        (make("ParseException"), False),
        (make("E", status="INVALID_ARGUMENT"), None),
        (ValueError("x"), None),
    ],
)
def test_is_transient(exc: Exception, want: bool | None) -> None:
    assert is_transient(exc) is want


def test_the_cause_chain_is_searched() -> None:
    outer = RuntimeError("wrapper")
    outer.__cause__ = make("E", condition="DELTA_CONCURRENT_APPEND")
    assert is_transient(outer) is True


def test_error_class_is_the_fallback_getter() -> None:
    exc = type("E", (Exception,), {"getErrorClass": lambda self: "DELTA_CONCURRENT_APPEND"})()
    assert is_transient(exc) is True
