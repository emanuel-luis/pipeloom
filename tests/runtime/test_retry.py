"""Classification, backoff, the cancel token and `write_json_once`."""

from __future__ import annotations

import io
import threading
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from hypothesis import given
from hypothesis import strategies as st
from rich.console import Console

from pipeloom.artifacts.io import write_json_once
from pipeloom.engine.errors import CommitConflict, TransientError
from pipeloom.events.bus import EventBus
from pipeloom.events.model import ErrorInfo, UnitRetrying, UnitTimedOut
from pipeloom.events.sinks.console import ConsoleSink
from pipeloom.runtime.control import CancelToken, RunControl, UnitInterrupted, interrupt_error
from pipeloom.runtime.retry import (
    BACKOFF_CAP,
    UnitPolicy,
    backoff_delay,
    classify,
    should_retry,
)
from pipeloom.testing.clock import FakeClock


class Opinion:
    def __init__(self, answer: bool | None) -> None:
        self.answer = answer

    def is_transient(self, exc: BaseException) -> bool | None:
        return self.answer


def chained(outer: BaseException, inner: BaseException) -> BaseException:
    outer.__cause__ = inner
    return outer


@pytest.mark.parametrize(
    ("exc", "phase", "expected"),
    [
        (TransientError("x"), "body", "transient"),
        (CommitConflict("x"), "write", "transient"),
        (chained(RuntimeError("w"), TransientError("x")), "body", "transient"),
        (ConnectionResetError("x"), "body", "transient"),
        (TimeoutError("x"), "body", "transient"),
        (OSError("x"), "read", "transient"),
        (OSError("x"), "write", "transient"),
        (OSError("x"), "state", "transient"),
        (OSError("x"), "body", "permanent"),
        (FileNotFoundError("x"), "read", "permanent"),
        (PermissionError("x"), "write", "permanent"),
        (IsADirectoryError("x"), "write", "permanent"),
        (FileExistsError("x"), "write", "permanent"),
        (ValueError("x"), "write", "permanent"),
        (chained(RuntimeError("w"), OSError("x")), "read", "transient"),
    ],
)
def test_classify_table(exc: BaseException, phase: str, expected: str) -> None:
    assert classify(exc, phase=phase, session=None) == expected  # type: ignore[arg-type]


def test_the_engine_opinion_comes_before_the_builtin_rules() -> None:
    assert classify(ValueError("x"), phase="body", session=Opinion(True)) == "transient"  # type: ignore[arg-type]
    assert classify(OSError("x"), phase="read", session=Opinion(False)) == "permanent"  # type: ignore[arg-type]
    assert classify(OSError("x"), phase="read", session=Opinion(None)) == "transient"  # type: ignore[arg-type]
    # a TransientError is transient whatever the engine thinks
    assert classify(TransientError("x"), phase="body", session=Opinion(False)) == "transient"  # type: ignore[arg-type]


def test_a_cause_chain_deeper_than_the_limit_is_not_followed() -> None:
    exc: BaseException = TransientError("deep")
    for _ in range(12):
        exc = chained(RuntimeError("w"), exc)
    assert classify(exc, phase="body", session=None) == "permanent"


@given(
    st.floats(0.0, 1000.0),
    st.integers(1, 20),
    st.text(max_size=8),
)
def test_backoff_stays_between_half_and_the_cap(base: float, n: int, seed: str) -> None:
    d = min(max(base, BACKOFF_CAP), base * 2 ** (n - 1))
    delay = backoff_delay(base, n, seed=seed)
    assert d / 2 - 1e-9 <= delay <= d + 1e-9
    assert delay <= max(base, BACKOFF_CAP)
    assert backoff_delay(base, n, seed=seed) == delay  # reproducible


def test_backoff_doubles_until_the_cap() -> None:
    assert backoff_delay(0, 5, seed="s") == 0
    ceilings = [backoff_delay(10, n, seed="s") for n in (1, 2, 3)]
    assert 5 <= ceilings[0] <= 10 and 10 <= ceilings[1] <= 20 and 20 <= ceilings[2] <= 40
    assert backoff_delay(10, 30, seed="s") <= BACKOFF_CAP


def test_should_retry_rules() -> None:
    transient = UnitPolicy(2, 1.0, "transient", None)
    anything = UnitPolicy(2, 1.0, "any", None)
    assert should_retry(transient, 0, "transient")
    assert not should_retry(transient, 0, "permanent")
    assert not should_retry(transient, 0, None)
    assert should_retry(anything, 1, "permanent")
    assert not should_retry(anything, 2, "transient")


def test_the_first_reason_wins_and_check_raises() -> None:
    token = CancelToken()
    token.check()
    assert not token.is_set()
    token.set("timeout")
    token.set("cancel")
    assert token.is_set() and token.reason == "timeout"
    with pytest.raises(UnitInterrupted) as caught:
        token.check()
    assert caught.value.reason == "timeout"


def test_run_control_is_sticky_and_thread_safe() -> None:
    control = RunControl()
    assert not control.stop_requested and not control.hard
    threads = [
        threading.Thread(target=control.request_stop, kwargs={"signal_name": "SIGINT"})
        for _ in range(4)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert control.stop_requested and not control.hard and control.signal_name == "SIGINT"
    control.request_stop(hard=True)
    assert control.hard


def test_interrupt_errors_carry_the_codes() -> None:
    assert interrupt_error("timeout", "u", 2, 1.5).code == "PLX102"
    assert "1.5s" in interrupt_error("timeout", "u", 2, 1.5).message
    assert interrupt_error("cancel", "u", 1, None).code == "PLX104"


def test_write_json_once_keeps_the_first_file(tmp_path: Path) -> None:
    path = tmp_path / "a" / "attempt-1.json"
    assert write_json_once(path, {"status": "timed_out"})
    assert not write_json_once(path, {"status": "succeeded"})
    assert '"timed_out"' in path.read_text(encoding="utf-8")
    assert [p.name for p in path.parent.iterdir()] == ["attempt-1.json"]  # no temp files left


def test_write_json_once_has_one_winner_among_threads(tmp_path: Path) -> None:
    path = tmp_path / "attempt-1.json"
    wins: list[bool] = []
    threads = [
        threading.Thread(target=lambda i=i: wins.append(write_json_once(path, {"i": i})))
        for i in range(8)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert wins.count(True) == 1
    assert [p.name for p in tmp_path.iterdir()] == ["attempt-1.json"]


def test_console_lines_for_retry_and_timeout() -> None:
    out = io.StringIO()
    console = Console(file=out, width=120, color_system=None, force_terminal=False)
    clock = FakeClock(datetime(2026, 1, 1, 12, tzinfo=UTC), step=timedelta(0))
    err = ErrorInfo(code="PLM204", category="transient", message="cannot read")
    with EventBus([(ConsoleSink(console, tz=UTC), 5)], clock=clock, invocation_id="i") as bus:
        bus.emit(
            UnitRetrying(attempt=1, next_attempt=2, delay_ms=2300, error=err),
            unit_id="a",
            model="a",
            attempt=1,
        )
        bus.emit(UnitTimedOut(timeout_s=30, attempt=1), unit_id="b", model="b", attempt=1)
    text = out.getvalue()
    assert "RETRY  a attempt 1 failed PLM204 (transient), next in 2.3s" in text
    assert "TIMEOUT b" in text and "PLX102 after 30s" in text
