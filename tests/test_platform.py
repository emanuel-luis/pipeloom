from __future__ import annotations

from pipeloom.platform import LocalIdentity, SystemClock


def test_clock_is_utc_aware() -> None:
    clock = SystemClock()
    assert clock.now().utcoffset() is not None and clock.now().utcoffset().total_seconds() == 0  # type: ignore[union-attr]
    assert clock.monotonic() <= clock.monotonic()


def test_identity_prefers_env() -> None:
    assert LocalIdentity({"PIPELOOM_USER": "ana"}).identity().user == "ana"
    assert LocalIdentity({}).identity().user  # falls back to the OS user
