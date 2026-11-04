from __future__ import annotations

from datetime import UTC, datetime

from pipeloom.events.model import Event, Level
from pipeloom.secrets.redaction import Redactor


def event(**kw: object) -> Event:
    base: dict[str, object] = {
        "id": "x",
        "run_id": None,
        "invocation_id": "i",
        "seq": 1,
        "ts": datetime(2026, 1, 1, tzinfo=UTC),
        "type": "t",
        "level": Level.INFO,
        "data": {},
        "context": {},
    }
    return Event.model_validate({**base, **kw})


def test_masks_registered_values_in_data_and_context() -> None:
    r = Redactor()
    r.register("s3cr3t-value")
    out = r.redact(
        event(
            data={"a": "x s3cr3t-value y", "n": [1, "s3cr3t-value", {"k": "s3cr3t-value"}], "i": 3},
            context={"tok": "s3cr3t-value", "other": "fine"},
        )
    )
    assert out.data == {"a": "x *** y", "n": [1, "***", {"k": "***"}], "i": 3}
    assert out.context == {"tok": "***", "other": "fine"}


def test_short_values_are_ignored() -> None:
    r = Redactor()
    for value in ("", "a", "abc"):
        r.register(value)
    e = event(data={"a": "abc a"})
    assert r.redact(e) is e
    r.register("abcd")
    assert r.redact(event(data={"a": "abcd abc"})).data == {"a": "*** abc"}


def test_longer_values_are_masked_whole() -> None:
    r = Redactor(("pass", "password123"))
    assert r.redact(event(data={"a": "password123"})).data == {"a": "***"}


def test_the_original_event_is_not_changed() -> None:
    r = Redactor(("secret!",))
    e = event(data={"a": "secret!"})
    r.redact(e)
    assert e.data == {"a": "secret!"}
