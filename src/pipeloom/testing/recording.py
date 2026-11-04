"""A sink that keeps every event, for assertions."""

from __future__ import annotations

from pipeloom.events.model import Event


class RecordingSink:
    """Records events. `fail_on` makes `emit` raise for that event type ("*" means every one)."""

    name = "recording"

    def __init__(self, fail_on: str | None = None) -> None:
        self.events: list[Event] = []
        self.fail_on = fail_on
        self.flushed = 0
        self.closed = False

    def emit(self, event: Event) -> None:
        if self.fail_on in ("*", event.type):
            raise RuntimeError(f"recording sink told to fail on {event.type}")
        self.events.append(event)

    def flush(self) -> None:
        self.flushed += 1

    def close(self) -> None:
        self.closed = True

    def types(self) -> list[str]:
        return [e.type for e in self.events]

    def assert_sequence(self, *types: str) -> None:
        """Assert the given types occur in this order; other events may sit between them."""
        remaining = iter(self.types())
        missing = [t for t in types if not any(t == got for got in remaining)]
        assert not missing, f"expected {list(types)} in order, recorded {self.types()}"
