"""Mask registered secret values in events before any sink sees them."""

from __future__ import annotations

import threading
from typing import Any, Final

from pipeloom.events.model import Event

MASK: Final = "***"
MIN_LENGTH: Final = 4


class Redactor:
    """Replaces every occurrence of a registered value in the strings of `data` and `context`."""

    def __init__(self, values: tuple[str, ...] = ()) -> None:
        self._lock = threading.Lock()
        self._values: tuple[str, ...] = ()
        for value in values:
            self.register(value)

    def register(self, value: str) -> None:
        """Add a value. Values shorter than 4 characters are ignored: they would mask too much."""
        if len(value) < MIN_LENGTH:
            return
        with self._lock:
            if value not in self._values:
                # Longest first, so a value that contains another is masked whole.
                self._values = tuple(sorted((*self._values, value), key=lambda v: (-len(v), v)))

    def mask(self, text: str) -> str:
        for value in self._values:
            text = text.replace(value, MASK)
        return text

    def _walk(self, node: Any) -> Any:
        if isinstance(node, str):
            return self.mask(node)
        if isinstance(node, list):
            return [self._walk(v) for v in node]
        if isinstance(node, dict):
            return {k: self._walk(v) for k, v in node.items()}
        return node

    def scrub(self, node: Any) -> Any:
        """A copy of any JSON-like value with the registered values masked in its strings.

        For text that is written outside the event bus: attempt files, run results, state."""
        return self._walk(node) if self._values else node

    def redact(self, event: Event) -> Event:
        if not self._values:
            return event
        data = self._walk(event.data)
        assert isinstance(data, dict)
        context = {k: self.mask(v) for k, v in event.context.items()}
        return event.model_copy(update={"data": data, "context": context})
