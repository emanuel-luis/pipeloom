"""The events of one invocation, as a jsonl file in the run folder."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import TextIO

from pipeloom.events.model import Event
from pipeloom.events.sinks.jsonl import dumps


class RunFileSink:
    """Writes `runs/<run_id>/events/<invocation_id>.jsonl`, whose path `path_for` gives.

    The run id is known only after the first events, so events without one wait until it is.
    Events relayed from a child process are skipped: the child writes its own file.
    """

    name = "run_file"
    forwarded = False

    def __init__(self, path_for: Callable[[str], Path]) -> None:
        self._path_for = path_for
        self._held: list[Event] = []
        self._fh: TextIO | None = None

    def emit(self, event: Event) -> None:
        if self._fh is None:
            if event.run_id is None:
                self._held.append(event)
                return
            path = self._path_for(event.run_id)
            path.parent.mkdir(parents=True, exist_ok=True)
            self._fh = path.open("w", encoding="utf-8", newline="\n")
            held, self._held = self._held, []
            for old in held:
                self._fh.write(dumps(old) + "\n")
        self._fh.write(dumps(event) + "\n")
        self._fh.flush()

    def flush(self) -> None:
        if self._fh is not None:
            self._fh.flush()

    def close(self) -> None:
        if self._fh is not None:
            self._fh.close()
            self._fh = None
