"""One JSON object per line: to stderr, or to a file under the project."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import TextIO

from pipeloom.config.types import RelativePosixPath
from pipeloom.events.model import Event
from pipeloom.plugins import API_VERSION, OptionsModel, PluginSpec


class JsonlOptions(OptionsModel):
    path: RelativePosixPath | None = None
    """File to write, relative to the project directory. Stderr when omitted."""


def dumps(event: Event) -> str:
    """The event as one ASCII JSON line (no trailing newline), safe on any console encoding."""
    return json.dumps(event.to_json(), ensure_ascii=True, separators=(",", ":"))


class JsonlSink:
    """Writes `Event.to_json()` lines. A file is created on the first event and truncated then.

    `path=None` writes to the current `sys.stderr`. A relative `path` resolves against `root`.
    """

    name = "jsonl"

    def __init__(self, path: Path | str | None = None, root: Path | None = None) -> None:
        self._path = None if path is None else Path(path)
        self.root = root
        self._fh: TextIO | None = None

    @property
    def location(self) -> str | None:
        """The configured path as given (relative to the project), None for stderr."""
        return None if self._path is None else self._path.as_posix()

    def _open(self, path: Path) -> TextIO:
        target = path if self.root is None else self.root / path
        target.parent.mkdir(parents=True, exist_ok=True)
        return target.open("w", encoding="utf-8", newline="\n")

    def emit(self, event: Event) -> None:
        fh: TextIO
        if self._path is None:
            fh = sys.stderr
        else:
            if self._fh is None:
                self._fh = self._open(self._path)
            fh = self._fh
        fh.write(dumps(event) + "\n")
        fh.flush()

    def flush(self) -> None:
        if self._fh is not None:
            self._fh.flush()

    def close(self) -> None:
        if self._fh is not None:
            self._fh.close()
            self._fh = None


def _build(opts: JsonlOptions) -> JsonlSink:
    return JsonlSink(opts.path)


SPEC: PluginSpec[JsonlSink] = PluginSpec(
    "jsonl",
    "jsonl",
    "sinks",
    API_VERSION,
    _build,
    JsonlOptions,
    description="One JSON event per line, to stderr or a file under the project.",
)
