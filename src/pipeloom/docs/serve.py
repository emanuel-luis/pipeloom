"""`docs serve`: the stdlib static file server over the generated folder."""

from __future__ import annotations

from collections.abc import Callable
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any


class _Handler(SimpleHTTPRequestHandler):
    def __init__(self, *args: Any, on_index: Callable[[], None] | None, **kw: Any) -> None:
        self._on_index = on_index
        super().__init__(*args, **kw)

    def do_GET(self) -> None:
        if self._on_index is not None and self.path.split("?", 1)[0] in ("/", "/index.html"):
            self._on_index()
        super().do_GET()


def make_server(
    directory: Path, host: str, port: int, on_index: Callable[[], None] | None = None
) -> ThreadingHTTPServer:
    """A server bound to `host:port` (port 0 picks a free one) that serves `directory`.
    `on_index` runs before each request for the index page, so it can regenerate it."""
    handler = partial(_Handler, directory=str(directory), on_index=on_index)
    return ThreadingHTTPServer((host, port), handler)
