"""`docs serve`: the stdlib static file server over the generated folder."""

from __future__ import annotations

from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


def make_server(directory: Path, host: str, port: int) -> ThreadingHTTPServer:
    """A server bound to `host:port` (port 0 picks a free one) that serves `directory`."""
    handler = partial(SimpleHTTPRequestHandler, directory=str(directory))
    return ThreadingHTTPServer((host, port), handler)
