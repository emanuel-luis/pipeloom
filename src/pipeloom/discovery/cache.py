"""The per-file discovery cache, `<project>/.pipeloom/cache/discovery.json` (ADR-0050 point 2).

It holds parse results only, never import results: a static file's models as encoded trees (see
`ast_resolver`), a dynamic file's flag with its decorator sites, and `none` for a file without
models. A file with parse failures is not cached. An entry is reused while the file's `mtime_ns`
and size match; a header mismatch or a corrupt file means a cold run, and a failed write is
ignored, so a read-only file system only costs speed.
"""

from __future__ import annotations

import json
import os
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from pipeloom._version import __version__
from pipeloom.discovery.ast_resolver import ParsedFile, Site, StaticModel
from pipeloom.discovery.model import Param

SCHEMA = 1
CACHE_FILE = Path(".pipeloom", "cache", "discovery.json")


@dataclass(frozen=True, slots=True)
class Entry:
    mtime_ns: int
    size: int
    parsed: ParsedFile


def _header() -> dict[str, Any]:
    return {
        "schema": SCHEMA,
        "pipeloom": __version__,
        "python": f"{sys.version_info.major}.{sys.version_info.minor}",
    }


def _site_out(s: Site) -> list[Any]:
    return [s.line, s.col, {k: list(v) for k, v in s.kwarg_locations.items()}]


def _site_in(data: list[Any]) -> Site:
    line, col, locs = data
    return Site(line, col, {k: (v[0], v[1]) for k, v in locs.items()})


def _entry_out(e: Entry) -> dict[str, Any]:
    p = e.parsed
    out: dict[str, Any] = {"mtime_ns": e.mtime_ns, "size": e.size, "kind": p.kind}
    if p.kind == "static":
        out["models"] = [
            {
                "attr": m.attr,
                "site": _site_out(m.site),
                "kwargs": m.kwargs,
                "params": [[x.name, x.kind, x.has_default] for x in m.params],
            }
            for m in p.models
        ]
    elif p.kind == "dynamic":
        out["sites"] = {k: _site_out(v) for k, v in p.sites.items()}
    return out


def _entry_in(data: dict[str, Any]) -> Entry:
    kind = data["kind"]
    if kind not in ("none", "static", "dynamic"):
        raise ValueError(kind)
    parsed = ParsedFile(kind)
    for m in data.get("models", []):
        params = tuple(Param(n, k, d) for n, k, d in m["params"])
        parsed.models.append(StaticModel(m["attr"], _site_in(m["site"]), m["kwargs"], params))
    parsed.sites = {k: _site_in(v) for k, v in data.get("sites", {}).items()}
    return Entry(int(data["mtime_ns"]), int(data["size"]), parsed)


def load(project_dir: Path) -> dict[str, Entry]:
    """Cached entries by POSIX path; empty when the cache is missing, stale or corrupt."""
    try:
        data = json.loads((project_dir / CACHE_FILE).read_text(encoding="utf-8"))
        if data["header"] != _header():
            return {}
        return {path: _entry_in(e) for path, e in data["files"].items()}
    except (OSError, ValueError, KeyError, TypeError, AttributeError):
        return {}


def save(project_dir: Path, entries: dict[str, Entry]) -> None:
    """Write the cache through a temporary file; any OS error is ignored."""
    target = project_dir / CACHE_FILE
    body = {
        "header": _header(),
        "files": {p: _entry_out(e) for p, e in sorted(entries.items()) if not e.parsed.failures},
    }
    tmp: str | None = None
    try:
        target.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp = tempfile.mkstemp(prefix="discovery.", suffix=".tmp", dir=target.parent)
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as fh:
            json.dump(body, fh, separators=(",", ":"))
        os.replace(tmp, target)
        tmp = None
    except OSError:
        pass
    finally:
        if tmp is not None:
            try:
                os.unlink(tmp)
            except OSError:
                pass
