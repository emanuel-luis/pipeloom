"""Hybrid model discovery (ADR-0050 point 2): parse every model file, import only the dynamic ones.

`discover()` and `Discovery` return `DiscoveredModel` records and `DiscoveryFailure` records. They
build no diagnostics: the caller maps failures to codes.
"""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path

from pipeloom.discovery import cache
from pipeloom.discovery.ast_resolver import Site, build_models, parse_file
from pipeloom.discovery.files import SourceFile, find_files
from pipeloom.discovery.loader import ModuleLoader
from pipeloom.discovery.model import (
    DiscoveredModel,
    DiscoveryFailure,
    DiscoveryResult,
    Param,
)

__all__ = [
    "DiscoveredModel",
    "Discovery",
    "DiscoveryFailure",
    "DiscoveryResult",
    "ModuleLoader",
    "Param",
    "discover",
]


class Discovery:
    """Discovery of one project, keeping the per-file parse results between calls.

    `discover()` starts from the cache file; `refresh()` starts from what this instance last
    saw: it stats every file again, re-parses the changed ones and imports every dynamic module
    afresh (import results are never kept, so a changed helper module is always picked up).
    """

    def __init__(
        self,
        project_dir: Path,
        model_paths: Sequence[str],
        model_packages: Sequence[str] = (),
        python_paths: Sequence[str] = (".",),
        *,
        use_cache: bool = True,
    ) -> None:
        self.project_dir = project_dir.resolve()
        self.model_paths = tuple(model_paths)
        self.model_packages = tuple(model_packages)
        self.python_paths = tuple(python_paths)
        self.use_cache = use_cache
        self._entries: dict[str, cache.Entry] | None = None

    def discover(self) -> DiscoveryResult:
        self._entries = cache.load(self.project_dir) if self.use_cache else {}
        return self._run(self._entries)

    def refresh(self) -> DiscoveryResult:
        if self._entries is None:
            return self.discover()
        return self._run(self._entries)

    def _run(self, previous: dict[str, cache.Entry]) -> DiscoveryResult:
        files, failures = find_files(
            self.project_dir, self.model_paths, self.model_packages, self.python_paths
        )
        entries: dict[str, cache.Entry] = {}
        models: list[DiscoveredModel] = []
        dynamic: list[SourceFile] = []
        sites: dict[str, dict[str, Site]] = {}
        skipped: set[str] = set()
        for sf in files:
            try:
                if sf.stat is None:
                    st = sf.abs.stat()
                    mtime_ns, size = st.st_mtime_ns, st.st_size
                else:
                    mtime_ns, size = sf.stat
                old = previous.get(sf.path)
                if old and (old.mtime_ns, old.size) == (mtime_ns, size):
                    parsed = old.parsed
                else:
                    parsed = parse_file(sf.abs.read_bytes(), sf.path)
            except OSError:
                continue  # removed while scanning
            entries[sf.path] = cache.Entry(mtime_ns, size, parsed)
            failures.extend(parsed.failures)
            if parsed.kind == "static":
                if sf.module is None:
                    failures.append(_outside(sf))
                    continue
                found, bad = build_models(parsed, sf.path, sf.module)
                models += found
                failures += bad
            elif parsed.kind == "dynamic" and not parsed.failures:
                dynamic.append(sf)
                sites[sf.path] = parsed.sites
            elif parsed.kind == "dynamic":
                skipped.update(parsed.sites)
        self._entries = entries
        # A file with parse failures is never stored, so it must not make the cache look stale.
        if self.use_cache and {p: e for p, e in entries.items() if not e.parsed.failures} != {
            p: e for p, e in previous.items() if not e.parsed.failures
        }:
            cache.save(self.project_dir, entries)

        if dynamic:
            known = {
                sf.module
                for sf in files
                if sf.module and (e := entries.get(sf.path)) and e.parsed.kind != "none"
            }
            outside = [sf.abs.parent for sf in files if sf.path.startswith("../")]
            with ModuleLoader(self.project_dir, self.python_paths, outside) as loader:
                found, bad = loader.import_models(dynamic, sites, known)
            models += found
            failures += bad

        return DiscoveryResult(
            tuple(sorted(models, key=lambda m: (m.file, m.line, m.attr))),
            tuple(sorted(failures, key=lambda f: (f.file, f.line or 0, f.col or 0, f.kind))),
            frozenset(skipped),
        )


def _outside(sf: SourceFile) -> DiscoveryFailure:
    return DiscoveryFailure(
        sf.path, None, None, "outside_roots", f"'{sf.path}' is not under any python_paths root"
    )


def discover(
    project_dir: Path,
    model_paths: Sequence[str],
    model_packages: Sequence[str] = (),
    python_paths: Sequence[str] = (".",),
    *,
    use_cache: bool = True,
) -> DiscoveryResult:
    """Discover the models of a project once."""
    return Discovery(
        project_dir, model_paths, model_packages, python_paths, use_cache=use_cache
    ).discover()
