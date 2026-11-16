"""Find the Python files that may hold models: `model_paths` entries and `model_packages`."""

from __future__ import annotations

import importlib.util
import os
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from pathlib import Path

from pipeloom.discovery.model import DiscoveryFailure

SKIP_DIRS = frozenset({"__pycache__", "node_modules", "site-packages", "dist-packages"})


@dataclass(frozen=True, slots=True)
class SourceFile:
    """`path` is POSIX and relative to the project dir (with `..` for a package outside it).
    `module` is the dotted import name, or None when no `python_paths` root contains the file."""

    path: str
    abs: Path
    module: str | None
    stat: tuple[int, int] | None = None
    """`(st_mtime_ns, st_size)` taken during the walk, when it was."""


Stat = tuple[int, int]


def _walk(root: Path) -> Iterator[tuple[Path, Stat | None]]:
    """Every `*.py` below `root` with its stat from the directory entry (no extra stat call on
    Windows, where `DirEntry.stat()` is served from the directory listing)."""
    stack = [str(root)]
    while stack:
        top = stack.pop()
        try:
            with os.scandir(top) as it:
                entries = list(it)
        except OSError:
            continue
        for e in entries:
            name = e.name
            if name.startswith("."):
                continue
            try:
                if e.is_dir(follow_symlinks=False):
                    if name not in SKIP_DIRS and not os.path.exists(
                        os.path.join(e.path, "pyvenv.cfg")
                    ):
                        stack.append(e.path)
                elif name.endswith(".py"):
                    st = e.stat()
                    yield Path(e.path), (st.st_mtime_ns, st.st_size)
            except OSError:
                continue


def _skipped(path: Path, base: Path) -> bool:
    parts = path.relative_to(base).parts if path.is_relative_to(base) else path.parts
    return any(p.startswith(".") or p in SKIP_DIRS for p in parts[:-1])


def module_name(file: Path, roots: Sequence[Path]) -> str | None:
    """The dotted name of `file` under the longest root that contains it."""
    best = max(
        (r for r in roots if file.is_relative_to(r)), key=lambda r: len(r.parts), default=None
    )
    if best is None:
        return None
    parts = list(file.relative_to(best).with_suffix("").parts)
    if parts and parts[-1] == "__init__":
        parts.pop()
    return ".".join(parts) or None


def _rel(path: Path, project_dir: Path) -> str | None:
    text, base = str(path), str(project_dir) + os.sep
    if text.startswith(base):  # the common case, without relpath
        return text[len(base) :].replace(os.sep, "/")
    try:
        return Path(os.path.relpath(path, project_dir)).as_posix()
    except ValueError:  # another drive on Windows
        return None


def _fast_module(path: Path, roots: Sequence[str]) -> str | None:
    """`module_name` with string operations; `roots` end with a separator, longest first."""
    text = str(path)
    for root in roots:
        if text.startswith(root):
            parts = text[len(root) : -len(".py")].split(os.sep)
            if parts[-1] == "__init__":
                parts.pop()
            return ".".join(parts) or None
    return None


def find_files(
    project_dir: Path,
    model_paths: Sequence[str],
    model_packages: Sequence[str],
    python_paths: Sequence[str],
) -> tuple[list[SourceFile], list[DiscoveryFailure]]:
    """Every `*.py` under the entries, de-duplicated and sorted by path. A directory is walked
    recursively, skipping dot entries, `__pycache__` and virtualenvs; an entry with `*` is a glob.
    A missing directory is the config loader's finding, not discovery's."""
    project_dir = project_dir.resolve()
    roots = [(project_dir / p).resolve() for p in python_paths]
    root_texts = sorted((str(r).rstrip(os.sep) + os.sep for r in roots), key=len, reverse=True)
    found: dict[Path, str | None] = {}
    stats: dict[Path, Stat | None] = {}
    for entry in model_paths:
        if "*" in entry:
            for match in sorted(project_dir.glob(entry)):
                if match.is_dir():
                    stats.update(_walk(match.resolve()))
                elif match.suffix == ".py" and not _skipped(match, project_dir):
                    stats[match.resolve()] = None
        elif (project_dir / entry).is_dir():
            stats.update(_walk((project_dir / entry).resolve()))
    found.update(dict.fromkeys(stats))

    failures: list[DiscoveryFailure] = []
    for dotted in model_packages:
        top, _, rest = dotted.partition(".")
        base = locate_package(top, roots)
        target = base.joinpath(*rest.split(".")) if base and rest else base
        if base is None or target is None or not target.is_dir():
            failures.append(
                DiscoveryFailure(
                    dotted, None, None, "not_found", f"model package '{dotted}' cannot be found"
                )
            )
            continue
        for file, st in _walk(target):
            # A package outside every root is importable under its own dotted name.
            if file not in found:
                found[file] = module_name(file, roots) or module_name(file, [base.parent])
                stats[file] = st

    files: list[SourceFile] = []
    for f, own in found.items():
        rel = _rel(f, project_dir)
        if rel is None:
            failures.append(
                DiscoveryFailure(
                    f.as_posix(),
                    None,
                    None,
                    "outside_roots",
                    f"'{f.as_posix()}' has no path relative to the project dir",
                )
            )
            continue
        module = own if own is not None else _fast_module(f, root_texts)
        files.append(SourceFile(rel, f, module, stats.get(f)))
    return sorted(files, key=lambda s: s.path), failures


def locate_package(top: str, roots: Sequence[Path]) -> Path | None:
    """The directory of a top-level package: under a `python_paths` root first, then wherever
    `find_spec` finds it. `find_spec` on a top-level name imports nothing."""
    for root in roots:
        if (root / top).is_dir():
            return root / top
    spec = importlib.util.find_spec(top)
    if spec is None or not spec.submodule_search_locations:
        return None
    return Path(next(iter(spec.submodule_search_locations))).resolve()
