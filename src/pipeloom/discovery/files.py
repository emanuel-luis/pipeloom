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


def _walk(root: Path) -> Iterator[Path]:
    for dirpath, dirnames, filenames in os.walk(root, followlinks=False):
        dirnames[:] = [
            d
            for d in dirnames
            if not d.startswith(".")
            and d not in SKIP_DIRS
            and not (Path(dirpath, d) / "pyvenv.cfg").exists()
        ]
        for name in filenames:
            if name.endswith(".py") and not name.startswith("."):
                yield Path(dirpath, name)


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
    try:
        return Path(os.path.relpath(path, project_dir)).as_posix()
    except ValueError:  # another drive on Windows
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
    found: dict[Path, str | None] = {}
    for entry in model_paths:
        if "*" in entry:
            for match in sorted(project_dir.glob(entry)):
                if match.is_dir():
                    found.update(dict.fromkeys(_walk(match.resolve())))
                elif match.suffix == ".py" and not _skipped(match, project_dir):
                    found[match.resolve()] = None
        elif (project_dir / entry).is_dir():
            found.update(dict.fromkeys(_walk((project_dir / entry).resolve())))

    failures: list[DiscoveryFailure] = []
    for dotted in model_packages:
        top, _, rest = dotted.partition(".")
        base = _locate_package(top, roots)
        target = base.joinpath(*rest.split(".")) if base and rest else base
        if base is None or target is None or not target.is_dir():
            failures.append(
                DiscoveryFailure(
                    dotted, None, None, "not_found", f"model package '{dotted}' cannot be found"
                )
            )
            continue
        for file in _walk(target):
            # A package outside every root is importable under its own dotted name.
            if file not in found:
                found[file] = module_name(file, roots) or module_name(file, [base.parent])

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
        files.append(SourceFile(rel, f, own if own is not None else module_name(f, roots)))
    return sorted(files, key=lambda s: s.path), failures


def _locate_package(top: str, roots: Sequence[Path]) -> Path | None:
    """The directory of a top-level package: under a `python_paths` root first, then wherever
    `find_spec` finds it. `find_spec` on a top-level name imports nothing."""
    for root in roots:
        if (root / top).is_dir():
            return root / top
    spec = importlib.util.find_spec(top)
    if spec is None or not spec.submodule_search_locations:
        return None
    return Path(next(iter(spec.submodule_search_locations))).resolve()
