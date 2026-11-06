"""Project templates for `pipeloom init`.

Each engine is a stack of template folders under `templates/`; a later folder's file wins over an
earlier one with the same path. Text files hold `{{key}}` placeholders. A `.tmpl` suffix is
dropped on write (it keeps template Python out of linters), and a file named `gitignore` becomes
`.gitignore`.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from importlib.resources import files
from importlib.resources.abc import Traversable
from pathlib import Path, PurePosixPath
from typing import Final

ENGINES: Final = ("polars", "pandas", "spark", "databricks")

_LAYERS: Final[Mapping[str, tuple[str, ...]]] = {
    "polars": ("common", "local", "sql", "polars"),
    "pandas": ("common", "local", "pandas"),
    "spark": ("common", "local", "sql", "spark"),
    "databricks": ("common", "sql", "databricks"),
}

_RUN: Final[Mapping[str, str]] = {
    "polars": "pipeloom run",
    "pandas": "pipeloom run",
    "spark": "pipeloom run",
    "databricks": "pipeloom databricks deploy",
}


@dataclass(frozen=True)
class Written:
    """What `scaffold` did: files created and existing files it left alone (POSIX, relative)."""

    created: tuple[str, ...]
    skipped: tuple[str, ...]


def run_command(engine: str) -> str:
    """The command that runs (or deploys) a new project on this engine."""
    return _RUN[engine]


def _walk(node: Traversable, rel: PurePosixPath) -> Iterator[tuple[PurePosixPath, Traversable]]:
    for child in node.iterdir():
        if child.is_dir():
            yield from _walk(child, rel / child.name)
        else:
            yield rel / child.name, child


def _render(text: str, values: Mapping[str, str]) -> str:
    for key, value in values.items():
        text = text.replace("{{" + key + "}}", value)
    return text


def render(engine: str, values: Mapping[str, str]) -> dict[str, bytes]:
    """The files of a new project on `engine`, keyed by POSIX path relative to the project."""
    root = files(__name__) / "templates"
    out: dict[str, bytes] = {}
    for layer in _LAYERS[engine]:
        for rel, item in _walk(root / layer, PurePosixPath()):
            name = rel.name.removesuffix(".tmpl")
            name = ".gitignore" if name == "gitignore" else name
            path = _render((rel.parent / name).as_posix(), values)
            out[path] = _render(item.read_text(encoding="utf-8"), values).encode("utf-8")
    return dict(sorted(out.items()))


def scaffold(directory: Path, engine: str, values: Mapping[str, str]) -> Written:
    """Write the project into `directory`. An existing file is never overwritten."""
    created: list[str] = []
    skipped: list[str] = []
    for rel, content in render(engine, values).items():
        target = directory / rel
        if target.exists():
            skipped.append(rel)
            continue
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(content)
        created.append(rel)
    return Written(tuple(created), tuple(skipped))
