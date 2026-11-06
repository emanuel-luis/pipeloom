"""Guard: YAML in the repo is block style. A flow mapping with content is flagged; an empty {} is
allowed, and so are short flow lists of scalars."""

from __future__ import annotations

import os
import re
from collections.abc import Iterator
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCAN_DIRS = ("examples", "tests", "plugins", "docs", ".github")
SKIP_DIRS = {"target", ".astro", ".venv", "site", ".worktrees", "node_modules", ".git"}
FLOW_MAP = re.compile(r"(^|[:\-\[,]\s*)\{(?!\s*\})")
QUOTED = re.compile(r"\"[^\"]*\"|'[^']*'")
FENCE = re.compile(r"^\s*(`{3,}|~{3,})\s*(\w*)")

# Whole files that keep flow syntax on purpose, each with its reason.
ALLOWED = {
    "tests/cases/PLY001.yml": "tests the YAML syntax error of an unclosed flow mapping (PLY001)",
    "tests/cases/PLD011.yml": "embeds states.json (JSON, not YAML) in a block scalar",
}


def _walk() -> Iterator[Path]:
    for entry in sorted(ROOT.iterdir()):
        if entry.is_file():
            yield entry
    for name in SCAN_DIRS:
        for base, dirs, files in os.walk(ROOT / name):
            dirs[:] = sorted(d for d in dirs if d not in SKIP_DIRS)
            for file in sorted(files):
                yield Path(base) / file


def _flagged(lines: Iterator[tuple[int, str]]) -> Iterator[int]:
    for number, line in lines:
        text = re.sub(r"\s#.*$", "", QUOTED.sub('""', line))
        if FLOW_MAP.search(text):
            yield number


def _yaml_blocks(text: str) -> Iterator[tuple[int, str]]:
    fence = ""
    for number, line in enumerate(text.splitlines(), 1):
        match = FENCE.match(line)
        if fence:
            if match and match.group(1) == fence and not match.group(2):
                fence = ""
            else:
                yield number, line
        elif match and match.group(2).lower() in {"yaml", "yml"}:
            fence = match.group(1)


def test_no_flow_mappings() -> None:
    problems: list[str] = []
    for path in _walk():
        rel = path.relative_to(ROOT).as_posix()
        if rel in ALLOWED:
            continue
        suffix = path.suffix.lower()
        if suffix not in {".yml", ".yaml", ".md"}:
            continue
        text = path.read_text(encoding="utf-8")
        lines = _yaml_blocks(text) if suffix == ".md" else enumerate(text.splitlines(), 1)
        problems += [f"{rel}:{n}: flow mapping" for n in _flagged(lines)]
    assert not problems, "YAML must be block style (decision 81):\n" + "\n".join(problems)


def test_allowlist_is_current() -> None:
    for rel in ALLOWED:
        assert (ROOT / rel).is_file(), f"{rel} is allowlisted but missing"
