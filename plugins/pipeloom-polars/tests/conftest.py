from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path


def write_files(root: Path, files: Mapping[str, str]) -> Path:
    """Write text files under `root` with LF endings."""
    for rel, content in files.items():
        target = root / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(content.encode("utf-8"))
    return root
