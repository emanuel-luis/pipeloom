from __future__ import annotations

import base64
import os
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any

import pytest
from hypothesis import settings

GOLDENS = Path(__file__).parent / "goldens"
FIXTURES = Path(__file__).parent / "fixtures"

settings.register_profile("dev", deadline=None)
settings.register_profile("ci", deadline=None, derandomize=True)
settings.load_profile("ci" if os.environ.get("CI") else "dev")


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--update-goldens", action="store_true", default=False, help="Rewrite golden files."
    )


def make_project(tmp_path: Path, files: Mapping[str, Any]) -> Path:
    """Write files into tmp_path. A value is text, or {'base64': ...} for raw bytes. LF only."""
    for rel, content in files.items():
        target = tmp_path / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        if isinstance(content, Mapping):
            target.write_bytes(base64.b64decode(content["base64"]))
        else:
            target.write_bytes(str(content).encode("utf-8"))
    return tmp_path


@pytest.fixture
def project_factory(tmp_path: Path) -> Callable[[Mapping[str, Any]], Path]:
    def factory(files: Mapping[str, Any]) -> Path:
        return make_project(tmp_path, files)

    return factory


@pytest.fixture
def golden(request: pytest.FixtureRequest) -> Callable[[str, str], None]:
    """Compare text with tests/goldens/<name>; --update-goldens rewrites it (LF)."""
    update = bool(request.config.getoption("--update-goldens"))

    def check(name: str, text: str) -> None:
        path = GOLDENS / name
        if update:
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("w", encoding="utf-8", newline="\n") as fh:
                fh.write(text)
            return
        assert path.exists(), f"missing golden {name}; run pytest --update-goldens"
        expected = path.read_bytes().decode("utf-8").replace("\r\n", "\n")
        assert text == expected

    return check
