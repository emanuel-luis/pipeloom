"""tools/versions.py keeps the workspace in lockstep."""

from __future__ import annotations

import shutil
from pathlib import Path

import pytest
from tools import versions

REPO = Path(__file__).resolve().parent.parent


@pytest.fixture
def workspace(tmp_path: Path) -> Path:
    shutil.copy(REPO / "pyproject.toml", tmp_path / "pyproject.toml")
    for member in versions.members(REPO):
        dest = tmp_path / "plugins" / member.parent.name
        dest.mkdir(parents=True)
        shutil.copy(member, dest / "pyproject.toml")
    return tmp_path


def test_the_repository_is_in_lockstep() -> None:
    assert versions.check(REPO) == []


def test_check_the_tag(workspace: Path) -> None:
    version = versions._load(workspace / "pyproject.toml")["project"]["version"]
    assert versions.check(workspace, f"v{version}") == []
    assert versions.check(workspace, "v9.9.9") == [
        f"tag v9.9.9 does not match the root version {version}"
    ]


def test_check_finds_each_kind_of_drift(workspace: Path) -> None:
    plugin = workspace / "plugins" / "pipeloom-polars" / "pyproject.toml"
    text = plugin.read_text(encoding="utf-8")
    plugin.write_text(
        text.replace('version = "0.1.0"', 'version = "0.1.1"').replace(
            "pipeloom~=0.1.0", "pipeloom"
        ),
        encoding="utf-8",
    )
    root = workspace / "pyproject.toml"
    root.write_text(
        root.read_text(encoding="utf-8").replace(
            "pipeloom-polars==0.1.0", "pipeloom-polars==0.0.9"
        ),
        encoding="utf-8",
    )
    assert versions.check(workspace) == [
        "pipeloom-polars is 0.1.1, the root is 0.1.0",
        "pipeloom-polars must depend on pipeloom~=0.1.0",
        "the root extra pins pipeloom-polars==0.0.9, not 0.1.0",
    ]


def test_a_member_without_an_extra_is_reported(workspace: Path) -> None:
    root = workspace / "pyproject.toml"
    root.write_text(
        root.read_text(encoding="utf-8").replace('polars = ["pipeloom-polars==0.1.0"]', ""),
        encoding="utf-8",
    )
    assert versions.check(workspace) == ["no root extra pins pipeloom-polars"]


def test_bump_rewrites_everything_and_checks(workspace: Path) -> None:
    assert versions.bump(workspace, "0.2.0", lock=False) == []
    assert versions.check(workspace, "v0.2.0") == []
    assert 'dependencies = ["pipeloom~=0.2.0"' in (
        workspace / "plugins" / "pipeloom-polars" / "pyproject.toml"
    ).read_text(encoding="utf-8")
    assert versions.bump(workspace, "0.2", lock=False) == ["'0.2' is not X.Y.Z"]


def test_main_exit_codes(workspace: Path, capsys: pytest.CaptureFixture[str]) -> None:
    assert versions.main(["check"], root=workspace) == 0
    assert versions.main(["check", "--tag", "v9.9.9"], root=workspace) == 1
    assert "does not match" in capsys.readouterr().err
    assert versions.main(["bump", "0.3.0", "--no-lock"], root=workspace) == 0
