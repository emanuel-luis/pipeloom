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


def _current() -> str:
    return str(versions._load(REPO / "pyproject.toml")["project"]["version"])


def test_check_finds_each_kind_of_drift(workspace: Path) -> None:
    cur = _current()
    minor = cur.rsplit(".", 1)[0] + ".0"
    plugin = workspace / "plugins" / "pipeloom-polars" / "pyproject.toml"
    text = plugin.read_text(encoding="utf-8")
    plugin.write_text(
        text.replace(f'version = "{cur}"', 'version = "0.0.1"').replace(
            f"pipeloom~={minor}", "pipeloom"
        ),
        encoding="utf-8",
    )
    root = workspace / "pyproject.toml"
    root.write_text(
        root.read_text(encoding="utf-8").replace(
            f"pipeloom-polars=={cur}", "pipeloom-polars==0.0.9"
        ),
        encoding="utf-8",
    )
    assert versions.check(workspace) == [
        f"pipeloom-polars is 0.0.1, the root is {cur}",
        f"pipeloom-polars must depend on pipeloom~={minor}",
        f"the root extra pins pipeloom-polars==0.0.9, not {cur}",
    ]


def test_a_member_without_an_extra_is_reported(workspace: Path) -> None:
    root = workspace / "pyproject.toml"
    root.write_text(
        root.read_text(encoding="utf-8").replace(f'polars = ["pipeloom-polars=={_current()}"]', ""),
        encoding="utf-8",
    )
    assert versions.check(workspace) == ["no root extra pins pipeloom-polars"]


def test_bump_rewrites_everything_and_checks(workspace: Path) -> None:
    assert versions.bump(workspace, "7.8.9", lock=False) == []
    assert versions.check(workspace, "v7.8.9") == []
    assert 'dependencies = ["pipeloom~=7.8.0"' in (
        workspace / "plugins" / "pipeloom-polars" / "pyproject.toml"
    ).read_text(encoding="utf-8")
    assert versions.bump(workspace, "0.2", lock=False) == ["'0.2' is not X.Y.Z"]


def test_main_exit_codes(workspace: Path, capsys: pytest.CaptureFixture[str]) -> None:
    assert versions.main(["check"], root=workspace) == 0
    assert versions.main(["check", "--tag", "v9.9.9"], root=workspace) == 1
    assert "does not match" in capsys.readouterr().err
    assert versions.main(["bump", "0.3.0", "--no-lock"], root=workspace) == 0


def test_an_extra_on_the_pinned_name_is_accepted(workspace: Path) -> None:
    cur = _current()
    root = workspace / "pyproject.toml"
    text = root.read_text(encoding="utf-8")
    assert f'"pipeloom-spark[local]=={cur}"' in text
    assert versions.check(workspace) == []
    root.write_text(
        text.replace(f"pipeloom-spark[local]=={cur}", "pipeloom-spark[local]==0.0.9"),
        encoding="utf-8",
    )
    assert versions.check(workspace) == [
        f"the root extra pins pipeloom-spark==0.0.9, not {cur}",
    ]
    assert versions.bump(workspace, "7.8.9", lock=False) == []
    assert '"pipeloom-spark[local]==7.8.9"' in root.read_text(encoding="utf-8")
