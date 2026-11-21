"""tools/versions.py keeps the workspace in lockstep."""

from __future__ import annotations

import shutil
from pathlib import Path

import pytest
from tools import versions

REPO = Path(__file__).resolve().parent.parent


@pytest.fixture
def workspace(tmp_path: Path) -> Path:
    """The pyprojects and the two files that hold the constants. `check` parses every source
    file for `PluginSpec` calls, so copying whole trees made each test take seconds; the real
    tree is covered by `test_the_repository_is_in_lockstep`."""
    shutil.copy(REPO / "pyproject.toml", tmp_path / "pyproject.toml")
    for source in (versions._API_SOURCE, versions._CONFIG_SOURCE):
        (tmp_path / source).parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(REPO / source, tmp_path / source)
    for member in versions.members(REPO):
        dest = tmp_path / "plugins" / member.parent.name
        (dest / "src").mkdir(parents=True)
        shutil.copy(member, dest / "pyproject.toml")
    return tmp_path


def _edit(path: Path, old: str, new: str) -> None:
    text = path.read_text(encoding="utf-8")
    assert old in text
    path.write_text(text.replace(old, new), encoding="utf-8")


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
    minor = versions._pin(cur)  # X.Y.0, or the version itself on a prerelease
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
    assert versions.bump(workspace, "0.2", lock=False) == ["'0.2' is not X.Y.Z or X.Y.Z(a|b|rc)N"]
    assert versions.bump(workspace, "1.0.0c1", lock=False) != []


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


def test_a_member_that_pins_another_member_follows_the_version(workspace: Path) -> None:
    cur = _current()
    minor = versions._pin(cur)  # X.Y.0, or the version itself on a prerelease
    plugin = workspace / "plugins" / "pipeloom-databricks" / "pyproject.toml"
    text = plugin.read_text(encoding="utf-8")
    assert f'"pipeloom-spark~={minor}"' in text
    plugin.write_text(text.replace(f"pipeloom-spark~={minor}", "pipeloom-spark~=0.1.0"), "utf-8")
    assert versions.check(workspace) == [
        f"pipeloom-databricks must depend on pipeloom-spark~={minor}"
    ]
    assert versions.bump(workspace, "7.8.9", lock=False) == []
    assert '"pipeloom-spark~=7.8.0"' in plugin.read_text(encoding="utf-8")
    assert '"pipeloom~=7.8.0"' in plugin.read_text(encoding="utf-8")


def test_a_prerelease_is_checked_bumped_and_tagged(workspace: Path) -> None:
    assert versions.bump(workspace, "1.0.0rc1", lock=False) == []
    assert versions.check(workspace, "v1.0.0rc1") == []
    assert versions.check(workspace, "v1.0.0") == [
        "tag v1.0.0 does not match the root version 1.0.0rc1"
    ]
    plugin = workspace / "plugins" / "pipeloom-databricks" / "pyproject.toml"
    text = plugin.read_text(encoding="utf-8")
    assert '"pipeloom~=1.0.0rc1"' in text and '"pipeloom-spark~=1.0.0rc1"' in text
    assert '"pipeloom-polars==1.0.0rc1"' in (workspace / "pyproject.toml").read_text("utf-8")
    assert versions.main(["check", "--tag", "v1.0.0rc1"], root=workspace) == 0
    _edit(plugin, '"pipeloom~=1.0.0rc1"', '"pipeloom~=1.0.0"')
    assert versions.check(workspace) == ["pipeloom-databricks must depend on pipeloom~=1.0.0rc1"]


def test_bump_round_trips_from_a_prerelease_to_the_final_version(workspace: Path) -> None:
    assert versions.bump(workspace, "1.0.0rc1", lock=False) == []
    assert versions.bump(workspace, "1.0.0", lock=False) == []
    plugin = workspace / "plugins" / "pipeloom-databricks" / "pyproject.toml"
    text = plugin.read_text(encoding="utf-8")
    assert '"pipeloom~=1.0.0"' in text and '"pipeloom-spark~=1.0.0"' in text
    assert "rc1" not in text and "rc1" not in (workspace / "pyproject.toml").read_text("utf-8")
    assert versions.check(workspace, "v1.0.0") == []


def test_the_prerelease_pin_accepts_the_rc_and_the_final_version() -> None:
    specifiers = pytest.importorskip("packaging.specifiers")
    spec = specifiers.SpecifierSet("~=1.0.0rc1")
    assert "1.0.0rc1" in spec and "1.0.0" in spec and "1.1.0" not in spec
    assert "1.0.0rc1" not in specifiers.SpecifierSet("~=1.0.0")


def test_the_root_declaration_must_match_the_source(workspace: Path) -> None:
    root = workspace / "pyproject.toml"
    _edit(root, 'plugin-api = "1.1"', 'plugin-api = "0.5"')
    _edit(root, "config-version = 1", "config-version = 2")
    errors = versions.check(workspace)
    assert "the root [tool.pipeloom] config-version is 2, the source has 1" in errors
    assert "the root [tool.pipeloom] plugin-api is '0.5', the source has '1.1'" in errors
    assert any(e.startswith("pipeloom-polars [tool.pipeloom] plugin-api") for e in errors)


def test_a_member_must_declare_what_the_root_declares(workspace: Path) -> None:
    plugin = workspace / "plugins" / "pipeloom-spark" / "pyproject.toml"
    _edit(plugin, 'plugin-api = "1.1"', 'plugin-api = "1.2"')
    assert versions.check(workspace) == [
        "pipeloom-spark [tool.pipeloom] plugin-api is '1.2', the root has '1.1'"
    ]


def test_a_missing_declaration_is_reported(workspace: Path) -> None:
    plugin = workspace / "plugins" / "pipeloom-pandas" / "pyproject.toml"
    _edit(plugin, '[tool.pipeloom]\nconfig-version = 1\nplugin-api = "1.1"\n', "")
    assert versions.check(workspace) == ["pipeloom-pandas has no [tool.pipeloom] table"]
    root = workspace / "pyproject.toml"
    _edit(root, '[tool.pipeloom]\nconfig-version = 1\nplugin-api = "1.1"\n', "")
    assert "the root pyproject.toml has no [tool.pipeloom] table" in versions.check(workspace)


@pytest.mark.parametrize(
    "call",
    [
        'PluginSpec("x", "x", "engines", (0, 5), f, M)',
        'PluginSpec(name="x", kind="x", group="engines", api_version=(0, 5), factory=f)',
        'PluginSpec("x", "x", "engines")',
    ],
)
def test_a_spec_with_a_literal_api_version_is_reported(workspace: Path, call: str) -> None:
    (workspace / "plugins" / "pipeloom-polars" / "src" / "extra.py").write_text(
        f"SPEC = {call}\n", encoding="utf-8"
    )
    assert versions.check(workspace) == [
        "plugins/pipeloom-polars/src/extra.py:1: PluginSpec must pass api_version=API_VERSION"
    ]


def test_a_spec_that_passes_api_version_is_accepted(workspace: Path) -> None:
    (workspace / "src" / "pipeloom" / "extra.py").write_text(
        'A = PluginSpec("x", "x", "engines", api_version=API_VERSION)\n'
        'B = PluginSpec[object]("x", "x", "engines", API_VERSION, f, M)\n',
        encoding="utf-8",
    )
    assert versions.check(workspace) == []
