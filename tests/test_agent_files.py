"""AGENTS.md and the skills `pipeloom init` writes (ADR-0041 amendment 1.2)."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from click.testing import CliRunner

from pipeloom.cli import main
from pipeloom.cli._agents import AGENTS_FILE, SKILLS_DIR, agent_files
from pipeloom.diagnostics.codes import CATALOG


def run(*args: str) -> Any:
    return CliRunner().invoke(main, list(args), env={})


def skills_text() -> str:
    return "\n".join(
        c.decode("utf-8") for p, c in agent_files().items() if p.startswith(SKILLS_DIR)
    )


@pytest.mark.parametrize("code", list(CATALOG))
def test_every_catalog_code_is_in_the_skills(code: str) -> None:
    assert code in skills_text()


def test_every_command_is_in_agents_md() -> None:
    text = agent_files()[AGENTS_FILE].decode("utf-8")
    for name in main.commands:
        assert f"`pipeloom {name}`" in text


def test_skills_have_frontmatter_matching_their_folder() -> None:
    for path, content in agent_files().items():
        if path.endswith("SKILL.md"):
            folder = path.split("/")[-2]
            assert content.decode("utf-8").startswith(f"---\nname: {folder}\ndescription: ")


def test_init_writes_the_files(tmp_path: Path) -> None:
    out = tmp_path / "p"
    assert run("init", str(out), "--no-input").exit_code == 0
    for rel, content in agent_files().items():
        assert (out / rel).read_bytes() == content


def test_update_agents_touches_only_the_agent_files(tmp_path: Path) -> None:
    out = tmp_path / "p"
    run("init", str(out), "--no-input")
    (out / AGENTS_FILE).write_text("stale", encoding="utf-8")
    yml = out / "pipeloom.yml"
    yml.write_text(yml.read_text(encoding="utf-8") + "# mine\n", encoding="utf-8")
    before = {p: p.read_bytes() for p in out.rglob("*") if p.is_file()}
    assert run("init", "--update-agents", str(out)).exit_code == 0
    after = {p: p.read_bytes() for p in out.rglob("*") if p.is_file()}
    changed = {p.relative_to(out).as_posix() for p in after if before.get(p) != after[p]}
    assert changed == {AGENTS_FILE}
    assert set(after) == set(before)


def test_update_agents_needs_a_project(tmp_path: Path) -> None:
    assert run("init", "--update-agents", str(tmp_path)).exit_code == 2


def test_update_agents_refuses_other_options(tmp_path: Path) -> None:
    assert run("init", "--update-agents", "--name", "x", str(tmp_path)).exit_code == 2


def test_update_agents_json_log(tmp_path: Path) -> None:
    run("init", str(tmp_path / "p"), "--no-input")
    result = run("init", "--update-agents", str(tmp_path / "p"), "--log-format", "json")
    assert result.exit_code == 0 and '"invocation.completed"' in result.stderr
