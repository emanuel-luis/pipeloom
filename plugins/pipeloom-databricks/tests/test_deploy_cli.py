"""`pipeloom databricks deploy`: every flag, and the group options --profile and --client-factory."""

from __future__ import annotations

import json
import sys
from collections.abc import Callable
from types import SimpleNamespace
from typing import Any

import pytest
from click.testing import CliRunner, Result

import pipeloom_databricks.cli as cli_module
from pipeloom_databricks.cli import databricks
from pipeloom_databricks.testing.fake import FakeDatabricks

Sim = Any


@pytest.fixture
def fake(monkeypatch: pytest.MonkeyPatch) -> FakeDatabricks:
    """A fake workspace that `--client-factory fakemod:make` hands to the command."""
    client = FakeDatabricks()
    monkeypatch.setitem(sys.modules, "fakemod", SimpleNamespace(make=lambda: client))
    return client


def invoke(s: Sim, *args: str, group: tuple[str, ...] = ()) -> Result:
    argv = [*group, "deploy", "--project-dir", str(s.root), "--target", "prod", *args]
    return CliRunner().invoke(databricks, argv)


def lines(result: Result) -> list[dict[str, Any]]:
    return [json.loads(line) for line in result.stdout.splitlines()]


FACTORY = ("--client-factory", "fakemod:make")


def test_deploys_every_pipeline_and_prints_one_json_line_each(
    sim: Callable[..., Sim], fake: FakeDatabricks
) -> None:
    s = sim()
    r = invoke(s, group=FACTORY)
    assert r.exit_code == 0, r.stderr
    out = lines(r)
    assert [o["pipeline"] for o in out] == ["extra", "main"]
    assert {o["action"] for o in out} == {"created"}
    assert sorted(o["job_id"] for o in out) == sorted(fake.jobs)
    assert all(o["root"].startswith("/Workspace/Users/me@example.com/.pipeloom/sim/") for o in out)
    again = invoke(s, group=FACTORY)
    assert {o["action"] for o in lines(again)} == {"unchanged"}


def test_pipeline_flag_is_repeatable(sim: Callable[..., Sim], fake: FakeDatabricks) -> None:
    s = sim()
    r = invoke(s, "--pipeline", "extra", group=FACTORY)
    assert [o["pipeline"] for o in lines(r)] == ["extra"]
    r = invoke(s, "--pipeline", "main", "--pipeline", "extra", group=FACTORY)
    assert [o["pipeline"] for o in lines(r)] == ["main", "extra"]


def test_unknown_pipeline_is_pld004_and_connects_to_nothing(
    sim: Callable[..., Sim], fake: FakeDatabricks
) -> None:
    r = invoke(sim(), "--pipeline", "nope", group=FACTORY)
    assert r.exit_code == 1 and "PLD004" in r.stderr and r.stdout == ""
    assert fake.calls == []


def test_dry_run_prints_the_settings_and_creates_no_client(sim: Callable[..., Sim]) -> None:
    s = sim()
    # The factory does not exist: a dry run must never look at it, or at any connection.
    r = invoke(s, "--dry-run", "--pipeline", "main", group=("--client-factory", "nope:nope"))
    assert r.exit_code == 0, r.stderr
    (doc,) = lines(r)
    assert doc["pipeline"] == "main"
    assert doc["root"].startswith("/Workspace/Users/<user>/.pipeloom/sim/main/prod/")
    settings = doc["settings"]
    assert settings["name"] == "pipeloom_sim_main_prod"
    assert settings["tags"]["pipeloom_job"] == "sim.main.prod"
    wheel = settings["tasks"][0]["python_wheel_task"]["parameters"]
    assert f"--project-dir={doc['root']}" in wheel
    assert r.stdout.isascii()


def test_dry_run_reports_render_errors(sim: Callable[..., Sim]) -> None:
    s = sim()
    (s.root / "pipeloom.yml").write_text(
        (s.root / "pipeloom.yml")
        .read_text(encoding="utf-8")
        .replace("task_grouping: none", "settings: {tags: {pipeloom_x: y}}"),
        encoding="utf-8",
    )
    r = invoke(s, "--dry-run")
    assert r.exit_code == 1 and "PLD004" in r.stderr


def test_without_a_connection_the_command_stops_with_pld010(sim: Callable[..., Sim]) -> None:
    r = invoke(sim())
    assert r.exit_code == 1 and "PLD010" in r.stderr and r.stdout == ""


def test_a_bad_client_factory_is_pld010(sim: Callable[..., Sim]) -> None:
    r = invoke(sim(), group=("--client-factory", "nope:nope"))
    assert r.exit_code == 1 and "PLD010" in r.stderr and "cannot be loaded" in r.stderr


def test_profile_flag_and_variable_reach_connect(
    sim: Callable[..., Sim], monkeypatch: pytest.MonkeyPatch
) -> None:
    seen: list[dict[str, Any]] = []

    def spy(**kwargs: Any) -> FakeDatabricks:
        seen.append({**kwargs, "profile_env": kwargs["env"].get("DATABRICKS_CONFIG_PROFILE")})
        return FakeDatabricks()

    monkeypatch.setattr(cli_module, "connect", spy)
    s = sim()
    argv = ["--profile", "chosen", "deploy", "--project-dir", str(s.root), "--target", "prod"]
    assert CliRunner().invoke(databricks, argv).exit_code == 0
    assert seen[0]["profile"] == "chosen" and seen[0]["factory"] is None
    plain = ["deploy", "--project-dir", str(s.root), "--target", "prod"]
    env = {"DATABRICKS_CONFIG_PROFILE": "from-env"}
    assert CliRunner().invoke(databricks, plain, env=env).exit_code == 0
    assert seen[1]["profile"] is None
    assert seen[1]["profile_env"] == "from-env"


def test_api_failures_are_pld009(sim: Callable[..., Sim]) -> None:
    r = invoke(sim(), group=("--client-factory", "pipeloom_databricks.testing.fake:api_down"))
    assert r.exit_code == 1 and "PLD009" in r.stderr and "HTTP 500" in r.stderr


def test_help_shows_profile_and_hides_the_factory() -> None:
    r = CliRunner().invoke(databricks, ["--help"])
    assert "--profile" in r.stdout and "--client-factory" not in r.stdout
    assert "deploy" in r.stdout
