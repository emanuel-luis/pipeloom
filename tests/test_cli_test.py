"""pipeloom test: every flag and environment variable (ADR-0045)."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from click.testing import CliRunner

from pipeloom.cli import main
from tests.conftest import make_project

PROJECT = """\
config_version: 1
name: shop
vars:
  k: 1
targets:
  dev:
    engine:
      kind: memory
    catalog:
      kind: memory
  other:
    engine:
      kind: memory
    catalog:
      kind: memory
default_target: dev
"""
MODELS = """\
sources:
  - name: raw
    location: raw
models:
  - name: a
    sql: select id * ${var:k} as id from src
    inputs:
      src:
        source: raw
    unit_tests:
      - name: times_k
        given:
          src:
            rows:
              - id: 1
        expect:
          rows:
            - id: 1
  - name: b
    sql: select id from src
    inputs:
      src:
        source: raw
    unit_tests:
      - name: same
        given:
          src:
            rows:
              - id: 5
        expect:
          rows:
            - id: 5
"""


def _root(tmp_path: Path) -> Path:
    return make_project(tmp_path, {"pipeloom.yml": PROJECT, "models/m.yml": MODELS})


def _test(root: Path, *args: str, env: dict[str, str] | None = None) -> Any:
    return CliRunner().invoke(
        main,
        ["test", "--project-dir", str(root), "--log-format", "json", *args],
        env={"PIPELOOM_TARGET": "", **(env or {})},
    )


def _events(result: Any) -> list[dict[str, Any]]:
    return [json.loads(line) for line in result.stderr.splitlines()]


def _cases(result: Any) -> dict[str, str]:
    return {
        e["data"]["unit_test"]: e["type"]
        for e in _events(result)
        if e["type"].startswith("unit_test.")
    }


def test_every_case_passes(tmp_path: Path) -> None:
    result = _test(_root(tmp_path))
    assert result.exit_code == 0, result.stderr
    assert _cases(result) == {"times_k": "unit_test.passed", "same": "unit_test.passed"}
    done = [e for e in _events(result) if e["type"] == "unit_tests.completed"]
    assert done[0]["data"] == {"passed": 2, "failed": 0}
    assert not (tmp_path / "target").exists()


def test_vars_change_the_body_and_fail_a_case(tmp_path: Path) -> None:
    result = _test(_root(tmp_path), "--vars", "k: 2")
    assert result.exit_code == 1
    assert _cases(result)["times_k"] == "unit_test.failed"
    completed = [e for e in _events(result) if e["type"] == "invocation.completed"]
    assert completed[0]["data"]["counts"]["errors"] == 1


def test_select_and_exclude(tmp_path: Path) -> None:
    root = _root(tmp_path)
    assert set(_cases(_test(root, "--select", "b"))) == {"same"}
    assert set(_cases(_test(root, "--exclude", "b"))) == {"times_k"}


def test_target_flag_and_env(tmp_path: Path) -> None:
    root = _root(tmp_path)
    for result in (
        _test(root, "--target", "other"),
        _test(root, env={"PIPELOOM_TARGET": "other"}),
    ):
        assert result.exit_code == 0, result.stderr
        started = [e for e in _events(result) if e["type"] == "invocation.started"]
        assert started[0]["data"]["target"] == "other"
    assert _test(root, "--target", "nope").exit_code == 1


def test_plugin_flag(tmp_path: Path) -> None:
    root = _root(tmp_path)
    assert _test(root, "--plugin", "tests.plugins.fakes:NOLIST").exit_code == 0
    assert _test(root, "--plugin", "no.such.module:SPEC").exit_code == 1


def test_state_flag(tmp_path: Path) -> None:
    root = _root(tmp_path)
    ref = root / "ref"
    done = CliRunner().invoke(
        main,
        ["compile", "--project-dir", str(root), "--stable", "--out", "ref/manifest.json"],
        env={"PIPELOOM_TARGET": ""},
    )
    assert done.exit_code == 0, done.stderr
    unchanged = _test(root, "--state", str(ref), "--select", "state:modified")
    assert unchanged.exit_code == 0 and _cases(unchanged) == {}
    missing = _test(root, "--state", str(root / "nowhere"))
    assert missing.exit_code == 1
    assert "PLX110" in missing.stderr


def test_project_dir_env_and_text_log_format(tmp_path: Path) -> None:
    root = _root(tmp_path)
    result = CliRunner().invoke(
        main,
        ["test", "--vars", "k: 3"],
        env={
            "PIPELOOM_PROJECT_DIR": str(root),
            "PIPELOOM_TARGET": "",
            "PIPELOOM_LOG_FORMAT": "text",
        },
    )
    assert result.exit_code == 1
    assert "PASS   b: same" in result.stderr
    assert "FAIL   a: times_k PLX114 1 missing, 1 unexpected row(s)" in result.stderr
    assert '- {"id": 1}' in result.stderr and '+ {"id": 3}' in result.stderr
    assert "unit tests: 1 passed, 1 failed" in result.stderr
