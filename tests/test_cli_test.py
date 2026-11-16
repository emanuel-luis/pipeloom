"""pipeloom test: every flag and environment variable (ADR-0045)."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from click.testing import CliRunner

from pipeloom.cli import main
from tests.conftest import make_project

PROJECT = """\
config_version: 1
name: shop
vars:
  k: 1
sources:
  raw:
    tables:
      raw:
        location: raw
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
MODULE = """\
from pipeloom import model, source


@model(inputs={"src": source("raw", "raw")})
def a(ctx, src):
    return ctx.sql(f"select id * {ctx.vars['k']} as id from src")


@model(inputs={"src": source("raw", "raw")})
def b(ctx, src):
    return ctx.sql("select id from src")
"""
MODELS = """\
models:
  - name: a
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
    return make_project(
        tmp_path, {"pipeloom.yml": PROJECT, "models/m.py": MODULE, "models/m.yml": MODELS}
    )


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


PY_PROJECT = """\
config_version: 1
name: shop
python_paths:
  - models
targets:
  dev:
    engine:
      kind: {kind}
    catalog:
      kind: filesystem
      root: target/warehouse
default_target: dev
"""
PY_MODELS = """\
import {lib} as lib
from pipeloom import model


@model
def bronze():
    return lib.DataFrame({{"id": [1]}})


@model
def via_sql(ctx, bronze):
    return ctx.sql("select id, id * 2 as doubled from bronze")


@model
def no_ctx(bronze):
    return bronze.with_columns(doubled=lib.col("id") * 2)  # polars only; pandas never runs it


@model
def kw_ctx(*, ctx, bronze):
    return ctx.sql("select id, id * 2 as doubled from bronze")
"""


def _py_cases(wrong: int = 2) -> str:
    cases = []
    for name in ("via_sql", "no_ctx", "kw_ctx"):
        cases.append(
            f"""\
  - name: {name}_case
    model: {name}
    given:
      bronze:
        rows:
          - id: 1
    expect:
      rows:
        - id: 1
          doubled: {wrong if name == "via_sql" else 2}
"""
        )
    return "unit_tests:\n" + "".join(cases)


def _py_root(tmp_path: Path, kind: str, lib: str, wrong: int = 2) -> Path:
    return make_project(
        tmp_path,
        {
            "pipeloom.yml": PY_PROJECT.format(kind=kind),
            "models/py_models.py": PY_MODELS.format(lib=lib),
            "models/_tests.yml": _py_cases(wrong),
        },
    )


def test_ctx_sql_model_unit_tests_on_polars(tmp_path: Path) -> None:
    pytest.importorskip("polars")
    result = _test(_py_root(tmp_path, "polars", "polars"))
    assert result.exit_code == 0, result.stderr
    assert set(_cases(result).values()) == {"unit_test.passed"}
    assert len(_cases(result)) == 3  # ctx first, no ctx, keyword-only ctx


def test_ctx_sql_model_unit_test_fails_with_the_diff(tmp_path: Path) -> None:
    pytest.importorskip("polars")
    result = _test(_py_root(tmp_path, "polars", "polars", wrong=3), "--select", "via_sql")
    assert result.exit_code == 1
    (failed,) = [e for e in _events(result) if e["type"] == "unit_test.failed"]
    assert failed["data"]["code"] == "PLX114"
    assert failed["data"]["missing"] == [{"id": 1, "doubled": 3}]
    assert failed["data"]["unexpected"] == [{"id": 1, "doubled": 2}]


def test_ctx_sql_model_unit_test_on_an_engine_without_sql_is_plx115(tmp_path: Path) -> None:
    pytest.importorskip("pandas")
    result = _test(_py_root(tmp_path, "pandas", "pandas"), "--select", "via_sql")
    assert result.exit_code == 1
    (failed,) = [e for e in _events(result) if e["type"] == "unit_test.failed"]
    assert failed["data"]["code"] == "PLX115"
    assert "ctx.sql" in failed["data"]["message"] and "no SQL" in failed["data"]["message"]


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
