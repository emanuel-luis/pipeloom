"""Machine-readable output: `--log-format json` on every command, the explain URL on diagnostics,
`compile --output json`, and one schema-validated golden per command (ADR-0018, ADR-0049)."""

from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest
from click.testing import CliRunner

from pipeloom.cli import main
from pipeloom.diagnostics.codes import CATALOG, docs_url
from tests.artifacts.test_artifact_schemas import STRICT
from tests.conftest import FIXTURES, make_project

BAD = "config_version: 1\nname: shop\nbogus: 1\ntargets:\n  dev:\n    engine: {kind: memory}\n"


def events(stderr: str) -> list[dict[str, Any]]:
    out = [json.loads(ln) for ln in stderr.splitlines() if ln.startswith("{")]
    for event in out:
        STRICT["event.json"].validate({"id": "e", "ts": "2026-01-01T00:00:00.000000Z", **event})
    return out


def normal(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Drop what changes on every invocation, so the list can be a golden."""
    out = []
    for e in items:
        e = {k: v for k, v in e.items() if k not in ("id", "ts", "invocation_id")}
        data = dict(e["data"])
        if "duration_ms" in data:
            data["duration_ms"] = 0
        if "pipeloom_version" in data:
            data["pipeloom_version"] = "X"
        if "args" in data:
            data["args"] = {k: "<path>" if "dir" in k else v for k, v in data["args"].items()}
        out.append({**e, "data": data})
    return out


def dump(items: list[dict[str, Any]]) -> str:
    return json.dumps(items, indent=2, sort_keys=True) + "\n"


def run(*args: str) -> Any:
    return CliRunner().invoke(main, list(args), env={})


def test_a_diagnostic_event_carries_the_explain_url(
    tmp_path: Path, golden: Callable[..., None]
) -> None:
    make_project(tmp_path, {"pipeloom.yml": BAD})
    result = run("validate", "--project-dir", str(tmp_path), "--log-format", "json")
    assert result.exit_code == 1
    found = events(result.stderr)
    diags = [e for e in found if e["type"] == "diagnostic"]
    assert diags
    for d in diags:
        assert d["data"]["explain_url"] == docs_url(d["data"]["code"])
    golden("machine/validate.json", dump(normal(found)))


def test_events_without_the_url_still_validate() -> None:
    """A 1.0 diagnostic event has no explain_url."""
    data = {
        "code": "PLX101",
        "severity": "error",
        "message": "m",
        "hint": None,
        "location": None,
        "context": {},
    }
    event = {
        "schema": "pipeloom.event/1",
        "id": "e",
        "run_id": None,
        "invocation_id": "i",
        "seq": 1,
        "ts": "2026-01-01T00:00:00.000000Z",
        "type": "diagnostic",
        "level": "error",
        "data": data,
        "context": {},
    }
    STRICT["event.json"].validate(event)


def test_unit_test_command_json_log(golden: Callable[..., None]) -> None:
    root = FIXTURES / "shop"
    result = run("test", "--project-dir", str(root), "--target", "ci", "--log-format", "json")
    found = events(result.stderr)
    assert found[0]["type"] == "invocation.started" and found[-1]["type"] == "invocation.completed"
    golden("machine/test.json", dump(normal(found)))


def test_explain_json_log(golden: Callable[..., None]) -> None:
    result = run("explain", "PLX101", "--log-format", "json")
    assert result.exit_code == 0 and "PLX101" in result.stdout
    found = events(result.stderr)
    assert [e["type"] for e in found] == ["invocation.started", "invocation.completed"]
    golden("machine/explain.json", dump(normal(found)))


def test_explain_text_log_is_the_default() -> None:
    assert not events(run("explain", "PLX101").stderr)


def test_init_json_log(tmp_path: Path, golden: Callable[..., None]) -> None:
    result = run("init", str(tmp_path / "p"), "--name", "p", "--no-input", "--log-format", "json")
    assert result.exit_code == 0
    found = events(result.stderr)
    assert found[0]["data"]["command"] == "init"
    golden("machine/init.json", dump(normal(found)))


def test_compile_output_json(tmp_path: Path, golden: Callable[..., None]) -> None:
    out = tmp_path / "m.json"
    result = run(
        "compile",
        *("--project-dir", str(FIXTURES / "shop"), "--target", "ci"),
        *("--out", str(out), "--stable", "--output", "json"),
    )
    assert result.exit_code == 0, result.stderr
    doc = json.loads(result.stdout)
    STRICT["compile_result.json"].validate(doc)
    assert doc["manifest"] == out.as_posix()
    assert "nodes" not in doc  # the manifest is not repeated
    doc["manifest"] = "m.json"
    doc["manifest_sha256"] = "<sha256>"  # it covers the pipeloom version
    golden("machine/compile_result.json", json.dumps(doc, indent=2, sort_keys=True) + "\n")


def test_compile_text_output_prints_the_path(tmp_path: Path) -> None:
    out = tmp_path / "m.json"
    result = run(
        "compile", "--project-dir", str(FIXTURES / "shop"), "--target", "ci", "--out", str(out)
    )
    assert result.stdout.strip() == out.as_posix()


@pytest.mark.parametrize("code", list(CATALOG))
def test_every_code_has_an_explain_url(code: str) -> None:
    assert docs_url(code).endswith(f"#{code.lower()}")
