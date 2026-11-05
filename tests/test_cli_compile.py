from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from click.testing import CliRunner

from pipeloom.cli import main
from tests.conftest import FIXTURES, make_project

SHOP = ["--project-dir", str(FIXTURES / "shop"), "--target", "ci"]
BASE = "config_version: 1\nname: shop\ntargets:\n  dev:\n    engine: {kind: memory}\n"


def compile_(args: list[str], env: dict[str, str] | None = None) -> Any:
    return CliRunner().invoke(main, ["compile", *args], env=env or {})


def manifest(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))  # type: ignore[no-any-return]


def test_compile_writes_default_manifest(tmp_path: Path) -> None:
    project = tmp_path / "shop"
    make_project(
        project, {"pipeloom.yml": BASE, "models/m.yml": "models:\n  - {name: a, sql: select 1}\n"}
    )
    result = compile_(["--project-dir", str(project)])
    assert result.exit_code == 0
    written = project / "target" / "manifest.json"
    assert result.stdout.strip() == written.as_posix()
    assert manifest(written)["schema"] == "pipeloom.manifest/1"


def test_out_relative_and_absolute(tmp_path: Path) -> None:
    rel = compile_([*SHOP, "--out", "build/m.json"])
    assert rel.exit_code == 0
    shop_dir = FIXTURES / "shop"
    try:
        assert rel.stdout.strip() == (shop_dir / "build" / "m.json").as_posix()
        assert (shop_dir / "build" / "m.json").is_file()
    finally:
        (shop_dir / "build" / "m.json").unlink(missing_ok=True)
        (shop_dir / "build").rmdir()
    target = tmp_path / "deep" / "abs.json"
    absolute = compile_([*SHOP, "--out", str(target)])
    assert absolute.exit_code == 0 and absolute.stdout.strip() == target.as_posix()
    assert target.is_file()


def test_compile_twice_is_byte_identical(tmp_path: Path) -> None:
    a, b = tmp_path / "a.json", tmp_path / "b.json"
    assert compile_([*SHOP, "--out", str(a)]).exit_code == 0
    assert compile_([*SHOP, "--out", str(b)]).exit_code == 0
    da, db = manifest(a), manifest(b)
    for d in (da, db):
        for k in ("generated_at", "invocation_id"):
            d["metadata"].pop(k)
    assert da == db
    assert da["metadata"]["plan_hash"] == db["metadata"]["plan_hash"]


def test_select_and_exclude(tmp_path: Path) -> None:
    out = tmp_path / "m.json"
    result = compile_([*SHOP, "-s", "tag:mart", "--exclude", "dim_customers", "--out", str(out)])
    assert result.exit_code == 0
    data = manifest(out)
    assert data["selectors"] == {
        "select": ["tag:mart"],
        "exclude": ["dim_customers"],
        "selected": ["model.shop.fct_orders"],
    }
    assert len(data["units"]) == 6


def test_boundary_values(tmp_path: Path) -> None:
    out = tmp_path / "process.json"
    assert compile_([*SHOP, "--boundary", "process", "--out", str(out)]).exit_code == 0
    assert manifest(out)["metadata"]["boundary"] == "process"
    assert compile_([*SHOP, "--boundary", "bogus"]).exit_code == 2


def test_boundary_task_needs_a_shared_state_store(tmp_path: Path) -> None:
    refused = compile_([*SHOP, "--boundary", "task", "--out", str(tmp_path / "no.json")])
    assert refused.exit_code == 1 and "PLC503" in refused.stderr
    shared = BASE.replace("targets:", "state: {kind: json}\ntargets:")
    project = make_project(tmp_path / "p", {"pipeloom.yml": shared})
    out = tmp_path / "task.json"
    args = ["--project-dir", str(project), "--boundary", "task", "--out", str(out)]
    assert compile_(args).exit_code == 0
    assert manifest(out)["metadata"]["boundary"] == "task"


def test_bad_selector_exits_2() -> None:
    result = compile_([*SHOP, "-s", "bogus:x", "--out", "unused.json"])
    assert result.exit_code == 2 and "PLG304" in result.stderr
    assert not (FIXTURES / "shop" / "unused.json").exists()


def test_errors_exit_1_and_write_nothing(tmp_path: Path) -> None:
    make_project(
        tmp_path,
        {
            "pipeloom.yml": BASE,
            "models/m.yml": "models:\n  - {name: a, sql: select 1, write: {strategy: merge}}\n",
        },
    )
    result = compile_(["--project-dir", str(tmp_path)])
    assert result.exit_code == 1 and "PLP401" in result.stderr
    assert not (tmp_path / "target").exists()


def test_warn_error(tmp_path: Path) -> None:
    make_project(
        tmp_path,
        {
            "pipeloom.yml": BASE,
            "models/m.yml": "models:\n  - {name: e, sql: select 1, materialized: ephemeral}\n",
        },
    )
    assert compile_(["--project-dir", str(tmp_path)]).exit_code == 0
    strict = compile_(["--project-dir", str(tmp_path), "--warn-error", "--out", "w.json"])
    assert strict.exit_code == 1 and "PLP408" in strict.stderr
    assert not (tmp_path / "w.json").exists()


def test_target_and_log_format_events(tmp_path: Path) -> None:
    out = tmp_path / "m.json"
    result = compile_(
        ["--project-dir", str(FIXTURES / "shop"), "--out", str(out), "--log-format", "json"],
        env={"PIPELOOM_TARGET": "dev", "PIPELOOM_USER": "Ana"},
    )
    assert result.exit_code == 0
    events = [json.loads(line) for line in result.stderr.splitlines()]
    assert [e["type"] for e in events] == [
        "invocation.started",
        "plan.compiled",
        "artifact.written",
        "invocation.completed",
    ]
    compiled, artifact = events[1]["data"], events[2]["data"]
    assert (
        compiled["units"] == 6 and compiled["ephemeral_inlined"] == 1 and compiled["selected"] == 7
    )
    assert compiled["by_materialization"] == {"table": 6}
    assert artifact["kind"] == "manifest" and artifact["bytes"] == out.stat().st_size
    assert manifest(out)["metadata"]["target"] == "dev"


def test_plugin_and_vars_flags(tmp_path: Path) -> None:
    out = tmp_path / "m.json"
    result = compile_(
        [
            *SHOP,
            "--vars",
            "{region: BR}",
            "--plugin",
            "tests.plugins.fakes:FAKE_SINK",
            "--out",
            str(out),
        ]
    )
    assert result.exit_code == 0
    assert manifest(out)["metadata"]["vars"] == {"region": "BR"}


def test_manifest_and_events_share_the_invocation_id(tmp_path: Path) -> None:
    out = tmp_path / "m.json"
    result = compile_([*SHOP, "--out", str(out), "--log-format", "json"])
    assert result.exit_code == 0
    ids = {json.loads(line)["invocation_id"] for line in result.stderr.splitlines()}
    assert len(ids) == 1
    assert manifest(out)["metadata"]["invocation_id"] == ids.pop()


def test_stable_manifest_is_reproducible_and_blank(tmp_path: Path) -> None:
    a, b, c = tmp_path / "a.json", tmp_path / "b.json", tmp_path / "c.json"
    assert compile_([*SHOP, "--stable", "--out", str(a)]).exit_code == 0
    assert compile_([*SHOP, "--stable", "--out", str(b)]).exit_code == 0
    assert a.read_bytes() == b.read_bytes()
    meta = manifest(a)["metadata"]
    assert (meta["generated_at"], meta["invocation_id"]) == ("", "")
    assert compile_([*SHOP, "--out", str(c)]).exit_code == 0
    meta = manifest(c)["metadata"]
    assert meta["generated_at"] and meta["invocation_id"]
