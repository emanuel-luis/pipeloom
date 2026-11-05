"""The `deploy` block: binding against `renderers`, the manifest record, the plan hash."""

from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path

import pytest

from pipeloom.api import Project, ProjectOptions
from pipeloom.config.schema import dump_schema, export_schemas
from pipeloom.diagnostics import Report
from pipeloom.plan.manifest import manifest_bytes, read_manifest, write_manifest
from pipeloom.plan.model import Plan
from pipeloom.testing.clock import FakeClock
from tests.conftest import make_project
from tests.plugins.fakes import FAKE_ENGINE, RENDERER

PLUGINS = ("tests.plugins.fakes:RENDERER",)
YML = """config_version: 1
name: shop
targets:
  dev:
    engine: {{kind: memory}}
    catalog: {{kind: memory}}
    deploy: {deploy}
"""
MODELS = "models:\n  - {name: a, sql: 'select 1 as x'}\n"


def make(tmp_path: Path, deploy: str, **kw: str) -> Project:
    root = make_project(
        tmp_path, {"pipeloom.yml": YML.format(deploy=deploy), "models/m.yml": MODELS}
    )
    opts = ProjectOptions(project_dir=root, plugins=kw.pop("plugins", PLUGINS) or (), env={})
    return Project(opts, clock=FakeClock(), **kw)  # type: ignore[arg-type]


def plan_of(project: Project) -> Plan:
    outcome = project.compile()
    assert outcome.value is not None, outcome.diagnostics
    return outcome.value


def test_binds_against_renderers_and_never_builds_it(tmp_path: Path) -> None:
    project = make(tmp_path, "{kind: fakejobs, pipelines: [main, daily]}")
    bound = project.bind().value
    assert bound is not None
    renderer = bound.plugins["renderers"]
    assert renderer.options.model_dump() == {
        "pipelines": ["main", "daily"],
        "task_grouping": "none",
    }
    assert bound.runtime.deploy is not None
    plan_of(project)  # compiling must not call the factory, which raises


def test_unknown_kind_is_plr204(tmp_path: Path) -> None:
    outcome = make(tmp_path, "{kind: nope}").bind()
    (d,) = [d for d in outcome.diagnostics if d.code == "PLR204"]
    assert "renderer kind 'nope'" in d.message and d.location is not None and d.location.line == 7


def test_unknown_kind_without_any_renderer_installed(tmp_path: Path) -> None:
    outcome = make(tmp_path, "{kind: fakejobs}", plugins=()).bind()
    assert [d.code for d in outcome.diagnostics] == ["PLR204"]


def test_option_errors_carry_line_marks(tmp_path: Path) -> None:
    outcome = make(tmp_path, "{kind: fakejobs, task_grouping: sideways, extra: 1}").bind()
    assert outcome.value is None
    errors = [d for d in outcome.diagnostics if d.severity.value == "error"]
    assert len(errors) == 2
    assert all(d.location is not None and d.location.line == 7 for d in errors)
    assert any(
        "/deploy/task_grouping" in (d.location.json_pointer or "") for d in errors if d.location
    )


def test_a_target_deploy_replaces_the_top_level_one(tmp_path: Path) -> None:
    yml = YML.format(deploy="{kind: fakejobs, pipelines: [t]}").replace(
        "targets:", "deploy: {kind: fakejobs, pipelines: [top]}\ntargets:"
    )
    root = make_project(tmp_path, {"pipeloom.yml": yml, "models/m.yml": MODELS})
    plan = plan_of(Project(ProjectOptions(project_dir=root, plugins=PLUGINS, env={})))
    assert plan.metadata.deploy == {"kind": "fakejobs", "pipelines": ["t"], "task_grouping": "none"}


def test_manifest_carries_deploy_and_round_trips(tmp_path: Path) -> None:
    plan = plan_of(make(tmp_path, "{kind: fakejobs, pipelines: [main, daily]}"))
    assert plan.metadata.deploy == {
        "kind": "fakejobs",
        "pipelines": ["main", "daily"],
        "task_grouping": "none",
    }
    path = tmp_path / "out" / "manifest.json"
    write_manifest(plan, path)
    assert json.loads(path.read_text("utf-8"))["metadata"]["deploy"]["kind"] == "fakejobs"
    report = Report()
    assert read_manifest(path, report) == plan and report.sorted() == ()


def test_no_deploy_block_leaves_the_manifest_without_the_key(tmp_path: Path) -> None:
    root = make_project(
        tmp_path,
        {
            "pipeloom.yml": "config_version: 1\nname: s\ntargets:\n  dev: {engine: {kind: memory}, catalog: {kind: memory}}\n",
            "models/m.yml": MODELS,
        },
    )
    plan = plan_of(Project(ProjectOptions(project_dir=root, env={})))
    assert plan.metadata.deploy is None
    assert "deploy" not in json.loads(manifest_bytes(plan))["metadata"]


def test_a_deploy_edit_keeps_the_plan_hash(tmp_path: Path) -> None:
    a = plan_of(make(tmp_path / "a", "{kind: fakejobs}"))
    b = plan_of(make(tmp_path / "b", "{kind: fakejobs, pipelines: [x, y], task_grouping: by_tag}"))
    assert a.metadata.deploy != b.metadata.deploy
    assert a.metadata.plan_hash == b.metadata.plan_hash


def test_stable_manifest_bytes_equal_across_compiles(tmp_path: Path) -> None:
    first = make(tmp_path / "a", "{kind: fakejobs}", invocation_id="one")  # type: ignore[arg-type]
    second = make(tmp_path / "b", "{kind: fakejobs}", invocation_id="two")  # type: ignore[arg-type]
    p1, p2 = plan_of(first), plan_of(second)
    p2 = p2.model_copy(
        update={
            "metadata": p2.metadata.model_copy(
                update={"generated_at": "2030-01-01T00:00:00.000000Z"}
            )
        }
    )
    assert manifest_bytes(p1) != manifest_bytes(p2)
    stable = manifest_bytes(p1, stable=True)
    assert stable == manifest_bytes(p2, stable=True)
    doc = json.loads(stable)
    assert doc["metadata"]["generated_at"] == doc["metadata"]["invocation_id"] == ""
    assert doc["selectors"] == {"select": [], "exclude": [], "selected": []}
    assert b"\r" not in stable and stable.endswith(b"}\n")


def test_stable_manifest_is_readable(tmp_path: Path) -> None:
    plan = plan_of(make(tmp_path, "{kind: fakejobs}"))
    path = tmp_path / "m.json"
    write_manifest(plan, path, stable=True)
    report = Report()
    read = read_manifest(path, report)
    assert read is not None and read.metadata.plan_hash == plan.metadata.plan_hash


def test_schema_has_deploy_as_one_of_on_kind(golden: Callable[[str, str], None]) -> None:
    schemas = export_schemas({"engines": [FAKE_ENGINE], "renderers": [RENDERER]})
    project = schemas["project.json"]
    branches = project["properties"]["deploy"]["oneOf"]
    assert branches[0]["properties"]["kind"]["const"] == "fakejobs"
    assert branches[-1] == {"type": "null"}
    assert (
        project["$defs"]["TargetConfig"]["properties"]["deploy"] == project["properties"]["deploy"]
    )
    golden("schema/project.deploy.json", dump_schema(project))


@pytest.mark.parametrize("name", ["project.json"])
def test_builtin_schema_deploy_is_open_without_renderers(name: str) -> None:
    schema = export_schemas({})[name]
    assert "deploy" in schema["properties"]
