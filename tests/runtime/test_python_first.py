"""Decorated models next to YAML models: resolve, compile and run on memory (ADR-0050)."""

from __future__ import annotations

from pathlib import Path

from pipeloom.api import Project, ProjectOptions
from pipeloom.plan.model import PythonBody
from pipeloom.runtime.run import RunOptions
from pipeloom.testing.clock import FakeClock
from tests.conftest import make_project

FILES = {
    "pipeloom.yml": """\
config_version: 1
name: shop
model_paths: [models]
python_paths: [models]
vars:
  min_id: 1
models:
  +tags: [project]
targets:
  dev:
    engine:
      kind: memory
    catalog:
      kind: memory
""",
    "models/legacy.yml": """\
models:
  - name: src
    python: "code:src"
""",
    "models/_properties.yml": """\
models:
  - name: big
    description: Rows above min_id.
    config:
      retries: 2
""",
    "models/code.py": """\
from pipeloom import model, var


def src(ctx):
    return [{"id": 1}, {"id": 2}, {"id": 3}]


@model(tags=["py"], meta={"min": var("min_id")})
def big(*, ctx, src):
    return ctx.sql("select id from src where id > %d" % ctx.meta["min"])


@model
def check(big):
    assert sorted(r["id"] for r in big) == [2, 3], big
    return big
""",
}


def test_decorated_models_resolve_compile_and_run(tmp_path: Path) -> None:
    root = make_project(tmp_path, FILES)
    project = Project(ProjectOptions(project_dir=root, env={}), clock=FakeClock())
    plan = project.compile()
    assert plan.value is not None, plan.diagnostics
    big = plan.value.nodes["model.shop.big"]
    assert big.body == PythonBody(callable="code:big", file="models/code.py")
    assert (big.file, big.line) == ("models/code.py", 8)
    assert big.inputs == {"src": "model.shop.src"}
    assert big.config.tags == ["project", "py"]
    assert big.config.retries == 2
    assert big.config.meta == {"min": 1}
    outcome = project.run(RunOptions(run_id="r", workers=1))
    assert [d.code for d in outcome.diagnostics if d.severity == "error"] == []
