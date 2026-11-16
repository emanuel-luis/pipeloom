"""Decorated models next to YAML models: resolve, compile and run on memory (ADR-0050)."""

from __future__ import annotations

from pathlib import Path

import pytest

from pipeloom.api import Project, ProjectOptions
from pipeloom.plan.model import PythonBody
from pipeloom.runtime.run import RunOptions
from pipeloom.testing.clock import FakeClock
from tests.conftest import make_project

LEGACY_YAML = True  # mixes a YAML `python:` model with decorated ones (ADR-0050 point 19)

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
    python: "shop_models:src"
""",
    "models/_properties.yml": """\
models:
  - name: big
    description: Rows above min_id.
    config:
      retries: 2
""",
    "models/shop_models.py": """\
from pipeloom import model, var


def src(ctx):
    return [{"id": 1}, {"id": 2}, {"id": 3}]


@model(tags=["py"], meta={"min": var("min_id")})
def big(*, ctx, src):
    return ctx.sql("select id from src where id > 1")


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
    assert big.body == PythonBody(callable="shop_models:big", file="models/shop_models.py")
    assert (big.file, big.line) == ("models/shop_models.py", 8)
    assert big.inputs == {"src": "model.shop.src"}
    assert big.config.tags == ["project", "py"]
    assert big.config.retries == 2
    assert big.config.meta == {"min": 1}
    outcome = project.run(RunOptions(run_id="r", workers=1))
    errors = [(d.code, d.message) for d in outcome.diagnostics if d.severity == "error"]
    assert errors == []


def _codes(tmp_path: Path, code: str) -> list[tuple[str, str | None, int | None, str | None]]:
    files = {**FILES, "models/shop_models.py": code}
    root = make_project(tmp_path, files)
    outcome = Project(ProjectOptions(project_dir=root, env={})).validate()
    return [
        (d.code, d.location.file, d.location.line, d.location.json_pointer)
        for d in outcome.diagnostics
        if d.location is not None and d.severity == "error"
    ]


def test_invalid_arguments_are_located_and_keep_the_name_known(tmp_path: Path) -> None:
    code = """\
from pipeloom import model


def src(ctx):
    return []


@model(
    retries=-1,
)
def big(src):
    return src


@model(tags=[1 + 1])
def check(big):
    return big
"""
    assert _codes(tmp_path, code) == [
        ("PLS102", "models/shop_models.py", 9, "/@model/retries"),
        ("PLS102", "models/shop_models.py", 15, "/@model/tags/0"),
    ]


def test_meta_values_must_be_json(tmp_path: Path) -> None:
    code = """\
import datetime

from pipeloom import model


def src(ctx):
    return []


@model(meta={"when": datetime.date(2024, 1, 2)})
def big(src):
    return src


@model
def check(big):
    return big
"""
    assert _codes(tmp_path, code) == [("PLS102", "models/shop_models.py", 10, "/@model/meta")]


def test_literal_ctx_sql_is_checked(tmp_path: Path) -> None:
    pytest.importorskip("sqlglot")
    code = """\
from pipeloom import model


def src(ctx):
    return []


@model
def big(ctx, src):
    return ctx.sql("select id from src join extra using (id) join more using (id)",
                   inputs={"more": []})
"""
    root = make_project(tmp_path, {**FILES, "models/shop_models.py": code})
    outcome = Project(ProjectOptions(project_dir=root, env={})).validate()
    found = [(d.code, d.location.line, d.location.col) for d in outcome.diagnostics if d.location]
    assert ("PLK603", 10, 20) in found
    assert [d.message for d in outcome.diagnostics if d.code == "PLK603"] == [
        "SQL of model 'big' reads 'extra', which is not an input alias"
    ]


LOOP = """\
from pipeloom import model


def src(ctx):
    return [{"id": 1}]


for _n in ("big", "check"):

    def _copy(src):
        return src

    globals()[_n] = model(name=_n)(_copy)
"""

FACTORY = """\
from pipeloom import model


def src(ctx):
    return [{"id": 1}]


def make(name):
    @model(name=name)
    def built(src):
        return src

    return built


big = make("big")
check = make("check")
"""


@pytest.mark.parametrize("code", [LOOP, FACTORY], ids=["loop", "factory"])
def test_models_built_in_a_loop_or_factory_validate(tmp_path: Path, code: str) -> None:
    root = make_project(tmp_path, {**FILES, "models/shop_models.py": code})
    outcome = Project(ProjectOptions(project_dir=root, env={})).validate()
    codes = [d.code for d in outcome.diagnostics]
    assert not {"PLK601", "PLK604"} & set(codes), outcome.diagnostics
    assert [d for d in outcome.diagnostics if d.severity == "error"] == []


TYPO = """\
from pipeloom import model


def src(ctx):
    return []


@model(retires=2{name})
def big(src):
    return src


@model
def check(big):
    return big
{extra}"""


@pytest.mark.parametrize(
    ("name", "extra"),
    [("", ""), (', name="big"', ""), ("", "\n\nother = model(name='other')(lambda src: src)\n")],
    ids=["def-name", "literal-name", "dynamic-file"],
)
def test_a_keyword_typo_is_pls101_only(tmp_path: Path, name: str, extra: str) -> None:
    code = TYPO.format(name=name, extra=extra)
    # In a dynamic file the typo skips the import, so `check` is unknown too.
    props = {"models/_check.yml": "models:\n  - name: check\n"}
    root = make_project(tmp_path, {**FILES, **props, "models/shop_models.py": code})
    outcome = Project(ProjectOptions(project_dir=root, env={})).validate()
    assert [d.code for d in outcome.diagnostics if d.severity == "error"] == ["PLS101"]


def test_a_model_module_named_like_the_standard_library_is_ply005(tmp_path: Path) -> None:
    code = "from pipeloom import model\n\n\n@model\ndef cal(ctx):\n    return []\n"
    root = make_project(tmp_path, {**FILES, "models/calendar.py": code})
    outcome = Project(ProjectOptions(project_dir=root, env={})).validate()
    found = [d for d in outcome.diagnostics if d.code == "PLY005"]
    assert [(d.location and d.location.file, d.context) for d in found] == [
        ("models/calendar.py", {"kind": "name_mismatch"})
    ]
    assert "shadows the standard library module 'calendar'" in found[0].message


def test_python_and_yaml_models_with_one_name(tmp_path: Path) -> None:
    code = """\
from pipeloom import model


def src(ctx):
    return []


@model(name="src")
def other():
    return []


@model
def big(src):
    return src
"""
    found = _codes(tmp_path, code)
    assert [(c, f, line) for c, f, line, _ in found] == [("PLR202", "models/shop_models.py", 8)]
