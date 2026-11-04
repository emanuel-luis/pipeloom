from __future__ import annotations

from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any

import pytest

from pipeloom.api import Project, ProjectOptions
from pipeloom.config.discover import find_model_files, read_file
from pipeloom.config.yaml import parse_yaml
from pipeloom.diagnostics import Report
from tests.conftest import FIXTURES

BASE = "config_version: 1\nname: shop\ntargets:\n  dev:\n    engine: {kind: memory}\n"
Factory = Callable[[Mapping[str, Any]], Path]


def load(project_dir: Path, **kw: Any) -> Any:
    kw.setdefault("env", {})
    return Project(ProjectOptions(project_dir=project_dir, **kw)).load()


def codes(outcome: Any) -> list[str]:
    return [d.code for d in outcome.diagnostics]


def test_fixture_project_loads() -> None:
    outcome = load(FIXTURES / "minimal")
    assert outcome.value is not None and outcome.diagnostics == ()
    project = outcome.value
    assert project.config.name == "minimal"
    assert [f.path for f in project.model_files] == ["models/staging/orders.yml"]
    assert [f.path for f in project.defaults_files] == ["models/_defaults.yml"]
    assert project.config.defaults.models.timeout == 1800


def test_load_is_memoized(project_factory: Factory) -> None:
    project = Project(ProjectOptions(project_dir=project_factory({"pipeloom.yml": BASE}), env={}))
    assert project.load() is project.load()


def test_missing_project_file(tmp_path: Path) -> None:
    outcome = load(tmp_path)
    assert outcome.value is None and codes(outcome) == ["PLY004"]


def test_unreadable_model_file(tmp_path: Path) -> None:
    report = Report()
    assert read_file(tmp_path, "models/x.yml", report) is None
    assert [d.code for d in report.sorted()] == ["PLY004"]


def test_config_version_checked_first_and_stops(project_factory: Factory) -> None:
    outcome = load(project_factory({"pipeloom.yml": "name: 1\nbogus: 1\n"}))
    assert codes(outcome) == ["PLS103"]
    assert outcome.value is None


def test_non_mapping_root(project_factory: Factory) -> None:
    assert codes(load(project_factory({"pipeloom.yml": "- a\n"}))) == ["PLS102"]


def test_secret_literal_vs_reference(project_factory: Factory) -> None:
    ok = BASE + "    catalog: {kind: u, token: '${env:TOK}', api_key: '${secret:kv/key}'}\n"
    assert codes(load(project_factory({"pipeloom.yml": ok, "models/.keep": ""}))) == []
    outcome = load(
        project_factory({"pipeloom.yml": BASE + "    catalog: {kind: u, password: hunter2}\n"})
    )
    assert codes(outcome) == ["PLS104"]
    assert all("hunter2" not in d.message for d in outcome.diagnostics)


def test_env_in_secret_key_stays_unresolved(project_factory: Factory) -> None:
    text = BASE + "    catalog: {kind: u, token: '${env:TOK}', root: '${env:ROOT}'}\n"
    project = load(project_factory({"pipeloom.yml": text}), env={"ROOT": "/r", "TOK": "t"}).value
    catalog = project.config.targets["dev"].catalog
    assert catalog is not None and catalog.model_extra == {"token": "${env:TOK}", "root": "/r"}


def test_vars_order_block_then_cli_then_project(project_factory: Factory) -> None:
    text = (
        BASE
        + "vars:\n  a: '${env:A}'\n  b: one\n"
        + "    \n".replace("    \n", "")
        + "defaults:\n  models: {meta: {x: '${var:a}', y: '${var:b}'}}\n"
    )
    outcome = load(
        project_factory({"pipeloom.yml": text, "models/.keep": ""}),
        env={"A": "from-env"},
        vars={"b": "cli"},
    )
    project = outcome.value
    assert project is not None
    assert project.vars == {"a": "from-env", "b": "cli"}
    assert project.config.defaults.models.meta == {"x": "from-env", "y": "cli"}
    assert {(r.kind, r.name) for r in project.interpolations} == {
        ("env", "A"),
        ("var", "a"),
        ("var", "b"),
    }


def test_vars_block_cannot_use_vars(project_factory: Factory) -> None:
    text = BASE + "vars:\n  a: 1\n  b: '${var:a}'\n"
    assert codes(load(project_factory({"pipeloom.yml": text}))) == ["PLR206", "PLS106"]


def test_models_collected_and_errors_reported_per_file(project_factory: Factory) -> None:
    files = {
        "pipeloom.yml": BASE,
        "models/a.yml": "models: [{name: a, sql: select 1}]\n",
        "models/z/_defaults.yml": "models: {retries: 2}\n",
        "models/z/b.yaml": "models: [{name: b}]\n",
        "models/.hidden/c.yml": "not: valid: yaml: [",
    }
    outcome = load(project_factory(files))
    assert outcome.value is None
    assert codes(outcome) == ["PLS108"]
    assert outcome.diagnostics[0].location is not None
    assert outcome.diagnostics[0].location.file == "models/z/b.yaml"


def test_discovery_sorted_posix_skips_dot_dirs_and_symlink_free(project_factory: Factory) -> None:
    root = project_factory(
        {
            "pipeloom.yml": BASE,
            "models/b.yml": "{}",
            "models/a/z.yml": "{}",
            "models/a/_defaults.yaml": "{}",
            "models/.git/x.yml": "{}",
            "models/readme.md": "",
        }
    )
    report = Report()
    doc = parse_yaml(BASE.encode(), "pipeloom.yml", report)
    assert doc is not None
    files = find_model_files(root, ["models", "models"], doc, report)
    assert files.models == ("models/a/z.yml", "models/b.yml")
    assert files.defaults == ("models/a/_defaults.yaml",)


@pytest.mark.parametrize("rel", ["models/ü.yml"])
def test_utf8_file_names_and_content(project_factory: Factory, rel: str) -> None:
    files = {"pipeloom.yml": BASE, rel: "models: [{name: m, sql: select 1, description: café}]\n"}
    outcome = load(project_factory(files))
    assert outcome.value is not None
    assert outcome.value.model_files[0].config.models[0].description == "café"


@pytest.mark.parametrize(
    "value",
    ["123456", "3.14", "true", "hunter2", "'${env:K:-hunter2}'", "'${var:v}'", "'x${env:K}'"],
)
def test_secret_like_literals_are_flagged(project_factory: Factory, value: str) -> None:
    text = BASE + f"    catalog: {{kind: u, password: {value}}}\n"
    outcome = load(project_factory({"pipeloom.yml": text, "models/.keep": ""}))
    assert codes(outcome) == ["PLS104"]


@pytest.mark.parametrize("value", ["'${env:K}'", "'${env:K:-}'", "'${secret:kv/db}'", "~", "''"])
def test_secret_like_references_pass(project_factory: Factory, value: str) -> None:
    text = BASE + f"    catalog: {{kind: u, password: {value}}}\n"
    assert codes(load(project_factory({"pipeloom.yml": text, "models/.keep": ""}))) == []
