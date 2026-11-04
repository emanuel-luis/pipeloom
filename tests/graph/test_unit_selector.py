"""The `unit:` selector method, unit selections and selection over a manifest's plan."""

from __future__ import annotations

from pathlib import Path

import pytest

from pipeloom.api import Project, ProjectOptions
from pipeloom.diagnostics import Report
from pipeloom.graph.selectors import (
    Selection,
    SelectorError,
    apply,
    apply_to_plan,
    is_unit_selection,
)
from tests.conftest import make_project

MODELS = """models:
  - {name: a, sql: select 1 as x, tags: [t]}
  - {name: e, sql: select * from a, inputs: {a: {ref: a}}, materialized: ephemeral}
  - {name: b, sql: select * from e, inputs: {e: {ref: e}}}
"""


@pytest.fixture
def project(tmp_path: Path) -> Project:
    make_project(
        tmp_path,
        {
            "pipeloom.yml": "config_version: 1\nname: p\ntargets:\n  d:\n"
            "    engine: {kind: memory}\n    catalog: {kind: memory}\n",
            "models/m.yml": MODELS,
        },
    )
    return Project(ProjectOptions(project_dir=tmp_path, env={}))


def names(ids: frozenset[str]) -> list[str]:
    return sorted(i.rsplit(".", 1)[-1] for i in ids)


def test_unit_method_matches_persisted_models_only(project: Project) -> None:
    pg = project.validate().value
    assert pg is not None
    got = apply(Selection(("unit:a unit:b",)), pg.graph, pg.project, Report())
    assert names(got) == ["a", "b"]
    assert names(apply(Selection(("unit:*",)), pg.graph, pg.project, Report())) == ["a", "b"]


@pytest.mark.parametrize("name", ["e", "nope"])
def test_unknown_unit_and_ephemeral_models_are_plg306(project: Project, name: str) -> None:
    pg = project.validate().value
    assert pg is not None
    with pytest.raises(SelectorError) as err:
        apply(Selection((f"unit:{name}",)), pg.graph, pg.project, Report())
    assert err.value.diagnostic.code == "PLG306"


def test_apply_to_plan_agrees_with_apply(project: Project) -> None:
    pg = project.validate().value
    plan = project.compile().value
    assert pg is not None and plan is not None
    for expr in ("a+", "+b", "tag:t", "materialized:ephemeral", "unit:a"):
        sel = Selection((expr,))
        assert apply_to_plan(sel, plan, Report()) == apply(sel, pg.graph, pg.project, Report())
    with pytest.raises(SelectorError):
        apply_to_plan(Selection(("unit:nope",)), plan, Report())


@pytest.mark.parametrize(
    ("select", "expected"),
    [
        ((), False),
        (("unit:a",), True),
        (("unit:a unit:b",), True),
        (("unit:a", "unit:b"), True),
        (("a",), False),
        (("unit:a", "b"), False),
        (("+unit:a",), False),
        (("unit:a+",), False),
        (("a,,b",), False),
    ],
)
def test_is_unit_selection(select: tuple[str, ...], expected: bool) -> None:
    assert is_unit_selection(Selection(select)) is expected
