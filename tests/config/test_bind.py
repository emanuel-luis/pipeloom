from __future__ import annotations

from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any

import pytest

from pipeloom.api import Project, ProjectOptions
from pipeloom.config.bind import BoundProject
from pipeloom.diagnostics import Diagnostic, Outcome
from pipeloom.engine.types import TargetRef
from pipeloom.platform import Identity
from pipeloom.testing.memory import MemoryEngine

Factory = Callable[[Mapping[str, Any]], Path]
FAKES = "tests.plugins.fakes"

TWO = """\
config_version: 1
name: shop
{top}targets:
  dev:
    engine: {{kind: memory}}
{dev}  prod:
    engine: {{kind: memory}}
"""


def project_file(top: str = "", dev: str = "") -> str:
    return TWO.format(top=top, dev=dev)


def bind(
    root: Path,
    *,
    env: Mapping[str, str] | None = None,
    target: str | None = None,
    plugins: tuple[str, ...] = (),
    boundary: Any = None,
) -> Outcome[BoundProject]:
    opts = ProjectOptions(
        project_dir=root,
        target=target,
        plugins=plugins,
        boundary=boundary,
        env={} if env is None else env,
    )
    return Project(opts).bind()


def codes(outcome: Outcome[Any]) -> list[str]:
    return [d.code for d in outcome.diagnostics if d.code != "PLR206"]


def only(outcome: Outcome[Any], code: str) -> Diagnostic:
    (d,) = [d for d in outcome.diagnostics if d.code == code]
    return d


@pytest.mark.parametrize(
    ("kwargs", "expected"),
    [
        ({"target": "prod", "env": {"PIPELOOM_TARGET": "dev"}}, "prod"),
        ({"env": {"PIPELOOM_TARGET": "prod"}}, "prod"),
        ({}, "dev"),  # default_target
    ],
)
def test_target_precedence(project_factory: Factory, kwargs: dict[str, Any], expected: str) -> None:
    root = project_factory({"pipeloom.yml": project_file(top="default_target: dev\n")})
    outcome = bind(root, **kwargs)
    assert outcome.value is not None and outcome.value.target == expected


def test_the_only_target_is_selected(project_factory: Factory) -> None:
    text = "config_version: 1\nname: s\ntargets:\n  solo: {engine: {kind: memory}}\n"
    outcome = bind(project_factory({"pipeloom.yml": text}))
    assert outcome.value is not None and outcome.value.target == "solo"


def test_plr207_when_several_targets_and_no_choice(project_factory: Factory) -> None:
    outcome = bind(project_factory({"pipeloom.yml": project_file()}))
    assert outcome.value is None and codes(outcome) == ["PLR207"]
    assert "dev, prod" in (only(outcome, "PLR207").hint or "")


@pytest.mark.parametrize(
    ("kwargs", "top", "pointer"),
    [
        ({"target": "prd"}, "", "/targets"),
        ({"env": {"PIPELOOM_TARGET": "prd"}}, "", "/targets"),
        ({}, "default_target: prd\n", "/default_target"),
    ],
)
def test_plr204_unknown_target(
    project_factory: Factory, kwargs: dict[str, Any], top: str, pointer: str
) -> None:
    outcome = bind(project_factory({"pipeloom.yml": project_file(top=top)}), **kwargs)
    d = only(outcome, "PLR204")
    assert outcome.value is None
    assert d.hint == "did you mean 'prod'?"
    assert d.location is not None and d.location.json_pointer == pointer


def test_plr204_unknown_plugin_kind_with_suggestion(project_factory: Factory) -> None:
    text = "config_version: 1\nname: s\ntargets:\n  dev: {catalog: {kind: memroy}}\n"
    outcome = bind(project_factory({"pipeloom.yml": text}))
    d = only(outcome, "PLR204")
    assert d.message == "unknown catalog kind 'memroy'" and d.hint == "did you mean 'memory'?"
    assert d.location is not None and d.location.json_pointer == "/targets/dev/catalog/kind"


def test_unknown_sink_kind_is_plr204(project_factory: Factory) -> None:
    text = (
        "config_version: 1\nname: s\ntargets:\n  dev: {}\nevents:\n  sinks:\n    - kind: console\n"
    )
    outcome = bind(project_factory({"pipeloom.yml": text}))
    d = only(outcome, "PLR204")
    assert d.location is not None and d.location.json_pointer == "/events/sinks/0/kind"


def test_defaults_when_nothing_is_set(project_factory: Factory) -> None:
    text = "config_version: 1\nname: s\ntargets:\n  dev: {}\n"
    value = bind(project_factory({"pipeloom.yml": text})).value
    assert value is not None
    rt = value.runtime
    assert (rt.workers, rt.boundary, rt.naming.kind, rt.engine) == (4, "process", "none", None)
    assert set(value.plugins) == {"naming"}
    strategy: Any = value.plugins["naming"].create()
    assert strategy.relation("m", None, Identity("u", "h")) == TargetRef(None, "main", "m")


def test_runtime_merge_target_beats_top_level_beats_default(project_factory: Factory) -> None:
    text = (
        "config_version: 1\nname: s\nworkers: 2\nboundary: task\n"
        "naming: {kind: prefix, schema_prefix: top_}\n"
        "engine: {kind: memory}\n"
        "targets:\n"
        "  dev: {workers: 8, naming: {kind: per_user, schema: dev}}\n"
        "  ci: {}\n"
    )
    root = project_factory({"pipeloom.yml": text})
    dev = bind(root, target="dev").value
    ci = bind(root, target="ci").value
    assert dev is not None and ci is not None
    assert (dev.runtime.workers, dev.runtime.boundary, dev.runtime.naming.kind) == (
        8,
        "task",
        "per_user",
    )
    assert (ci.runtime.workers, ci.runtime.naming.kind) == (2, "prefix")
    assert dev.plugins["naming"].pointer == "/targets/dev/naming"
    assert ci.plugins["naming"].pointer == "/naming"
    assert ci.plugins["engines"].pointer == "/engine"
    assert isinstance(ci.plugins["engines"].create(), MemoryEngine)


def test_cli_boundary_beats_the_file(project_factory: Factory) -> None:
    root = project_factory({"pipeloom.yml": project_file(top="boundary: task\n")})
    value = bind(root, target="dev", boundary="process").value
    assert value is not None and value.runtime.boundary == "process"


def test_unselected_targets_are_not_bound(project_factory: Factory) -> None:
    text = (
        "config_version: 1\nname: s\ntargets:\n"
        "  dev: {engine: {kind: memory}}\n  bad: {engine: {kind: nope}}\n"
    )
    assert bind(project_factory({"pipeloom.yml": text}), target="dev").value is not None


def option_case(block: str) -> str:
    return f"config_version: 1\nname: s\ntargets:\n  dev:\n    engine: {block}\n"


def test_plugin_options_are_validated_with_a_pointer(project_factory: Factory) -> None:
    text = option_case("{kind: fake, pth: x, size: nope}")
    root = project_factory({"pipeloom.yml": text})
    outcome = bind(root, plugins=(f"{FAKES}:FAKE_ENGINE",))
    got = {(d.code, d.location.json_pointer if d.location else None) for d in outcome.diagnostics}
    assert ("PLS101", "/targets/dev/engine/pth") in got
    assert ("PLS102", "/targets/dev/engine/size") in got
    assert only(outcome, "PLS101").hint == "did you mean 'path'?"
    assert outcome.value is None


def test_plugin_required_option_missing_is_pls105(project_factory: Factory) -> None:
    root = project_factory({"pipeloom.yml": option_case("{kind: strict}")})
    outcome = bind(root, plugins=(f"{FAKES}:STRICT",))
    d = only(outcome, "PLS105")
    assert "url" in d.message and d.location is not None
    assert d.location.json_pointer == "/targets/dev/engine"


def test_valid_plugin_options_are_kept(project_factory: Factory) -> None:
    root = project_factory({"pipeloom.yml": option_case("{kind: strict, url: db://x}")})
    value = bind(root, plugins=(f"{FAKES}:STRICT",)).value
    assert value is not None
    assert value.plugins["engines"].options.model_dump() == {"url": "db://x", "retries": 0}


def test_sink_options_exclude_the_core_sink_keys(project_factory: Factory) -> None:
    text = (
        "config_version: 1\nname: s\ntargets:\n  dev: {}\nevents:\n  sinks:\n"
        "    - {kind: tap, buffered: true, failure_budget: 2, path: out.log}\n"
    )
    value = bind(project_factory({"pipeloom.yml": text}), plugins=(f"{FAKES}:FAKE_SINK",)).value
    assert value is not None
    (sink,) = value.sinks
    assert sink.options.model_dump() == {"path": "out.log", "size": 1}


def test_cli_plugin_load_failure_is_reported(project_factory: Factory) -> None:
    root = project_factory({"pipeloom.yml": project_file(top="default_target: dev\n")})
    outcome = bind(root, plugins=("no_such_module_xyz:SPEC",))
    assert codes(outcome) == ["PLU003"]
    assert outcome.value is not None  # a failed --plugin only matters if a config needs it


def test_load_errors_stop_binding(project_factory: Factory) -> None:
    outcome = bind(project_factory({"pipeloom.yml": "config_version: 2\n"}))
    assert codes(outcome) == ["PLS103"] and outcome.value is None


def test_bind_is_memoized(project_factory: Factory) -> None:
    project = Project(
        ProjectOptions(
            project_dir=project_factory(
                {"pipeloom.yml": project_file(top="default_target: dev\n")}
            ),
            env={},
        )
    )
    assert project.bind() is project.bind()
