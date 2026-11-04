from __future__ import annotations

from typing import Any

import pytest
from pydantic import ValidationError

from pipeloom.config.models import (
    InputConfig,
    ModelConfig,
    ModelSettings,
    PluginBlock,
    ProjectConfig,
    ResolvedModelConfig,
    SinkBlock,
    SourceConfig,
)
from pipeloom.config.types import parse_duration


@pytest.mark.parametrize(
    ("value", "seconds"),
    [("30m", 1800), ("1h30m", 5400), ("2d", 172800), (90, 90), ("45", 45), ("1d2h3m4s", 93784)],
)
def test_duration(value: Any, seconds: int) -> None:
    assert ModelSettings.model_validate({"timeout": value}).timeout == seconds
    assert parse_duration(value) == seconds


@pytest.mark.parametrize("bad", ["", "abc", "5x", "-1", "1.5h", -5])
def test_duration_rejects(bad: Any) -> None:
    with pytest.raises(ValidationError):
        ModelSettings.model_validate({"timeout": bad})


def test_duration_serializes_as_int() -> None:
    assert ModelSettings.model_validate({"timeout": "1h"}).model_dump()["timeout"] == 3600


def project(**extra: Any) -> dict[str, Any]:
    return {"config_version": 1, "name": "p", "targets": {"dev": {}}, **extra}


@pytest.mark.parametrize("path", ["/abs", "C:/x", "..", "a/../b", "a\\b", "\\x"])
def test_paths_reject_absolute_and_parent(path: str) -> None:
    with pytest.raises(ValidationError):
        ProjectConfig.model_validate(project(model_paths=[path]))


def test_paths_accept_relative() -> None:
    cfg = ProjectConfig.model_validate(project(python_paths=[".", "src/x"]))
    assert cfg.python_paths == [".", "src/x"] and cfg.model_paths == ["models"]


def test_identifier_and_targets() -> None:
    with pytest.raises(ValidationError):
        ProjectConfig.model_validate(project(name="1bad"))
    with pytest.raises(ValidationError):
        ProjectConfig.model_validate(project(targets={}))
    with pytest.raises(ValidationError):
        ProjectConfig.model_validate(project(targets={"bad-name": {}}))


def test_config_version_must_be_1() -> None:
    with pytest.raises(ValidationError):
        ProjectConfig.model_validate({**project(), "config_version": 2})


def test_extra_forbidden_but_plugin_blocks_are_open() -> None:
    with pytest.raises(ValidationError):
        ProjectConfig.model_validate(project(nope=1))
    block = PluginBlock.model_validate({"kind": "x", "root": "/r", "n": 3})
    assert block.model_extra == {"root": "/r", "n": 3}
    sink = SinkBlock.model_validate({"kind": "jsonl", "path": "a"})
    assert (sink.failure_budget, sink.buffered, sink.buffer_size) == (5, False, 10_000)


def test_models_are_frozen() -> None:
    cfg = ProjectConfig.model_validate(project())
    with pytest.raises(ValidationError):
        cfg.name = "other"


def test_exclusive_fields() -> None:
    with pytest.raises(ValidationError):
        ModelConfig.model_validate({"name": "m"})
    with pytest.raises(ValidationError):
        ModelConfig.model_validate({"name": "m", "sql": "x", "python": "a:b"})
    with pytest.raises(ValidationError):
        InputConfig.model_validate({"ref": "a", "source": "b"})
    with pytest.raises(ValidationError):
        SourceConfig.model_validate({"name": "s"})
    assert SourceConfig.model_validate({"name": "s", "table": "t"}).table == "t"


@pytest.mark.parametrize("alias", ["ctx", "class", "for"])
def test_input_alias_rejected(alias: str) -> None:
    with pytest.raises(ValidationError):
        ModelConfig.model_validate({"name": "m", "sql": "x", "inputs": {alias: {"ref": "a"}}})


def test_schema_alias() -> None:
    cfg = ModelConfig.model_validate({"name": "m", "sql": "x", "schema": "mart"})
    assert cfg.schema_ == "mart"


def test_expectation_needs_check_or_expr() -> None:
    base = {"name": "m", "sql": "x"}
    good = {"name": "e", "check": {"not_null": ["id"]}}
    assert ModelConfig.model_validate({**base, "expectations": [good]})
    for bad in (
        {"name": "e"},
        {"name": "e", "expr": "a > 1", "check": {"not_null": ["id"]}},
        {"name": "e", "check": {"not_null": ["id"], "unique": ["id"]}},
    ):
        with pytest.raises(ValidationError):
            ModelConfig.model_validate({**base, "expectations": [bad]})


def test_resolved_fields_match_settings() -> None:
    assert list(ResolvedModelConfig.model_fields) == list(ModelSettings.model_fields)
    resolved = ResolvedModelConfig()
    assert (resolved.materialized, resolved.retries, resolved.frame_api) == ("table", 0, "native")
    assert resolved.write.on_schema_change == "fail"


def test_attribute_docstrings_become_descriptions() -> None:
    assert ModelConfig.model_fields["sql"].description == "SQL body."
