from __future__ import annotations

from typing import Any

from pydantic import ValidationError

from pipeloom.config.errors import from_validation_error
from pipeloom.config.models import ProjectConfig, PropertiesFile
from pipeloom.config.yaml import parse_yaml
from pipeloom.diagnostics import Diagnostic, Report


def check(model: Any, text: str, base: str = "") -> list[Diagnostic]:
    report = Report()
    doc = parse_yaml(text.encode(), "f.yml", report)
    assert doc is not None
    try:
        model.model_validate(doc.data if not base else doc.data[base.lstrip("/")])
    except ValidationError as err:
        from_validation_error(err, doc, model, report, base_pointer=base)
    return list(report.sorted())


def test_unknown_field_nested_uses_key_mark_and_model_suggestion() -> None:
    text = "models:\n  - name: m\n    config:\n      materialised: table\n"
    (d,) = check(PropertiesFile, text)
    assert d.code == "PLS101" and d.hint == "did you mean 'materialized'?"
    assert d.location is not None
    assert (d.location.line, d.location.col, d.location.json_pointer) == (
        4,
        7,
        "/models/0/config/materialised",
    )


def test_unknown_field_inside_dict_of_models() -> None:
    text = "config_version: 1\nname: p\ntargets:\n  dev:\n    workerz: 2\n"
    (d,) = check(ProjectConfig, text)
    assert d.code == "PLS101" and d.hint == "did you mean 'workers'?"


def test_missing_uses_parent_mapping() -> None:
    (d,) = check(PropertiesFile, "models:\n  - description: x\n")
    assert d.code == "PLS105" and d.location is not None and d.location.json_pointer == "/models/0"
    assert "'name'" in d.message


def test_exclusive_and_other_errors() -> None:
    both = (
        "config_version: 1\nname: p\ntargets:\n  dev: {}\n"
        "sources:\n  g:\n    tables:\n      t:\n        location: a\n        table: b\n"
    )
    assert [d.code for d in check(ProjectConfig, both)] == ["PLS108"]
    bad = "models:\n  - name: m\n    config:\n      retries: lots\n"
    assert [d.code for d in check(PropertiesFile, bad)] == ["PLS102"]


def test_base_pointer() -> None:
    text = "wrapper:\n  models:\n    - name: m\n      sqll: x\n"
    (d,) = check(PropertiesFile, text, base="/wrapper")
    assert d.code == "PLS101"
    assert d.location is not None
    assert d.location.json_pointer == "/wrapper/models/0/sqll"
    assert (d.location.line, d.location.col) == (4, 7)


def test_union_branch_segments_are_skipped() -> None:
    (d,) = check(ProjectConfig, "config_version: 1\nname: p\ntargets:\n  dev: {boundary: nope}\n")
    assert d.code == "PLS102"
    assert d.location is not None and d.location.json_pointer == "/targets/dev/boundary"


def test_input_value_not_leaked() -> None:
    (d,) = check(
        ProjectConfig, "config_version: 1\nname: p\ntargets: {dev: {workers: 'sekret-1'}}\n"
    )
    assert "sekret-1" not in d.message
