from __future__ import annotations

from typing import Any

from pipeloom.config.yaml import parse_yaml
from pipeloom.diagnostics import Report
from pipeloom.secrets.interpolation import interpolate


def run(
    text: str,
    env: dict[str, str] | None = None,
    vars_: dict[str, Any] | None = None,
    skip: Any = lambda p: False,
) -> tuple[Any, list[Any], Report]:
    report = Report()
    doc = parse_yaml(text.encode(), "f.yml", report)
    assert doc is not None
    data, records = interpolate(doc, env=env or {}, vars=vars_ or {}, report=report, skip=skip)
    return data, records, report


def test_env_and_default() -> None:
    data, records, report = run("a: '${env:A}'\nb: '${env:B:-fallback}'\n", {"A": "1"})
    assert data == {"a": "1", "b": "fallback"} and not report.has_errors
    assert [(r.name, r.default_used) for r in records] == [("A", False), ("B", True)]


def test_empty_default_and_default_with_colon() -> None:
    data, _, _ = run("a: '${env:A:-}'\nb: '${env:B:-x:y}'\n")
    assert data == {"a": "", "b": "x:y"}


def test_var_whole_scalar_keeps_type_and_embedded_is_str() -> None:
    data, _, _ = run("a: '${var:n}'\nb: 'n=${var:n}'\n", vars_={"n": 5})
    assert data == {"a": 5, "b": "n=5"}


def test_unset_env_is_pls106_and_value_kept() -> None:
    data, _, report = run("a: '${env:NOPE}'\n")
    assert [d.code for d in report.sorted()] == ["PLS106"]
    assert data == {"a": "${env:NOPE}"}


def test_unknown_var_suggests() -> None:
    _, _, report = run("a: '${var:contry}'\n", vars_={"country": "BR"})
    d = report.sorted()[0]
    assert d.code == "PLS106" and d.hint == "did you mean 'country'?"


def test_malformed() -> None:
    for text in ("${env:}", "${env:1x}", "${var:a-b}", "${env: X}"):
        _, _, report = run(f"a: '{text}'\n")
        assert [d.code for d in report.sorted()] == ["PLS107"], text


def test_secret_and_foreign_expressions_untouched() -> None:
    data, _, report = run("a: '${secret:kv/db}'\nb: 'select ${x} from t'\nc: '${env}'\n")
    assert data == {"a": "${secret:kv/db}", "b": "select ${x} from t", "c": "${env}"}
    assert not report.has_errors


def test_escape() -> None:
    data, _, _ = run("a: '$${env:A} and ${env:A}'\n", {"A": "v"})
    assert data == {"a": "${env:A} and v"}


def test_skip_leaves_value() -> None:
    data, records, report = run(
        "a: '${env:A}'\nb: '${env:A}'\n", {"A": "v"}, skip=lambda p: p == "/a"
    )
    assert data == {"a": "${env:A}", "b": "v"} and len(records) == 1 and not report.has_errors


def test_keys_and_non_strings_untouched() -> None:
    data, _, _ = run("'${env:A}': 1\nl: [1, true, null]\n", {"A": "v"})
    assert data == {"${env:A}": 1, "l": [1, True, None]}
