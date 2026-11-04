from __future__ import annotations

from pipeloom.diagnostics import (
    Diagnostic,
    Location,
    Report,
    Severity,
    format_diagnostic,
    suggest,
)


def test_report_defaults_severity_from_catalog_and_sorts() -> None:
    report = Report()
    report.add("PLS101", "b", location=Location("z.yml", 1, 1))
    report.add("PLR206", "w", location=Location("a.yml", 9, 1))
    report.add("PLS101", "a", location=Location("a.yml", 2, 5), hint="h")
    got = report.sorted()
    assert [d.message for d in got] == ["a", "w", "b"]
    assert got[1].severity is Severity.WARNING
    assert report.has_errors and report.error_count == 2


def test_warning_only_report_has_no_errors() -> None:
    report = Report()
    report.add("PLR206", "w")
    assert not report.has_errors


def test_json_round_trip() -> None:
    d = Diagnostic(
        "PLS101",
        Severity.ERROR,
        "m",
        hint="h",
        location=Location("f.yml", 1, 2, "/a"),
        context={"k": [1, "x"]},
    )
    assert Diagnostic.from_json(d.to_json()) == d
    bare = Diagnostic("PLR206", Severity.WARNING, "m")
    assert Diagnostic.from_json(bare.to_json()) == bare


def test_format() -> None:
    d = Diagnostic("PLS101", Severity.ERROR, "bad", "try x", Location("m/a.yml", 12, 14))
    assert format_diagnostic(d) == "error PLS101 m/a.yml:12:14 bad\n      hint: try x"
    assert format_diagnostic(Diagnostic("PLE401", Severity.WARNING, "x")) == "warning PLE401 x"


def test_suggest() -> None:
    assert suggest("targts", ["targets", "name"]) == "did you mean 'targets'?"
    assert suggest("zzz", ["targets", "name"]) is None
