from __future__ import annotations

import sys

import pytest

from pipeloom.diagnostics import Report
from pipeloom.plugins import API_VERSION, GROUPS, PluginRegistry, PluginSpec, check_api
from tests.plugins.fakes import (
    API02,
    API05,
    FakeDist,
    FakeEntryPoint,
    FakeOptions,
    install_entry_points,
)

FAKES = "tests.plugins.fakes"


def ep(group: str, name: str, attr: str, dist: str = "fake-dist") -> FakeEntryPoint:
    return FakeEntryPoint(name, f"{FAKES}:{attr}", group, FakeDist(dist, "1.2.3"))


def codes(report: Report) -> list[str]:
    return [d.code for d in report.sorted()]


def test_builtins_are_listed_without_importing_them() -> None:
    sys.modules.pop("pipeloom.testing.memory", None)
    reg = PluginRegistry.discover(autoload=False)
    kinds = {(e.group, e.kind) for e in reg.entries()}
    assert ("engines", "memory") in kinds and ("naming", "per_user") in kinds
    assert all(e.source == "builtin" and e.distribution == "pipeloom" for e in reg.entries())
    assert "pipeloom.testing.memory" not in sys.modules


def test_entries_are_sorted_by_group_then_kind() -> None:
    entries = PluginRegistry.discover(autoload=False).entries()
    assert [(e.group, e.kind) for e in entries] == sorted((e.group, e.kind) for e in entries)


def test_entry_points_are_read_as_metadata_only(monkeypatch: pytest.MonkeyPatch) -> None:
    asked = install_entry_points(monkeypatch, [ep("engines", "fake", "FAKE_ENGINE")])
    reg = PluginRegistry.discover(env={})
    assert asked == [f"pipeloom.{g}" for g in GROUPS]
    entry = reg.entries("engines")[0]
    assert (entry.kind, entry.source, entry.distribution, entry.version) == (
        "fake",
        "entry_point",
        "fake-dist",
        "1.2.3",
    )


def test_same_distribution_seen_twice_is_deduplicated(monkeypatch: pytest.MonkeyPatch) -> None:
    point = ep("engines", "fake", "FAKE_ENGINE")
    install_entry_points(monkeypatch, [point, point])
    reg = PluginRegistry.discover(env={})
    assert [e.kind for e in reg.entries("engines")].count("fake") == 1
    assert not reg.duplicates()


@pytest.mark.parametrize(
    ("env", "autoload", "expect_third_party"),
    [
        ({"PIPELOOM_DISABLE_PLUGIN_AUTOLOAD": "1"}, None, False),
        ({}, None, True),
        ({"PIPELOOM_DISABLE_PLUGIN_AUTOLOAD": "1"}, True, True),
        ({}, False, False),
    ],
)
def test_autoload_switch(
    monkeypatch: pytest.MonkeyPatch,
    env: dict[str, str],
    autoload: bool | None,
    expect_third_party: bool,
) -> None:
    install_entry_points(monkeypatch, [ep("engines", "fake", "FAKE_ENGINE")])
    reg = PluginRegistry.discover(env=env, autoload=autoload)
    kinds = {e.kind for e in reg.entries("engines")}
    assert ("fake" in kinds) is expect_third_party
    assert "memory" in kinds  # built-ins stay on


def test_autoload_disabled_still_loads_cli_plugins(monkeypatch: pytest.MonkeyPatch) -> None:
    install_entry_points(monkeypatch, [])
    report = Report()
    reg = PluginRegistry.discover(
        cli_plugins=[f"{FAKES}:FAKE_ENGINE"],
        env={"PIPELOOM_DISABLE_PLUGIN_AUTOLOAD": "1"},
        report=report,
    )
    (cli,) = [e for e in reg.entries("engines") if e.source == "cli"]
    assert cli.kind == "fake" and cli.distribution == "(cli)"
    assert reg.load("engines", "fake", report) is not None and codes(report) == []


def test_load_returns_spec_and_caches(monkeypatch: pytest.MonkeyPatch) -> None:
    install_entry_points(monkeypatch, [ep("engines", "fake", "FAKE_ENGINE")])
    reg = PluginRegistry.discover(env={})
    report = Report()
    spec = reg.load("engines", "fake", report)
    assert spec is not None and spec.config_model is FakeOptions
    assert reg.load("engines", "fake", report) is spec


def test_unknown_kind_is_none_without_a_diagnostic() -> None:
    report = Report()
    assert PluginRegistry.discover(autoload=False).load("engines", "nope", report) is None
    assert codes(report) == []


def test_plu001_api_mismatch(monkeypatch: pytest.MonkeyPatch) -> None:
    install_entry_points(monkeypatch, [ep("engines", "future", "API05")])
    report = Report()
    assert PluginRegistry.discover(env={}).load("engines", "future", report) is None
    (d,) = report.sorted()
    assert d.code == "PLU001" and "0.5" in d.message and "0.4" in d.message


def test_plu002_duplicate_kind(monkeypatch: pytest.MonkeyPatch) -> None:
    install_entry_points(monkeypatch, [ep("engines", "memory", "MEMORY_CLONE", "other")])
    reg = PluginRegistry.discover(env={})
    assert list(reg.duplicates()) == [("engines", "memory")]
    report = Report()
    assert reg.load("engines", "memory", report) is None
    assert codes(report) == ["PLU002"]


@pytest.mark.parametrize(
    ("value", "needle"),
    [
        ("no_such_module_xyz:SPEC", "cannot load"),
        (f"{FAKES}:MISSING", "cannot load"),
        (f"{FAKES}:NOT_A_SPEC", "is not a PluginSpec"),
        (f"{FAKES}:WRONG_GROUP", "registered as engines"),
        (f"{FAKES}:DUP_A", "registered as engines 'other'"),
        ("not-a-reference", "module:attr"),
    ],
)
def test_plu003_load_failures(monkeypatch: pytest.MonkeyPatch, value: str, needle: str) -> None:
    install_entry_points(monkeypatch, [FakeEntryPoint("other", value, "engines")])
    report = Report()
    assert PluginRegistry.discover(env={}).load("engines", "other", report) is None
    (d,) = report.sorted()
    assert d.code == "PLU003" and needle in d.message


@pytest.mark.parametrize("value", ["no_such_module_xyz:SPEC", f"{FAKES}:NOT_A_SPEC"])
def test_plu003_for_cli_plugins(value: str) -> None:
    report = Report()
    reg = PluginRegistry.discover(cli_plugins=[value], autoload=False, report=report)
    assert codes(report) == ["PLU003"]
    assert not [e for e in reg.entries() if e.source == "cli"]


def test_check_api_rules() -> None:
    def spec(v: tuple[int, int]) -> PluginSpec[object]:
        return PluginSpec("x", "x", "engines", v, lambda o: o, FakeOptions)

    assert check_api(spec(API_VERSION)) is None
    assert check_api(spec((0, 0))) is not None  # below the core is still a mismatch before 1.0
    assert check_api(spec((0, 3))) is not None  # the previous API, before 0.4
    assert check_api(spec((0, 5))) is not None
    assert check_api(spec((1, 0))) is not None
    assert check_api(API05) is not None
    assert check_api(API02) is not None


def test_load_all_skips_duplicates_and_reports_failures(monkeypatch: pytest.MonkeyPatch) -> None:
    install_entry_points(
        monkeypatch,
        [
            ep("engines", "memory", "MEMORY_CLONE", "other"),
            ep("engines", "future", "API05"),
            ep("engines", "fake", "FAKE_ENGINE"),
        ],
    )
    report = Report()
    specs = PluginRegistry.discover(env={}).load_all(report)
    assert sorted(s.kind for s in specs["engines"]) == ["fake"]
    assert sorted(codes(report)) == ["PLU001", "PLU002"]
