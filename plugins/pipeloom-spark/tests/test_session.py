"""Session mode resolution and ownership rules (no Spark started)."""

from __future__ import annotations

import sys
from types import ModuleType
from typing import Any

import pytest

from pipeloom_spark import session
from pipeloom_spark.session import open_session, resolve_mode
from pipeloom_spark.spec import SparkEngineOptions

REAL_ACTIVE_SESSION = session.active_session
"""The function the autouse fixture below replaces, for the tests that want it back."""


@pytest.fixture(autouse=True)
def no_active_session(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(session, "active_session", lambda: None)


@pytest.mark.parametrize(
    ("opts", "env", "want"),
    [
        ({}, {}, "local"),
        ({}, {"DATABRICKS_RUNTIME_VERSION": "15.4"}, "active"),
        ({}, {"SPARK_REMOTE": "sc://host"}, "connect"),
        ({"remote": "sc://host"}, {}, "connect"),
        ({"remote": "sc://host"}, {"DATABRICKS_RUNTIME_VERSION": "15.4"}, "active"),
        ({"mode": "local"}, {"DATABRICKS_RUNTIME_VERSION": "15.4"}, "local"),
        ({"mode": "connect"}, {}, "connect"),
        ({"mode": "active"}, {}, "active"),
    ],
)
def test_resolve_mode(opts: dict[str, Any], env: dict[str, str], want: str) -> None:
    assert resolve_mode(SparkEngineOptions(**opts), env) == want


def test_an_existing_session_means_active(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(session, "active_session", lambda: object())
    assert resolve_mode(SparkEngineOptions(), {}) == "active"


def test_conf_is_rejected_with_active_before_anything_opens() -> None:
    opts = SparkEngineOptions(mode="active", conf={"spark.x": "1"})
    with pytest.raises(ValueError, match="mode active"), open_session(opts, {}):
        pass
    auto = SparkEngineOptions(conf={"spark.x": "1"})
    env = {"DATABRICKS_RUNTIME_VERSION": "15.4"}
    with pytest.raises(ValueError, match="mode active"), open_session(auto, env):
        pass


class _Spark:
    """A stand-in for a Spark session that fails the test if it is configured or stopped."""

    def __getattr__(self, name: str) -> Any:
        raise AssertionError(f"the session was touched: {name}")


def fake_pyspark(
    monkeypatch: pytest.MonkeyPatch, active: object | None, default: object | None = None
) -> list[str]:
    """Install a `pyspark.sql.SparkSession`; the returned list records builder calls."""
    calls: list[str] = []
    created = _Spark()

    class Builder:
        def getOrCreate(self) -> _Spark:
            calls.append("getOrCreate")
            return created

        def __getattr__(self, name: str) -> Any:
            raise AssertionError(f"the builder was configured: {name}")

    class SparkSession:
        builder = Builder()

        @staticmethod
        def getActiveSession() -> object | None:
            return active

        @staticmethod
        def active() -> object:
            if default is None:
                raise RuntimeError("no default session")
            return default

    root, sql = ModuleType("pyspark"), ModuleType("pyspark.sql")
    sql.SparkSession = SparkSession  # type: ignore[attr-defined]
    root.sql = sql  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "pyspark", root)
    monkeypatch.setitem(sys.modules, "pyspark.sql", sql)
    monkeypatch.setattr(session, "active_session", REAL_ACTIVE_SESSION)
    return calls


def test_active_mode_uses_the_active_session_and_never_builds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    existing = _Spark()
    calls = fake_pyspark(monkeypatch, existing)
    env = {"DATABRICKS_RUNTIME_VERSION": "15.4"}
    with open_session(SparkEngineOptions(), env) as handle:
        assert handle.spark is existing
        assert (handle.mode, handle.owned) == ("active", False)
    assert calls == []


def test_on_databricks_without_a_session_active_mode_attaches_with_no_conf(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = fake_pyspark(monkeypatch, None)
    env = {"DATABRICKS_RUNTIME_VERSION": "15.4"}
    with open_session(SparkEngineOptions(), env) as handle:
        assert (handle.mode, handle.owned) == ("active", False)
    assert calls == ["getOrCreate"]  # a configured or stopped session would have raised


def test_a_default_session_from_another_thread_means_auto_picks_active(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    default = _Spark()
    calls = fake_pyspark(monkeypatch, None, default)  # getActiveSession() is None here
    opts = SparkEngineOptions()
    assert resolve_mode(opts, {}) == "active"
    with open_session(opts, {}) as handle:
        assert handle.spark is default
        assert (handle.mode, handle.owned) == ("active", False)
    assert calls == []  # nothing built, configured or stopped (the stand-in raises on both)


def test_active_mode_off_databricks_needs_a_session(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = fake_pyspark(monkeypatch, None)
    with (
        pytest.raises(RuntimeError, match="none is active"),
        open_session(SparkEngineOptions(mode="active"), {}),
    ):
        pass
    assert calls == []
