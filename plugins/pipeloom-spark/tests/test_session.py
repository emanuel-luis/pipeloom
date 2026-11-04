"""Session mode resolution and ownership rules (no Spark started)."""

from __future__ import annotations

from typing import Any

import pytest

from pipeloom_spark import session
from pipeloom_spark.session import open_session, resolve_mode
from pipeloom_spark.spec import SparkEngineOptions


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
