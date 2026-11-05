"""The connection rules: no profile is ever picked automatically (ADR-0036)."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import pytest

from pipeloom_databricks.client import ConnectionNotConfigured, connect
from pipeloom_databricks.client.sdk import SdkClient
from pipeloom_databricks.testing.fake import FakeDatabricks

pytestmark = pytest.mark.filterwarnings("ignore")


class Recorder:
    """Stands in for `Config` and `WorkspaceClient`, and remembers every construction."""

    configs: list[dict[str, Any]]
    workspaces: list[Any]

    def __init__(self) -> None:
        self.configs = []
        self.workspaces = []
        self.fail: Exception | None = None

    def config(self, **kwargs: Any) -> Any:
        if self.fail is not None:
            raise self.fail
        self.configs.append(kwargs)
        out = type("Cfg", (), {})()
        out.host = "https://ws.example/"
        out.token = "tok-0123456789"
        out.client_secret = None
        out.password = None
        return out

    def workspace(self, *, config: Any) -> Any:
        ws = type("Ws", (), {})()
        ws.config = config
        ws.api_client = object()
        self.workspaces.append(ws)
        return ws


@pytest.fixture
def sdk(monkeypatch: pytest.MonkeyPatch) -> Recorder:
    import databricks.sdk as sdk_module
    from databricks.sdk import core

    rec = Recorder()
    monkeypatch.setattr(core, "Config", rec.config)
    monkeypatch.setattr(sdk_module, "WorkspaceClient", rec.workspace)
    return rec


def test_the_guard_fixture_blocks_a_real_client() -> None:
    import databricks.sdk as sdk_module

    with pytest.raises(AssertionError, match="Databricks SDK client"):
        sdk_module.WorkspaceClient()
    assert not Path(os.environ["DATABRICKS_CONFIG_FILE"]).exists()


def test_profile_flag(sdk: Recorder) -> None:
    client = connect(profile="p1", env={})
    assert sdk.configs == [{"profile": "p1"}]
    assert isinstance(client, SdkClient) and client.host == "https://ws.example"


def test_user_set_profile_variable(sdk: Recorder) -> None:
    connect(profile=None, env={"DATABRICKS_CONFIG_PROFILE": "from-env"})
    assert sdk.configs == [{"profile": "from-env"}]


def test_flag_beats_the_variable_and_the_host(sdk: Recorder) -> None:
    connect(
        profile="flag",
        env={"DATABRICKS_CONFIG_PROFILE": "env", "DATABRICKS_HOST": "https://h.example"},
    )
    assert sdk.configs == [{"profile": "flag"}]


def test_host_env_auth_never_reads_a_config_file(sdk: Recorder) -> None:
    connect(
        profile=None,
        env={
            "DATABRICKS_HOST": "https://h.example",
            "DATABRICKS_TOKEN": "t",
            "DATABRICKS_CLIENT_ID": "id",
            "DATABRICKS_CLIENT_SECRET": "s",
            "DATABRICKS_RUNTIME_VERSION": "15.4",
        },
    )
    (kwargs,) = sdk.configs
    cfg = Path(kwargs.pop("config_file"))
    assert not cfg.exists() and not cfg.parent.exists()
    assert kwargs == {
        "host": "https://h.example",
        "token": "t",
        "client_id": "id",
        "client_secret": "s",
    }


def test_runtime_auth(sdk: Recorder) -> None:
    connect(profile=None, env={"DATABRICKS_RUNTIME_VERSION": "15.4"})
    (kwargs,) = sdk.configs
    assert not Path(kwargs.pop("config_file")).exists()
    assert kwargs == {"auth_type": "runtime"}


@pytest.mark.parametrize("env", [{}, {"DATABRICKS_CONFIG_PROFILE": ""}, {"DATABRICKS_TOKEN": "t"}])
def test_nothing_configured_is_pld010_and_builds_nothing(
    sdk: Recorder, env: dict[str, str]
) -> None:
    with pytest.raises(ConnectionNotConfigured, match="no Databricks connection"):
        connect(profile=None, env=env)
    assert sdk.configs == [] and sdk.workspaces == []


def test_an_invalid_connection_is_reported_not_raised_as_value_error(sdk: Recorder) -> None:
    sdk.fail = ValueError("profile 'x' not found")
    with pytest.raises(ConnectionNotConfigured, match="not found"):
        connect(profile="x", env={})


def test_the_connection_masks_its_own_credentials(sdk: Recorder) -> None:
    from databricks.sdk.errors.platform import PermissionDenied

    client = connect(profile=None, env={"DATABRICKS_HOST": "https://h.example"})
    assert isinstance(client, SdkClient)

    class Api:
        def do(self, *args: Any, **kwargs: Any) -> Any:
            raise PermissionDenied("rejected tok-0123456789", error_code="PERMISSION_DENIED")

    client._api = Api()  # type: ignore[assignment]
    with pytest.raises(Exception, match=r"rejected \*\*\*"):
        client.current_user()


def test_factory_skips_every_rule(sdk: Recorder) -> None:
    client = connect(profile=None, env={}, factory="pipeloom_databricks.testing.fake:empty")
    assert isinstance(client, FakeDatabricks)
    assert sdk.configs == []


@pytest.mark.parametrize(
    "spec", ["nocolon", "no_such_module_x:thing", "pipeloom_databricks.testing.fake:nope"]
)
def test_a_bad_factory_is_pld010(spec: str) -> None:
    with pytest.raises(ConnectionNotConfigured):
        connect(profile=None, env={}, factory=spec)
