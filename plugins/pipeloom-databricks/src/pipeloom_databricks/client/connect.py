"""Choose the workspace connection (ADR-0036). No profile is ever picked automatically.

Order: an explicit profile (`--profile`), a profile the user set in `DATABRICKS_CONFIG_PROFILE`,
host and credentials in `DATABRICKS_HOST` and friends, runtime auth when
`DATABRICKS_RUNTIME_VERSION` is set, else `ConnectionNotConfigured` (PLD010). Without a profile
the SDK gets a `config_file` that does not exist, so it cannot fall back to a `DEFAULT` profile of
`~/.databrickscfg`. The SDK loads lazily, only here and in `sdk.py`.
"""

from __future__ import annotations

import importlib
import tempfile
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Final

from pipeloom.secrets.redaction import Redactor
from pipeloom_databricks.client import ConnectionNotConfigured, DatabricksClient

_CREDENTIALS: Final = {
    "DATABRICKS_TOKEN": "token",
    "DATABRICKS_CLIENT_ID": "client_id",
    "DATABRICKS_CLIENT_SECRET": "client_secret",
}
_SECRET_ATTRS: Final = ("token", "client_secret", "password")


def _factory(spec: str) -> DatabricksClient:
    """`module:attr` called with no arguments (a test double; `--client-factory`)."""
    module, sep, attr = spec.partition(":")
    if not (module and sep and attr):
        raise ConnectionNotConfigured(f"--client-factory '{spec}' is not MODULE:ATTR")
    try:
        make = getattr(importlib.import_module(module), attr)
    except (ImportError, AttributeError) as err:
        raise ConnectionNotConfigured(
            f"--client-factory '{spec}' cannot be loaded: {err}"
        ) from None
    client: DatabricksClient = make()
    return client


def connect(
    *, profile: str | None, env: Mapping[str, str], factory: str | None = None
) -> DatabricksClient:
    if factory:
        return _factory(factory)
    profile = profile or env.get("DATABRICKS_CONFIG_PROFILE") or None
    host = env.get("DATABRICKS_HOST")
    if profile:
        kwargs: dict[str, Any] = {"profile": profile}
    elif host:
        kwargs = {"host": host}
        kwargs.update({attr: env[name] for name, attr in _CREDENTIALS.items() if env.get(name)})
    elif env.get("DATABRICKS_RUNTIME_VERSION"):
        kwargs = {"auth_type": "runtime"}
    else:
        raise ConnectionNotConfigured(
            "no Databricks connection: pass --profile, set DATABRICKS_CONFIG_PROFILE, set "
            "DATABRICKS_HOST with credentials, or run on a Databricks runtime"
        )

    from databricks.sdk import WorkspaceClient
    from databricks.sdk.core import Config

    try:
        if profile:
            config = Config(**kwargs)
        else:
            with tempfile.TemporaryDirectory(prefix="pipeloom-") as folder:
                config = Config(config_file=str(Path(folder) / "none.cfg"), **kwargs)
        workspace = WorkspaceClient(config=config)
    except ValueError as err:
        raise ConnectionNotConfigured(f"the Databricks connection is invalid: {err}") from None

    from pipeloom_databricks.client.sdk import SdkClient

    redactor = Redactor()
    for attr in _SECRET_ATTRS:
        value = getattr(config, attr, None)
        if isinstance(value, str):
            redactor.register(value)
    return SdkClient(workspace.api_client, str(config.host or "").rstrip("/"), redactor)
