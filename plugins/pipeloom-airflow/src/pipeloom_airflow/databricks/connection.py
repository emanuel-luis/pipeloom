"""An Airflow connection as Databricks credentials. Pure: no Airflow, and the SDK only when a
client is made.

The fields follow the Databricks provider's convention, so an existing `databricks_default`
connection works: the host in `host`; a personal access token in `password` (or `extra.token`);
with `extra.service_principal_oauth` true, an OAuth machine-to-machine client id in `login` and
its secret in `password`. No profile, file or `DATABRICKS_*` variable is ever read.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Final, Literal, Protocol

from pipeloom.diagnostics import Report
from pipeloom_databricks.api import DatabricksClient

AZURE_EXTRAS: Final = (
    "azure_ad_endpoint",
    "azure_resource_id",
    "azure_tenant_id",
    "use_azure_managed_identity",
    "use_default_azure_credential",
)
"""Provider extras for Azure-only authentication, which pipeloom does not support."""


class ConnectionLike(Protocol):
    @property
    def host(self) -> str | None: ...
    @property
    def login(self) -> str | None: ...
    @property
    def password(self) -> str | None: ...
    @property
    def extra_dejson(self) -> Mapping[str, Any]: ...


@dataclass(frozen=True, repr=False)
class DatabricksCredentials:
    """Never printed: `repr` is off so a secret cannot reach a log through it."""

    host: str
    auth_type: Literal["pat", "oauth-m2m"]
    token: str | None
    client_id: str | None
    client_secret: str | None

    def secrets(self) -> tuple[str, ...]:
        return tuple(s for s in (self.token, self.client_secret) if s)


def _true(value: object) -> bool:
    if isinstance(value, str):
        return value.strip().lower() in ("true", "1", "yes")
    return bool(value)


def credentials_from_connection(
    conn: ConnectionLike, conn_id: str, report: Report
) -> DatabricksCredentials | None:
    """The credentials of `conn`, or None after PLD010. Messages name `conn_id`, never a value."""

    def missing(what: str) -> None:
        report.add(
            "PLD010",
            f"Airflow connection '{conn_id}' {what}",
            hint="set the host, and a token in the password, or service_principal_oauth with "
            "the client id in login and the secret in password",
            context={"conn_id": conn_id},
        )

    extra = dict(conn.extra_dejson or {})
    azure = sorted(k for k in AZURE_EXTRAS if extra.get(k))
    if azure:
        missing(f"uses Azure authentication ({', '.join(azure)}), which pipeloom does not support")
        return None
    host = (conn.host or "").strip().rstrip("/")
    if not host:
        missing("has no host")
        return None
    if "://" not in host:
        host = f"https://{host}"
    if _true(extra.get("service_principal_oauth")):
        if not conn.login or not conn.password:
            missing("sets service_principal_oauth without a client id (login) and secret")
            return None
        return DatabricksCredentials(host, "oauth-m2m", None, conn.login, conn.password)
    token = conn.password or extra.get("token")
    if not isinstance(token, str) or not token:
        missing("has no token (password or extra.token)")
        return None
    return DatabricksCredentials(host, "pat", token, None, None)


def make_client(creds: DatabricksCredentials | None, *, factory: str | None) -> DatabricksClient:
    """A client for `creds`, or the test double `factory` names (`module:attr`).

    Raises `ConnectionNotConfigured` when the SDK rejects the credentials.
    """
    from pipeloom_databricks.api import connect, connect_explicit

    if factory:
        return connect(profile=None, env={}, factory=factory)
    if creds is None:
        raise ValueError("make_client needs credentials or a factory")
    return connect_explicit(
        host=creds.host,
        auth_type=creds.auth_type,
        token=creds.token,
        client_id=creds.client_id,
        client_secret=creds.client_secret,
    )
