"""An Airflow connection as Databricks credentials; no Airflow and no SDK client."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

import pytest

from pipeloom.diagnostics import Report
from pipeloom_airflow.databricks.connection import (
    DatabricksCredentials,
    credentials_from_connection,
    make_client,
)
from pipeloom_databricks.testing.fake import FakeDatabricks

SECRET = "dapi-secret-value"


@dataclass
class Conn:
    host: str | None = "adb-1.example.net"
    login: str | None = None
    password: str | None = None
    extra_dejson: Mapping[str, Any] = field(default_factory=dict)


@pytest.mark.parametrize(
    ("conn", "expected"),
    [
        (Conn(password=SECRET), ("https://adb-1.example.net", "pat", SECRET, None, None)),
        (
            Conn(host="https://h.example/", extra_dejson={"token": SECRET}),
            ("https://h.example", "pat", SECRET, None, None),
        ),
        (
            Conn(login="cid", password=SECRET, extra_dejson={"service_principal_oauth": "true"}),
            ("https://adb-1.example.net", "oauth-m2m", None, "cid", SECRET),
        ),
    ],
)
def test_credentials(conn: Conn, expected: tuple[Any, ...]) -> None:
    report = Report()
    creds = credentials_from_connection(conn, "dbx", report)
    assert creds is not None and not report.sorted()
    got = (creds.host, creds.auth_type, creds.token, creds.client_id, creds.client_secret)
    assert got == expected
    assert SECRET not in repr(creds)


@pytest.mark.parametrize(
    ("conn", "words"),
    [
        (Conn(host=None, password=SECRET), "has no host"),
        (Conn(), "has no token"),
        (Conn(password=SECRET, extra_dejson={"service_principal_oauth": True}), "client id"),
        (Conn(password=SECRET, extra_dejson={"azure_tenant_id": "t"}), "Azure"),
    ],
)
def test_unusable_connections_are_pld010_without_values(conn: Conn, words: str) -> None:
    report = Report()
    assert credentials_from_connection(conn, "dbx", report) is None
    (d,) = report.sorted()
    assert d.code == "PLD010" and "'dbx'" in d.message and words in d.message
    assert SECRET not in d.message and SECRET not in str(d.hint)


def test_make_client_uses_the_factory_or_explicit_credentials(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = make_client(None, factory="pipeloom_databricks.testing.fake:empty")
    assert isinstance(client, FakeDatabricks)
    seen: list[dict[str, Any]] = []
    from pipeloom_databricks import api

    monkeypatch.setattr(api, "connect_explicit", lambda **kw: seen.append(kw) or "client")
    creds = DatabricksCredentials("https://h", "pat", SECRET, None, None)
    assert make_client(creds, factory=None) == "client"
    assert seen == [
        {
            "host": "https://h",
            "auth_type": "pat",
            "token": SECRET,
            "client_id": None,
            "client_secret": None,
        }
    ]
    with pytest.raises(ValueError, match="credentials or a factory"):
        make_client(None, factory=None)
