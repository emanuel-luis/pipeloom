"""`${secret:databricks/<scope>/<key>}`: a Databricks secret scope entry (ADR-0036).

The client is built on the first reference and kept for the life of the provider, which the core
builds once per invocation. No profile is chosen: the connection rules of `connect` apply.
"""

from __future__ import annotations

import base64
import binascii
import os
from collections.abc import Mapping

from pipeloom.secrets.resolve import SecretError
from pipeloom_databricks.client import (
    ApiError,
    ConnectionNotConfigured,
    DatabricksClient,
    connect,
)

PROVIDER = "databricks"


class DatabricksSecretProvider:
    def __init__(self, env: Mapping[str, str] | None = None) -> None:
        self._env = os.environ if env is None else env
        self._client: DatabricksClient | None = None

    def _connection(self, path: str) -> DatabricksClient:
        if self._client is None:
            try:
                self._client = connect(profile=None, env=self._env)
            except ConnectionNotConfigured as err:
                raise SecretError(PROVIDER, path, f"no Databricks connection ({err})") from None
        return self._client

    def resolve(self, path: str) -> str:
        scope, sep, key = path.partition("/")
        if not (scope and sep and key):
            raise SecretError(PROVIDER, path, "the path must be '<scope>/<key>'")
        client = self._connection(path)
        try:
            encoded = client.get_secret(scope, key)
        except ApiError as err:
            raise SecretError(PROVIDER, path, f"API error {err.status}: {err.message}") from None
        try:
            return base64.b64decode(encoded, validate=True).decode("utf-8")
        except (binascii.Error, UnicodeDecodeError, ValueError):
            raise SecretError(PROVIDER, path, "the value is not base64 encoded UTF-8") from None
