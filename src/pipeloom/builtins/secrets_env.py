"""Secret provider that reads environment variables."""

from __future__ import annotations

import os
from collections.abc import Mapping

from pipeloom.engine.protocols import SecretProvider
from pipeloom.plugins import API_VERSION, OptionsModel, PluginSpec


class EnvSecretsOptions(OptionsModel):
    pass


class EnvSecretProvider:
    """`${secret:env/NAME}` resolves to the variable NAME."""

    def __init__(self, env: Mapping[str, str] | None = None) -> None:
        self._env = os.environ if env is None else env

    def resolve(self, path: str) -> str:
        try:
            return self._env[path]
        except KeyError:
            raise KeyError(f"environment variable '{path}' is not set") from None


SPEC: PluginSpec[SecretProvider] = PluginSpec(
    "env",
    "env",
    "secret_providers",
    API_VERSION,
    lambda _opts: EnvSecretProvider(),
    EnvSecretsOptions,
    description="Reads secrets from environment variables.",
)
