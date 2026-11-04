"""Runtime resolution of `${secret:provider/path}` references.

Compile-time interpolation leaves these references as they are. They are resolved only when a
command executes (`run`, and `finalize` once it exists), while the options of the plugins it
builds are bound. The resolved values live in memory, in those plugin objects only: they are
registered with the redactor, and they never reach the manifest, the plan, events or artifacts.
Compile, validate and every other command keep the reference text.
"""

from __future__ import annotations

import re
from collections.abc import Callable
from typing import Any, Final

from pipeloom.diagnostics import Diagnostic, JsonValue, Report
from pipeloom.engine.protocols import SecretProvider
from pipeloom.plugins import PluginRegistry
from pipeloom.secrets.redaction import Redactor

SECRET_REF: Final = re.compile(r"\$\{secret:([A-Za-z_][A-Za-z0-9_]*)/([^}]+)\}")


class SecretError(Exception):
    """A reference that could not be resolved. Names the provider and path, never a value."""

    def __init__(self, provider: str, path: str, reason: str) -> None:
        self.provider, self.path, self.reason = provider, path, reason
        what = f"provider '{provider}', path '{path}'" if path else f"provider '{provider}'"
        super().__init__(f"{what}: {reason}" if provider else reason)


def contains_secret(value: Any) -> bool:
    """True when a string anywhere in `value` holds a `${secret:...}` reference."""
    if isinstance(value, str):
        return SECRET_REF.search(value) is not None
    if isinstance(value, dict):
        return any(contains_secret(v) for v in value.values())
    if isinstance(value, list | tuple):
        return any(contains_secret(v) for v in value)
    return False


def resolve_secrets(
    value: JsonValue, provider: Callable[[str], SecretProvider], redactor: Redactor
) -> JsonValue:
    """A copy of `value` with every reference in its strings replaced by the secret.

    Each resolved value is registered with `redactor`. `provider(name)` returns the provider for a
    name. Raises SecretError; the error never holds the value.
    """
    if isinstance(value, str):

        def repl(m: re.Match[str]) -> str:
            name, path = m.group(1), m.group(2)
            try:
                secret = provider(name).resolve(path)
            except SecretError:
                raise
            except Exception as exc:  # noqa: BLE001 - any failure of provider code
                raise SecretError(name, path, f"{type(exc).__name__}: {exc}") from None
            if not isinstance(secret, str):
                raise SecretError(name, path, "the provider did not return a string")
            redactor.register(secret)
            return secret

        return SECRET_REF.sub(repl, value)
    if isinstance(value, list):
        return [resolve_secrets(v, provider, redactor) for v in value]
    if isinstance(value, dict):
        return {k: resolve_secrets(v, provider, redactor) for k, v in value.items()}
    return value


class SecretResolver:
    """Resolves references for one invocation. Each provider is built once, with default options."""

    def __init__(self, registry: PluginRegistry, redactor: Redactor) -> None:
        self._registry = registry
        self.redactor = redactor
        self._providers: dict[str, SecretProvider] = {}

    def provider(self, name: str) -> SecretProvider:
        found = self._providers.get(name)
        if found is None:
            report = Report()
            spec = self._registry.load("secret_providers", name, report)
            if spec is None:
                why = report.sorted()[0].message if report.sorted() else "unknown secret provider"
                raise SecretError(name, "", why)
            try:
                found = spec.factory(spec.config_model.model_validate({}))
            except Exception as exc:  # noqa: BLE001 - any failure of provider code
                raise SecretError(name, "", f"cannot build it: {type(exc).__name__}") from None
            self._providers[name] = found
        return found

    def resolve(self, value: JsonValue) -> JsonValue:
        return resolve_secrets(value, self.provider, self.redactor)


def secret_diagnostic(err: SecretError) -> Diagnostic:
    """PLX109 for a failed resolution."""
    return Report().add(
        "PLX109",
        f"cannot resolve a secret reference ({err})",
        hint="check the provider name and path in ${secret:provider/path}",
        context={"provider": err.provider, "path": err.path},
    )
