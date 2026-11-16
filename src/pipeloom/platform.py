"""Platform seams: clock and identity, so tests can replace both."""

from __future__ import annotations

import getpass
import os
import re
import socket
import time
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Protocol


class Clock(Protocol):
    def now(self) -> datetime: ...

    def monotonic(self) -> float: ...


class SystemClock:
    def now(self) -> datetime:
        """Timezone-aware UTC now."""
        return datetime.now(UTC)

    def monotonic(self) -> float:
        return time.monotonic()


# A location kept as written: an absolute path (/x, /Volumes/...), a Windows drive (C:) or a URI
# with a scheme of two letters or more (s3://, file:/x, dbfs:/Volumes/...).
_ABSOLUTE_LOCATION = re.compile(r"/|[A-Za-z]:|[A-Za-z][A-Za-z0-9+.-]+:")


def is_absolute_location(location: str) -> bool:
    """True when a source location must not be joined to a base directory or location."""
    return _ABSOLUTE_LOCATION.match(location) is not None


_ORCHESTRATOR_KEY = re.compile(r"[a-z][a-z0-9_]{0,63}")


def check_orchestrator_id(key: str, value: str) -> None:
    """Raise ValueError unless `key` is `[a-z][a-z0-9_]{0,63}` and `value` 1 to 256 printable
    characters."""
    if not _ORCHESTRATOR_KEY.fullmatch(key):
        raise ValueError(f"orchestrator id key '{key}' must match [a-z][a-z0-9_]{{0,63}}")
    if not 1 <= len(value) <= 256 or not value.isprintable():
        raise ValueError(f"orchestrator id '{key}' needs 1 to 256 printable characters")


@dataclass(frozen=True)
class Identity:
    user: str
    host: str
    orchestrator: str | None = None
    orchestrator_ids: Mapping[str, str] = field(default_factory=dict)


class IdentityProvider(Protocol):
    def identity(self) -> Identity: ...


class LocalIdentity:
    """Identity of the local process: PIPELOOM_USER, else the OS user."""

    def __init__(self, env: Mapping[str, str] | None = None) -> None:
        self._env = os.environ if env is None else env

    def identity(self) -> Identity:
        user = self._env.get("PIPELOOM_USER") or getpass.getuser()
        return Identity(user=user, host=socket.gethostname())
