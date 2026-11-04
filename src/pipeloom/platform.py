"""Platform seams: clock and identity, so tests can replace both."""

from __future__ import annotations

import getpass
import os
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
