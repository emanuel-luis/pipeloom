"""The public API: the same calls the CLI makes."""

from __future__ import annotations

from typing import Any

from pipeloom.config.load import LoadedProject, ProjectOptions, load_project
from pipeloom.diagnostics import Outcome, Report
from pipeloom.platform import Clock, Identity, IdentityProvider, LocalIdentity, SystemClock

__all__ = ["Project", "ProjectOptions"]


class Project:
    """A project on disk. Each step runs once and is remembered."""

    def __init__(
        self,
        options: ProjectOptions,
        *,
        registry: Any | None = None,
        clock: Clock | None = None,
        identity: IdentityProvider | None = None,
    ) -> None:
        self.options = options
        self.registry = registry  # a PluginRegistry once plugins land
        self.clock: Clock = clock or SystemClock()
        self._identity = identity or LocalIdentity(options.env)
        self._loaded: Outcome[LoadedProject] | None = None

    @property
    def identity(self) -> Identity:
        return self._identity.identity()

    def load(self) -> Outcome[LoadedProject]:
        """Read and check pipeloom.yml, model files and defaults (layers L0 and L1)."""
        if self._loaded is None:
            report = Report()
            value = load_project(self.options, report)
            self._loaded = Outcome(value, report.sorted())
        return self._loaded
