"""The public API: the same calls the CLI makes."""

from __future__ import annotations

from pipeloom.config.bind import BoundProject, bind_project
from pipeloom.config.load import LoadedProject, ProjectOptions, load_project
from pipeloom.config.resolve import resolve_project
from pipeloom.diagnostics import Diagnostic, Outcome, Report
from pipeloom.graph.dag import ProjectGraph, build_graph
from pipeloom.platform import Clock, Identity, IdentityProvider, LocalIdentity, SystemClock
from pipeloom.plugins import PluginRegistry
from pipeloom.validate.references import check_references

__all__ = ["Project", "ProjectOptions"]


class Project:
    """A project on disk. Each step runs once and is remembered."""

    def __init__(
        self,
        options: ProjectOptions,
        *,
        registry: PluginRegistry | None = None,
        clock: Clock | None = None,
        identity: IdentityProvider | None = None,
    ) -> None:
        self.options = options
        self._registry = registry
        self._registry_problems: tuple[Diagnostic, ...] = ()
        self.clock: Clock = clock or SystemClock()
        self._identity = identity or LocalIdentity(options.env)
        self._loaded: Outcome[LoadedProject] | None = None
        self._bound: Outcome[BoundProject] | None = None
        self._resolved: Outcome[ProjectGraph] | None = None

    @property
    def identity(self) -> Identity:
        return self._identity.identity()

    @property
    def registry(self) -> PluginRegistry:
        """Discovered on first use from the options' `--plugin` specs and the environment."""
        if self._registry is None:
            report = Report()
            self._registry = PluginRegistry.discover(
                cli_plugins=self.options.plugins, env=self.options.env, report=report
            )
            self._registry_problems = report.sorted()
        return self._registry

    def load(self) -> Outcome[LoadedProject]:
        """Read and check pipeloom.yml, model files and defaults (layers L0 and L1)."""
        if self._loaded is None:
            report = Report()
            value = load_project(self.options, report)
            self._loaded = Outcome(value, report.sorted())
        return self._loaded

    def bind(self) -> Outcome[BoundProject]:
        """Load, then pick the target and bind its plugins."""
        if self._bound is None:
            loaded = self.load()
            if loaded.value is None:
                self._bound = Outcome(None, loaded.diagnostics)
            else:
                registry = self.registry
                report = Report()
                report.extend(loaded.diagnostics)
                report.extend(self._registry_problems)
                value = bind_project(loaded.value, self.options, registry, report)
                self._bound = Outcome(value, report.sorted())
        return self._bound

    def resolve(self) -> Outcome[ProjectGraph]:
        """Bind, apply defaults, check references (L2) and build the graph (L3)."""
        if self._resolved is None:
            bound = self.bind()
            if bound.value is None:
                self._resolved = Outcome(None, bound.diagnostics)
            else:
                report = Report()
                report.extend(bound.diagnostics)
                project = resolve_project(bound.value)
                check_references(project, report)
                graph = None if report.has_errors else build_graph(project, report)
                value = None if graph is None else ProjectGraph(project, graph)
                self._resolved = Outcome(value, report.sorted())
        return self._resolved
