"""The public API: the same calls the CLI makes."""

from __future__ import annotations

import uuid
from typing import Literal

from pipeloom.config.bind import BoundProject, bind_project
from pipeloom.config.load import LoadedProject, ProjectOptions, load_project
from pipeloom.config.resolve import resolve_project
from pipeloom.diagnostics import Diagnostic, Outcome, Report
from pipeloom.graph.dag import ProjectGraph, build_graph
from pipeloom.graph.selectors import Selection
from pipeloom.plan.compile import compile_plan
from pipeloom.plan.model import Plan
from pipeloom.platform import Clock, Identity, IdentityProvider, LocalIdentity, SystemClock
from pipeloom.plugins import PluginRegistry
from pipeloom.validate import validate_project
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
        invocation_id: str | None = None,
    ) -> None:
        self.options = options
        self._registry = registry
        self._registry_problems: tuple[Diagnostic, ...] = ()
        self.clock: Clock = clock or SystemClock()
        self._identity = identity or LocalIdentity(options.env)
        self._loaded: Outcome[LoadedProject] | None = None
        self._bound: Outcome[BoundProject] | None = None
        self._resolved: Outcome[ProjectGraph] | None = None
        self._validated: Outcome[ProjectGraph] | None = None
        self._plans: dict[tuple[Selection, str | None], Outcome[Plan]] = {}
        self.invocation_id = invocation_id or uuid.uuid4().hex

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

    def validate(self) -> Outcome[ProjectGraph]:
        """Resolve, then run the plan checks (L4 to L6). None when any layer found an error."""
        if self._validated is None:
            self._validated = validate_project(self.resolve())
        return self._validated

    def compile(
        self,
        selection: Selection | None = None,
        boundary: Literal["process", "task"] | None = None,
    ) -> Outcome[Plan]:
        """Validate, then build the plan. Units always cover the whole project; the selection
        only marks which models are selected. `boundary` defaults to the target's."""
        sel = selection or Selection()
        key = (sel, boundary)
        if key not in self._plans:
            validated = self.validate()
            if validated.value is None:
                self._plans[key] = Outcome(None, validated.diagnostics)
            else:
                report = Report()
                report.extend(validated.diagnostics)
                plan = compile_plan(
                    validated.value,
                    selection=sel,
                    boundary=boundary,
                    registry=self.registry,
                    identity=self.identity,
                    clock=self.clock,
                    invocation_id=self.invocation_id,
                    report=report,
                )
                self._plans[key] = Outcome(plan, report.sorted())
        return self._plans[key]
