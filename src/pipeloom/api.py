"""The public API: the same calls the CLI makes."""

from __future__ import annotations

import uuid
from collections.abc import Mapping
from dataclasses import dataclass, replace
from typing import Any, Literal

from pipeloom.config.bind import BoundProject, bind_project
from pipeloom.config.load import LoadedProject, ProjectOptions, load_project
from pipeloom.config.resolve import model_id, resolve_project
from pipeloom.diagnostics import Diagnostic, Outcome, Report
from pipeloom.discovery import Discovery
from pipeloom.discovery.loader import ModuleLoader
from pipeloom.discovery.project import python_models
from pipeloom.engine.protocols import Catalog, Engine
from pipeloom.events.bus import EventBus
from pipeloom.events.model import (
    EventPayload,
    UnitTestFailed,
    UnitTestPassed,
    UnitTestsCompleted,
)
from pipeloom.graph.dag import ProjectGraph, build_graph
from pipeloom.graph.selectors import Selection
from pipeloom.plan.compile import compile_plan
from pipeloom.plan.model import Plan
from pipeloom.platform import Clock, Identity, IdentityProvider, LocalIdentity, SystemClock
from pipeloom.plugins import PluginRegistry
from pipeloom.runtime.context import RuntimeContext
from pipeloom.runtime.control import RunControl
from pipeloom.runtime.finalize_run import FinalizeOptions, finalize_run
from pipeloom.runtime.rerun import inherited_vars, retry_options
from pipeloom.runtime.run import (
    USAGE_CODES,
    RunOptions,
    RunOutcome,
    _early,
    _missing,
    execute_run,
)
from pipeloom.runtime.unit_tests import run_unit_test
from pipeloom.secrets.redaction import Redactor
from pipeloom.secrets.resolve import SecretError, SecretResolver, secret_diagnostic
from pipeloom.validate import validate_project
from pipeloom.validate.references import check_references

__all__ = ["Project", "ProjectOptions", "TestRunOutcome"]


@dataclass(frozen=True)
class TestRunOutcome:
    """What `Project.test` found. Exit code 0 when every case passed, 1 when one failed or the
    project does not validate, 2 for a usage error."""

    __test__ = False  # not a pytest class

    exit_code: int
    events: tuple[EventPayload, ...]
    """`unit_test.passed` or `unit_test.failed` per case, then `unit_tests.completed`."""
    diagnostics: tuple[Diagnostic, ...]
    failed: int = 0


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
        redactor: Redactor | None = None,
    ) -> None:
        self.options = options
        self.redactor = redactor or Redactor()
        """Holds every secret the run resolves; pass the same one to the event bus."""
        self._secrets: SecretResolver | None = None
        self._registry = registry
        self._registry_problems: tuple[Diagnostic, ...] = ()
        self.clock: Clock = clock or SystemClock()
        self._identity = identity or LocalIdentity(options.env)
        self._loaded: Outcome[LoadedProject] | None = None
        self._bound: Outcome[BoundProject] | None = None
        self._resolved: Outcome[ProjectGraph] | None = None
        self.discovery: Discovery | None = None
        """The project's model discovery, kept so `docs serve` can refresh it."""
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

    @property
    def secrets(self) -> SecretResolver:
        """Resolves `${secret:...}` references for plugins built to execute. One per project, so
        each provider is built once per invocation."""
        if self._secrets is None:
            self._secrets = SecretResolver(self.registry, self.redactor)
        return self._secrets

    def with_vars(self, vars_: Mapping[str, Any]) -> Project:
        """This project with other vars, which are read at load; self when they are the same.
        It keeps the registry, clock, identity, invocation id and redactor."""
        if dict(vars_) == dict(self.options.vars):
            return self
        other = Project(
            replace(self.options, vars=vars_),
            registry=self.registry,
            clock=self.clock,
            identity=self._identity,
            invocation_id=self.invocation_id,
            redactor=self.redactor,
        )
        other._registry_problems = self._registry_problems
        return other

    def load(self) -> Outcome[LoadedProject]:
        """Read and check pipeloom.yml and properties files (layers L0 and L1)."""
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
                loaded = bound.value.loaded
                if self.discovery is None:
                    self.discovery = Discovery(
                        loaded.project_dir,
                        loaded.config.model_paths,
                        loaded.config.model_packages,
                        loaded.config.python_paths,
                    )
                found = python_models(self.discovery.discover(), loaded.vars, report)
                project = resolve_project(bound.value, found.models, found.broken)
                check_references(project, report, models_incomplete=found.incomplete)
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

    def run(
        self,
        opts: RunOptions | None = None,
        bus: EventBus | None = None,
        control: RunControl | None = None,
    ) -> RunOutcome:
        """Validate, plan and run the selected units. Events go to `bus`; without one they are dropped.

        `control.request_stop()` stops the run gracefully from any thread; `hard=True` skips the
        grace period. The outcome is then `cancelled` with exit code 130."""
        opts = opts or RunOptions()
        if control is not None:
            opts = replace(opts, control=control)
        own = bus is None
        events = bus or EventBus(
            [], clock=self.clock, invocation_id=self.invocation_id, redactor=self.redactor
        )
        try:
            return execute_run(self, opts, events)
        finally:
            if own:
                events.close()

    def retry(
        self,
        parent_run_id: str,
        opts: RunOptions | None = None,
        bus: EventBus | None = None,
        control: RunControl | None = None,
        *,
        full_refresh: bool | None = None,
    ) -> RunOutcome:
        """Run again what run `parent_run_id` did not finish: its failed, skipped and cancelled
        units. The new run has its own id and records the parent as `parent_run_id`; it inherits
        the parent's vars (under this project's own), full_refresh (`full_refresh` wins), state
        and defer, and compiles the project fresh. A parent that cannot be retried is PLX113; one
        with nothing to retry gives a succeeded outcome with no run results."""
        project = self.with_vars(inherited_vars(self, parent_run_id))
        resolved, found = retry_options(project, parent_run_id, opts or RunOptions(), full_refresh)
        own = bus is None
        events = bus or EventBus(
            [], clock=self.clock, invocation_id=self.invocation_id, redactor=self.redactor
        )
        try:
            if resolved is None:
                return _early(events, found) if found else RunOutcome("succeeded", 0, None, ())
            if control is not None:
                resolved = replace(resolved, control=control)
            return execute_run(project, resolved, events)
        finally:
            if own:
                events.close()

    def build(
        self,
        opts: RunOptions | None = None,
        bus: EventBus | None = None,
        control: RunControl | None = None,
    ) -> RunOutcome:
        """Run the unit tests of the selected models, then, when every case passed, `run` them
        with data tests on (ADR-0045). A failed case stops the build with exit 1 before a run id
        exists and before any write."""
        opts = opts or RunOptions()
        tested = self.test(opts.selection)
        own = bus is None
        events = bus or EventBus(
            [], clock=self.clock, invocation_id=self.invocation_id, redactor=self.redactor
        )
        try:
            if tested.exit_code:
                outcome = _early(events, tested.diagnostics)
                for payload in tested.events:
                    events.emit(payload)
                return replace(outcome, exit_code=tested.exit_code, extra_errors=tested.failed)
            for payload in tested.events:
                events.emit(payload)
            return self.run(replace(opts, data_tests=True), events, control)
        finally:
            if own:
                events.close()

    def test(self, selection: Selection | None = None) -> TestRunOutcome:
        """Run the unit tests of the selected models (ADR-0045). It writes nothing: no run id, no
        lock, no artifacts. The outcome holds one event payload per case and a summary."""
        compiled = self.compile(selection)
        diags = list(compiled.diagnostics)
        plan, bound = compiled.value, self.bind().value
        if plan is None or bound is None:
            usage = any(d.code in USAGE_CODES for d in diags)
            return TestRunOutcome(2 if usage else 1, (), tuple(diags))
        validated = self.validate().value
        assert validated is not None
        project = validated.project
        order = {uid: i for i, uid in enumerate(plan.nodes)}
        chosen = set(plan.selection.selected)
        cases = sorted(
            (c for c in project.unit_tests if model_id(plan.metadata.project, c.model) in chosen),
            key=lambda c: order[model_id(plan.metadata.project, c.model)],
        )
        events: list[EventPayload] = []
        failed = 0
        if cases:
            engine_plugin, catalog_plugin = (
                bound.plugins.get("engines"),
                bound.plugins.get("catalogs"),
            )
            if engine_plugin is None or catalog_plugin is None:
                missing = _missing(bound, "engine" if engine_plugin is None else "catalog")
                return TestRunOutcome(1, (), (*diags, missing))
            try:
                engine: Engine = engine_plugin.create(self.secrets)
                catalog: Catalog = catalog_plugin.create(self.secrets)
            except SecretError as err:
                return TestRunOutcome(1, (), (*diags, secret_diagnostic(err)))
            project_dir = bound.loaded.project_dir.resolve()
            rt = RuntimeContext(
                run_id=self.invocation_id,
                invocation_id=self.invocation_id,
                project=plan.metadata.project,
                target=plan.metadata.target,
                project_dir=project_dir,
                clock=self.clock,
                identity=self.identity,
                vars=plan.metadata.vars,
            )
            with (
                engine.session(rt, catalog=catalog) as session,
                ModuleLoader(project_dir, plan.metadata.python_paths) as loader,
            ):
                for case in cases:
                    node = plan.nodes[model_id(plan.metadata.project, case.model)]
                    upstream = {
                        alias: plan.nodes[uid].columns if uid in plan.nodes else None
                        for alias, uid in node.inputs.items()
                    }
                    result = run_unit_test(
                        session,
                        node,
                        case.config,
                        load=loader.load,
                        rt=rt,
                        upstream=upstream,
                        sql_dialect=engine.capabilities.sql_dialect,
                    )
                    if result.passed:
                        events.append(UnitTestPassed(model=case.model, unit_test=case.name))
                        continue
                    failed += 1
                    where = project.locate(case.file, f"{case.pointer}/name")
                    events.append(
                        UnitTestFailed(
                            model=case.model,
                            unit_test=case.name,
                            code=result.code or "PLX115",
                            message=result.message,
                            location=where.to_json(),
                            missing=result.missing,
                            unexpected=result.unexpected,
                        )
                    )
        events.append(UnitTestsCompleted(passed=len(cases) - failed, failed=failed))
        return TestRunOutcome(1 if failed else 0, tuple(events), tuple(diags), failed)

    def finalize(self, opts: FinalizeOptions, bus: EventBus | None = None) -> RunOutcome:
        """Close a run an orchestrator drove unit by unit: merge the unit results, record the run
        and emit `run.completed`. Repeating the call rewrites the same results."""
        own = bus is None
        events = bus or EventBus(
            [], clock=self.clock, invocation_id=self.invocation_id, redactor=self.redactor
        )
        try:
            return finalize_run(self, opts, events)
        finally:
            if own:
                events.close()
