from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import quote

import pytest
from pydantic import JsonValue

from pipeloom.api import Project, ProjectOptions
from pipeloom.diagnostics import Report
from pipeloom.plan.manifest import manifest_bytes
from pipeloom.plan.model import Plan
from pipeloom_databricks.jobs_model import to_json
from pipeloom_databricks.render import Locations, RenderedJob, render_job

GOLDENS = Path(__file__).parent / "goldens"
SHOP = Path(__file__).resolve().parents[3] / "tests" / "fixtures" / "shop"
VERSIONS = {"pipeloom": "1.2.3", "pipeloom-databricks": "1.2.3"}
"""Fixed so a release bump leaves the goldens alone."""
LOCS = Locations(
    project_root="/Workspace/pipeloom/shop",
    manifest_path="/Workspace/pipeloom/shop/manifest.json",
    artifacts_root="/Volumes/main/ops/pipeloom",
    state_shared=True,
)


Deploy = Callable[..., Plan]


def compile_dir(project_dir: Path, target: str = "ci") -> Plan:
    outcome = Project(ProjectOptions(project_dir=project_dir, target=target, env={})).compile()
    assert outcome.value is not None, [d.message for d in outcome.diagnostics]
    return outcome.value


def with_deploy(plan: Plan, **options: Any) -> Plan:
    """The plan as if its target had this deploy block (the core records it, never builds it)."""
    deploy = {"kind": "databricks", **options}
    return plan.model_copy(update={"metadata": plan.metadata.model_copy(update={"deploy": deploy})})


@pytest.fixture
def locs() -> Locations:
    return LOCS


@pytest.fixture
def versions() -> dict[str, str]:
    return dict(VERSIONS)


Render = Callable[..., tuple[RenderedJob | None, Report]]


@pytest.fixture
def render() -> Render:
    """render(plan, pipeline="main", locs=LOCS) -> (job or None, the report)."""

    def run(
        plan: Plan, pipeline: str = "main", where: Locations = LOCS
    ) -> tuple[RenderedJob | None, Report]:
        report = Report()
        return render_job(plan, pipeline, where, report, versions=VERSIONS), report

    return run


@pytest.fixture(scope="session")
def shop() -> Plan:
    """The shop fixture, compiled for its `ci` target, with no deploy block."""
    return compile_dir(SHOP)


@pytest.fixture
def deploy() -> Deploy:
    return with_deploy


@pytest.fixture
def models_plan(tmp_path: Path) -> Callable[[Mapping[str, str]], Plan]:
    """Compile a project from model files: path under the project -> YAML text.

    A model entry may be written as a flow mapping; every model is a SQL table.
    """

    def make(files: Mapping[str, str]) -> Plan:
        for rel, text in files.items():
            target = tmp_path / rel
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(text, encoding="utf-8")
        (tmp_path / "pipeloom.yml").write_text(
            "config_version: 1\nname: gen\nengine: {kind: memory}\ncatalog: {kind: memory}\n"
            "defaults:\n  models: {materialized: table}\ntargets:\n  ci:\n    naming: {kind: none}\n",
            encoding="utf-8",
        )
        return compile_dir(tmp_path)

    return make


SIM_PROJECT = """config_version: 1
name: sim
engine: {{kind: memory, root: "{tables}"}}
catalog: {{kind: memory, root: "{tables}"}}
state: {{kind: json, path: "{state}"}}
artifacts: {{root: "{artifacts}"}}
deploy:
  kind: databricks
  pipelines: {{main: {{}}, extra: {{select: ["e"]}}}}
  task_grouping: {grouping}
targets:
  prod: {{}}
"""
SIM_MODELS = """sources:
  - {name: raw, format: memory, location: raw_in}
models:
  - {name: a, sql: select 1 as x, tags: [grp]}
  - {name: b, sql: select * from a, inputs: {a: {ref: a}}, tags: [grp]}
  - {name: c, sql: select * from raw, inputs: {raw: {source: raw}}, tags: [bad]}
  - {name: d, sql: select * from c, inputs: {c: {ref: c}}, tags: [bad]}
  - {name: e, sql: select 3 as x}
"""


@dataclass
class Sim:
    """A project that runs on the memory engine, with its rendered job and shipped manifest."""

    root: Path
    artifacts: Path
    tables: Path
    manifest: Path
    plan: Plan
    settings: dict[str, JsonValue]

    def run_dir(self, run_id: str) -> Path:
        return self.artifacts / "runs" / run_id

    def results(self, run_id: str) -> dict[str, Any]:
        doc: dict[str, Any] = json.loads(
            (self.run_dir(run_id) / "run_results.json").read_bytes().decode("utf-8")
        )
        return doc

    def rows(self, run_id: str) -> dict[str, dict[str, Any]]:
        return {r["unit_id"]: r for r in self.results(run_id)["results"]}

    def attempts(self, run_id: str, unit: str) -> list[str]:
        folder = self.run_dir(run_id) / "units" / unit
        return sorted(p.name for p in folder.glob("attempt-*.json")) if folder.is_dir() else []

    def make_source_available(self) -> None:
        """Create the table that model c reads, so c stops failing."""
        # A relative source location is read as a path below the project dir.
        key = quote((self.root.resolve() / "raw_in").as_posix(), safe="")
        (self.tables / f"{key}.json").write_text('[{"x": 1}]', encoding="utf-8")


@pytest.fixture
def sim(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Callable[..., Sim]:
    """sim(grouping="none") -> a Sim whose models are a -> b, c -> d (c fails until its source
    exists) and e. Models a and b share the tag grp, c and d the tag bad."""
    for name in (
        "DATABRICKS_RUNTIME_VERSION",
        "PIPELOOM_TARGET",
        "PIPELOOM_WORKERS",
        "PIPELOOM_LOG_FORMAT",
        "PIPELOOM_RUN_STARTED_AT",
        "PIPELOOM_DISABLE_PLUGIN_AUTOLOAD",
    ):
        monkeypatch.delenv(name, raising=False)

    def make(grouping: str = "none") -> Sim:
        root, artifacts, tables = tmp_path / "proj", tmp_path / "art", tmp_path / "tables"
        files = {
            "pipeloom.yml": SIM_PROJECT.format(
                tables=tables.as_posix(),
                state=(tmp_path / "state").as_posix(),
                artifacts=artifacts.as_posix(),
                grouping=grouping,
            ),
            "models/m.yml": SIM_MODELS,
        }
        for rel, text in files.items():
            path = root / rel
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(text.encode("utf-8"))
        plan = compile_dir(root, "prod")
        manifest = tmp_path / "manifest.json"
        manifest.write_bytes(manifest_bytes(plan, stable=True))
        locs = Locations(
            project_root=root.as_posix(),
            manifest_path=manifest.as_posix(),
            artifacts_root=artifacts.as_posix(),
            state_shared=True,
        )
        report = Report()
        job = render_job(plan, "main", locs, report, versions=VERSIONS)
        assert job is not None, [d.message for d in report.sorted()]
        return Sim(root, artifacts, tables, manifest, plan, to_json(job.settings))

    return make


@pytest.fixture
def golden(request: pytest.FixtureRequest) -> Callable[[str, bytes], None]:
    """Compare bytes with tests/goldens/<name>; --update-goldens rewrites it (LF).

    The flag is registered by the root tests/conftest.py, so an update run also names a root
    test path: `pytest --update-goldens tests/test_catalog.py plugins/pipeloom-databricks/tests`.
    """
    update = bool(request.config.getoption("--update-goldens", default=False))

    def check(name: str, data: bytes) -> None:
        path = GOLDENS / name
        text = data.decode("utf-8")
        if update:
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("w", encoding="utf-8", newline="\n") as fh:
                fh.write(text)
            return
        assert path.exists(), f"missing golden {name}; run pytest --update-goldens"
        assert text == path.read_bytes().decode("utf-8").replace("\r\n", "\n")

    return check
