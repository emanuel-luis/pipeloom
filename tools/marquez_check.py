"""Check the OpenLineage sink against a running Marquez (ADR-0046).

    uv run --all-packages python tools/marquez_check.py [--keep] [--url URL]

Start Marquez first with `examples/marquez/compose.yml`; this tool never starts or stops Docker.
It copies `examples/tpch_polars` to a temp folder, adds the openlineage sink with an http
transport to `--url` (default http://localhost:15000), runs the project on the generated TPC-H
data, then reads the Marquez API and prints one line per fact:

    CHECK <name> ok|FAIL <detail>

The facts: one job per persisted model plus the parent, the lineage edges of every job equal to
the manifest's, schema fields on every output, quality assertions on the models with
expectations, and the parent run id on every unit run. Exit 1 on any FAIL. `--keep` leaves the
copy (manifest, warehouse, run artifacts) and prints its path. The comparison functions are pure,
so `tests/test_marquez_check.py` runs them on recorded API payloads.
"""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
import tempfile
import urllib.request
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import quote

from pipeloom.api import Project, ProjectOptions
from pipeloom.plan.manifest import manifest_dict
from pipeloom.plan.model import Plan
from pipeloom_openlineage.sink import run_uuid, unit_lineage, unit_uuid

ROOT = Path(__file__).resolve().parent.parent
EXAMPLE = ROOT / "examples" / "tpch_polars"
DATA = ROOT / "examples" / "data" / "tpch"
TARGET = "dev"
NAMESPACE = f"pipeloom.{TARGET}"
DEFAULT_URL = "http://localhost:15000"

Api = Callable[[str], Any]
"""GET a Marquez API path (starting with /api/v1) and return the decoded JSON."""
Dataset = tuple[str, str]
"""(namespace, name)."""


@dataclass(frozen=True)
class Check:
    name: str
    ok: bool
    detail: str = ""

    def line(self) -> str:
        return f"CHECK {self.name} ok" if self.ok else f"CHECK {self.name} FAIL {self.detail}"


@dataclass(frozen=True)
class Expected:
    project: str
    run_id: str
    edges: dict[str, tuple[frozenset[Dataset], frozenset[Dataset]]]
    """Unit job name -> (inputs, outputs)."""
    quality: frozenset[Dataset] = field(default_factory=frozenset)
    """Outputs of the models with expectations."""

    @property
    def jobs(self) -> frozenset[str]:
        return frozenset({self.project, *self.edges})


def expected(plan: Plan, project_dir: Path, run_id: str) -> Expected:
    """What Marquez should hold for this plan, with the sink's own naming."""
    manifest = manifest_dict(plan)
    project = plan.metadata.project
    edges: dict[str, tuple[frozenset[Dataset], frozenset[Dataset]]] = {}
    quality: set[Dataset] = set()
    for unit_id, unit in plan.units.items():
        ol = unit_lineage(manifest, project_dir, unit_id)
        outputs = frozenset((d.namespace, d.name) for d in ol.outputs or ())
        if not outputs:
            continue
        edges[ol.job.name] = (frozenset((d.namespace, d.name) for d in ol.inputs or ()), outputs)
        if plan.nodes[unit.nodes[-1]].config.expectations:
            quality |= outputs
    return Expected(project, run_id, edges, frozenset(quality))


def _pairs(items: Any) -> frozenset[Dataset]:
    return frozenset((str(d["namespace"]), str(d["name"])) for d in items or ())


def _dataset(api: Api, ds: Dataset) -> Any:
    return api(f"/api/v1/namespaces/{quote(ds[0], safe='')}/datasets/{quote(ds[1], safe='')}")


def _short(items: Any) -> str:
    return ", ".join(sorted(str(i) for i in items))


def checks(exp: Expected, api: Api, namespace: str = NAMESPACE) -> list[Check]:
    ns = quote(namespace, safe="")
    jobs = {j["name"]: j for j in api(f"/api/v1/namespaces/{ns}/jobs?limit=1000")["jobs"]}
    out = [Check("jobs", not exp.jobs - set(jobs), f"missing {_short(exp.jobs - set(jobs))}")]

    bad_edges = []
    for name, (inputs, outputs) in sorted(exp.edges.items()):
        job = jobs.get(name, {})
        if (_pairs(job.get("inputs")), _pairs(job.get("outputs"))) != (inputs, outputs):
            bad_edges.append(name)
    out.append(Check("lineage_edges", not bad_edges, f"differ for {_short(bad_edges)}"))

    outputs = sorted({o for _, outs in exp.edges.values() for o in outs})
    no_fields = [o[1] for o in outputs if not _dataset(api, o).get("fields")]
    out.append(Check("schema_fields", not no_fields, f"no fields on {_short(no_fields)}"))

    no_quality = [
        o[1]
        for o in sorted(exp.quality)
        if not (_dataset(api, o).get("facets") or {})
        .get("dataQualityAssertions", {})
        .get("assertions")
    ]
    out.append(
        Check(
            "quality_assertions",
            bool(exp.quality) and not no_quality,
            f"no assertions on {_short(no_quality)}" if exp.quality else "no model has any",
        )
    )

    parent = run_uuid(exp.project, exp.run_id)
    bad_parent = []
    for name in sorted(exp.edges):
        unit = name.removeprefix(f"{exp.project}.")
        want = unit_uuid(exp.project, exp.run_id, unit, 1)
        runs = api(f"/api/v1/namespaces/{ns}/jobs/{quote(name, safe='')}/runs?limit=100")["runs"]
        run = next((r for r in runs if r.get("id") == want), None)
        got = (((run or {}).get("facets") or {}).get("parent") or {}).get("run", {}).get("runId")
        if got != parent:
            bad_parent.append(name)
    out.append(Check("parent_run_ids", not bad_parent, f"wrong or missing on {_short(bad_parent)}"))
    return out


def sink_block(url: str) -> str:
    return f"""events:
  sinks:
    - kind: openlineage
      transport:
        type: http
        url: {url}
"""


def http_api(url: str) -> Api:
    def get(path: str) -> Any:
        with urllib.request.urlopen(url.rstrip("/") + path, timeout=30) as resp:
            return json.loads(resp.read().decode("utf-8"))

    return get


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--keep", action="store_true", help="keep the copy of the example")
    parser.add_argument("--url", default=DEFAULT_URL, help="Marquez API base URL")
    args = parser.parse_args(argv)
    if not (DATA / "orders.parquet").is_file():
        print(f"no TPC-H data under {DATA}; run `uv run examples/tpch_data.py` first")
        return 2
    work = Path(tempfile.mkdtemp(prefix="pipeloom-marquez-"))
    project_dir = work / EXAMPLE.name
    try:
        shutil.copytree(EXAMPLE, project_dir, ignore=shutil.ignore_patterns("target", ".pipeloom"))
        yml = project_dir / "pipeloom.yml"
        yml.write_text(yml.read_text(encoding="utf-8") + sink_block(args.url), encoding="utf-8")
        run_id = f"marquez-check-{uuid.uuid4().hex[:8]}"
        vars_json = json.dumps({"tpch_dir": DATA.as_posix()})
        argv_run = [sys.executable, "-m", "pipeloom", "run", "--project-dir", str(project_dir)]
        argv_run += ["--target", TARGET, "--run-id", run_id, "--vars", vars_json]
        code = subprocess.run(argv_run, check=False).returncode
        results = [Check("run", code == 0, f"pipeloom run exited with {code}")]
        if code == 0:
            outcome = Project(
                ProjectOptions(
                    project_dir=project_dir, target=TARGET, vars={"tpch_dir": DATA.as_posix()}
                )
            ).compile()
            if outcome.value is None:
                results.append(Check("compile", False, "the copy did not compile"))
            else:
                exp = expected(outcome.value, project_dir.resolve(), run_id)
                results += checks(exp, http_api(args.url))
    finally:
        if args.keep:
            print(f"kept {project_dir}")
        else:
            shutil.rmtree(work, ignore_errors=True)
    for c in results:
        print(c.line())
    return 0 if all(c.ok for c in results) else 1


if __name__ == "__main__":
    sys.exit(main())
