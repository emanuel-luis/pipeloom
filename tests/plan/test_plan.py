from __future__ import annotations

import json
import shutil
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

import pipeloom
from pipeloom.api import Project, ProjectOptions
from pipeloom.diagnostics import Report
from pipeloom.graph.selectors import Selection
from pipeloom.plan.manifest import compute_plan_hash, manifest_bytes, read_manifest, write_manifest
from pipeloom.plan.model import Plan, PythonBody, SqlBody
from pipeloom.platform import Identity
from tests.conftest import FIXTURES

SHOP = FIXTURES / "shop"


class FixedIdentity:
    def __init__(self, user: str) -> None:
        self.user = user

    def identity(self) -> Identity:
        return Identity(user=self.user, host="h")


def compile_shop(
    project_dir: Path = SHOP, target: str = "ci", user: str = "Ana.Silva", **kw: Any
) -> Plan:
    opts = ProjectOptions(project_dir=project_dir, target=target, env={})
    outcome = Project(opts, identity=FixedIdentity(user)).compile(**kw)
    assert outcome.value is not None, outcome.diagnostics
    return outcome.value


def copy_shop(tmp_path: Path) -> Path:
    dest = tmp_path / "shop"
    shutil.copytree(SHOP, dest, ignore=shutil.ignore_patterns("__pycache__"))
    return dest


def test_units_one_per_persisted_model() -> None:
    plan = compile_shop()
    assert sorted(plan.units) == [
        "dim_customers",
        "fct_orders",
        "rpt_revenue",
        "stg_customers",
        "stg_orders",
        "stg_payments",
    ]
    fct = plan.units["fct_orders"]
    assert fct.nodes == ("model.shop.orders_enriched", "model.shop.fct_orders")
    assert fct.parents == ("stg_customers", "stg_orders", "stg_payments")
    assert plan.units["dim_customers"].parents == ("stg_customers", "stg_orders")
    assert plan.units["rpt_revenue"].parents == ("fct_orders",)
    assert plan.units["stg_orders"].parents == ()
    eph = plan.nodes["model.shop.orders_enriched"]
    assert eph.unit_ids == ("dim_customers", "fct_orders") and eph.relation is None
    assert plan.nodes["model.shop.fct_orders"].unit_ids == ("fct_orders",)


def test_units_ignore_selection_and_boundary() -> None:
    full = compile_shop()
    part = compile_shop(selection=Selection(select=("rpt_revenue",)), boundary="task")
    assert part.units == full.units
    assert part.parent_map == full.parent_map
    assert part.selection.selected == ("model.shop.rpt_revenue",)
    assert part.metadata.boundary == "task" and full.metadata.boundary == "process"
    assert len(full.selection.selected) == 7


def test_bodies_and_naming() -> None:
    plan = compile_shop(target="dev", user="Ana.Silva")
    assert isinstance(plan.nodes["model.shop.stg_orders"].body, SqlBody)
    body = plan.nodes["model.shop.fct_orders"].body
    assert isinstance(body, PythonBody) and body.file == "src/shop/marts.py"
    rel = plan.nodes["model.shop.fct_orders"].relation
    assert rel is not None and rel.relation_name == "main_ana_silva.fct_orders"
    ci = compile_shop(target="ci").nodes["model.shop.fct_orders"].relation
    assert ci is not None and ci.relation_name == "main.fct_orders"


def test_plan_hash_stable_and_sensitive(tmp_path: Path) -> None:
    a, b = compile_shop(), compile_shop()
    assert a.metadata.plan_hash == b.metadata.plan_hash == compute_plan_hash(a)
    assert len(a.metadata.plan_hash) == 64
    assert compile_shop(user="someone_else").metadata.plan_hash == a.metadata.plan_hash
    assert compile_shop(target="dev").metadata.plan_hash != a.metadata.plan_hash
    copy = copy_shop(tmp_path)
    assert compile_shop(copy).metadata.plan_hash == a.metadata.plan_hash
    sql = copy / "models/staging/stg.yml"
    sql.write_bytes(sql.read_bytes().replace(b"lower(email)", b"upper(email)"))
    changed = compile_shop(copy)
    assert changed.metadata.plan_hash != a.metadata.plan_hash
    cid = "model.shop.stg_customers"
    assert changed.nodes[cid].checksum != a.nodes[cid].checksum
    oid = "model.shop.stg_orders"
    assert changed.nodes[oid].checksum == a.nodes[oid].checksum


def test_python_edit_changes_checksum_but_crlf_does_not(tmp_path: Path) -> None:
    uid = "model.shop.dim_customers"
    base = compile_shop().nodes[uid].checksum
    copy = copy_shop(tmp_path)
    py = copy / "src/shop/marts.py"
    py.write_bytes(py.read_bytes().replace(b"\r\n", b"\n").replace(b"\n", b"\r\n"))
    assert compile_shop(copy).nodes[uid].checksum == base
    py.write_bytes(py.read_bytes() + b"# edit\r\n")
    assert compile_shop(copy).nodes[uid].checksum != base


def test_selection_does_not_change_hash_but_boundary_does() -> None:
    a = compile_shop()
    assert compile_shop(selection=Selection(select=("tag:mart",))).metadata.plan_hash == (
        a.metadata.plan_hash
    )
    assert compile_shop(boundary="task").metadata.plan_hash != a.metadata.plan_hash


def normalize(text: str) -> str:
    data = json.loads(text)
    meta = data["metadata"]
    meta["pipeloom_version"] = "<version>"
    meta["generated_at"] = "<generated_at>"
    meta["invocation_id"] = "<invocation_id>"
    for info in meta["plugins"].values():
        info["version"] = "<version>"
    return json.dumps(data, indent=2, ensure_ascii=False) + "\n"


def test_manifest_golden(golden: Callable[[str, str], None]) -> None:
    text = manifest_bytes(compile_shop()).decode("utf-8")
    assert text.endswith("}\n") and "\r" not in text
    assert json.loads(text)["metadata"]["pipeloom_version"] == pipeloom.__version__
    golden("manifest/shop-ci.json", normalize(text))


def test_manifest_roundtrip(tmp_path: Path) -> None:
    plan = compile_shop(selection=Selection(select=("tag:mart",)))
    path = tmp_path / "target" / "manifest.json"
    content = write_manifest(plan, path)
    assert path.read_bytes() == content
    report = Report()
    assert read_manifest(path, report) == plan
    assert report.sorted() == ()


@pytest.mark.parametrize(
    "mutate",
    [
        lambda d: d.update(schema="other/1"),
        lambda d: d.pop("nodes"),
        lambda d: d["nodes"]["model.shop.stg_orders"]["body"].update(sql="select 2"),
    ],
    ids=["schema", "missing-key", "edited"],
)
def test_read_manifest_rejects_bad_files(
    tmp_path: Path, mutate: Callable[[dict[str, Any]], object]
) -> None:
    path = tmp_path / "m.json"
    write_manifest(compile_shop(), path)
    data = json.loads(path.read_text(encoding="utf-8"))
    mutate(data)
    path.write_text(json.dumps(data), encoding="utf-8")
    report = Report()
    assert read_manifest(path, report) is None
    assert [d.code for d in report.sorted()] == ["PLX107"]


def test_read_manifest_missing_and_not_json(tmp_path: Path) -> None:
    for name, content in (("none.json", None), ("bad.json", b"{")):
        path = tmp_path / name
        if content is not None:
            path.write_bytes(content)
        report = Report()
        assert read_manifest(path, report) is None
        assert [d.code for d in report.sorted()] == ["PLX107"]


def test_compile_is_memoized_and_stops_on_errors(tmp_path: Path) -> None:
    project = Project(ProjectOptions(project_dir=SHOP, target="ci", env={}))
    assert project.compile() is project.compile()
    assert project.compile() is not project.compile(boundary="task")
    assert project.validate() is project.validate()
    bad = copy_shop(tmp_path)
    (bad / "src/shop/marts.py").write_text("def fct_orders(ctx): ...\n", encoding="utf-8")
    outcome = Project(ProjectOptions(project_dir=bad, target="ci", env={})).compile()
    assert outcome.value is None and "PLK602" in {d.code for d in outcome.diagnostics}


def test_bad_selector_returns_diagnostic() -> None:
    project = Project(ProjectOptions(project_dir=SHOP, target="ci", env={}))
    outcome = project.compile(Selection(select=("bogus:x",)))
    assert outcome.value is None and [d.code for d in outcome.diagnostics] == ["PLG304"]
