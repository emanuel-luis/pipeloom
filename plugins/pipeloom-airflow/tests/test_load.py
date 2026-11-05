from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest
from pydantic import JsonValue

import pipeloom_airflow.load as load_module
from pipeloom.api import Project
from pipeloom.diagnostics import Report
from pipeloom.plan.manifest import write_manifest
from pipeloom_airflow.config import LoadMode, ProjectConfig, RenderConfig
from pipeloom_airflow.load import LoadedPlan, load_plan

SHOP = Path(__file__).resolve().parents[3] / "tests" / "fixtures" / "shop"

PROJECT = RenderConfig(load_method=LoadMode.PROJECT)


@pytest.fixture
def project(tmp_path: Path) -> Path:
    dest = tmp_path / "shop"
    shutil.copytree(SHOP, dest)
    return dest


@pytest.fixture
def compiles(monkeypatch: pytest.MonkeyPatch) -> list[int]:
    """One entry per real compile."""
    calls: list[int] = []
    real = Project.compile

    def counting(self: Project, *args: Any, **kwargs: Any) -> Any:
        calls.append(1)
        return real(self, *args, **kwargs)

    monkeypatch.setattr(Project, "compile", counting)
    return calls


def go(
    project: Path,
    cache: Path,
    env: dict[str, str] | None = None,
    vars_: dict[str, JsonValue] | None = None,
) -> tuple[LoadedPlan | None, Report]:
    report = Report()
    cfg = ProjectConfig(project.as_posix(), cache_dir=cache.as_posix(), vars=vars_ or {})
    return load_plan(cfg, PROJECT, "ci", report, env=env or {}), report


def test_manifest_mode(project: Path, tmp_path: Path, shop: LoadedPlan) -> None:
    path = tmp_path / "m.json"
    write_manifest(shop.plan, path, stable=True)
    report = Report()
    cfg = ProjectConfig(project.as_posix(), manifest_path=path.as_posix())
    loaded = load_plan(cfg, RenderConfig(), "ci", report, env={})
    assert loaded is not None and loaded.source == "manifest"
    assert loaded.plan_hash == shop.plan_hash and loaded.manifest_path == path.as_posix()
    assert report.sorted() == ()


def test_manifest_for_another_target_is_plx107(
    project: Path, tmp_path: Path, shop: LoadedPlan
) -> None:
    path = tmp_path / "m.json"
    write_manifest(shop.plan, path, stable=True)
    report = Report()
    cfg = ProjectConfig(project.as_posix(), manifest_path=path.as_posix())
    assert load_plan(cfg, RenderConfig(), "dev", report, env={}) is None
    assert [d.code for d in report.sorted()] == ["PLX107"]


def test_a_missing_manifest_is_plx107(project: Path, tmp_path: Path) -> None:
    report = Report()
    cfg = ProjectConfig(project.as_posix(), manifest_path=(tmp_path / "none.json").as_posix())
    assert load_plan(cfg, RenderConfig(), "ci", report, env={}) is None
    assert [d.code for d in report.sorted()] == ["PLX107"]


def test_cache_miss_then_hit(project: Path, tmp_path: Path, compiles: list[int]) -> None:
    cache = tmp_path / "cache"
    first, _ = go(project, cache)
    assert first is not None and first.source == "project" and first.manifest_path is None
    second, report = go(project, cache)
    assert second is not None and second.source == "cache"
    assert second.plan_hash == first.plan_hash and report.sorted() == ()
    assert len(compiles) == 1


def test_a_touched_file_is_a_miss(project: Path, tmp_path: Path, compiles: list[int]) -> None:
    cache = tmp_path / "cache"
    go(project, cache)
    stg = project / "models" / "staging" / "stg.yml"
    stg.write_text(stg.read_text(encoding="utf-8") + "\n", encoding="utf-8")
    again, _ = go(project, cache)
    assert again is not None and again.source == "project" and len(compiles) == 2


def test_new_vars_are_a_miss(project: Path, tmp_path: Path, compiles: list[int]) -> None:
    cache = tmp_path / "cache"
    go(project, cache)
    go(project, cache, vars_={"x": 1})
    assert len(compiles) == 2


def test_a_changed_env_value_is_a_miss(tmp_path: Path, compiles: list[int]) -> None:
    proj = tmp_path / "p"
    (proj / "models").mkdir(parents=True)
    (proj / "pipeloom.yml").write_text(
        "config_version: 1\nname: gen\nengine: {kind: memory}\ncatalog: {kind: memory}\n"
        "targets:\n  ci: {}\n",
        encoding="utf-8",
    )
    (proj / "models" / "m.yml").write_text(
        'models:\n  - {name: a, sql: select 1 as x, meta: {k: "${env:PL_AIRFLOW_X:-d}"}}\n',
        encoding="utf-8",
    )
    cache = tmp_path / "cache"
    assert go(proj, cache, {"PL_AIRFLOW_X": "1"})[0] is not None
    hit, _ = go(proj, cache, {"PL_AIRFLOW_X": "1"})
    assert hit is not None and hit.source == "cache"
    miss, _ = go(proj, cache, {"PL_AIRFLOW_X": "2"})
    assert miss is not None and miss.source == "project"
    assert len(compiles) == 2


def test_an_unwritable_cache_is_pla006_and_still_a_plan(project: Path, tmp_path: Path) -> None:
    blocked = tmp_path / "blocked"
    blocked.write_text("file", encoding="utf-8")
    loaded, report = go(project, blocked)
    assert loaded is not None and loaded.source == "project"
    assert [d.code for d in report.sorted()] == ["PLA006"]


def test_the_cache_keeps_the_newest_entries(project: Path, tmp_path: Path) -> None:
    cache = tmp_path / "cache"
    for i in range(load_module.KEEP + 3):
        go(project, cache, vars_={"n": i})
    (folder,) = [p for p in cache.iterdir() if p.is_dir()]
    assert len(list(folder.glob("*.entry.json"))) == load_module.KEEP
    assert len(list(folder.glob("*.manifest.json"))) == load_module.KEEP


def test_a_broken_project_reports_and_returns_none(tmp_path: Path) -> None:
    proj = tmp_path / "bad"
    proj.mkdir()
    (proj / "pipeloom.yml").write_text("config_version: 2\n", encoding="utf-8")
    loaded, report = go(proj, tmp_path / "cache")
    assert loaded is None
    assert "PLS103" in {d.code for d in report.sorted()}


def test_a_missing_project_file_still_compiles_to_diagnostics(tmp_path: Path) -> None:
    loaded, report = go(tmp_path, tmp_path / "cache")
    codes = {d.code for d in report.sorted()}
    assert loaded is None and {"PLA006", "PLY004"} <= codes


CHILD = """
import sys
from pipeloom.diagnostics import Report
from pipeloom_airflow.config import LoadMode, ProjectConfig, RenderConfig
from pipeloom_airflow.load import load_plan
project, cache = sys.argv[1], sys.argv[2]
loaded = load_plan(ProjectConfig(project, cache_dir=cache), RenderConfig(load_method=LoadMode.PROJECT),
                   "ci", Report(), env={})
assert loaded is not None
"""


def test_cache_files_are_identical_across_hash_seeds(project: Path, tmp_path: Path) -> None:
    caches = []
    for seed in ("1", "12345"):
        cache = tmp_path / f"cache{seed}"
        env = {**os.environ, "PYTHONHASHSEED": seed}
        proc = subprocess.run(
            [sys.executable, "-I", "-c", CHILD, project.as_posix(), cache.as_posix()],
            capture_output=True,
            text=True,
            check=False,
            env=env,
        )
        assert proc.returncode == 0, proc.stderr
        caches.append(cache)
    files = [sorted(p.relative_to(c) for p in c.rglob("*") if p.is_file()) for c in caches]
    assert files[0] == files[1] and len(files[0]) == 2
    for rel in files[0]:
        assert (caches[0] / rel).read_bytes() == (caches[1] / rel).read_bytes()
