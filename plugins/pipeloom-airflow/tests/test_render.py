from __future__ import annotations

import sys
import time
from collections.abc import Callable
from pathlib import Path

import pytest

from pipeloom.diagnostics import Report
from pipeloom.plan.manifest import write_manifest
from pipeloom_airflow.config import ExecutionConfig, ExecutionMode, RenderConfig
from pipeloom_airflow.load import LoadedPlan
from pipeloom_airflow.render import TaskPlan, render_plan

Models = Callable[..., LoadedPlan]


def render(
    loaded: LoadedPlan,
    render_cfg: RenderConfig | None = None,
    execution: ExecutionConfig | None = None,
) -> tuple[TaskPlan, list[str]]:
    report = Report()
    out = render_plan(
        loaded, "ci", render_cfg or RenderConfig(), execution or ExecutionConfig(), report
    )
    return out, [d.code for d in report.sorted()]


def test_the_shop_graph(shop: LoadedPlan) -> None:
    plan, codes = render(shop)
    assert codes == ["PLA007"]
    assert plan.project == "shop" and plan.target == "ci" and plan.plan_hash == shop.plan_hash
    assert [t.task_id for t in plan.tasks] == [
        "dim_customers",
        "fct_orders",
        "rpt_revenue",
        "stg_customers",
        "stg_orders",
        "stg_payments",
    ]
    by = {t.task_id: t for t in plan.tasks}
    assert by["fct_orders"].upstream == ("stg_customers", "stg_orders", "stg_payments")
    assert by["dim_customers"].upstream == ("stg_customers", "stg_orders")
    assert by["rpt_revenue"].upstream == ("fct_orders",)
    assert by["stg_orders"].upstream == ()
    assert plan.leaves == ("dim_customers", "rpt_revenue")
    assert by["fct_orders"].models == ("fct_orders",)
    assert plan.cancel_grace_s == 30.0


def test_select_and_exclude(shop: LoadedPlan) -> None:
    plan, _ = render(shop, RenderConfig(select=("fct_orders+",), exclude=("stg_payments",)))
    assert [t.task_id for t in plan.tasks] == ["fct_orders", "rpt_revenue"]
    assert plan.select == ("fct_orders+",) and plan.exclude == ("stg_payments",)
    by = {t.task_id: t for t in plan.tasks}
    assert by["fct_orders"].upstream == ()
    assert plan.leaves == ("rpt_revenue",)


def test_a_selection_that_renders_nothing_is_pla003(shop: LoadedPlan) -> None:
    plan, codes = render(shop, RenderConfig(select=("tag:nothing",)))
    assert plan.tasks == () and plan.leaves == ()
    assert "PLA003" in codes


def test_a_bad_selector_renders_nothing(shop: LoadedPlan) -> None:
    plan, codes = render(shop, RenderConfig(select=("bad:x",)))
    assert plan.tasks == ()
    assert "PLA003" in codes and "PLG304" in codes


def test_group_paths(shop: LoadedPlan) -> None:
    by_tag, _ = render(shop, RenderConfig(group_by="tag"))
    assert {t.task_id: t.group for t in by_tag.tasks} == {
        "dim_customers": ("mart",),
        "fct_orders": ("mart",),
        "rpt_revenue": ("untagged",),
        "stg_customers": ("staging",),
        "stg_orders": ("staging",),
        "stg_payments": ("staging",),
    }
    by_folder, _ = render(shop, RenderConfig(group_by="folder"))
    assert {t.group for t in by_folder.tasks} == {("marts",), ("staging",)}
    none, _ = render(shop)
    assert {t.group for t in none.tasks} == {()}


def test_a_model_in_the_model_path_itself_is_in_root(models_loaded: Models) -> None:
    loaded = models_loaded({"models/a.yml": "models:\n  - {name: a, sql: select 1 as x}\n"})
    plan, _ = render(loaded, RenderConfig(group_by="folder"))
    assert [t.group for t in plan.tasks] == [("root",)]


def test_assets(shop: LoadedPlan) -> None:
    plan, _ = render(shop, RenderConfig(emit_assets=True))
    by = {t.task_id: t for t in plan.tasks}
    assert by["rpt_revenue"].assets == ("pipeloom://shop/ci/main.rpt_revenue",)
    assert render(shop)[0].tasks[0].assets == ()
    custom, _ = render(shop, RenderConfig(emit_assets=True, asset_uri="a://{model}/{unique_id}"))
    assert {t.task_id: t.assets for t in custom.tasks}["stg_orders"] == (
        "a://stg_orders/model.shop.stg_orders",
    )


def test_retry_and_timeout_mapping(models_loaded: Models) -> None:
    loaded = models_loaded(
        {
            "models/m.yml": "models:\n"
            "  - {name: a, sql: select 1 as x, retries: 2, retry_delay: 5, retry_on: any,"
            " timeout: 60}\n"
            "  - {name: b, sql: select 1 as x}\n"
        }
    )
    by = {t.task_id: t for t in render(loaded)[0].tasks}
    assert (by["a"].retries, by["a"].retry_delay_s, by["a"].retry_on) == (2, 5.0, "any")
    assert (by["a"].timeout_s, by["a"].max_attempts) == (60.0, 3)
    assert (by["b"].retries, by["b"].timeout_s, by["b"].max_attempts) == (0, None, 1)
    assert by["b"].retry_on == "transient"


def test_the_task_boundary_has_no_pla007(shop: LoadedPlan) -> None:
    task = shop.plan.model_copy(
        update={"metadata": shop.plan.metadata.model_copy(update={"boundary": "task"})}
    )
    _, codes = render(LoadedPlan(task, shop.plan_hash, None, "project"))
    assert codes == []


def test_task_ids_avoid_the_reserved_ones(models_loaded: Models) -> None:
    loaded = models_loaded(
        {"models/m.yml": "models:\n  - {name: pipeloom_finalize, sql: select 1}\n"}
    )
    (task,) = render(loaded)[0].tasks
    assert task.unit_id == "pipeloom_finalize" and task.task_id != "pipeloom_finalize"


DEPLOY = "deploy:\n  kind: databricks\n  task_grouping: by_tag\n"
DBX_MODELS = {
    "models/m.yml": "models:\n"
    "  - {name: a, sql: select 1 as x, tags: [g], retries: 1, timeout: 10}\n"
    "  - {name: b, sql: select 1 as x, tags: [g], retries: 3, timeout: 20}\n"
    "  - {name: c, sql: select 1 as x, tags: [h], inputs: {a: {ref: a}}}\n"
}


def test_databricks_by_tag(models_loaded: Models) -> None:
    loaded = models_loaded(DBX_MODELS, DEPLOY)
    plan, codes = render(loaded, execution=ExecutionConfig(mode=ExecutionMode.DATABRICKS))
    assert codes == ["PLA007"]
    by = {t.task_id: t for t in plan.tasks}
    assert sorted(by) == ["tag_g", "tag_h"]
    g, h = by["tag_g"], by["tag_h"]
    assert (g.databricks_key, g.units, g.models) == ("tag_g", ("a", "b"), ("a", "b"))
    assert (g.retries, g.retry_delay_s, g.max_attempts, g.timeout_s) == (0, 0.0, 4, 30.0)
    assert h.upstream == ("tag_g",) and h.timeout_s is None and h.max_attempts == 1
    assert plan.leaves == ("tag_h",)


def test_databricks_needs_a_deploy_block(shop: LoadedPlan) -> None:
    plan, codes = render(shop, execution=ExecutionConfig(mode=ExecutionMode.DATABRICKS))
    assert plan.tasks == ()
    assert "PLD004" in codes and "PLA003" in codes


def test_a_500_unit_manifest_renders_fast(tmp_path: Path, models_loaded: Models) -> None:
    lines = ["models:"]
    for i in range(500):
        parent = f", inputs: {{p: {{ref: m{i - 1}}}}}" if i else ""
        lines.append(f"  - {{name: m{i}, sql: select 1 as x{parent}}}")
    loaded = models_loaded({"models/m.yml": "\n".join(lines) + "\n"})
    manifest = tmp_path / "out" / "manifest.json"
    write_manifest(loaded.plan, manifest, stable=True)
    from pipeloom.plan.manifest import read_manifest

    plan = read_manifest(manifest, Report())
    assert plan is not None
    start = time.perf_counter()
    out, _ = render(LoadedPlan(plan, plan.metadata.plan_hash, None, "manifest"))
    assert time.perf_counter() - start < 1.0
    assert len(out.tasks) == 500 and out.leaves == ("m499",)


@pytest.mark.parametrize("mode", [ExecutionMode.LOCAL, ExecutionMode.VIRTUALENV])
def test_the_mode_does_not_change_the_unit_graph(shop: LoadedPlan, mode: ExecutionMode) -> None:
    assert render(shop, execution=ExecutionConfig(mode=mode))[0] == render(shop)[0]


def test_databricks_mode_without_the_package_is_pla001(
    shop: LoadedPlan, monkeypatch: pytest.MonkeyPatch
) -> None:
    for name in ("pipeloom_databricks.grouping", "pipeloom_databricks.render"):
        monkeypatch.setitem(sys.modules, name, None)
    plan, codes = render(shop, execution=ExecutionConfig(mode=ExecutionMode.DATABRICKS))
    assert plan.tasks == () and "PLA001" in codes
