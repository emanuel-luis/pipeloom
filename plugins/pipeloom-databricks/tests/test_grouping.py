from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any

from pipeloom.diagnostics import Report
from pipeloom.plan.model import Plan
from pipeloom_databricks import keys
from pipeloom_databricks.grouping import build_tasks, pipeline_units, task_key_map
from pipeloom_databricks.options import DatabricksDeployOptions

Models = Callable[[Mapping[str, str]], Plan]


def _opts(**options: Any) -> DatabricksDeployOptions:
    return DatabricksDeployOptions.model_validate(options)


def _codes(report: Report) -> list[str]:
    return [d.code for d in report.sorted()]


def test_one_task_per_unit_by_default(shop: Plan) -> None:
    report = Report()
    tasks = build_tasks(shop, _opts(), "main", report)
    assert _codes(report) == []
    assert [t.key for t in tasks] == [
        "dim_customers",
        "fct_orders",
        "rpt_revenue",
        "stg_customers",
        "stg_orders",
        "stg_payments",
    ]
    by_key = {t.key: t for t in tasks}
    assert by_key["rpt_revenue"].parents == ("fct_orders",)
    assert by_key["fct_orders"].parents == ("stg_customers", "stg_orders", "stg_payments")
    assert by_key["stg_orders"].parents == ()
    assert {t.compute for t in tasks} == {"default"}


def test_group_by_tag_uses_the_first_tag(shop: Plan) -> None:
    tasks = build_tasks(shop, _opts(task_grouping="by_tag"), "main", Report())
    assert {t.key: t.units for t in tasks} == {
        "tag_mart": ("dim_customers", "fct_orders"),
        "tag_staging": ("stg_customers", "stg_orders", "stg_payments"),
        "tag_untagged": ("rpt_revenue",),
    }
    by_key = {t.key: t for t in tasks}
    assert by_key["tag_untagged"].parents == ("tag_mart",)
    assert by_key["tag_mart"].parents == ("tag_staging",)


def test_group_by_folder_uses_the_parent_folder(shop: Plan) -> None:
    tasks = build_tasks(shop, _opts(task_grouping="by_folder"), "main", Report())
    assert {t.key: t.units for t in tasks} == {
        "dir_models_marts": ("dim_customers", "fct_orders", "rpt_revenue"),
        "dir_models_staging": ("stg_customers", "stg_orders", "stg_payments"),
    }
    assert tasks[0].parents == ("dir_models_staging",)


def test_task_key_map_names_the_task_of_each_unit(shop: Plan) -> None:
    mapping = task_key_map(shop, _opts(task_grouping="by_tag"), "main")
    assert mapping["fct_orders"] == "tag_mart"
    assert mapping["stg_orders"] == "tag_staging"
    assert len(mapping) == 6


def test_pipelines_select_their_units(shop: Plan) -> None:
    opts = _opts(pipelines={"main": {}, "marts": {"select": ["tag:mart"]}})
    report = Report()
    assert pipeline_units(shop, opts, "marts", report) == ("dim_customers", "fct_orders")
    assert len(pipeline_units(shop, opts, "main", report)) == 6
    assert _codes(report) == []
    tasks = build_tasks(shop, opts, "marts", report)
    # A parent outside the pipeline is not a dependency of its job.
    assert [t.parents for t in tasks] == [(), ()]


def test_compute_rules_pick_the_first_match(shop: Plan) -> None:
    opts = _opts(
        compute={"default": {"kind": "serverless"}, "big": {"kind": "serverless"}},
        compute_rules=[
            {"select": ["name:fct_orders"], "compute": "big"},
            {"select": ["tag:mart"], "compute": "default"},
        ],
    )
    report = Report()
    computes = {t.key: t.compute for t in build_tasks(shop, opts, "main", report)}
    assert computes["fct_orders"] == "big"
    assert computes["dim_customers"] == "default"
    assert _codes(report) == []


def test_a_rule_that_matches_nothing_is_a_warning(shop: Plan) -> None:
    report = Report()
    build_tasks(
        shop, _opts(compute_rules=[{"select": ["tag:ghost"], "compute": "default"}]), "main", report
    )
    assert _codes(report) == ["PLD005"]
    assert not report.has_errors


def test_a_group_on_two_computes_is_PLD004(shop: Plan) -> None:
    opts = _opts(
        task_grouping="by_tag",
        compute={"default": {"kind": "serverless"}, "big": {"kind": "serverless"}},
        compute_rules=[{"select": ["name:fct_orders"], "compute": "big"}],
    )
    report = Report()
    build_tasks(shop, opts, "main", report)
    assert _codes(report) == ["PLD004"]
    assert "tag_mart" in report.sorted()[0].message


def test_a_cycle_among_groups_is_PLD003(models_plan: Models) -> None:
    # Folder a holds a1 and a2, folder b holds b1, and a1 -> b1 -> a2: a and b wait for each other.
    plan = models_plan(
        {
            "models/a/a.yml": (
                "models:\n"
                "  - {name: a1, sql: 'select 1 as x'}\n"
                "  - {name: a2, sql: 'select 1 as x from b1', inputs: {b1: {ref: b1}}}\n"
            ),
            "models/b/b.yml": (
                "models:\n  - {name: b1, sql: 'select 1 as x from a1', inputs: {a1: {ref: a1}}}\n"
            ),
        }
    )
    report = Report()
    build_tasks(plan, _opts(task_grouping="by_folder"), "main", report)
    assert _codes(report) == ["PLD003"]
    # The message names task keys, not raw folder names.
    assert "dir_models_a -> dir_models_b -> dir_models_a" in report.sorted()[0].message
    # The same units are fine one task each.
    assert [t.key for t in build_tasks(plan, _opts(), "main", Report())] == ["a1", "a2", "b1"]


def test_unknown_pipeline_selector_and_empty_selection_are_PLD004(shop: Plan) -> None:
    for pipelines, name in (
        ({"main": {}}, "other"),
        ({"main": {"select": ["bad:selector"]}}, "main"),
        ({"main": {"select": ["tag:ghost"]}}, "main"),
    ):
        report = Report()
        assert pipeline_units(shop, _opts(pipelines=pipelines), name, report) == ()
        assert _codes(report) == ["PLD004"]


def test_the_task_limit_counts_the_finalize_task(models_plan: Models) -> None:
    lines = [f"  - {{name: m{i:04d}, sql: 'select 1 as x'}}" for i in range(1000)]
    plan = models_plan({"models/m.yml": "models:\n" + "\n".join(lines) + "\n"})
    opts = _opts(pipelines={"all": {}, "most": {"exclude": ["name:m0999"]}})
    report = Report()
    assert len(build_tasks(plan, opts, "most", report)) == 999
    assert _codes(report) == []
    build_tasks(plan, opts, "all", report)
    assert _codes(report) == ["PLD001"]
    assert keys.MAX_TASKS == 1000
