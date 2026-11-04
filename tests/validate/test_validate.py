from __future__ import annotations

from pathlib import Path

from pipeloom.api import Project, ProjectOptions
from tests.conftest import FIXTURES, make_project

BASE = (
    "config_version: 1\nname: shop\npython_paths: [src]\n"
    "targets:\n  dev:\n    engine: {kind: memory}\n"
)
A = "  - {name: a, sql: select 1}\n"


def codes(
    tmp_path: Path, models: str, extra: dict[str, str] | None = None, base: str = BASE
) -> set[str]:
    make_project(tmp_path, {"pipeloom.yml": base, "models/m.yml": models, **(extra or {})})
    out = Project(ProjectOptions(project_dir=tmp_path, env={})).validate()
    return {d.code for d in out.diagnostics}


def test_shop_is_clean() -> None:
    out = Project(ProjectOptions(project_dir=FIXTURES / "shop", target="ci", env={})).validate()
    assert out.value is not None and out.diagnostics == ()


def test_l2_failure_stops_before_l4(tmp_path: Path) -> None:
    models = (
        "models:\n  - {name: a, sql: select 1, inputs: {x: {ref: nope}}, "
        "write: {strategy: merge}}\n"
    )
    assert codes(tmp_path, models) == {"PLR201"}


def test_l4_to_l6_run_together(tmp_path: Path) -> None:
    models = (
        "models:\n"
        "  - {name: a, sql: select 1, write: {strategy: replace_partitions}}\n"
        "  - {name: b, python: 'm:b'}\n"
    )
    assert codes(tmp_path, models, {"src/m.py": "def b(): ...\n"}) == {
        "PLP407",
        "PLK602",
    }


def test_kwargs_accepts_any_alias(tmp_path: Path) -> None:
    models = "models:\n" + A + "  - {name: b, python: 'm:b', inputs: {a: {ref: a}}}\n"
    assert codes(tmp_path, models, {"src/m.py": "def b(ctx, **frames): ...\n"}) == set()
    assert codes(tmp_path, models, {"src/m.py": "def b(ctx): ...\n"}) == {"PLK602"}


def test_package_module_is_found(tmp_path: Path) -> None:
    models = "models:\n  - {name: b, python: 'pkg.m:b'}\n"
    assert codes(tmp_path, models, {"src/pkg/m/__init__.py": "def b(ctx): ...\n"}) == set()


def test_syntax_error_is_plk601(tmp_path: Path) -> None:
    models = "models:\n  - {name: b, python: 'm:b'}\n"
    assert codes(tmp_path, models, {"src/m.py": "def b(:\n"}) == {"PLK601"}


def test_async_def_is_accepted(tmp_path: Path) -> None:
    models = "models:\n" + A + "  - {name: b, python: 'm:b', inputs: {a: {ref: a}}}\n"
    assert codes(tmp_path, models, {"src/m.py": "async def b(ctx, a): ...\n"}) == set()


def test_missing_function_is_plk601(tmp_path: Path) -> None:
    models = "models:\n  - {name: b, python: 'm:b'}\n"
    assert codes(tmp_path, models, {"src/m.py": "def other(ctx): ...\n"}) == {"PLK601"}


def test_defaults_do_not_trip_plp405_on_matching_strategy(tmp_path: Path) -> None:
    models = "models:\n  - {name: a, sql: select 1, write: {strategy: overwrite}}\n"
    assert codes(tmp_path, models) == set()


EPH = (
    "models:\n"
    "  - {name: e, sql: select 1, materialized: ephemeral%s}\n"
    "  - {name: a, sql: select * from e, inputs: {e: {ref: e}}}\n"
)


def with_defaults(models: str) -> str:
    return BASE.replace("targets:", f"defaults:\n  models: {models}\ntargets:")


def test_inherited_options_do_not_trigger_plp405(tmp_path: Path) -> None:
    for defaults in (
        "{write: {on_schema_change: append_new_columns}}",
        "{write: {unique_key: [id]}}",
        "{partition_by: [d], cluster_by: [d]}",
    ):
        assert codes(tmp_path, EPH % "", base=with_defaults(defaults)) == set(), defaults


def test_declared_options_on_ephemeral_trigger_plp405(tmp_path: Path) -> None:
    for extra in (
        ", write: {on_schema_change: ignore}",
        ", partition_by: [d]",
        ", cluster_by: [d]",
    ):
        assert codes(tmp_path, EPH % extra) == {"PLP405"}, extra


def test_view_with_write_or_partitioning_is_plp405(tmp_path: Path) -> None:
    models = "models:\n  - {name: v, sql: select 1, materialized: view, %s}\n"
    for extra in ("write: {strategy: merge, unique_key: [id]}", "partition_by: [d]"):
        assert "PLP405" in codes(tmp_path, models % extra), extra


def test_inherited_unique_key_on_an_overwrite_model_is_fine(tmp_path: Path) -> None:
    models = "models:\n  - {name: a, sql: select 1}\n"
    assert codes(tmp_path, models, base=with_defaults("{write: {unique_key: [id]}}")) == set()
    declared = "models:\n  - {name: a, sql: select 1, write: {unique_key: [id]}}\n"
    assert codes(tmp_path, declared) == {"PLP405"}
