from __future__ import annotations

from typing import Any

from pipeloom.diagnostics import Report
from pipeloom_airflow.config import (
    ExecutionConfig,
    ExecutionMode,
    KubernetesConfig,
    LoadMode,
    ProjectConfig,
    RenderConfig,
    validate_configs,
)
from pipeloom_airflow.errors import PipeloomAirflowError
from pipeloom_airflow.version import check_airflow_version


def check(
    project: ProjectConfig | None = None,
    render: RenderConfig | None = None,
    execution: ExecutionConfig | None = None,
    args: dict[str, Any] | None = None,
) -> list[tuple[str, str]]:
    report = Report()
    validate_configs(
        project or ProjectConfig("/p", manifest_path="/p/m.json"),
        render or RenderConfig(),
        execution or ExecutionConfig(),
        args or {},
        report,
    )
    return [(d.code, d.message) for d in report.sorted()]


def test_a_good_configuration_is_clean() -> None:
    assert check() == []
    assert check(render=RenderConfig(load_method=LoadMode.PROJECT)) == []


def test_databricks_timeout_and_poll_interval_must_be_positive() -> None:
    bad = ExecutionConfig(
        mode=ExecutionMode.DATABRICKS, databricks_timeout=0, databricks_poll_interval=-1
    )
    assert check(execution=bad) == [
        ("PLA002", "execution.databricks_poll_interval must be a positive number of seconds"),
        ("PLA002", "execution.databricks_timeout must be a positive number of seconds"),
    ]
    assert ExecutionConfig().databricks_timeout == 86400.0


def test_docker_is_pla001() -> None:
    assert [c for c, _ in check(execution=ExecutionConfig(mode=ExecutionMode.DOCKER))] == ["PLA001"]


def test_manifest_needs_a_path() -> None:
    found = check(project=ProjectConfig("/p"))
    assert found == [("PLA002", "load_method 'manifest' needs project.manifest_path")]


def test_relative_paths_are_pla002_on_either_platform() -> None:
    found = check(project=ProjectConfig("/p", manifest_path="m.json", cache_dir="c"))
    assert [m for _, m in found] == [
        "project.cache_dir must be an absolute path, got 'c'",
        "project.manifest_path must be an absolute path, got 'm.json'",
    ]
    ok = ProjectConfig("C:\\proj", manifest_path="\\\\host\\share\\m.json")
    assert check(project=ok) == []


def test_virtualenv_needs_a_directory() -> None:
    found = check(execution=ExecutionConfig(mode=ExecutionMode.VIRTUALENV))
    assert [c for c, _ in found] == ["PLA002"]
    assert (
        check(execution=ExecutionConfig(mode=ExecutionMode.VIRTUALENV, virtualenv_dir="/v")) == []
    )


def test_kubernetes_needs_an_image_and_owns_some_pod_keys() -> None:
    k8s = ExecutionConfig(mode=ExecutionMode.KUBERNETES)
    assert [c for c, _ in check(execution=k8s)] == ["PLA002"]
    empty = ExecutionConfig(mode=ExecutionMode.KUBERNETES, kubernetes=KubernetesConfig(""))
    assert [c for c, _ in check(execution=empty)] == ["PLA002"]
    owned = {k: 1 for k in ("cmds", "arguments", "image", "name", "task_id", "deferrable")}
    owned["do_xcom_push"] = 1
    bad = ExecutionConfig(
        mode=ExecutionMode.KUBERNETES, kubernetes=KubernetesConfig("i", pod_kwargs=owned)
    )
    assert len(check(execution=bad)) == 7
    fine = ExecutionConfig(
        mode=ExecutionMode.KUBERNETES,
        kubernetes=KubernetesConfig("i", pod_kwargs={"volumes": []}),
    )
    assert check(execution=fine) == []


def test_operator_args_own_keys() -> None:
    keys = [
        "task_id",
        "retries",
        "retry_delay",
        "trigger_rule",
        "outlets",
        "execution_timeout",
        "deferrable",
    ]
    assert len(check(args=dict.fromkeys(keys, 1))) == 7
    assert check(args={"pool": "p", "queue": "q"}) == []


def test_asset_uri_placeholders() -> None:
    assert check(render=RenderConfig(asset_uri="x://{project}/{model}/{unique_id}")) == []
    found = check(render=RenderConfig(asset_uri="x://{schema}"))
    assert found == [
        ("PLA002", "asset_uri has an unknown placeholder '{schema}'"),
    ]
    assert [c for c, _ in check(render=RenderConfig(asset_uri="x://{"))] == ["PLA002"]


def test_the_error_formats_its_diagnostics() -> None:
    report = Report()
    report.add("PLA002", "boom")
    err = PipeloomAirflowError(report.sorted())
    assert "PLA002" in str(err) and "boom" in str(err)
    assert err.diagnostics == report.sorted()


def test_version_table() -> None:
    table = {
        "3.2.0": True,
        "3.3.2": True,
        "3.4.0rc2": True,
        "3.1.9": False,
        "3.0.6": False,
        "2.11.0": False,
        "4.0.0": False,
        "junk": False,
    }
    for version, ok in table.items():
        report = Report()
        assert check_airflow_version(version, report) is ok
        assert [d.code for d in report.sorted()] == ([] if ok else ["PLA005"])
