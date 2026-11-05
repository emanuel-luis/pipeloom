"""KUBERNETES mode: the pod operators' spec and argv. `KubernetesPodOperator.execute` is replaced,
so no pod is ever created and no cluster is contacted."""

from __future__ import annotations

import importlib.util
import json
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

import pytest
from kubernetes.client import models as k8s

from pipeloom_airflow import _compat, naming
from pipeloom_airflow import dag as dag_module
from pipeloom_airflow._compat_k8s import KubernetesPodOperator, merge_env_vars, pod_name
from pipeloom_airflow.config import (
    ExecutionConfig,
    ExecutionMode,
    KubernetesConfig,
    ProjectConfig,
)
from pipeloom_airflow.dag import PipeloomDag, PipeloomTaskGroup
from pipeloom_airflow.errors import PipeloomAirflowError
from pipeloom_airflow.operators import kubernetes as k8s_ops
from pipeloom_airflow.testing.context import FakeTaskInstance, make_context

START = datetime(2026, 1, 1)
AFTER = datetime(2026, 11, 5, 12, 0, tzinfo=UTC)
VOLUME = k8s.V1Volume(
    name="artifacts",
    persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claim_name="pipeloom-rwx"),
)
MOUNT = k8s.V1VolumeMount(name="artifacts", mount_path="/work/project/target")


def execution(**pod_kwargs: Any) -> ExecutionConfig:
    return ExecutionConfig(
        mode=ExecutionMode.KUBERNETES,
        project_dir="/work/project",
        manifest_path="/work/manifest.json",
        env={"PIPELOOM_ENV": "test"},
        kubernetes=KubernetesConfig(
            image="registry.example/pipeloom:0.3.0",
            namespace="data",
            python="/opt/venv/bin/python",
            pod_kwargs={"volumes": [VOLUME], "volume_mounts": [MOUNT], **pod_kwargs},
        ),
    )


def make_dag(af_project: Any, **kw: Any) -> PipeloomDag:
    project, manifest, _ = af_project
    kw.setdefault("execution_config", execution())
    return PipeloomDag(
        dag_id="d",
        schedule=None,
        start_date=START,
        project_config=ProjectConfig(project.as_posix(), manifest_path=manifest.as_posix()),
        target="dev",
        **kw,
    )


Captured = list[dict[str, Any]]


@pytest.fixture
def pods(monkeypatch: pytest.MonkeyPatch) -> Captured:
    """Replace the provider's execute: record what the pod would run with, create nothing."""
    seen: Captured = []

    def fake(self: Any, context: Any) -> str:
        seen.append(
            {
                "task_id": self.task_id,
                "cmds": self.cmds,
                "arguments": self.arguments,
                "image": self.image,
                "volumes": self.volumes,
                "volume_mounts": self.volume_mounts,
            }
        )
        return "ignored"

    monkeypatch.setattr(KubernetesPodOperator, "execute", fake)
    return seen


def test_pods_get_the_image_the_pod_kwargs_and_the_env(af_project: Any, af_plan: Any) -> None:
    dag = make_dag(af_project)
    for t in af_plan[0].tasks:
        op = dag.get_task(t.task_id)
        assert type(op).__name__ == "PipeloomKubernetesOperator"
        assert (op.image, op.namespace) == ("registry.example/pipeloom:0.3.0", "data")
        assert op.volumes == [VOLUME] and op.volume_mounts == [MOUNT]
        assert op.env_vars == {"PIPELOOM_ENV": "test"}
        assert op.do_xcom_push is False and op.deferrable is False
        assert op.name == pod_name(t.task_id) and op.name.startswith("pipeloom-")
        assert op.retries == t.retries
        assert op.termination_grace_period == 30
        assert not op.cmds and not op.arguments  # set in execute, after templating
    finalize = dag.get_task(naming.FINALIZE_ID)
    assert type(finalize).__name__ == "PipeloomKubernetesFinalizeOperator"
    assert (finalize.image, finalize.volumes) == ("registry.example/pipeloom:0.3.0", [VOLUME])
    assert finalize.trigger_rule == "all_done" and finalize.retries == 1
    assert finalize.deferrable is False and finalize.do_xcom_push is False
    assert finalize.upstream_task_ids == set(af_plan[0].leaves)


def test_unit_pods_run_the_core_with_the_dag_run_identity(
    af_project: Any, af_plan: Any, pods: Captured
) -> None:
    dag = make_dag(af_project)
    plan, command = af_plan
    unit = plan.tasks[0]
    op = dag.get_task(unit.task_id)
    ti = FakeTaskInstance(dag_id="d", run_id="manual__1", task_id=unit.task_id)
    assert op.execute(make_context(ti, run_after=AFTER, params={})) == "ignored"
    (pod,) = pods
    rid = naming.airflow_run_id("d", "manual__1")
    argv = pod["cmds"] + pod["arguments"]
    assert pod["cmds"] == ["/opt/venv/bin/python"]
    assert argv[1:5] == ["-m", "pipeloom", "run", "--log-format"] and argv[5] == "text"
    assert argv[argv.index("--project-dir") + 1] == "/work/project"
    assert argv[argv.index("--manifest") + 1] == "/work/manifest.json"
    assert argv[argv.index("--expect-plan-hash") + 1] == plan.plan_hash
    assert argv[argv.index("--select") + 1] == f"unit:{unit.unit_id}"
    assert argv[argv.index("--run-id") + 1] == rid
    assert argv[argv.index("--full-refresh") + 1] == "false"
    assert "airflow_dag_id=d" in argv and "airflow_try_number=1" in argv
    assert pod["image"] == "registry.example/pipeloom:0.3.0" and pod["volumes"] == [VOLUME]
    assert argv[0] == "/opt/venv/bin/python" and command.target == "dev"


def test_full_refresh_and_a_bad_value(af_project: Any, af_plan: Any, pods: Captured) -> None:
    dag = make_dag(af_project)
    op = dag.get_task(af_plan[0].tasks[0].task_id)
    ti = FakeTaskInstance(dag_id="d", run_id="r", task_id=op.task_id)
    op.execute(make_context(ti, run_after=AFTER, params={"full_refresh": True}))
    assert pods[0]["arguments"][pods[0]["arguments"].index("--full-refresh") + 1] == "true"
    with pytest.raises(_compat.AirflowFailException, match="PLA002"):
        op.execute(make_context(ti, run_after=AFTER, params={"full_refresh": "maybe"}))


def test_the_finalize_pod_gets_compact_unit_states_inline(
    af_project: Any, af_plan: Any, pods: Captured
) -> None:
    dag = make_dag(af_project)
    plan = af_plan[0]
    first, second = plan.tasks[0].task_id, plan.tasks[1].task_id
    finalize = dag.get_task(naming.FINALIZE_ID)
    ti = FakeTaskInstance(
        dag_id="d",
        run_id="manual__1",
        task_id=finalize.task_id,
        states={first: "failed", second: "upstream_failed"},
    )
    finalize.execute(make_context(ti, run_after=AFTER, params={}))
    (pod,) = pods
    args = pod["arguments"]
    assert pod["cmds"] == ["/opt/venv/bin/python"] and args[:3] == ["-m", "pipeloom", "finalize"]
    doc = json.loads(args[args.index("--unit-states-json") + 1])
    assert doc["schema"] == "pipeloom.unit_states/1"
    failed = doc["units"][plan.tasks[0].unit_id]
    assert failed["status"] == "failed" and failed["code"] == "PLA004"
    assert "orchestrator_ids" not in failed
    assert doc["units"][plan.tasks[1].unit_id]["reason"] == "upstream_failed"
    assert "--unit-states" not in args
    assert args[args.index("--run-id") + 1] == naming.airflow_run_id("d", "manual__1")
    selected = [args[i + 1] for i, a in enumerate(args) if a == "--select"]
    assert selected == sorted(f"unit:{u}" for t in plan.tasks for u in t.units)


def test_oversized_unit_states_fail_with_pla002(
    af_project: Any, af_plan: Any, pods: Captured, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(k8s_ops, "MAX_UNIT_STATES_BYTES", 20)
    dag = make_dag(af_project)
    task = af_plan[0].tasks[0].task_id
    finalize = dag.get_task(naming.FINALIZE_ID)
    ti = FakeTaskInstance(dag_id="d", run_id="r", task_id=finalize.task_id, states={task: "failed"})
    with pytest.raises(_compat.AirflowFailException, match=r"PLA002.*render_config\.select"):
        finalize.execute(make_context(ti, run_after=AFTER, params={}))
    assert pods == []


def test_a_task_group_prefixes_ids_pod_names_and_the_run_id(
    af_project: Any, af_plan: Any, pods: Captured
) -> None:
    project, manifest, _ = af_project
    with _compat.DAG(dag_id="outer", schedule=None, start_date=START) as dag:
        PipeloomTaskGroup(
            "Shop",
            project_config=ProjectConfig(project.as_posix(), manifest_path=manifest.as_posix()),
            target="dev",
            execution_config=execution(),
        )
    unit = af_plan[0].tasks[0]
    op = dag.get_task(f"Shop.{unit.task_id}")
    assert op.name == pod_name(op.task_id) and set(op.name) <= set(
        "abcdefghijklmnopqrstuvwxyz0123456789-"
    )
    ti = FakeTaskInstance(dag_id="outer", run_id="r1", task_id=op.task_id)
    op.execute(make_context(ti, run_after=AFTER, params={}))
    args = pods[0]["arguments"]
    assert args[args.index("--run-id") + 1] == naming.airflow_run_id("outer", "r1", "Shop")


def test_pod_names_are_dns_labels_under_the_provider_limit() -> None:
    assert pod_name("a.b_C") == "pipeloom-a-b-c"
    long = pod_name("x" * 300)
    assert len(long) == 54 and not long.endswith("-")


def test_env_merges_with_dict_and_list_pod_env(af_project: Any) -> None:
    assert merge_env_vars({}, None) is None
    assert merge_env_vars({"A": "1"}, {"A": "2", "B": "3"}) == {"A": "2", "B": "3"}
    listed = merge_env_vars({"A": "1", "B": "2"}, [k8s.V1EnvVar(name="A", value="9")])
    assert [(v.name, v.value) for v in listed] == [("B", "2"), ("A", "9")]
    dag = make_dag(af_project, execution_config=execution(env_vars={"X": "y"}))
    op = dag.get_task(dag.task_ids[0])
    assert op.env_vars == {"PIPELOOM_ENV": "test", "X": "y"}


@pytest.mark.parametrize(
    ("kw", "needle"),
    [
        ({"execution_config": execution(deferrable=True)}, "pod_kwargs.deferrable"),
        ({"execution_config": execution(do_xcom_push=True)}, "pod_kwargs.do_xcom_push"),
        ({"execution_config": execution(arguments=["x"])}, "pod_kwargs.arguments"),
        *[
            ({"execution_config": execution(**{key: 1})}, f"pod_kwargs.{key}")
            for key in (
                "retries",
                "retry_delay",
                "execution_timeout",
                "outlets",
                "task_group",
                "trigger_rule",
                "task_id",
                "name",
                "image",
                "cmds",
            )
        ],
        ({"operator_args": {"deferrable": True}}, "operator_args.deferrable"),
        ({"operator_args": {"retries": 2}}, "operator_args.retries"),
        (
            {"execution_config": execution(pool="p"), "operator_args": {"pool": "q"}},
            "'pool' is in both pod_kwargs and operator_args",
        ),
        (
            {"execution_config": ExecutionConfig(mode=ExecutionMode.KUBERNETES)},
            "execution.kubernetes.image",
        ),
    ],
)
def test_forbidden_pod_settings_are_pla002(
    af_project: Any, kw: dict[str, Any], needle: str
) -> None:
    with pytest.raises(PipeloomAirflowError) as info:
        make_dag(af_project, **kw)
    assert {d.code for d in info.value.diagnostics} == {"PLA002"}
    assert needle in str(info.value)


def test_a_missing_provider_is_pla001(af_project: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    real: Callable[..., Any] = importlib.util.find_spec

    def find_spec(name: str, *a: Any) -> Any:
        return None if name == "airflow.providers.cncf.kubernetes" else real(name, *a)

    monkeypatch.setattr(dag_module.importlib.util, "find_spec", find_spec)
    with pytest.raises(PipeloomAirflowError) as info:
        make_dag(af_project)
    assert [d.code for d in info.value.diagnostics] == ["PLA001"]
    assert "pipeloom-airflow[kubernetes]" in str(info.value)
