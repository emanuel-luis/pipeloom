"""KUBERNETES mode: each unit, and the finalize step, is one pod built by `KubernetesPodOperator`.
Importing this needs the extra `kubernetes` (the cncf-kubernetes provider)."""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any, Final

from pipeloom_airflow._compat import AirflowFailException, Context, get_task_states
from pipeloom_airflow._compat_k8s import KubernetesPodOperator, pod_name
from pipeloom_airflow.commands import CoreCommand
from pipeloom_airflow.operators.local import _context, finalize_select
from pipeloom_airflow.states import unit_states, unit_states_document

MAX_UNIT_STATES_BYTES: Final = 120 * 1024
"""The inline unit states are one exec argument, and Linux caps a single argument at 128 KiB
(MAX_ARG_STRLEN), so the cap sits below that."""


def _pod_kwargs(kpo_kwargs: dict[str, Any]) -> dict[str, Any]:
    """What pipeloom decides for every pod. The image's entrypoint is replaced by the interpreter
    (`cmds`); the output is text, nothing goes to XCom and the pod is never deferrable."""
    kpo_kwargs["do_xcom_push"] = False
    kpo_kwargs["deferrable"] = False
    return kpo_kwargs


class PipeloomKubernetesOperator(KubernetesPodOperator):
    """Runs one unit: `python -m pipeloom run --select unit:<id>` in a pod. The arguments are set
    in `execute`, after templating, so the run id never sits in a templated field. `retry_on` is
    not enforced: the pod's exit code is all Airflow sees, so every failure retries."""

    def __init__(
        self,
        *,
        unit_id: str,
        command: CoreCommand,
        python: str,
        group_id: str | None,
        **kpo_kwargs: Any,
    ) -> None:
        super().__init__(**_pod_kwargs(kpo_kwargs))
        self.name = pod_name(self.task_id)
        self.unit_id = unit_id
        self.command = command
        self.python = python
        self.group_id = group_id

    def execute(self, context: Context) -> Any:
        ctx = _context(context, self.group_id, self.task_id)
        argv = self.command.run_argv([self.python], self.unit_id, ctx, log_format="text")
        self.cmds, self.arguments = argv[:1], argv[1:]
        return super().execute(context)


class PipeloomKubernetesFinalizeOperator(KubernetesPodOperator):
    """Runs `pipeloom finalize` in a pod with the same image and `pod_kwargs`. The unit states go
    inline (`--unit-states-json`), in the compact form: a pod has no file shared with the worker."""

    def __init__(
        self,
        *,
        command: CoreCommand,
        python: str,
        task_units: Mapping[str, tuple[str, ...]],
        select: tuple[str, ...],
        exclude: tuple[str, ...],
        group_id: str | None,
        **kpo_kwargs: Any,
    ) -> None:
        kpo_kwargs.setdefault("trigger_rule", "all_done")
        kpo_kwargs.setdefault("retries", 1)
        super().__init__(**_pod_kwargs(kpo_kwargs))
        self.name = pod_name(self.task_id)
        self.command = command
        self.python = python
        self.task_units = {k: tuple(v) for k, v in task_units.items()}
        self.select = select
        self.exclude = exclude
        self.group_id = group_id

    def execute(self, context: Context) -> Any:
        ctx = _context(context, self.group_id, self.task_id)
        ti = context["ti"]
        states = get_task_states(ti, ti.dag_id, sorted(self.task_units), ti.run_id)
        document = unit_states_document(unit_states(states, self.task_units), compact=True)
        text = json.dumps(document, ensure_ascii=True)
        if len(text.encode("ascii")) > MAX_UNIT_STATES_BYTES:
            raise AirflowFailException(
                f"PLA002 the unit states of this run are {len(text)} bytes, above "
                f"{MAX_UNIT_STATES_BYTES}; split the project with render_config.select"
            )
        select = finalize_select(self.select, self.exclude, self.task_units)
        argv = self.command.finalize_argv(
            [self.python], ctx, select=select, exclude=self.exclude, unit_states_json=text
        )
        self.cmds, self.arguments = argv[:1], argv[1:]
        return super().execute(context)
