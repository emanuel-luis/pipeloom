"""LOCAL and VIRTUALENV modes: each task runs `pipeloom` in a child process on the worker."""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import tempfile
from collections.abc import Mapping
from typing import Any, Literal

from pipeloom_airflow._compat import (
    AirflowException,
    AirflowFailException,
    BaseOperator,
    Context,
    get_task_states,
)
from pipeloom_airflow.commands import CoreCommand, RunContext, run_context, venv_python
from pipeloom_airflow.errors import PipeloomAirflowError
from pipeloom_airflow.states import (
    ChildTerminal,
    parse_child_line,
    unit_states,
    unit_states_document,
)

EXIT_FAILED = 1
EXIT_USAGE = 2
EXIT_CANCELLED = 130


def _python(venv_dir: str | None) -> list[str]:
    """`sys.executable` (LOCAL), or the virtualenv's interpreter (VIRTUALENV)."""
    if venv_dir is None:
        return [sys.executable]
    python = venv_python(venv_dir, platform=sys.platform)
    if not os.path.isfile(python):
        raise AirflowFailException(
            f"PLA002 execution.virtualenv_dir has no interpreter at '{python}'; "
            "create the virtualenv on every worker with pipeloom and its plugins installed"
        )
    return [python]


def _context(context: Context, group_id: str | None, task_id: str) -> RunContext:
    ti = context["ti"]
    params = context.get("params") or {}
    try:
        return run_context(
            dag_id=ti.dag_id,
            airflow_run_id=ti.run_id,
            run_after=context["dag_run"].run_after,
            task_id=task_id,
            try_number=ti.try_number,
            group_id=group_id,
            full_refresh=params.get("full_refresh", False),
        )
    except PipeloomAirflowError as err:
        raise AirflowFailException(str(err)) from err


def finalize_select(
    select: tuple[str, ...], exclude: tuple[str, ...], task_units: Mapping[str, tuple[str, ...]]
) -> tuple[str, ...]:
    """The `--select` of a finalize call. A stable manifest has an empty `selected`, and the core
    keeps it when no selector is given, so finalize would see no unit in scope and drop every
    external state. Without a render selection, name the rendered units instead."""
    if select or exclude:
        return select
    return tuple(sorted(f"unit:{u}" for us in task_units.values() for u in us))


class _ChildOperator(BaseOperator):
    """Runs one `pipeloom` child in its own process group and logs its output line by line."""

    _proc: subprocess.Popen[str] | None = None

    def __init__(
        self,
        *,
        command: CoreCommand,
        venv_dir: str | None,
        env: Mapping[str, str],
        cancel_grace_s: float,
        group_id: str | None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.command = command
        self.venv_dir = venv_dir
        self.env = dict(env)
        self.cancel_grace_s = cancel_grace_s
        self.group_id = group_id

    def _run(self, argv: list[str]) -> tuple[int, ChildTerminal | None]:
        env = os.environ | {"PYTHONIOENCODING": "utf-8"} | self.env
        terminal: ChildTerminal | None = None
        self.log.info("running %s", " ".join(argv))
        self._proc = subprocess.Popen(
            argv,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            start_new_session=True,
        )
        assert self._proc.stdout is not None
        for line in self._proc.stdout:
            event, text = parse_child_line(line)
            terminal = event or terminal
            if text:
                self.log.info("%s", text)
        return self._proc.wait(), terminal

    def on_kill(self) -> None:
        """SIGTERM to the child's process group, SIGKILL after `cancel_grace_s`."""
        proc = self._proc
        if proc is None or proc.poll() is not None:
            return
        if sys.platform == "win32":  # pragma: no cover - Airflow does not run on Windows
            proc.kill()
            return
        try:
            os.killpg(proc.pid, signal.SIGTERM)
            proc.wait(self.cancel_grace_s)
        except ProcessLookupError:
            return
        except subprocess.TimeoutExpired:
            self.log.warning("pipeloom did not stop in %ss; killing it", self.cancel_grace_s)
            try:
                os.killpg(proc.pid, signal.SIGKILL)
            except ProcessLookupError:
                return
            proc.wait()


class PipeloomRunOperator(_ChildOperator):
    """Runs one unit: `pipeloom run --select unit:<id>` with the DAG run's identity."""

    def __init__(
        self,
        *,
        unit_id: str,
        command: CoreCommand,
        venv_dir: str | None,
        env: Mapping[str, str],
        retry_on: Literal["transient", "any"],
        cancel_grace_s: float,
        group_id: str | None,
        **kwargs: Any,
    ) -> None:
        super().__init__(
            command=command,
            venv_dir=venv_dir,
            env=env,
            cancel_grace_s=cancel_grace_s,
            group_id=group_id,
            **kwargs,
        )
        self.unit_id = unit_id
        self.retry_on = retry_on

    def execute(self, context: Context) -> None:
        ctx = _context(context, self.group_id, self.task_id)
        argv = self.command.run_argv(_python(self.venv_dir), self.unit_id, ctx, log_format="json")
        code, terminal = self._run(argv)
        if code == 0:
            return
        detail = ""
        if terminal is not None and (terminal.code or terminal.message):
            detail = f": {terminal.code or ''} {terminal.message or ''}".rstrip()
        message = f"unit {self.unit_id} exited with {code}{detail}"
        if code == EXIT_USAGE:
            raise AirflowFailException(message)
        if code == EXIT_CANCELLED:
            raise AirflowException(message)
        permanent = terminal is not None and terminal.category == "permanent"
        if code == EXIT_FAILED and permanent and self.retry_on == "transient":
            raise AirflowFailException(message)
        raise AirflowException(message)


class PipeloomFinalizeOperator(_ChildOperator):
    """Runs `pipeloom finalize` with the unit states Airflow knows about. Fails when the run
    failed, so the DAG run fails too."""

    def __init__(
        self,
        *,
        command: CoreCommand,
        venv_dir: str | None,
        env: Mapping[str, str],
        task_units: Mapping[str, tuple[str, ...]],
        select: tuple[str, ...],
        exclude: tuple[str, ...],
        group_id: str | None,
        cancel_grace_s: float = 30.0,
        **kwargs: Any,
    ) -> None:
        kwargs.setdefault("trigger_rule", "all_done")
        kwargs.setdefault("retries", 1)
        super().__init__(
            command=command,
            venv_dir=venv_dir,
            env=env,
            cancel_grace_s=cancel_grace_s,
            group_id=group_id,
            **kwargs,
        )
        self.task_units = {k: tuple(v) for k, v in task_units.items()}
        self.select = select
        self.exclude = exclude

    def execute(self, context: Context) -> None:
        ctx = _context(context, self.group_id, self.task_id)
        ti = context["ti"]
        states = get_task_states(ti, ti.dag_id, sorted(self.task_units), ti.run_id)
        document = unit_states_document(unit_states(states, self.task_units), compact=False)
        fd, path = tempfile.mkstemp(prefix="pipeloom-unit-states-", suffix=".json")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(document, f, ensure_ascii=True)
            select = finalize_select(self.select, self.exclude, self.task_units)
            argv = self.command.finalize_argv(
                _python(self.venv_dir),
                ctx,
                select=select,
                exclude=self.exclude,
                unit_states_path=path,
            )
            code, _ = self._run(argv)
        finally:
            os.unlink(path)
        if code == EXIT_FAILED:
            raise AirflowFailException(f"pipeloom run {ctx.run_id} failed")
        if code != 0:
            raise AirflowException(f"pipeloom finalize exited with {code}")
