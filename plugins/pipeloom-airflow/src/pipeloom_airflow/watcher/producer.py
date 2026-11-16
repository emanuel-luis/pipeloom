"""The WATCHER producer (ADR-0048, rules 2 and 4): one `pipeloom run` of the whole selection per
DAG run. Its results belong to the watchers, so a failed pipeloom run is still a success here;
`pipeloom_finalize` fails the DAG run."""

from __future__ import annotations

import time
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from pipeloom.artifacts.layout import RunPaths
from pipeloom_airflow._compat import AirflowFailException, AirflowSkipException, Context
from pipeloom_airflow.commands import CoreCommand
from pipeloom_airflow.operators.local import EXIT_FAILED, _ChildOperator, _context, _python
from pipeloom_airflow.watcher.state import VISIBILITY_GRACE_S


class PipeloomWatcherProducerOperator(_ChildOperator):
    """Runs the selection once. A try above 1 is skipped: the core scheduler already applied the
    model retries in process, and a watcher reruns its own unit when cleared."""

    visibility_grace_s: float = VISIBILITY_GRACE_S
    """Seconds `run_results.json` may take to show under `run_root`; tests lower it."""

    def __init__(
        self,
        *,
        command: CoreCommand,
        venv_dir: str | None,
        env: Mapping[str, str],
        select: Sequence[str],
        exclude: Sequence[str],
        workers: int | None,
        run_root: str,
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
        self.select = tuple(select)
        self.exclude = tuple(exclude)
        self.workers = workers
        self.run_root = run_root

    def execute(self, context: Context) -> None:
        if context["ti"].try_number > 1:
            raise AirflowSkipException(
                "the producer runs once per DAG run; clear a watcher to rerun its unit"
            )
        ctx = _context(context, self.group_id, self.task_id)
        argv = self.command.producer_argv(
            _python(self.venv_dir),
            ctx,
            select=self.select,
            exclude=self.exclude,
            workers=self.workers,
        )
        code, terminal = self._run(argv)
        if code not in (0, EXIT_FAILED):
            detail = ""
            if terminal is not None and (terminal.code or terminal.message):
                detail = f": {terminal.code or ''} {terminal.message or ''}".rstrip()
            raise AirflowFailException(f"pipeloom run {ctx.run_id} exited with {code}{detail}")
        results = RunPaths(Path(self.run_root), ctx.run_id).run_results
        deadline = time.monotonic() + self.visibility_grace_s
        while not results.is_file():
            if time.monotonic() >= deadline:
                raise AirflowFailException(
                    f"PLA008 pipeloom run {ctx.run_id} exited with {code}, but {results} is not "
                    "visible; set execution.watcher_artifacts_root to the root the run writes to"
                )
            time.sleep(1.0)
        self.log.info(
            "pipeloom run %s exited with %s; the watchers report the units", ctx.run_id, code
        )
