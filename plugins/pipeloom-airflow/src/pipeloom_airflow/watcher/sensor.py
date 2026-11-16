"""One watcher per unit (ADR-0048, rule 6): it reports what the producer's run wrote for its unit,
and reruns the unit itself when cleared after the producer is done with it."""

from __future__ import annotations

import json
import time
from collections import deque
from datetime import timedelta
from pathlib import Path
from typing import Any

from pipeloom.artifacts.layout import RunPaths
from pipeloom.runtime.retry import UnitPolicy
from pipeloom_airflow._compat import (
    AirflowException,
    AirflowFailException,
    Context,
    get_task_states,
)
from pipeloom_airflow.operators.local import PipeloomRunOperator, _context
from pipeloom_airflow.watcher.state import (
    RECHECK_S,
    TERMINAL_TASK_STATES,
    VISIBILITY_GRACE_S,
    AttemptView,
    Decision,
    UnitView,
    decide,
    in_progress,
    read_unit,
)
from pipeloom_airflow.watcher.triggers import PipeloomWatcherTrigger

LOG_TAIL_LINES = 200


class PipeloomWatcherSensor(PipeloomRunOperator):
    """Succeeds or fails as the producer's run did for unit `unit_id`, deferring on
    `PipeloomWatcherTrigger` while it is open. At try 2 or later, once the producer
    `producer_task_id` is not running the unit, it runs the unit as `PipeloomRunOperator` does.
    `watch_timeout_s` bounds one try."""

    def __init__(
        self,
        *,
        producer_task_id: str,
        run_root: str,
        poll_interval: float,
        watch_timeout_s: float,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.producer_task_id = producer_task_id
        self.run_root = run_root
        self.poll_interval = poll_interval
        self.watch_timeout_s = watch_timeout_s

    def _policy(self) -> UnitPolicy:
        return UnitPolicy(int(self.retries or 0), 0.0, self.retry_on, None)

    def _read(self, context: Context) -> tuple[RunPaths, UnitView, bool]:
        """The run's paths, the unit's files and whether the producer is still active."""
        paths = RunPaths(Path(self.run_root), _context(context, self.group_id, self.task_id).run_id)
        view = read_unit(paths.run_dir, self.unit_id, probe=int(self.retries or 0) + 1)
        ti = context["ti"]
        states = get_task_states(ti, ti.dag_id, [self.producer_task_id], ti.run_id)
        return paths, view, states.get(self.producer_task_id) not in TERMINAL_TASK_STATES

    def execute(self, context: Context) -> None:
        paths, view, active = self._read(context)
        try_number = context["ti"].try_number
        if try_number > 1 and not in_progress(view, self._policy(), producer_active=active):
            self.log.info(
                "try %s: the producer is done with unit %s; running it", try_number, self.unit_id
            )
            super().execute(context)
            return
        self._settle(context, paths, view, active, time.time() + self.watch_timeout_s, False)

    def execute_complete(
        self, context: Context, event: dict[str, Any], deadline: float, grace: bool = False
    ) -> None:
        if event.get("status") == "invisible":
            raise AirflowException(f"{event.get('code')} {event.get('message')}")
        paths, view, active = self._read(context)
        self._settle(context, paths, view, active, deadline, grace)

    def _settle(
        self,
        context: Context,
        paths: RunPaths,
        view: UnitView,
        active: bool,
        deadline: float,
        grace: bool,
    ) -> None:
        """Act on what the files say; defer while they say nothing final. `grace` is set when the
        producer had already ended without results at the previous check."""
        d = decide(view, self._policy(), producer_ended=not active)
        if d.kind == "success":
            assert d.attempt is not None
            self._log_success(paths, d.attempt)
            return
        if d.kind == "failed":
            raise AirflowFailException(self._failure(d))
        if d.kind == "run":
            self.log.info("the run skipped unit %s; running it here", self.unit_id)
            PipeloomRunOperator.execute(self, context)
            return
        if d.kind == "no_results" and grace:
            raise AirflowException(f"{d.code} {d.message}")
        remaining = deadline - time.time()
        if remaining <= 0:
            raise AirflowException(
                f"unit {self.unit_id} has no verdict after {self.watch_timeout_s}s "
                "(execution.watcher_timeout)"
            )
        recheck = VISIBILITY_GRACE_S if d.kind == "no_results" else RECHECK_S
        self.defer(
            trigger=PipeloomWatcherTrigger(
                run_dir=str(paths.run_dir),
                unit_id=self.unit_id,
                retries=int(self.retries or 0),
                retry_on=self.retry_on,
                poll_interval=self.poll_interval,
                recheck_after=min(recheck, remaining),
            ),
            method_name="execute_complete",
            kwargs={"deadline": deadline, "grace": d.kind == "no_results"},
            timeout=timedelta(seconds=remaining),
        )

    def _log_success(self, paths: RunPaths, attempt: AttemptView) -> None:
        self.log.info(
            "unit %s attempt %s succeeded: %s",
            self.unit_id,
            attempt.number,
            json.dumps(attempt.row, sort_keys=True, default=str),
        )
        log = paths.log(self.unit_id, attempt.number)
        try:
            with log.open(encoding="utf-8", errors="replace") as f:
                tail = deque(f, LOG_TAIL_LINES)
        except OSError:
            self.log.info("no log at %s", log)
            return
        self.log.info("last %s lines of %s:\n%s", len(tail), log, "".join(tail).rstrip())

    def _failure(self, d: Decision) -> str:
        """The failure text, logged with the trace excerpt."""
        number = f" attempt {d.attempt.number}" if d.attempt is not None else ""
        text = f"unit {self.unit_id}{number} failed: {d.code or ''} {d.message or ''}".rstrip()
        error = d.attempt.row.get("error") if d.attempt is not None else None
        trace = error.get("trace_excerpt") if isinstance(error, dict) else None
        self.log.error("%s", text)
        if isinstance(trace, list) and trace:
            self.log.error("trace excerpt:\n%s", "\n".join(str(t) for t in trace))
        return text
