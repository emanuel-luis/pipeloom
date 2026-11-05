"""Repair on `FakeDatabricks`: the rerun set, the repair id chain, PLD014 and nothing to repair."""

from __future__ import annotations

import pytest

from pipeloom.diagnostics import Report
from pipeloom_databricks.client import ApiError, TaskRunInfo
from pipeloom_databricks.keys import FINALIZE_KEY
from pipeloom_databricks.repair import REPAIRABLE, repair, rerun_set
from pipeloom_databricks.testing.fake import FakeDatabricks


def t(key: str, result: str | None) -> TaskRunInfo:
    return TaskRunInfo(key, 1, 1, "TERMINATED", result, None, None)


def codes(report: Report) -> list[str]:
    return [d.code for d in report.sorted()]


def finished(fake: FakeDatabricks, *tasks: TaskRunInfo, repair_id: int | None = None) -> None:
    fake.set_run(50, job_id=1, result_state="FAILED", tasks=tasks, latest_repair_id=repair_id)


def test_the_rerun_set_is_every_unsuccessful_task_plus_finalize() -> None:
    fake = FakeDatabricks()
    finished(
        fake,
        t("a", "SUCCESS"),
        t("b", "FAILED"),
        t("c", "TIMEDOUT"),
        t("d", "CANCELED"),
        t("e", "UPSTREAM_FAILED"),
        t("f", "UPSTREAM_CANCELED"),
        t("g", "MAXIMUM_CONCURRENT_RUNS_REACHED"),
        t("h", "EXCLUDED"),
        t("i", "SUCCESS_WITH_FAILURES"),
        t(FINALIZE_KEY, "FAILED"),
    )
    report = Report()
    assert repair(fake, 50, report) == fake.runs[50].latest_repair_id
    assert not report.sorted()
    ((run_id, rerun, latest),) = fake.repairs
    assert run_id == 50 and latest is None
    assert rerun == ("b", "c", "d", "e", "f", "g", FINALIZE_KEY)
    assert "SUCCESS" not in REPAIRABLE and "EXCLUDED" not in REPAIRABLE


def test_a_repair_passes_the_run_job_parameters_again() -> None:
    # without them the repair resolves {{job.parameters.pipeloom_run_id}} to its empty default
    fake = FakeDatabricks()
    params = {"pipeloom_run_id": "nightly.1", "full_refresh": "false"}
    fake.set_run(
        50, job_id=1, result_state="FAILED", tasks=(t("a", "FAILED"),), job_parameters=params
    )
    repair(fake, 50, Report())
    assert fake.repair_parameters == [params]


def test_finalize_joins_the_set_when_a_unit_failed_even_if_it_succeeded() -> None:
    fake = FakeDatabricks()
    finished(fake, t("a", "FAILED"), t(FINALIZE_KEY, "SUCCESS"))
    assert rerun_set(fake.runs[50]) == ["a", FINALIZE_KEY]
    # a run without a finalize task (a one-time run) adds none
    finished(fake, t("a", "FAILED"))
    assert rerun_set(fake.runs[50]) == ["a"]


def test_a_second_repair_carries_the_latest_repair_id() -> None:
    fake = FakeDatabricks()
    finished(fake, t("a", "FAILED"), t(FINALIZE_KEY, "UPSTREAM_FAILED"))
    first = repair(fake, 50, Report())
    assert first is not None
    finished(fake, t("a", "FAILED"), t(FINALIZE_KEY, "FAILED"), repair_id=first)
    second = repair(fake, 50, Report())
    assert second is not None and second != first
    assert [latest for _, _, latest in fake.repairs] == [None, first]


def test_an_active_run_is_pld014_and_nothing_is_repaired() -> None:
    fake = FakeDatabricks()
    for state in ("PENDING", "QUEUED", "RUNNING", "TERMINATING", "BLOCKED"):
        fake.set_run(50, job_id=1, life_cycle_state=state, tasks=[t("a", "FAILED")])
        report = Report()
        assert repair(fake, 50, report) is None
        assert codes(report) == ["PLD014"] and state in report.sorted()[0].message
    assert fake.repairs == []


def test_nothing_to_repair_is_none_without_an_error() -> None:
    fake = FakeDatabricks()
    fake.set_run(
        50, job_id=1, result_state="SUCCESS", tasks=[t("a", "SUCCESS"), t("b", "EXCLUDED")]
    )
    report = Report()
    assert repair(fake, 50, report) is None
    assert not report.sorted() and fake.repairs == []
    fake.set_run(51, job_id=1)
    assert repair(fake, 51, report) is None and not report.sorted()


@pytest.mark.parametrize("failing", ["get_run", "repair_run"])
def test_api_errors_are_pld009(failing: str) -> None:
    fake = FakeDatabricks()
    finished(fake, t("a", "FAILED"))
    fake.fail[failing] = ApiError(500, "down", "INTERNAL_ERROR")
    report = Report()
    assert repair(fake, 50, report) is None and codes(report) == ["PLD009"]


def test_an_unknown_run_is_pld009_with_the_404() -> None:
    report = Report()
    assert repair(FakeDatabricks(), 404, report) is None
    assert codes(report) == ["PLD009"] and "HTTP 404" in report.sorted()[0].message
