"""tools/databricks_live.py: the check logic and the flow, against FakeDatabricks only."""

from __future__ import annotations

import json
import re
from collections.abc import Sequence
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest
from click.testing import CliRunner
from tools import databricks_live as live

from pipeloom_databricks.cli import databricks
from pipeloom_databricks.client import ApiError, RunInfo, TaskRunInfo
from pipeloom_databricks.testing.fake import FakeDatabricks

ROOT = "/Volumes/main/ops/live/artifacts"


def row(
    unit: str, status: str, attempt: int, job_run_id: int, ids: str | None = None
) -> dict[str, Any]:
    return {
        "unit_id": unit,
        "status": status,
        "attempt": attempt,
        "orchestrator_ids": {"databricks_job_run_id": ids or str(job_run_id)},
    }


def doc(
    run_id: str,
    status: str,
    *rows: dict[str, Any],
    generated_at: str = "t0",
    started_at: str = "2026-11-05T10:00:00Z",
) -> dict[str, Any]:
    return {
        "metadata": {"generated_at": generated_at},
        "run": {"run_id": run_id, "status": status, "started_at": started_at},
        "results": list(rows),
    }


def task(key: str, result: str) -> TaskRunInfo:
    return TaskRunInfo(key, 1, 1, "TERMINATED", result, None, None)


def run_info(run_id: int, result: str, *tasks: TaskRunInfo) -> RunInfo:
    return RunInfo(run_id, 7, "TERMINATED", result, tasks=tasks, start_time_ms=1)


def facts(checks: list[live.Check]) -> dict[str, bool]:
    return {c.fact: c.ok for c in checks}


def test_full_run_checks_pass_and_fail() -> None:
    run = run_info(500, "SUCCESS")
    good = doc("dbx-500", "succeeded", row("a", "succeeded", 1, 500), row("b", "succeeded", 1, 500))
    assert all(c.ok for c in live.check_full_run(run, good, 0))
    bad = doc("literal", "succeeded", row("a", "succeeded", 1, 500, ids="{{job.run_id}}"))
    got = facts(live.check_full_run(run, bad, 0))
    assert not got["empty_parameter_default_resolves_run_id"]
    assert not got["dynamic_references_in_wheel_parameters"]
    assert not got["spark_session_in_wheel_task"]


def test_select_check_needs_an_excluded_task_and_a_green_finalize() -> None:
    results = doc("r", "succeeded", row("a", "succeeded", 1, 1))
    ok = run_info(
        1,
        "SUCCESS",
        task("a", "SUCCESS"),
        task("b", "EXCLUDED"),
        task("pipeloom__finalize", "SUCCESS"),
    )
    assert live.check_select(ok, results, 0)[0].ok
    disabled = run_info(
        1,
        "SUCCESS",
        task("a", "SUCCESS"),
        task("b", "DISABLED"),
        task("pipeloom__finalize", "SUCCESS"),
    )
    assert live.check_select(disabled, results, 0)[0].ok
    skipped = run_info(
        1,
        "SUCCESS",
        task("a", "SUCCESS"),
        task("b", "EXCLUDED"),
        task("pipeloom__finalize", "UPSTREAM_FAILED"),
    )
    assert not live.check_select(skipped, results, 0)[0].ok


def test_ephemeral_check_needs_one_failed_attempt_and_no_retry() -> None:
    results = doc("r", "failed", row("c", "failed", 1, 1))
    once = run_info(1, "FAILED", task("c", "FAILED"), task("pipeloom__finalize", "FAILED"))
    assert live.check_ephemeral(once, results, 1)[0].ok
    # serverless auto-optimization retried it: a second task attempt, and the run passed
    retried = run_info(1, "SUCCESS", task("c", "FAILED"), task("c", "SUCCESS"))
    check = live.check_ephemeral(retried, results, 0)[0]
    assert not check.ok and "2 task attempt(s)" in check.detail


def test_repair_checks() -> None:
    before = doc("r", "failed", row("c", "failed", 1, 9))
    after = doc("r", "succeeded", row("c", "succeeded", 2, 9), generated_at="t1")
    failed, fixed = run_info(9, "FAILED"), run_info(9, "SUCCESS")
    assert all(c.ok for c in live.check_repair(failed, 1, before, fixed, 0, after))
    again = doc("r", "succeeded", row("c", "succeeded", 1, 9), generated_at="t1")
    assert not facts(live.check_repair(failed, 1, before, fixed, 0, again))[
        "repair_runs_attempt_two"
    ]


def test_start_time_across_repairs_needs_a_rewritten_file_and_a_good_repair() -> None:
    before = doc("r", "failed", row("c", "failed", 1, 9))
    failed, fixed = run_info(9, "FAILED"), run_info(9, "SUCCESS")
    ok_row = row("c", "succeeded", 2, 9)
    stale = doc("r", "succeeded", ok_row)  # same generated_at: finalize left the old file
    got = live.check_repair(failed, 1, before, fixed, 0, stale)[2]
    assert not got.ok and "not rewritten" in got.detail
    bad_repair = doc("r", "failed", row("c", "failed", 1, 9), generated_at="t1")
    got = live.check_repair(failed, 1, before, failed, 1, bad_repair)[2]
    assert not got.ok and "repair did not succeed" in got.detail
    moved = doc("r", "succeeded", ok_row, generated_at="t1", started_at="2026-11-05T11:00:00Z")
    assert not live.check_repair(failed, 1, before, fixed, 0, moved)[2].ok


def test_isolation_needs_a_host_and_a_missing_config_file(tmp_path: Path) -> None:
    good = {"DATABRICKS_HOST": "https://h", "DATABRICKS_CONFIG_FILE": str(tmp_path / "nope")}
    assert live.isolation_problem(good) is None
    assert "DATABRICKS_HOST" in (live.isolation_problem({**good, "DATABRICKS_HOST": ""}) or "")
    no_file = {k: v for k, v in good.items() if k != "DATABRICKS_CONFIG_FILE"}
    assert "CONFIG_FILE" in (live.isolation_problem(no_file) or "")
    present = tmp_path / "cfg"
    present.write_text("[DEFAULT]\n", encoding="utf-8")
    assert "exists" in (
        live.isolation_problem({**good, "DATABRICKS_CONFIG_FILE": str(present)}) or ""
    )


def test_wheels_are_picked_in_fixture_order(tmp_path: Path) -> None:
    for name in (
        "pipeloom_spark-0.5.0-py3-none-any.whl",
        "pipeloom-0.5.0-py3-none-any.whl",
        "pipeloom_databricks-0.5.0-py3-none-any.whl",
        "pipeloom_polars-0.5.0-py3-none-any.whl",
    ):
        (tmp_path / name).write_bytes(b"")
    paths = live.wheel_paths(tmp_path, "/Volumes/c/s/v/", "abc")
    picked = live.pick_wheels(paths)
    assert [p.rsplit("/", 1)[1].split("-")[0] for p in picked] == list(live.WHEELS)
    assert picked[0].startswith("/Volumes/c/s/v/wheels/abc/")
    (tmp_path / "pipeloom-0.5.1-py3-none-any.whl").write_bytes(b"")
    with pytest.raises(SystemExit):
        live.pick_wheels(live.wheel_paths(tmp_path, "/Volumes/c/s/v", "abc"))


class Script:
    """A stand-in for `pipeloom databricks` that records what the workspace would hold."""

    def __init__(self, fake: FakeDatabricks, *, repair_works: bool = True) -> None:
        self.fake = fake
        self.repair_works = repair_works
        self.jobs: dict[str, int] = {}
        self.runs: dict[str, int] = {}
        self.calls: list[str] = []

    def _set(self, run_id: int, job_id: int, result: str, *tasks: TaskRunInfo) -> None:
        self.fake.set_run(run_id, job_id=job_id, result_state=result, tasks=tasks)
        self.fake.runs[run_id] = replace(self.fake.runs[run_id], start_time_ms=1)

    def _results(
        self,
        run_id: str,
        *rows: dict[str, Any],
        status: str = "succeeded",
        generated_at: str = "t0",
    ) -> None:
        self.fake.volume_files[f"{ROOT}/runs/{run_id}/run_results.json"] = json.dumps(
            doc(run_id, status, *rows, generated_at=generated_at)
        ).encode()

    def __call__(self, args: Sequence[str]) -> tuple[int, str]:
        self.calls.append(args[0])
        if args[0] == "deploy":
            lines = []
            for name in ("stable", "flaky"):
                self.jobs[name] = self.fake.add_job({"name": name, "tags": {}})
                lines.append(
                    json.dumps(
                        {
                            "pipeline": name,
                            "job_id": self.jobs[name],
                            "action": "created",
                            "root": f"/ws/{name}/h",
                        }
                    )
                )
            return 0, "\n".join(lines)
        pipeline = args[args.index("--pipeline") + 1]
        given = args[args.index("--run-id") + 1] if "--run-id" in args else None
        if args[0] == "run":
            run_id = self.fake._id()
            self.runs[pipeline] = run_id
            name = given or f"dbx-{run_id}"
            job = self.jobs[pipeline]
            if pipeline == "flaky":
                self._set(run_id, job, "FAILED", task("c", "FAILED"))
                self._results(name, row("c", "failed", 1, run_id), status="failed")
                code = 1
            elif "--select" in args:
                self._set(
                    run_id,
                    job,
                    "SUCCESS",
                    task("a", "SUCCESS"),
                    task("b", "EXCLUDED"),
                    task("pipeloom__finalize", "SUCCESS"),
                )
                self._results(name, row("a", "succeeded", 1, run_id))
                code = 0
            else:
                self._set(run_id, job, "SUCCESS")
                self._results(
                    name, row("a", "succeeded", 1, run_id), row("b", "succeeded", 1, run_id)
                )
                code = 0
            return code, json.dumps(
                {
                    "job_id": self.jobs[pipeline],
                    "job_run_id": run_id,
                    "run_id": given,
                    "run_page_url": None,
                }
            )
        run_id = self.runs[pipeline]
        if self.repair_works:
            self._set(run_id, self.jobs[pipeline], "SUCCESS")
            self._results(given or "", row("c", "succeeded", 2, run_id), generated_at="t1")
            return 0, ""
        return 1, ""


class Hooks:
    def __init__(self) -> None:
        self.deleted: list[str] = []

    def cleanup(self) -> live.Cleanup:
        return live.Cleanup(
            delete_job=lambda j: self.deleted.append(f"job {j}"),
            drop_schema=lambda: self.deleted.append("schema"),
            delete_folder=lambda p: self.deleted.append(f"folder {p}"),
        )


def execute(
    script: Script, hooks: Hooks, capsys: pytest.CaptureFixture[str], *, keep: bool
) -> tuple[int, list[str]]:
    code = live.execute(
        script.fake,
        script,
        hooks.cleanup(),
        artifacts_root=ROOT,
        keep_job=keep,
        tag="t1",
        say=lambda line: None,
    )
    return code, capsys.readouterr().out.splitlines()


def test_the_flow_prints_a_line_per_fact_and_cleans_up(capsys: pytest.CaptureFixture[str]) -> None:
    hooks = Hooks()
    code, lines = execute(Script(FakeDatabricks()), hooks, capsys, keep=False)
    assert code == 0
    assert all(re.fullmatch(r"CHECK [a-z_]+ ok", line) for line in lines)
    assert "CHECK repair_runs_attempt_two ok" in lines
    assert hooks.deleted == [
        "job 101",
        "job 102",
        "folder /ws/stable/h",
        "folder /ws/flaky/h",
        "schema",
    ]


def test_every_run_and_repair_is_bounded(capsys: pytest.CaptureFixture[str]) -> None:
    seen: list[Sequence[str]] = []

    class Spy(Script):
        def __call__(self, args: Sequence[str]) -> tuple[int, str]:
            seen.append(args)
            return super().__call__(args)

    execute(Spy(FakeDatabricks()), Hooks(), capsys, keep=False)
    bounded = [a for a in seen if a[0] in ("run", "repair")]
    assert len(bounded) == 5
    assert all(a[a.index("--timeout") + 1] == live.WAIT_SECONDS for a in bounded)


def test_an_api_error_halfway_reports_the_rest_as_not_reached(
    capsys: pytest.CaptureFixture[str],
) -> None:
    fake = FakeDatabricks()
    fake.fail["get_run"] = ApiError(500, "unavailable")
    hooks = Hooks()
    code, lines = execute(Script(fake), hooks, capsys, keep=False)
    assert code == 1
    assert "CHECK deploy ok" in lines
    assert "CHECK workspace_api failed" in lines
    assert "CHECK repair_runs_attempt_two failed (not reached)" in lines
    for fact in live.EXPECTED_FACTS:
        assert any(line.startswith(f"CHECK {fact} ") for line in lines), fact
    assert "schema" in hooks.deleted  # the cleanup still ran


def test_keep_job_skips_the_cleanup(capsys: pytest.CaptureFixture[str]) -> None:
    hooks = Hooks()
    code, _ = execute(Script(FakeDatabricks()), hooks, capsys, keep=True)
    assert code == 0 and hooks.deleted == []


def test_a_failed_repair_exits_1(capsys: pytest.CaptureFixture[str]) -> None:
    code, lines = execute(Script(FakeDatabricks(), repair_works=False), Hooks(), capsys, keep=False)
    assert code == 1
    assert "CHECK repair_runs_attempt_two failed" in lines


def test_the_fixture_renders_both_pipelines(tmp_path: Path) -> None:
    # the `table` state store belongs to pipeloom-spark on main; any shared store renders the same
    project = tmp_path / "live"
    for src in live.FIXTURE.rglob("*"):
        if src.is_file():
            dest = project / src.relative_to(live.FIXTURE)
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_bytes(src.read_bytes())
    yml = project / "pipeloom.yml"
    text = yml.read_text(encoding="utf-8")
    yml.write_text(
        re.sub(
            r"state:\n      kind: table\n      table: .*\n",
            "state:\n      kind: json\n      path: .state\n",
            text,
        ),
        encoding="utf-8",
    )
    wheels = [f"/Volumes/c/s/v/wheels/x/{w}.whl" for w in live.WHEELS]
    vars_json = live._vars(
        catalog="c", schema="s1", volume="/Volumes/c/s/v", environment_version="5", wheels=wheels
    )
    for pipeline in ("stable", "flaky"):
        result = CliRunner().invoke(
            databricks,
            [
                "deploy",
                "--project-dir",
                str(project),
                "--target",
                live.TARGET,
                "--vars",
                vars_json,
                "--pipeline",
                pipeline,
                "--dry-run",
            ],
        )
        assert result.exit_code == 0, result.output
        settings = json.loads(result.stdout.splitlines()[-1])["settings"]
        env = settings["environments"][0]["spec"]
        assert env["dependencies"] == wheels
        assert settings["name"] == f"pipeloom_live_{pipeline}_live"
