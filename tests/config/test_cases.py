from __future__ import annotations

import json
import re
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest
import yaml
from click.testing import CliRunner

from pipeloom.api import Project, ProjectOptions
from pipeloom.cli import main
from pipeloom.diagnostics import Report, format_diagnostic
from pipeloom.events.bus import EventBus
from pipeloom.events.model import Event
from pipeloom.graph.selectors import Selection, SelectorError, apply
from pipeloom.runtime.control import RunControl
from pipeloom.runtime.run import RunOptions
from pipeloom.testing.clock import FakeClock
from tests.conftest import make_project
from tests.plugins.fakes import FakeDist, FakeEntryPoint, install_entry_points

CASES = Path(__file__).parent.parent / "cases"


@pytest.mark.parametrize("case_file", sorted(CASES.glob("*.yml")), ids=lambda p: p.stem)
def test_case(
    case_file: Path,
    tmp_path: Path,
    golden: Callable[[str, str], None],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    code = case_file.stem
    spec: dict[str, Any] = yaml.safe_load(case_file.read_text(encoding="utf-8"))
    project_dir = make_project(tmp_path, spec["files"])
    install_entry_points(
        monkeypatch,
        [
            FakeEntryPoint(e["name"], e["value"], e["group"], FakeDist(e["dist"], e["version"]))
            for e in spec.get("entry_points", [])
        ],
    )
    if "cli" in spec:
        run_cli_case(code, spec, project_dir, golden)
        return
    options = ProjectOptions(
        project_dir=project_dir,
        target=spec.get("target"),
        plugins=tuple(spec.get("plugins", ())),
        env={},
    )
    if spec.get("run"):  # runtime codes: run the project; failed units are diagnostics
        run_case(code, spec, options, project_dir, golden)
        return
    outcome = Project(options).validate()
    diagnostics = outcome.diagnostics
    if "select" in spec or "exclude" in spec:
        assert outcome.value is not None
        report = Report()
        report.extend(diagnostics)
        sel = Selection(tuple(spec.get("select", ())), tuple(spec.get("exclude", ())))
        try:
            apply(sel, outcome.value.graph, outcome.value.project, report)
        except SelectorError as err:
            report.extend([err.diagnostic])
        diagnostics = report.sorted()
    assert code in {d.code for d in diagnostics}
    text = "".join(format_diagnostic(d) + "\n" for d in diagnostics)
    as_json = json.dumps([d.to_json() for d in diagnostics], indent=2, ensure_ascii=True)
    golden(f"diagnostics/{code}.txt", text)
    golden(f"diagnostics/{code}.json", as_json + "\n")


class StopSink:
    """Requests a hard stop when the first unit starts."""

    name = "stop"

    def __init__(self, control: RunControl) -> None:
        self._control = control

    def emit(self, event: Event) -> None:
        if event.type == "unit.started":
            self._control.request_stop(hard=True)

    def flush(self) -> None:
        pass

    def close(self) -> None:
        pass


VOLATILE = ("id", "ts", "invocation_id")
_TIME = re.compile(r"\d{4}-\d\d-\d\dT[\d:.]+Z")
_OWNER = re.compile(r"[^\s'\"]+:\d+:[0-9a-f]{8}")


def run_case(
    code: str,
    spec: dict[str, Any],
    options: ProjectOptions,
    project_dir: Path,
    golden: Callable[[str, str], None],
) -> None:
    """Run the project. `run` may be a mapping: RunOptions fields (`manifest` is relative to the
    project dir) and `hold_lock`, which takes the run lock first so the run finds it held."""
    given: dict[str, Any] = dict(spec["run"]) if isinstance(spec["run"], dict) else {}
    hold = bool(given.pop("hold_lock", False))
    stop = given.pop("hard_stop_on_start", False)  # a hard stop as soon as a unit starts
    if "manifest" in given:
        given["manifest"] = project_dir / given["manifest"]
    given["selection"] = Selection(tuple(given.pop("select", ())))
    run_opts = RunOptions(**{"run_id": "case", "workers": 1, **given})
    project = Project(options, clock=FakeClock(), invocation_id="inv")
    held = None
    if hold:
        bound = Project(options).bind().value
        assert bound is not None
        held = bound.plugins["state_stores"].create()
        lock = held.lock("shop/dev", "other")
        lock.__enter__()
    try:
        if stop:
            control = RunControl()
            sink = StopSink(control)
            with EventBus([(sink, 5)], clock=project.clock, invocation_id="inv") as bus:
                outcome = project.run(run_opts, bus, control)
        else:
            outcome = project.run(run_opts)
    finally:
        if held is not None:
            lock.__exit__(None, None, None)
            held.close()
    text = "".join(format_diagnostic(d) + "\n" for d in outcome.diagnostics)
    assert code in text
    as_json = json.dumps([d.to_json() for d in outcome.diagnostics], indent=2)
    for pattern, label in ((_TIME, "<time>"), (_OWNER, "<owner>")):
        text, as_json = pattern.sub(label, text), pattern.sub(label, as_json)
    golden(f"diagnostics/{code}.txt", text)
    golden(f"diagnostics/{code}.json", as_json + "\n")


def run_cli_case(
    code: str, spec: dict[str, Any], project_dir: Path, golden: Callable[[str, str], None]
) -> None:
    """Run the CLI and keep the events and text lines that carry the code, without volatile fields."""
    args = [
        *[a.replace("{project_dir}", project_dir.as_posix()) for a in spec["cli"]],
        "--project-dir",
        str(project_dir),
        *[a for p in spec.get("plugins", ()) for a in ("--plugin", p)],
    ]
    env = {"PIPELOOM_TARGET": "", "PIPELOOM_DISABLE_PLUGIN_AUTOLOAD": ""}
    as_json = CliRunner().invoke(main, [*args, "--log-format", "json"], env=env)
    events = [json.loads(line) for line in as_json.stderr.splitlines()]
    kept = [
        {k: v for k, v in e.items() if k not in VOLATILE}
        for e in events
        if e["data"].get("code") == code
    ]
    assert kept, as_json.stderr
    as_text = CliRunner().invoke(main, [*args, "--log-format", "text"], env=env)
    lines = [ln for ln in as_text.stderr.splitlines() if code in ln]
    golden(f"diagnostics/{code}.json", json.dumps(kept, indent=2, ensure_ascii=True) + "\n")
    golden(
        f"diagnostics/{code}.txt",
        "".join(re.sub(r"^\d\d:\d\d:\d\d ", "", ln) + "\n" for ln in lines),
    )
