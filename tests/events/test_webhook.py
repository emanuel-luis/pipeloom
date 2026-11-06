"""The `webhook` sink against a local stdlib HTTP server on 127.0.0.1 (no network)."""

from __future__ import annotations

import json
import threading
from collections.abc import Iterator
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

import pytest
from click.testing import CliRunner

from pipeloom.cli import main
from pipeloom.events.bus import EventBus
from pipeloom.events.model import Event, Level, RunCompleted
from pipeloom.events.sinks.webhook import WebhookSink, summary
from pipeloom.testing.clock import FakeClock
from pipeloom.testing.recording import RecordingSink
from tests.conftest import make_project

ENV = {"PIPELOOM_TARGET": "", "PIPELOOM_DISABLE_PLUGIN_AUTOLOAD": "", "PIPELOOM_LOG_FORMAT": ""}


class Hook:
    """A local endpoint that records every POST body and answers `status`."""

    def __init__(self) -> None:
        self.bodies: list[dict[str, Any]] = []
        self.status = 200
        hook = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self) -> None:
                length = int(self.headers["Content-Length"])
                hook.bodies.append(json.loads(self.rfile.read(length)))
                assert self.headers["Content-Type"] == "application/json"
                self.send_response(hook.status)
                self.end_headers()

            def log_message(self, *args: Any) -> None:
                pass

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self.server.server_address[1]}/hook/s3cr3t-path"


@pytest.fixture
def hook() -> Iterator[Hook]:
    h = Hook()
    thread = threading.Thread(target=h.server.serve_forever, daemon=True)
    thread.start()
    try:
        yield h
    finally:
        h.server.shutdown()
        h.server.server_close()


def event(type_: str = "run.completed", level: Level = Level.ERROR, **data: Any) -> Event:
    return Event(
        id="e",
        run_id="r1",
        invocation_id="i",
        seq=1,
        ts=datetime(2026, 1, 1, tzinfo=UTC),
        type=type_,
        level=level,
        data=data,
    )


FAILED = {"status": "failed", "failures": [{"unit_id": "a", "code": "PLM201"}]}

YML = """config_version: 1
name: shop
python_paths:
  - src
events:
  sinks:
    - kind: webhook
      secret_url: ${secret:env/PL_HOOK}
      buffered: true
targets:
  dev:
    engine:
      kind: memory
    catalog:
      kind: memory
"""
MODELS = """models:
  - name: a
    python: m:body
"""
BODY = "def body(ctx):\n    raise RuntimeError('boom')\n"


def test_a_failed_run_posts_exactly_once(hook: Hook, tmp_path: Path) -> None:
    root = make_project(tmp_path, {"pipeloom.yml": YML, "models/m.yml": MODELS, "src/m.py": BODY})
    args = ["run", "--project-dir", str(root), "--target", "dev", "--run-id", "r1"]
    done = CliRunner().invoke(
        main, [*args, "--log-format", "json"], env={**ENV, "PL_HOOK": hook.url}
    )
    assert done.exit_code == 1, done.stderr
    (body,) = hook.bodies
    assert body["event"]["type"] == "run.completed" and body["event"]["data"]["status"] == "failed"
    assert body["text"].startswith("pipeloom run r1 failed: a ")
    assert "s3cr3t-path" not in done.stdout + done.stderr
    assert not [p for p in root.rglob("*") if p.is_file() and b"s3cr3t-path" in p.read_bytes()]


def test_validate_builds_the_sink_without_the_secret_and_posts_nothing(
    hook: Hook, tmp_path: Path
) -> None:
    root = make_project(tmp_path, {"pipeloom.yml": YML, "models/m.yml": MODELS, "src/m.py": BODY})
    done = CliRunner().invoke(main, ["validate", "--project-dir", str(root)], env=ENV)
    assert done.exit_code == 0, done.stderr
    assert hook.bodies == []


def test_a_literal_url_is_pls104(tmp_path: Path) -> None:
    yml = YML.replace("${secret:env/PL_HOOK}", "https://example.invalid/x")
    root = make_project(tmp_path, {"pipeloom.yml": yml, "models/m.yml": MODELS, "src/m.py": BODY})
    done = CliRunner().invoke(main, ["validate", "--project-dir", str(root)], env=ENV)
    assert done.exit_code == 1 and "PLS104" in done.stderr


def test_selection(hook: Hook) -> None:
    sink = WebhookSink(hook.url)
    sink.emit(event(**FAILED))
    sink.emit(event(level=Level.INFO, status="succeeded"))
    sink.emit(event("unit.failed", message="no"))
    assert len(hook.bodies) == 1
    loud = WebhookSink(hook.url, statuses=("failed", "cancelled"), min_level="warning")
    loud.emit(event(level=Level.ERROR, status="cancelled"))
    loud.emit(event("diagnostic", Level.WARNING, message="careful"))
    loud.emit(event("unit.started", Level.INFO))
    assert [b["text"] for b in hook.bodies[1:]] == [
        "pipeloom run r1 cancelled",
        "pipeloom warning diagnostic in run r1: careful",
    ]


def test_the_failure_budget_stops_posting_and_the_bus_reports_once(hook: Hook) -> None:
    hook.status = 500
    sink = WebhookSink(hook.url, max_failures=2)
    recorder = RecordingSink()
    failed = RunCompleted(
        status="failed",
        exit_code=1,
        duration_ms=0,
        counts={},
        rows_written=0,
        bytes_written=0,
        failures=[],
        artifacts=[],
    )
    with EventBus([(recorder, 5), (sink, 3)], clock=FakeClock(), invocation_id="i") as bus:
        for _ in range(5):
            bus.emit(failed)
    assert len(hook.bodies) == 2  # the third delivery fails without a request
    (disabled,) = [e for e in recorder.events if e.type == "sink.disabled"]
    assert disabled.data["sink"] == "webhook" and "stopped after 2" in str(disabled.data["error"])
    assert "s3cr3t" not in json.dumps(disabled.to_json())


def test_failed_posts_raise_without_the_url_then_stop(hook: Hook) -> None:
    hook.status = 500
    sink = WebhookSink(hook.url, max_failures=2)
    for _ in range(2):
        with pytest.raises(RuntimeError, match=r"^webhook post failed: HTTP 500$"):
            sink.emit(event(**FAILED))
    with pytest.raises(RuntimeError, match="stopped after 2 failed posts"):
        sink.emit(event(**FAILED))
    assert len(hook.bodies) == 2 and "s3cr3t" not in repr(sink)


def test_a_success_resets_the_streak(hook: Hook) -> None:
    sink = WebhookSink(hook.url, max_failures=2)
    hook.status = 503
    with pytest.raises(RuntimeError):
        sink.emit(event(**FAILED))
    hook.status = 204
    sink.emit(event(**FAILED))
    assert sink.failures == 0


def test_an_unreachable_url_names_only_the_error_type() -> None:
    sink = WebhookSink("http://127.0.0.1:9/s3cr3t-path", timeout=2)
    with pytest.raises(RuntimeError) as err:
        sink.emit(event(**FAILED))
    assert "s3cr3t" not in str(err.value) and "127.0.0.1" not in str(err.value)


def test_unresolved_reference_is_inert_and_other_schemes_are_refused() -> None:
    WebhookSink("${secret:env/X}").emit(event(**FAILED))  # no request, no error
    with pytest.raises(ValueError, match="http or https"):
        WebhookSink("file:///etc/passwd")


def test_summary_of_a_run_without_failures() -> None:
    assert summary(event(status="failed")) == "pipeloom run r1 failed"
