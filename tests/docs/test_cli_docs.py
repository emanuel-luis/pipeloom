"""`pipeloom docs generate` and `pipeloom docs serve`: every flag and usage error."""

from __future__ import annotations

import json
import re
import sys
import threading
import urllib.request
from collections.abc import Callable
from html.parser import HTMLParser
from http.server import ThreadingHTTPServer
from pathlib import Path

import pytest
from click.testing import CliRunner, Result

from pipeloom.cli import main
from pipeloom.docs.serve import make_server
from tests.conftest import FIXTURES, make_project

SHOP = ["--project-dir", str(FIXTURES / "shop"), "--target", "ci"]
ENV = {"PIPELOOM_TARGET": "", "PIPELOOM_LOG_FORMAT": "", "PIPELOOM_PROJECT_DIR": ""}
YML = "config_version: 1\nname: p\ntargets:\n  ci:\n    engine: {kind: memory}\n    catalog: {kind: memory}\n"
MODELS = """sources:
  - {name: raw, format: memory, location: raw/missing}
models:
  - {name: a, sql: select 1 as x}
  - {name: b, sql: select * from a, inputs: {a: {ref: a}}}
  - {name: bad, sql: select * from raw, inputs: {raw: {source: raw}}}
  - {name: after_bad, sql: select * from bad, inputs: {bad: {ref: bad}}}
"""


def cli(*args: str) -> Result:
    return CliRunner().invoke(main, list(args), env=ENV)


class _Balanced(HTMLParser):
    VOID = {"meta", "input", "br", "hr", "link", "img"}

    def __init__(self) -> None:
        super().__init__()
        self.stack: list[str] = []
        self.errors: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag not in self.VOID:
            self.stack.append(tag)

    def handle_startendtag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        pass

    def handle_endtag(self, tag: str) -> None:
        if not self.stack or self.stack.pop() != tag:
            self.errors.append(tag)


def assert_balanced(page: str) -> None:
    parser = _Balanced()
    parser.feed(page)
    parser.close()
    assert parser.errors == [] and parser.stack == []


def svg_of(page: str) -> str:
    match = re.search(r'<svg id="graph-svg".*?</svg>', page, re.S)
    assert match
    return match.group(0)


@pytest.fixture
def project(tmp_path: Path) -> Path:
    return make_project(tmp_path / "p", {"pipeloom.yml": YML, "models/m.yml": MODELS})


def page_of(out: Path) -> str:
    return (out / "index.html").read_text(encoding="utf-8")


def test_default_out_is_target_docs(project: Path) -> None:
    result = cli("docs", "generate", "--project-dir", str(project))
    assert result.exit_code == 0, result.stderr
    page = project / "target" / "docs" / "index.html"
    assert result.stdout.strip() == page.as_posix()
    assert_balanced(page.read_text(encoding="utf-8"))


def test_out_relative_and_absolute(project: Path, tmp_path: Path) -> None:
    rel = cli("docs", "generate", "--project-dir", str(project), "--out", "site")
    assert rel.exit_code == 0 and (project / "site" / "index.html").is_file()
    absolute = tmp_path / "deep" / "abs"
    assert (
        cli("docs", "generate", "--project-dir", str(project), "--out", str(absolute)).exit_code
        == 0
    )
    assert [p.name for p in absolute.iterdir()] == ["index.html"]


def test_output_is_deterministic(project: Path, tmp_path: Path) -> None:
    pages = []
    for name in ("a", "b"):
        out = tmp_path / name
        assert (
            cli("docs", "generate", "--project-dir", str(project), "--out", str(out)).exit_code == 0
        )
        pages.append(page_of(out))
    strip = re.compile(r", compiled [^<]*")
    assert strip.sub("", pages[0]) == strip.sub("", pages[1])


def test_target_vars_plugin_and_log_format(tmp_path: Path) -> None:
    out = tmp_path / "o"
    result = cli(
        "docs", "generate", *SHOP, "--vars", "{region: BR}", "--plugin",
        "tests.plugins.fakes:FAKE_SINK", "--log-format", "json", "--out", str(out),
    )  # fmt: skip
    assert result.exit_code == 0, result.stderr
    types = [json.loads(line)["type"] for line in result.stderr.splitlines()]
    assert types[0] == "invocation.started" and "artifact.written" in types
    assert "target ci" in page_of(out)


def test_target_from_environment(tmp_path: Path) -> None:
    out = tmp_path / "o"
    result = CliRunner().invoke(
        main,
        ["docs", "generate", "--project-dir", str(FIXTURES / "shop"), "--out", str(out)],
        env={**ENV, "PIPELOOM_TARGET": "dev", "PIPELOOM_USER": "Ana"},
    )
    assert result.exit_code == 0 and "target dev" in page_of(out)


def test_compile_errors_exit_1(tmp_path: Path) -> None:
    root = make_project(
        tmp_path / "bad", {"pipeloom.yml": "config_version: 1\nname: x\ntargets: {}\n"}
    )
    result = cli("docs", "generate", "--project-dir", str(root))
    assert result.exit_code == 1 and result.stderr
    assert not (root / "target" / "docs").exists()


def test_golden_graph_of_the_shop(tmp_path: Path, golden: Callable[[str, str], None]) -> None:
    manifest = tmp_path / "m.json"
    assert cli("compile", *SHOP, "--stable", "--out", str(manifest)).exit_code == 0
    out = tmp_path / "o"
    assert cli("docs", "generate", "--manifest", str(manifest), "--out", str(out)).exit_code == 0
    page = page_of(out)
    golden("docs/shop_graph.svg", svg_of(page) + "\n")
    assert "compiled" not in page  # a stable manifest has a blank generated_at
    assert "last run" not in page  # --manifest alone reads no results


def test_manifest_failure_is_plx107(tmp_path: Path) -> None:
    bad = tmp_path / "m.json"
    bad.write_text("{}", encoding="utf-8")
    result = cli("docs", "generate", "--manifest", str(bad), "--out", str(tmp_path / "o"))
    assert result.exit_code == 1 and "PLX107" in result.stderr
    assert not (tmp_path / "o").exists()


def test_missing_manifest_and_results_files_are_usage_errors(project: Path) -> None:
    assert cli("docs", "generate", "--manifest", str(project / "nope.json")).exit_code == 2
    result = cli(
        "docs", "generate", "--project-dir", str(project), "--run-results", str(project / "no.json")
    )
    assert result.exit_code == 2


def test_results_show_statuses_and_stale_plan_note(project: Path, tmp_path: Path) -> None:
    run = cli("run", "--project-dir", str(project), "--target", "ci", "--log-format", "json")
    assert run.exit_code == 1  # `bad` reads a missing source
    out = tmp_path / "o"
    assert cli("docs", "generate", "--project-dir", str(project), "--out", str(out)).exit_code == 0
    page = page_of(out)
    svg = svg_of(page)
    for name, status in {
        "a": "succeeded",
        "b": "succeeded",
        "bad": "failed",
        "after_bad": "skipped",
    }.items():
        assert re.search(rf'class="node model st-{status}" data-id="model\.p\.{name}"', svg), name
    assert 'class="node source st-none" data-id="source.p.raw"' in svg
    assert "last run" in page and "failed" in page and "pipeloom explain " in page
    assert "The plan changed" not in page
    assert_balanced(page)

    models = project / "models" / "m.yml"
    models.write_text(MODELS + "  - {name: extra, sql: select 3 as y}\n", encoding="utf-8")
    assert cli("docs", "generate", "--project-dir", str(project), "--out", str(out)).exit_code == 0
    stale = page_of(out)
    assert "The plan changed" in stale
    assert 'class="node model st-succeeded" data-id="model.p.a"' in stale
    assert 'class="node model st-none" data-id="model.p.extra"' in stale


def test_run_results_flag_and_manifest_together(project: Path, tmp_path: Path) -> None:
    cli("run", "--project-dir", str(project), "--target", "ci", "--log-format", "json")
    root_results = project / "target" / "run_results.json"
    manifest = project / "target" / "manifest.json"
    out = tmp_path / "o"
    base = ["docs", "generate", "--manifest", str(manifest), "--out", str(out)]
    assert cli(*base).exit_code == 0
    assert "last run" not in page_of(out)
    assert cli(*base, "--run-results", str(root_results)).exit_code == 0
    assert "last run" in page_of(out)


def test_results_of_another_project_or_schema_are_usage_errors(
    project: Path, tmp_path: Path
) -> None:
    cli("run", "--project-dir", str(project), "--target", "ci", "--log-format", "json")
    doc = json.loads((project / "target" / "run_results.json").read_text(encoding="utf-8"))
    doc["run"]["project"] = "other"
    foreign = tmp_path / "foreign.json"
    foreign.write_text(json.dumps(doc), encoding="utf-8")
    junk = tmp_path / "junk.json"
    junk.write_text('{"schema": "x"}', encoding="utf-8")
    for path in (foreign, junk):
        result = cli("docs", "generate", "--project-dir", str(project), "--run-results", str(path))
        assert result.exit_code == 2 and path.as_posix() in result.stderr


def test_serve_requires_index(tmp_path: Path) -> None:
    result = cli("docs", "serve", "--project-dir", str(tmp_path))
    assert result.exit_code == 2 and "pipeloom docs generate" in result.stderr


def test_serve_stops_on_ctrl_c(project: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    assert cli("docs", "generate", "--project-dir", str(project), "--out", "site").exit_code == 0

    def interrupt(self: ThreadingHTTPServer, poll_interval: float = 0.5) -> None:
        raise KeyboardInterrupt

    monkeypatch.setattr(ThreadingHTTPServer, "serve_forever", interrupt)
    result = cli(
        "docs",
        "serve",
        "--project-dir",
        str(project),
        "--dir",
        "site",
        "--host",
        "127.0.0.1",
        "--port",
        "0",
    )
    assert result.exit_code == 0
    assert result.stdout.startswith(f"serving {(project / 'site').as_posix()} at http://127.0.0.1:")
    default = cli(
        "docs",
        "serve",
        "--project-dir",
        str(project),
        "--dir",
        str(project / "site"),
        "--port",
        "0",
    )
    assert default.exit_code == 0


def test_serve_default_dir_and_busy_port(project: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    assert cli("docs", "generate", "--project-dir", str(project)).exit_code == 0

    def refuse(directory: Path, host: str, port: int) -> ThreadingHTTPServer:
        raise OSError("address in use")

    # `pipeloom.cli.docs` as an attribute path is the click group, so go through sys.modules
    monkeypatch.setattr(sys.modules["pipeloom.cli.docs"], "make_server", refuse)
    result = cli("docs", "serve", "--project-dir", str(project))
    assert result.exit_code == 2 and "cannot listen on 127.0.0.1:8000" in result.stderr


def test_make_server_serves_the_page(project: Path) -> None:
    assert cli("docs", "generate", "--project-dir", str(project), "--out", "site").exit_code == 0
    server = make_server(project / "site", "127.0.0.1", 0)
    thread = threading.Thread(target=server.serve_forever)
    thread.start()
    try:
        url = f"http://127.0.0.1:{server.server_address[1]}/"
        with urllib.request.urlopen(url, timeout=10) as resp:  # noqa: S310
            body = resp.read().decode("utf-8")
        assert resp.status == 200 and "<title>p docs</title>" in body
    finally:
        server.shutdown()
        thread.join()
        server.server_close()
