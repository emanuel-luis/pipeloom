"""An `intermediate` model end to end on the memory engine: written under the artifacts root,
read by its consumer, and deleted by finalize according to `intermediate_ttl`."""

from __future__ import annotations

import json
import os
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from pipeloom.api import Project, ProjectOptions
from pipeloom.events.bus import EventBus
from pipeloom.runtime.run import RunOptions, RunOutcome
from pipeloom.testing.clock import FakeClock
from pipeloom.testing.memory import JsonTableStore
from pipeloom.testing.recording import RecordingSink

ORDERS = [{"id": 1, "v": 5}, {"id": 2, "v": -1}, {"id": 3, "v": 7}]


def project(tmp_path: Path, ttl: str | None = None, extra: str = "") -> Path:
    engine = {
        "kind": "memory",
        "root": (tmp_path / "store").as_posix(),
        "tables": {f"{tmp_path.as_posix()}/raw/orders": ORDERS},
    }
    artifacts = "" if ttl is None else f"    artifacts: {{intermediate_ttl: {ttl}}}\n"
    files = {
        "pipeloom.yml": (
            "config_version: 1\nname: shop\npython_paths: [src]\n"
            f"targets:\n  dev:\n    engine: {json.dumps(engine)}\n"
            "    catalog: {kind: memory, root: store}\n"
            "    state: {kind: json, path: .pipeloom/state}\n    naming: {kind: none}\n"
            f"{artifacts}"
        ),
        "models/sources.yml": (
            "sources:\n  - {name: raw_orders, format: memory, location: raw/orders}\n"
        ),
        "models/m.yml": (
            "models:\n"
            "  - name: stg\n    materialized: intermediate\n"
            "    sql: select id, v from raw where v > 0\n"
            "    inputs: {raw: {source: raw_orders}}\n"
            "  - name: rpt\n    materialized: table\n"
            "    sql: select count(*) as n, sum(v) as total from stg\n"
            "    inputs: {stg: {ref: stg}}\n"
            f"{extra}"
        ),
    }
    for rel, text in files.items():
        (tmp_path / rel).parent.mkdir(parents=True, exist_ok=True)
        (tmp_path / rel).write_bytes(text.encode("utf-8"))
    return tmp_path


def run(root: Path, run_id: str = "r1", sink: RecordingSink | None = None) -> RunOutcome:
    clock = FakeClock(datetime(2026, 1, 1, tzinfo=UTC), step=timedelta(0))
    proj = Project(ProjectOptions(project_dir=root, target="dev", env={}), clock=clock)
    with EventBus([] if sink is None else [(sink, 5)], clock=clock, invocation_id="inv") as bus:
        return proj.run(RunOptions(run_id=run_id, workers=1), bus)


def results(root: Path, run_id: str = "r1") -> dict[str, Any]:
    doc: dict[str, Any] = json.loads(
        (root / "target" / "runs" / run_id / "run_results.json").read_bytes()
    )
    return doc


def test_an_intermediate_is_written_under_the_run_and_read_by_its_consumer(
    tmp_path: Path,
) -> None:
    root = project(tmp_path)
    outcome = run(root)
    assert outcome.exit_code == 0, outcome.diagnostics
    doc = results(root)
    rows = {r["unit_id"]: r for r in doc["results"]}
    place = (root / "target" / "runs" / "r1" / "intermediate" / "stg").resolve().as_posix()
    assert rows["stg"]["materialization"] == "intermediate"
    assert rows["stg"]["relation_name"] == "target/runs/r1/intermediate/stg"
    assert rows["stg"]["rows_written"] == 2 and rows["stg"]["idempotency_token"] == "r1:stg"
    assert rows["stg"]["strategy"] is None
    assert JsonTableStore(root / "store")["main.rpt"] == ({"n": 2, "total": 12},)
    assert doc["run"]["warnings"] == []
    assert (Path(place) / "rows.json").is_file()  # the default ttl of a day keeps it


def test_ttl_zero_deletes_the_intermediate_when_the_run_succeeds(tmp_path: Path) -> None:
    root = project(tmp_path, ttl="0")
    assert run(root).exit_code == 0
    assert not (root / "target" / "runs" / "r1" / "intermediate").exists()
    assert (root / "target" / "runs" / "r1" / "run_results.json").is_file()


def test_ttl_zero_keeps_the_intermediate_of_a_failed_run(tmp_path: Path) -> None:
    extra = "  - name: bad\n    sql: select nope from stg\n    inputs: {stg: {ref: stg}}\n"
    root = project(tmp_path, ttl="0", extra=extra)
    assert run(root).exit_code == 1
    assert (root / "target" / "runs" / "r1" / "intermediate" / "stg" / "rows.json").is_file()


def test_another_run_deletes_an_expired_intermediate(tmp_path: Path) -> None:
    root = project(tmp_path, ttl="1h")
    assert run(root, "r1").exit_code == 0
    old = root / "target" / "runs" / "r1" / "run_results.json"
    doc = json.loads(old.read_bytes())
    doc["run"]["ended_at"] = "2025-12-31T00:00:00+00:00"
    old.write_bytes(json.dumps(doc).encode("utf-8"))
    assert doc["run"]["intermediate_ttl"] == 3600
    stamp = datetime(2025, 12, 31, tzinfo=UTC).timestamp()
    folder = root / "target" / "runs" / "r1" / "intermediate"
    for path in (*folder.rglob("*"), folder):
        os.utime(path, (stamp, stamp))
    assert run(root, "r2").exit_code == 0
    assert not (root / "target" / "runs" / "r1" / "intermediate").exists()
    assert (root / "target" / "runs" / "r2" / "intermediate" / "stg").is_dir()


def test_a_rerun_with_the_same_run_id_replays_the_write(tmp_path: Path) -> None:
    root = project(tmp_path)
    run(root)
    sink = RecordingSink()
    again = run(root, sink=sink)
    assert again.exit_code == 0
    stg = next(r for r in results(root)["results"] if r["unit_id"] == "stg")
    assert stg["rows_written"] == 2
    assert [
        e.data["skipped"] for e in sink.events if e.type == "model.written" and e.model == "stg"
    ] == [True]
