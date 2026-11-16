"""Time hybrid discovery on synthetic projects (ADR-0050 point 2).

Writes projects of 50, 200 and 1000 models, all static and with 10 percent dynamic modules, to a
new folder under the system temp directory, times a cold run (no cache) and a warm run (cache
present, nothing changed), prints a table and removes the folder.

    uv run --directory <repo> python tools/bench_discovery.py
"""

from __future__ import annotations

import shutil
import tempfile
import time
from pathlib import Path

from pipeloom.discovery import Discovery

SIZES = (50, 200, 1000)

STATIC = """\
from pipeloom import Expectation, model, not_null

@model(
    materialized="table",
    tags=["bench"],
    write={{"strategy": "overwrite"}},
    expectations=[Expectation(name="k", check=not_null("id"), on_violation="fail")],
)
def m{i:04d}(upstream):
    return upstream
"""

DYNAMIC = """\
from pipeloom import model

TAGS = ["bench", "dyn"]

@model(materialized="table", tags=TAGS)
def m{i:04d}(upstream):
    return upstream
"""


def write_project(root: Path, n: int, dynamic_share: float) -> None:
    every = round(1 / dynamic_share) if dynamic_share else 0
    for i in range(n):
        folder = root / "models" / f"f{i // 50:02d}"
        folder.mkdir(parents=True, exist_ok=True)
        body = DYNAMIC if every and i % every == 0 else STATIC
        (folder / f"m{i:04d}.py").write_text(body.format(i=i), encoding="utf-8")


def timed(project: Path) -> tuple[float, int, int]:
    start = time.perf_counter()
    result = Discovery(project, ["models"]).discover()
    elapsed = time.perf_counter() - start
    return elapsed, len(result.models), len(result.failures)


def main() -> None:
    base = Path(tempfile.mkdtemp(prefix="pipeloom-bench-"))
    rows: list[tuple[int, str, float, float]] = []
    try:
        for n in SIZES:
            for label, share in (("0%", 0.0), ("10%", 0.1)):
                project = base / f"p{n}_{label.rstrip('%')}"
                write_project(project, n, share)
                cold, found, failed = timed(project)
                warm, found_again, _ = timed(project)
                if found != n or found_again != n or failed:
                    raise SystemExit(f"{project.name}: found {found}, {failed} failures")
                rows.append((n, label, cold, warm))
    finally:
        shutil.rmtree(base, ignore_errors=True)

    print(f"{'models':>6}  {'dynamic':>7}  {'cold s':>7}  {'warm s':>7}")
    for n, label, cold, warm in rows:
        print(f"{n:>6}  {label:>7}  {cold:>7.3f}  {warm:>7.3f}")


if __name__ == "__main__":
    main()
