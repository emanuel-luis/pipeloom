"""A whole DAG run: `airflow db migrate` into a throwaway sqlite database, then `dag.test()` of
shop_local in a subprocess. Every unit runs as a real `pipeloom run` child and finalize writes
the run's results."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

from pipeloom_airflow import naming

HERE = Path(__file__).resolve().parent


@pytest.mark.airflow_e2e
@pytest.mark.timeout(600)
def test_shop_local_runs_end_to_end(tmp_path: Path, af_project: Any) -> None:
    project, manifest, _ = af_project
    home = tmp_path / "home"
    home.mkdir()
    env = {
        "PATH": os.environ["PATH"],
        "HOME": str(tmp_path),
        "AIRFLOW_HOME": str(home),
        "AIRFLOW__CORE__LOAD_EXAMPLES": "false",
        "AIRFLOW__CORE__DAGS_FOLDER": str(HERE / "dags"),
        "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN": f"sqlite:///{home}/airflow.db",
        "PYTHONHASHSEED": "0",
        "PIPELOOM_AF_PROJECT": project.as_posix(),
        "PIPELOOM_AF_MANIFEST": manifest.as_posix(),
    }

    def run(*argv: str) -> subprocess.CompletedProcess[str]:
        done = subprocess.run(
            [sys.executable, "-I", *argv],
            env=env,
            timeout=300,
            capture_output=True,
            text=True,
            check=False,
        )
        assert done.returncode == 0, (done.stdout[-3000:], done.stderr[-3000:])
        return done

    run("-m", "airflow", "db", "migrate")
    done = run(str(HERE / "_dag_test_helper.py"), str(HERE / "dags" / "shop_local.py"))
    outcome = json.loads(done.stdout.strip().splitlines()[-1])
    assert outcome["state"] == "success", done.stderr[-3000:]
    rid = naming.airflow_run_id("shop_local", outcome["run_id"])
    results = json.loads(
        (project / "target" / "runs" / rid / "run_results.json").read_text(encoding="utf-8")
    )
    assert results["run"]["run_id"] == rid
    assert results["run"]["status"] == "succeeded"
    assert results["run"]["counts"]["succeeded"] == 3
