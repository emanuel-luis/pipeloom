"""Run `dag.test()` on the `dag` of one DAG file and print {run_id, state} as JSON.

Run as `python -I _dag_test_helper.py <dag file>` in a subprocess with an explicit environment
whose AIRFLOW_HOME holds a migrated throwaway sqlite database (see test_e2e.py)."""

from __future__ import annotations

import json
import runpy
import sys


def main(dag_file: str) -> None:
    dag = runpy.run_path(dag_file)["dag"]
    run = dag.test()
    state = getattr(run.state, "value", run.state)
    print(json.dumps({"run_id": run.run_id, "state": str(state)}))


if __name__ == "__main__":
    main(sys.argv[1])
