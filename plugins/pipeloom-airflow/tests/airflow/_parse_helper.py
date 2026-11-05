"""Parse one DAG file the way the DAG processor does and describe the result as JSON.

Run as `python -I _parse_helper.py <dag file or folder> <out.json>` in a subprocess with an
explicit environment (see test_parse.py). Any network access raises: a parse must never reach a
running Airflow, its database or anything else."""

from __future__ import annotations

import inspect
import json
import sys
import time
from typing import Any


def _no_network(event: str, args: tuple[Any, ...]) -> None:
    if event in ("socket.connect", "socket.getaddrinfo"):
        raise RuntimeError(f"network access while parsing DAGs: {event} {args!r}")


def main(dags: str, out: str) -> None:
    sys.addaudithook(_no_network)
    import airflow

    try:
        from airflow.dag_processing.dagbag import DagBag
    except ImportError:  # before 3.3
        from airflow.models.dagbag import DagBag  # type: ignore[attr-defined,no-redef,unused-ignore]
    try:
        from airflow.serialization.serialized_objects import DagSerialization

        serialize = DagSerialization.serialize_dag
    except ImportError:  # before 3.3
        from airflow.serialization.serialized_objects import (  # type: ignore[attr-defined,unused-ignore]
            SerializedDAG,
        )

        serialize = SerializedDAG.serialize_dag

    kwargs: dict[str, Any] = {"dag_folder": dags}
    if "include_examples" in inspect.signature(DagBag).parameters:
        kwargs["include_examples"] = False
    start = time.perf_counter()
    bag = DagBag(**kwargs)
    parse_s = time.perf_counter() - start
    described: dict[str, Any] = {}
    for dag_id, dag in sorted(bag.dags.items()):
        serialized = serialize(dag)
        tasks = {}
        for task in sorted(dag.tasks, key=lambda t: t.task_id):
            tasks[task.task_id] = {
                "class": type(task).__name__,
                "trigger_rule": str(getattr(task.trigger_rule, "value", task.trigger_rule)),
                "retries": task.retries,
                "upstream": sorted(task.upstream_task_ids),
                "outlets": sorted(str(getattr(o, "uri", o)) for o in task.outlets),
            }
        described[dag_id] = {
            "params": sorted(dag.params.keys()),
            "serialized_tasks": len(serialized["tasks"]),
            "tasks": tasks,
        }
    result = {
        "airflow_version": airflow.__version__,
        "import_errors": {str(k): str(v) for k, v in bag.import_errors.items()},
        "parse_s": parse_s,
        "dags": described,
    }
    with open(out, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=1, sort_keys=True)


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
