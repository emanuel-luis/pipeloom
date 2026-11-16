"""Turn pipeloom run and unit events into OpenLineage run events (ADR-0046).

The pipeloom run is the job `<project>`; each persisted model is the job `<project>.<model>`,
whose runs point at the pipeline run through the parent facet. Run ids are uuid5 values, so every
process of one pipeloom run (unit tasks, finalize, an Airflow operator) derives the same ids.
Datasets and job facets come from the manifest that `plan_ready` hands over.
"""

from __future__ import annotations

import json
import uuid
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import Any, Final, cast
from urllib.parse import urlsplit

from openlineage.client import OpenLineageClient
from openlineage.client.event_v2 import (
    InputDataset,
    Job,
    OutputDataset,
    Run,
    RunEvent,
    RunState,
)
from openlineage.client.facet_v2 import (
    DatasetFacet,
    JobFacet,
    OutputDatasetFacet,
    RunFacet,
    data_quality_assertions_dataset,
    error_message_run,
    job_type_job,
    output_statistics_output_dataset,
    parent_run,
    schema_dataset,
    source_code_location_job,
)
from openlineage.client.transport import Transport

from pipeloom.events.bus import PlanInfo
from pipeloom.events.model import Event, Level
from pipeloom.secrets.resolve import contains_secret

PRODUCER: Final = "https://github.com/emanuel-luis/pipeloom/tree/main/plugins/pipeloom-openlineage"
AIRFLOW_KEY: Final = "orchestrator_id.airflow_dag_id"
_ID_SPACE: Final = uuid.uuid5(uuid.NAMESPACE_URL, "https://github.com/emanuel-luis/pipeloom")
_UNIT_END: Final = {
    "unit.succeeded": RunState.COMPLETE,
    "unit.failed": RunState.FAIL,
    "unit.timed_out": RunState.FAIL,
    "unit.cancelled": RunState.ABORT,
}
_RUN_END: Final = {
    "succeeded": RunState.COMPLETE,
    "failed": RunState.FAIL,
    "cancelled": RunState.ABORT,
}


def run_uuid(project: str, run_id: str) -> str:
    """The OpenLineage run id of a pipeloom run. The project scopes custom run ids."""
    return str(uuid.uuid5(_ID_SPACE, f"run:{project}:{run_id}"))


def unit_uuid(project: str, run_id: str, unit_id: str, attempt: int) -> str:
    """The OpenLineage run id of one attempt of one unit."""
    return str(uuid.uuid5(_ID_SPACE, f"unit:{project}:{run_id}:{unit_id}:{attempt}"))


# -- manifest accessors: the only code that knows the manifest's field names ------------------


def _project(manifest: Mapping[str, Any]) -> str:
    return str(manifest["metadata"]["project"])


def _target(manifest: Mapping[str, Any]) -> str:
    return str(manifest["metadata"]["target"])


def _unit_nodes(manifest: Mapping[str, Any], unit_id: str) -> list[str]:
    return list(manifest.get("units", {}).get(unit_id, ()))


def _node(manifest: Mapping[str, Any], unique_id: str) -> Mapping[str, Any] | None:
    node: Mapping[str, Any] | None = manifest.get("nodes", {}).get(unique_id)
    return node


def _source(manifest: Mapping[str, Any], unique_id: str) -> Mapping[str, Any] | None:
    source: Mapping[str, Any] | None = manifest.get("sources", {}).get(unique_id)
    return source


def _node_inputs(node: Mapping[str, Any]) -> list[str]:
    return list(node.get("inputs", {}).values())


def _node_relation(node: Mapping[str, Any]) -> str | None:
    rel = node.get("relation")
    if not rel:
        return None
    return ".".join(p for p in (rel.get("catalog"), rel.get("schema"), rel.get("name")) if p)


def _node_columns(node: Mapping[str, Any]) -> list[Mapping[str, Any]] | None:
    columns: list[Mapping[str, Any]] | None = node.get("columns")
    return columns


def _node_location(node: Mapping[str, Any]) -> tuple[str, int | None]:
    return str(node["file"]), node.get("line")


def _source_location(source: Mapping[str, Any]) -> tuple[str | None, str | None]:
    """(location, table) of a source."""
    return source.get("location"), source.get("table")


# -- the sink -----------------------------------------------------------------------------------


class OpenLineageSink:
    """Emits OpenLineage run events for runs and units. Silent inside Airflow unless told."""

    name = "openlineage"

    def __init__(
        self,
        *,
        namespace: str = "pipeloom.{target}",
        dataset_namespace: str | None = None,
        transport: Mapping[str, Any] | None = None,
        inside_airflow: str = "skip",
        client: OpenLineageClient | None = None,
    ) -> None:
        self._namespace = namespace
        self._dataset_namespace = dataset_namespace
        self._transport = None if transport is None else dict(transport)
        self._inside_airflow = inside_airflow
        self._client = client
        self._manifest: Mapping[str, Any] | None = None
        self._project_dir: Path | None = None
        self._open: dict[str, int] = {}
        """Unit id to the attempt that started and has not ended."""
        self._written: dict[str, Mapping[str, Any]] = {}
        self._quality: dict[str, list[Mapping[str, Any]]] = {}

    def __repr__(self) -> str:  # a transport config may hold credentials
        return "OpenLineageSink()"

    # -- the sink protocol --------------------------------------------------------------------

    def plan_ready(self, info: PlanInfo) -> None:
        self._manifest = info.manifest
        self._project_dir = info.project_dir

    def emit(self, event: Event) -> None:
        if self._inside_airflow == "skip" and AIRFLOW_KEY in event.context:
            return  # the Airflow provider reports this task
        if event.run_id is None:
            return
        if event.type == "run.started":
            self._run_started(event)
        elif event.type == "run.completed":
            self._run_completed(event)
        elif event.type == "unit.started" and event.unit_id and event.attempt is not None:
            self._open[event.unit_id] = event.attempt
            self._written.pop(event.unit_id, None)
            self._quality.pop(event.unit_id, None)
            self._send(self._unit_event(event, RunState.START, event.attempt))
        elif event.type == "model.written" and event.unit_id:
            self._written[event.unit_id] = event.data
        elif event.type == "model.quality_evaluated" and event.unit_id:
            results = event.data.get("results")
            self._quality[event.unit_id] = cast(list[Mapping[str, Any]], results or [])
        elif event.type in _UNIT_END and event.unit_id:
            attempt = self._open.pop(event.unit_id, None)
            attempt = event.attempt if event.attempt is not None else attempt
            if attempt is not None:  # a unit cancelled before it started has no OpenLineage run
                self._send(self._unit_event(event, _UNIT_END[event.type], attempt))

    def flush(self) -> None:
        pass

    def close(self) -> None:
        if self._client is not None:
            self._client.close()

    # -- events -------------------------------------------------------------------------------

    def _send(self, ol_event: RunEvent) -> None:
        client = self._get_client()
        if client is not None:
            client.emit(ol_event)

    def _get_client(self) -> OpenLineageClient | None:
        if self._client is None:
            if self._transport is None:
                self._client = OpenLineageClient()
            elif contains_secret(json.dumps(self._transport)):
                return None  # built without secret resolution: a command that never runs
            else:
                self._client = OpenLineageClient(config={"transport": self._transport})
        return self._client

    def _names(self, event: Event) -> tuple[str, str]:
        """(project, target) from the manifest, else from a run event's data."""
        if self._manifest is not None:
            return _project(self._manifest), _target(self._manifest)
        return str(event.data.get("project", "")), str(event.data.get("target", ""))

    def _job_namespace(self, project: str, target: str) -> str:
        return self._namespace.format(project=project, target=target)

    def _run_started(self, event: Event) -> None:
        project, target = self._names(event)
        ns = self._job_namespace(project, target)
        assert event.run_id is not None
        self._send(
            RunEvent(
                eventType=RunState.START,
                eventTime=_ts(event),
                run=Run(runId=run_uuid(project, event.run_id)),
                job=Job(namespace=ns, name=project, facets=_job_type("JOB")),
                producer=PRODUCER,
            )
        )

    def _run_completed(self, event: Event) -> None:
        if event.data.get("replay") is True:
            return  # a retried finalize: the first one already reported the run
        project, target = self._names(event)
        state = _RUN_END.get(str(event.data.get("status")), RunState.FAIL)
        facets: dict[str, RunFacet] = {}
        failures = event.data.get("failures")
        if state is RunState.FAIL and isinstance(failures, list) and failures:
            lines = [
                f"{f.get('unit_id')} {f.get('code')}: {f.get('message')}"
                for f in failures
                if isinstance(f, dict)
            ]
            facets["errorMessage"] = error_message_run.ErrorMessageRunFacet(
                message="\n".join(lines), programmingLanguage="python"
            )
        assert event.run_id is not None
        self._send(
            RunEvent(
                eventType=state,
                eventTime=_ts(event),
                run=Run(runId=run_uuid(project, event.run_id), facets=facets),
                job=Job(
                    namespace=self._job_namespace(project, target),
                    name=project,
                    facets=_job_type("JOB"),
                ),
                producer=PRODUCER,
            )
        )

    def _unit_event(self, event: Event, state: RunState, attempt: int) -> RunEvent:
        assert event.run_id is not None and event.unit_id is not None
        unit_id = event.unit_id
        project, target = self._names(event)
        ns = self._job_namespace(project, target)
        parent_id = run_uuid(project, event.run_id)
        run_facets: dict[str, RunFacet] = {
            "parent": parent_run.ParentRunFacet(
                run=parent_run.Run(runId=parent_id),
                job=parent_run.Job(namespace=ns, name=project),
                root=parent_run.Root(
                    run=parent_run.RootRun(runId=parent_id),
                    job=parent_run.RootJob(namespace=ns, name=project),
                ),
            )
        }
        error = _error_facet(event)
        if error is not None:
            run_facets["errorMessage"] = error
        job_facets = _job_type("MODEL")
        inputs: list[InputDataset] = []
        outputs: list[OutputDataset] = []
        if self._manifest is not None:
            nodes = _unit_nodes(self._manifest, unit_id)
            if nodes:
                node = _node(self._manifest, nodes[-1])
                if node is not None:
                    job_facets["sourceCodeLocation"] = _code_location(node)
                    output = self._output(node, state, unit_id, event, ns)
                    if output is not None:
                        outputs.append(output)
                inputs = self._inputs(nodes, ns)
        return RunEvent(
            eventType=state,
            eventTime=_ts(event),
            run=Run(runId=unit_uuid(project, event.run_id, unit_id, attempt), facets=run_facets),
            job=Job(namespace=ns, name=f"{project}.{unit_id}", facets=job_facets),
            inputs=inputs,
            outputs=outputs,
            producer=PRODUCER,
        )

    # -- datasets -----------------------------------------------------------------------------

    def _relation_namespace(self, job_ns: str) -> str:
        return self._dataset_namespace or job_ns

    def _inputs(self, nodes: Sequence[str], job_ns: str) -> list[InputDataset]:
        assert self._manifest is not None
        inside = set(nodes)
        seen: dict[tuple[str, str], InputDataset] = {}
        for uid in nodes:
            node = _node(self._manifest, uid)
            for parent in [] if node is None else _node_inputs(node):
                if parent in inside:
                    continue  # an inlined ephemeral model
                found = self._input(parent, job_ns)
                if found is not None:
                    seen.setdefault((found.namespace, found.name), found)
        return list(seen.values())

    def _input(self, unique_id: str, job_ns: str) -> InputDataset | None:
        assert self._manifest is not None
        source = _source(self._manifest, unique_id)
        if source is not None:
            location, table = _source_location(source)
            if table:
                return InputDataset(namespace=self._relation_namespace(job_ns), name=table)
            if location:
                ns, name = self._location(location)
                return InputDataset(namespace=ns, name=name)
            return None
        node = _node(self._manifest, unique_id)
        relation = None if node is None else _node_relation(node)
        if relation is None:
            return None
        return InputDataset(namespace=self._relation_namespace(job_ns), name=relation)

    def _location(self, location: str) -> tuple[str, str]:
        """OpenLineage naming for a path: `scheme://authority` and the path, or `file` and an
        absolute local path."""
        parts = urlsplit(location)
        if parts.scheme and parts.netloc:
            return f"{parts.scheme}://{parts.netloc}", parts.path or "/"
        path = PurePosixPath(location)
        if not path.is_absolute() and self._project_dir is not None:
            return "file", (self._project_dir / location).as_posix()
        return "file", path.as_posix()

    def _output(
        self, node: Mapping[str, Any], state: RunState, unit_id: str, event: Event, job_ns: str
    ) -> OutputDataset | None:
        relation = _node_relation(node)
        if relation is None:
            return None
        facets: dict[str, DatasetFacet] = {}
        output_facets: dict[str, OutputDatasetFacet] = {}
        written = self._written.get(unit_id, {})
        columns = written.get("columns") or _node_columns(node)
        if columns:
            facets["schema"] = schema_dataset.SchemaDatasetFacet(
                fields=[
                    schema_dataset.SchemaDatasetFacetFields(
                        name=str(c["name"]), type=str(c["dtype"]), ordinal_position=i
                    )
                    for i, c in enumerate(columns, start=1)
                ]
            )
        if state is RunState.COMPLETE:
            rows, size = event.data.get("rows_written"), event.data.get("bytes_written")
            if rows is not None or size is not None:
                output_facets["outputStatistics"] = (
                    output_statistics_output_dataset.OutputStatisticsOutputDatasetFacet(
                        rowCount=_int(rows), size=_int(size)
                    )
                )
            assertions = _assertions(self._quality.get(unit_id, []))
            if assertions:
                # An input facet in the spec; on the output dataset it describes the rows the
                # expectations checked, which is what Marquez shows on the dataset page.
                facets["dataQualityAssertions"] = cast(
                    DatasetFacet,
                    data_quality_assertions_dataset.DataQualityAssertionsDatasetFacet(
                        assertions=assertions
                    ),
                )
        return OutputDataset(
            namespace=self._relation_namespace(job_ns),
            name=relation,
            facets=facets,
            outputFacets=output_facets,
        )


# -- facets -------------------------------------------------------------------------------------


def _ts(event: Event) -> str:
    return str(event.to_json()["ts"])


def _int(value: Any) -> int | None:
    return value if isinstance(value, int) and not isinstance(value, bool) else None


def _job_type(kind: str) -> dict[str, JobFacet]:
    return {
        "jobType": job_type_job.JobTypeJobFacet(
            processingType="BATCH", integration="PIPELOOM", jobType=kind
        )
    }


def _code_location(node: Mapping[str, Any]) -> JobFacet:
    """The model's file, relative to the project dir, with its line when the manifest has it."""
    file, line = _node_location(node)
    return source_code_location_job.SourceCodeLocationJobFacet(
        type="file", url=f"{file}#L{line}" if line else file, path=file
    )


def _error_facet(event: Event) -> RunFacet | None:
    if event.type == "unit.failed":
        error = event.data.get("error")
        err = error if isinstance(error, dict) else {}
        trace = event.data.get("traceback")
        return error_message_run.ErrorMessageRunFacet(
            message=f"{err.get('code')}: {err.get('message')}",
            programmingLanguage="python",
            stackTrace=trace if isinstance(trace, str) else None,
        )
    if event.type == "unit.timed_out":
        return error_message_run.ErrorMessageRunFacet(
            message=f"timed out after {event.data.get('timeout_s')} s",
            programmingLanguage="python",
        )
    return None


def _assertions(
    results: Sequence[Mapping[str, Any]],
) -> list[data_quality_assertions_dataset.Assertion]:
    return [
        data_quality_assertions_dataset.Assertion(
            assertion=str(r.get("kind")),
            success=not r.get("failed"),
            name=None if r.get("name") is None else str(r.get("name")),
            severity=None if r.get("action") is None else str(r.get("action")),
            actual=None if r.get("value") is None else str(r.get("value")),
        )
        for r in results
    ]


# -- one unit, for the Airflow operators ----------------------------------------------------------


class _Capture(Transport):
    kind = "pipeloom-capture"

    def __init__(self) -> None:
        self.events: list[RunEvent] = []

    def emit(self, event: Any) -> None:
        self.events.append(event)


def unit_lineage(
    manifest: Mapping[str, Any],
    project_dir: Path,
    unit_id: str,
    events: Sequence[Event] = (),
    *,
    namespace: str = "pipeloom.{target}",
    dataset_namespace: str | None = None,
) -> RunEvent:
    """The last OpenLineage event the sink sends for one unit, given that unit's pipeloom events.
    Without a terminal event among them it is a START built from the manifest alone (the static
    datasets a KUBERNETES task or a task that has not run yet reports)."""
    transport = _Capture()
    sink = OpenLineageSink(
        namespace=namespace,
        dataset_namespace=dataset_namespace,
        inside_airflow="emit",
        client=OpenLineageClient(transport=transport),
    )
    sink.plan_ready(PlanInfo(manifest, project_dir))
    for event in events:
        if event.unit_id == unit_id:
            sink.emit(event)
    if not transport.events or transport.events[-1].eventType is RunState.START:
        transport.events.clear()
        sink.emit(
            Event(
                id="static",
                run_id="static",
                invocation_id="static",
                seq=0,
                ts=datetime.now(UTC),
                type="unit.started",
                level=Level.INFO,
                unit_id=unit_id,
                attempt=1,
                data={},
            )
        )
    return transport.events[-1]


__all__ = [
    "AIRFLOW_KEY",
    "PRODUCER",
    "OpenLineageSink",
    "run_uuid",
    "unit_lineage",
    "unit_uuid",
]
