"""The plugin spec. It imports no OpenLineage code: the client loads when the sink is built."""

from __future__ import annotations

from typing import Any, Literal

from pipeloom.events.bus import EventSink
from pipeloom.plugins import API_VERSION, OptionsModel, PluginSpec


class OpenLineageOptions(OptionsModel):
    namespace: str = "pipeloom.{target}"
    """OpenLineage namespace of the jobs; {target} and {project} are filled in."""
    dataset_namespace: str | None = None
    """Namespace of catalog relations; defaults to the job namespace. Sources with a location
    use the location's scheme and authority, or `file` for a local path."""
    transport: dict[str, Any] | None = None
    """An openlineage-python transport config, as `type: http` plus its keys. Without it the
    client reads OPENLINEAGE_URL or openlineage.yml."""
    inside_airflow: Literal["skip", "emit"] = "skip"
    """Inside an Airflow task the Airflow provider reports lineage; `emit` reports it here too."""


def _build(opts: Any) -> EventSink:
    from pipeloom_openlineage.sink import OpenLineageSink

    return OpenLineageSink(
        namespace=opts.namespace,
        dataset_namespace=opts.dataset_namespace,
        transport=opts.transport,
        inside_airflow=opts.inside_airflow,
    )


SPEC: PluginSpec[EventSink] = PluginSpec(
    "openlineage",
    "openlineage",
    "sinks",
    API_VERSION,
    _build,
    OpenLineageOptions,
    description="Sends runs, jobs, datasets and facets as OpenLineage run events.",
)
