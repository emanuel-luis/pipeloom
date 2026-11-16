"""OpenLineage facets for the Airflow OpenLineage provider (ADR-0046).

The provider reports every task with the DAG run as parent and asks the operator for datasets
through `get_openlineage_facets_on_*`. The facets come from `pipeloom_openlineage`, the code the
sink uses, imported lazily: without that distribution the hooks return an empty OperatorLineage, and
None without the provider. LOCAL and VIRTUALENV replay the child's events; KUBERNETES reports the static datasets of the
manifest; DATABRICKS has no hooks, the sink in the Databricks task reports the units.
"""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from pipeloom.events.model import EVENT_SCHEMA, Event
from pipeloom.plan.manifest import manifest_dict
from pipeloom.plan.model import Plan


def child_event(line: str) -> Event | None:
    """A pipeloom event from one line of a child's `--log-format json` output."""
    try:
        doc = json.loads(line)
    except ValueError:
        return None
    if not isinstance(doc, dict) or doc.get("schema") != EVENT_SCHEMA:
        return None
    try:
        return Event.model_validate(doc)
    except ValueError:
        return None


def unit_facets(
    plan: Plan, project_dir: str, unit_id: str, events: Sequence[Event]
) -> tuple[list[Any], list[Any], dict[str, Any], dict[str, Any]] | None:
    """(inputs, outputs, run facets, job facets) of one unit task, or None without
    `pipeloom-openlineage`."""
    try:
        from pipeloom_openlineage.sink import unit_lineage
    except ImportError:
        return None
    ol = unit_lineage(manifest_dict(plan), Path(project_dir), unit_id, events)
    run: Mapping[str, Any] = ol.run.facets or {}
    job: Mapping[str, Any] = ol.job.facets or {}
    # the provider sets these itself: the DAG run as parent and the Airflow job type
    return (
        list(ol.inputs or ()),
        list(ol.outputs or ()),
        {k: v for k, v in run.items() if k != "parent"},
        {k: v for k, v in job.items() if k != "jobType"},
    )


class OpenLineageMixin:
    """The provider's hooks for an operator that runs one unit. `lineage_plan` is None until the
    DAG builder sets it (every task of a DAG shares one plan); `_lineage_events` holds the
    events of the unit's child, empty when the task cannot see them."""

    unit_id: str
    lineage_plan: Plan | None = None
    lineage_project_dir: str = "."
    _lineage_events: Sequence[Event] = ()

    def _openlineage(self, events: Sequence[Event]) -> Any:
        if self.lineage_plan is None:
            return None
        from pipeloom_airflow._compat import operator_lineage

        found = unit_facets(self.lineage_plan, self.lineage_project_dir, self.unit_id, events)
        # without pipeloom-openlineage: empty lineage (None only when the provider is missing)
        return operator_lineage(*(found or ([], [], {}, {})))

    def get_openlineage_facets_on_start(self) -> Any:
        return self._openlineage(())

    def get_openlineage_facets_on_complete(self, task_instance: Any) -> Any:
        return self._openlineage(self._lineage_events)

    def get_openlineage_facets_on_failure(self, task_instance: Any) -> Any:
        return self._openlineage(self._lineage_events)
