"""The subset of the Jobs API that pipeloom renders, as frozen dataclasses.

Field order is output order. `to_json` drops None and empty collections, and appends the keys of
an `extra` mapping, sorted, after the declared fields.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field, fields, is_dataclass
from typing import Literal

from pydantic import JsonValue


@dataclass(frozen=True)
class JobParameter:
    name: str
    default: str


@dataclass(frozen=True)
class QueueSettings:
    enabled: bool = True


@dataclass(frozen=True)
class EnvironmentSpec:
    environment_version: str
    dependencies: tuple[str, ...] = ()


@dataclass(frozen=True)
class JobEnvironment:
    environment_key: str
    spec: EnvironmentSpec


@dataclass(frozen=True)
class JobCluster:
    job_cluster_key: str
    new_cluster: Mapping[str, JsonValue]


@dataclass(frozen=True)
class PythonWheelTask:
    package_name: str
    entry_point: str
    parameters: tuple[str, ...]


@dataclass(frozen=True)
class TaskDependency:
    task_key: str


@dataclass(frozen=True)
class Task:
    task_key: str
    depends_on: tuple[TaskDependency, ...]
    run_if: Literal["ALL_DONE"] | None
    environment_key: str | None
    job_cluster_key: str | None
    libraries: tuple[Mapping[str, JsonValue], ...]
    python_wheel_task: PythonWheelTask
    max_retries: int | None
    min_retry_interval_millis: int | None
    timeout_seconds: int | None
    disable_auto_optimization: bool | None = None
    """True on serverless: auto-optimization retries a failed task on top of `max_retries`."""


@dataclass(frozen=True)
class JobSettings:
    name: str
    tags: Mapping[str, str]
    parameters: tuple[JobParameter, ...]
    max_concurrent_runs: int
    queue: QueueSettings
    environments: tuple[JobEnvironment, ...]
    job_clusters: tuple[JobCluster, ...]
    tasks: tuple[Task, ...]
    extra: Mapping[str, JsonValue] = field(default_factory=dict)


@dataclass(frozen=True)
class SubmitSpec:
    """The body of runs/submit: a one-time run, no job."""

    run_name: str
    tasks: tuple[Task, ...]
    environments: tuple[JobEnvironment, ...]
    queue: QueueSettings


def _value(v: object) -> JsonValue:
    if is_dataclass(v) and not isinstance(v, type):
        return to_json(v)
    if isinstance(v, (tuple, list)):
        return [_value(x) for x in v]
    if isinstance(v, Mapping):
        return {str(k): _value(x) for k, x in v.items()}
    return v  # type: ignore[return-value]  # str, int, float, bool, None


def to_json(obj: object) -> dict[str, JsonValue]:
    """The JSON object of a model dataclass, in field order."""
    out: dict[str, JsonValue] = {}
    extra: Mapping[str, JsonValue] = {}
    for f in fields(obj):  # type: ignore[arg-type]
        v = getattr(obj, f.name)
        if f.name == "extra":
            extra = v
        elif v is not None and not (isinstance(v, (tuple, list, Mapping)) and not v):
            out[f.name] = _value(v)
    for key in sorted(extra):
        out[key] = extra[key]
    return out
