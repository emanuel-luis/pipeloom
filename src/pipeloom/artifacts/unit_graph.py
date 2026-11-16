"""The unit graph (`pipeloom.unit_graph/1`, ADR-0049): the execution units of a plan, their edges
and the command line of each, so an orchestrator needs no code of its own to read the manifest.

Everything comes from the plan (compiled or read from a manifest), never from the config models.
"""

from __future__ import annotations

from typing import Any, Final, Literal
from urllib.parse import quote

from pydantic import BaseModel, ConfigDict, Field

from pipeloom import __version__
from pipeloom.graph.dag import Graph
from pipeloom.plan.model import Plan, PlanNode

UNIT_GRAPH_SCHEMA: Final = "pipeloom.unit_graph/1"
PLACEHOLDERS: Final = ("project_dir", "manifest", "run_id")
"""Written as `{name}` in an argv; the orchestrator substitutes them."""


class _Closed(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", populate_by_name=True)


class Relation(_Closed):
    catalog: str | None
    schema_: str = Field(alias="schema")
    name: str


class Unit(_Closed):
    unit_id: str
    unique_id: str
    """The persisted model the unit writes."""
    nodes: tuple[str, ...]
    """Unique ids in run order: inlined ephemeral models first, the persisted model last."""
    materialized: str
    relation: Relation | None
    relation_name: str | None
    assets: tuple[str, ...]
    """`pipeloom://{project}/{target}/{relation_name}`; none for intermediates."""
    parents: tuple[str, ...]
    """Selected unit ids this unit reads from."""
    tags: tuple[str, ...]
    retries: int
    retry_delay_s: float
    retry_on: str
    timeout_s: float | None
    argv: tuple[str, ...]


class Edge(_Closed):
    from_: str = Field(alias="from")
    to: str


class Selectors(_Closed):
    select: tuple[str, ...]
    exclude: tuple[str, ...]


class Finalize(_Closed):
    argv: tuple[str, ...]


class UnitGraph(_Closed):
    schema_: str = Field(default=UNIT_GRAPH_SCHEMA, alias="schema")
    pipeloom_version: str
    project: str
    target: str
    plan_hash: str
    selectors: Selectors
    placeholders: tuple[str, ...]
    units: tuple[Unit, ...]
    """Topological order, ties broken by unit id."""
    edges: tuple[Edge, ...]
    finalize: Finalize


def asset_uri(project: str, target: str, relation_name: str) -> str:
    """Each part percent-encoded with `._-~` kept, the rule of the Airflow plugin's default
    `asset_uri` (reproduced here: the core never imports a plugin)."""
    parts = (quote(p, safe="._-~") for p in (project, target, relation_name))
    return "pipeloom://{}/{}/{}".format(*parts)


def _unit(plan: Plan, node: PlanNode, unit_id: str, parents: tuple[str, ...]) -> Unit:
    meta, c, rel = plan.metadata, node.config, node.relation
    relation = None
    assets: tuple[str, ...] = ()
    if rel is not None:
        relation = Relation(catalog=rel.catalog, schema_=rel.schema, name=rel.name)
        if node.materialized != "intermediate":
            assets = (asset_uri(meta.project, meta.target, rel.relation_name),)
    return Unit(
        unit_id=unit_id,
        unique_id=node.unique_id,
        nodes=plan.units[unit_id].nodes,
        materialized=node.materialized,
        relation=relation,
        relation_name=None if rel is None else rel.relation_name,
        assets=assets,
        parents=parents,
        tags=node.tags,
        retries=c.retries,
        retry_delay_s=float(c.retry_delay),
        retry_on=c.retry_on,
        timeout_s=None if c.timeout is None else float(c.timeout),
        argv=(
            "pipeloom",
            "run",
            *_common(plan),
            "--select",
            f"unit:{unit_id}",
            "--run-id",
            "{run_id}",
            "--expect-plan-hash",
            meta.plan_hash,
        ),
    )


def _common(plan: Plan) -> tuple[str, ...]:
    target = plan.metadata.target
    return ("--project-dir", "{project_dir}", "--target", target, "--manifest", "{manifest}")


def build_unit_graph(plan: Plan) -> UnitGraph:
    """The units whose persisted node the plan selects, with parents and edges among them."""
    meta, sel = plan.metadata, plan.selection
    chosen = set(sel.selected)
    scope = {u for u, unit in plan.units.items() if unit.nodes[-1] in chosen}
    parents = {u: tuple(p for p in plan.units[u].parents if p in scope) for u in scope}
    children = {u: tuple(c for c in scope if u in parents[c]) for u in scope}
    kinds: dict[str, Literal["model", "source"]] = dict.fromkeys(scope, "model")
    order = Graph(parents, children, kinds).topo_order()
    selectors: list[str] = []
    for s in sel.select:
        selectors += ["--select", s]
    for s in sel.exclude:
        selectors += ["--exclude", s]
    return UnitGraph(
        pipeloom_version=__version__,
        project=meta.project,
        target=meta.target,
        plan_hash=meta.plan_hash,
        selectors=Selectors(select=sel.select, exclude=sel.exclude),
        placeholders=PLACEHOLDERS,
        units=tuple(
            _unit(plan, plan.nodes[plan.units[u].nodes[-1]], u, parents[u]) for u in order
        ),
        edges=tuple(Edge(from_=p, to=u) for u in order for p in parents[u]),
        finalize=Finalize(
            argv=(
                "pipeloom",
                "finalize",
                *_common(plan),
                *selectors,
                "--run-id",
                "{run_id}",
                "--expect-plan-hash",
                meta.plan_hash,
            )
        ),
    )


def unit_graph_dict(plan: Plan) -> dict[str, Any]:
    """The document as JSON data."""
    return build_unit_graph(plan).model_dump(mode="json", by_alias=True)
