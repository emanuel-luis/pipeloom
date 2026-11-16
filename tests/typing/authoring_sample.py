"""Type-checked by `mypy --strict` in CI, never run or collected by pytest: the `@model` overloads
keep the decorated function's type, and the helpers and aliases have typed signatures."""

from __future__ import annotations

from collections.abc import Callable

from pipeloom import (
    Column,
    DataTest,
    Deletes,
    Expectation,
    Incremental,
    ModelContext,
    Quality,
    Write,
    accepted_values,
    between,
    freshness,
    model,
    not_null,
    ref,
    regex,
    row_count,
    source,
    unique,
    var,
)
from pipeloom.config.models import CheckConfig, DataCheckConfig


@model
def bare(ctx: ModelContext, orders: list[int]) -> list[int]:
    return orders


@model(
    name="enriched",
    inputs={"orders": ref("stg_orders"), "customer": source("tpch", "customer"), "x": "y"},
    materialized="incremental",
    incremental=Incremental(input="orders", column="order_date"),
    write=Write(strategy="merge", unique_key=["id"], deletes=Deletes(column="gone")),
    partition_by=["d"],
    columns=[Column(name="id", dtype="int64", nullable=False), {"name": "d", "dtype": "date"}],
    expectations=[Expectation(name="keys", check=not_null("id"), on_violation="fail")],
    data_tests=[
        DataTest(name="id_unique", check=DataCheckConfig(unique=["id"])),
        {
            "name": "has_parent",
            "check": {"relationships": {"column": "id", "to": {"ref": "p"}, "field": "id"}},
        },
    ],
    quality=Quality(quarantine=True),
    tags=["silver"],
    retries=2,
    retry_delay="30s",
    timeout=60.0,
    frame_api="narwhals",
    meta={"path": var("root", default="data")},
)
def enriched(orders: list[int], customer: list[str]) -> int:
    return len(orders) + len(customer)


keeps_type: Callable[[ModelContext, list[int]], list[int]] = bare
keeps_type_too: Callable[[list[int], list[str]], int] = enriched
checks: list[CheckConfig] = [
    not_null("a", "b"),
    unique("a"),
    accepted_values("s", ["x"]),
    between("p", min=0, max=1.5),
    regex("e", ".+"),
    row_count(min=1),
    freshness("t", "1h"),
]
