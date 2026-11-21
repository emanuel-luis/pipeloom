from __future__ import annotations

from pathlib import Path

from pipeloom.plan.identity import (
    URI_MAX,
    asset_uri,
    input_assets,
    location_dataset,
    output_assets,
    relation_name,
    source_dataset,
)
from tests.plan.test_plan import compile_shop


def test_relation_name_skips_an_empty_catalog() -> None:
    assert relation_name(None, "s", "t") == "s.t"
    assert relation_name("c", "s", "t") == "c.s.t"


def test_a_long_uri_gets_a_hash_suffix_and_a_short_one_never_changes() -> None:
    kw = {"project": "p", "target": "t", "model": "m", "unique_id": "u"}
    assert (
        asset_uri("pipeloom://{project}/{relation}", relation="a b", **kw) == "pipeloom://p/a%20b"
    )
    long = asset_uri("pipeloom://{relation}", relation="x" * 2000, **kw)
    other = asset_uri("pipeloom://{relation}", relation="x" * 2001, **kw)
    assert len(long) == URI_MAX and long != other and long.startswith("pipeloom://xxx")
    assert long[-9] == "_"


def test_source_names() -> None:
    assert source_dataset("ns", table="t", location="x", project_dir=None) == ("ns", "t")
    assert source_dataset("ns", table=None, location=None) is None
    assert location_dataset("s3://b/k/f") == ("s3://b", "k/f")
    assert location_dataset("file:///a/b") == ("file", "/a/b")
    assert location_dataset("rel/f", Path("/p")) == ("file", "/p/rel/f")


def test_an_ephemeral_input_resolves_to_its_persisted_upstream() -> None:
    plan = compile_shop()
    assert len(plan.units["fct_orders"].nodes) > 1  # it inlines an ephemeral model
    inputs = input_assets(plan, "fct_orders")
    assert sorted(plan.nodes[u].name for u in inputs) == [
        "stg_customers",
        "stg_orders",
        "stg_payments",
    ]
    assert output_assets(plan, "fct_orders") == ("model.shop.fct_orders",)
