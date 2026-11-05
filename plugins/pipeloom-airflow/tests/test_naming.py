from __future__ import annotations

import hashlib

from pipeloom_airflow.naming import (
    FINALIZE_ID,
    TASK_ID_MAX,
    airflow_run_id,
    asset_uri,
    task_ids,
)


def test_run_id_vectors() -> None:
    run = "manual__2026-11-05T10:00:00+00:00"
    assert airflow_run_id("shop", run) == "af-f4c034e0fd5416f5"
    assert airflow_run_id("shop", run, "g1") == "af-9f67ccf8cd211856"
    assert airflow_run_id("a", "bc") == "af-40bb547d936bbd31"
    assert airflow_run_id("ab", "c") == "af-6c032e631d39a14d"


def test_ids_replace_unsafe_characters() -> None:
    assert task_ids(["a b", "c/d", "ok-1.x"], reserved=()) == {
        "a b": "a_b",
        "c/d": "c_d",
        "ok-1.x": "ok-1.x",
    }


def test_a_long_id_is_cut_and_hashed() -> None:
    raw = "x" * 250
    out = task_ids([raw], reserved=())[raw]
    assert len(out) == TASK_ID_MAX
    assert out == "x" * 191 + "_" + hashlib.sha256(raw.encode()).hexdigest()[:8]
    assert out.endswith("_086d4a1c")


def test_every_colliding_id_gets_a_suffix() -> None:
    out = task_ids(["a b", "a_b", "c"], reserved=())
    assert out["c"] == "c"
    assert out["a b"] != out["a_b"]
    assert all(out[k].startswith("a_b_") and len(out[k]) == 12 for k in ("a b", "a_b"))


def test_a_reserved_id_gets_a_suffix() -> None:
    out = task_ids([FINALIZE_ID, "x"], reserved={FINALIZE_ID})
    assert out[FINALIZE_ID] != FINALIZE_ID and out[FINALIZE_ID].startswith(FINALIZE_ID + "_")
    assert out["x"] == "x"


def test_repeated_raw_names_are_one_entry() -> None:
    assert task_ids(["a", "a"], reserved=()) == {"a": "a"}


def test_asset_uri_encodes_values() -> None:
    uri = asset_uri(
        "pipeloom://{project}/{target}/{relation}",
        project="shop",
        target="dev",
        model="m",
        relation="main.sales/x y",
        unique_id="model.shop.m",
    )
    assert uri == "pipeloom://shop/dev/main.sales%2Fx%20y"
    assert (
        asset_uri("{unique_id}", project="p", target="t", model="m", relation="r", unique_id="u")
        == "u"
    )
