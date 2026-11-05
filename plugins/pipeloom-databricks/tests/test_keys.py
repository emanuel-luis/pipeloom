from __future__ import annotations

import hashlib
import re

from pipeloom_databricks.keys import FINALIZE_KEY, MAX_KEY_LEN, assign_keys, sanitize

VALID = re.compile(r"^[A-Za-z0-9_-]{1,100}$")


def _hash(raw: str) -> str:
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:8]


def test_sanitize_replaces_unsafe_characters() -> None:
    assert sanitize("models/staging dir.v2") == "models_staging_dir_v2"
    assert sanitize("keep-this_9") == "keep-this_9"
    assert sanitize("é") == "_"
    assert sanitize("") == "_"


def test_a_key_of_exactly_100_characters_is_kept() -> None:
    raw = "a" * 100
    assert assign_keys([raw]) == {raw: raw}


def test_a_key_of_101_characters_is_shortened_with_a_hash() -> None:
    raw = "a" * 101
    key = assign_keys([raw])[raw]
    assert key == "a" * 91 + "_" + _hash(raw)
    assert len(key) == MAX_KEY_LEN


def test_long_ids_that_share_a_prefix_stay_apart() -> None:
    first, second = "b" * 120 + "1", "b" * 120 + "2"
    keys = assign_keys([first, second])
    assert len(set(keys.values())) == 2
    assert all(VALID.match(k) for k in keys.values())


def test_every_id_in_a_collision_gets_the_hash_suffix() -> None:
    raws = ["a b", "a_b", "a.b", "other"]
    keys = assign_keys(raws)
    assert keys["other"] == "other"
    for raw in raws[:3]:
        assert keys[raw] == f"a_b_{_hash(raw)}"
    assert len(set(keys.values())) == 4
    assert assign_keys(list(reversed(raws))) == keys


def test_a_model_named_like_the_finalize_task_is_renamed() -> None:
    keys = assign_keys([FINALIZE_KEY, "x"])
    assert keys[FINALIZE_KEY] == f"{FINALIZE_KEY}_{_hash(FINALIZE_KEY)}"
    assert keys["x"] == "x"


def test_a_sanitized_id_that_becomes_the_finalize_key_is_renamed() -> None:
    raw = "pipeloom..finalize"
    assert assign_keys([raw])[raw] != FINALIZE_KEY
    assert assign_keys(["pipeloom__finalize"]) != {"pipeloom__finalize": FINALIZE_KEY}


def test_keys_are_always_valid() -> None:
    raws = ["x" * 300, "dir/with space", "éè", "ok", "a.b", "a_b"]
    assert all(VALID.match(k) for k in assign_keys(raws).values())
