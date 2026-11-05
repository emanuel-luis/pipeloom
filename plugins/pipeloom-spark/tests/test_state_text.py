"""The `table` state store's SQL text and options: pure, no Spark."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from pipeloom_spark.spec import TableStateOptions
from pipeloom_spark.sqltext import literal
from pipeloom_spark.state import CAS, TAKE_LOCK, UPSERT, create_sql, merge_sql, release_sql


def test_merge_matches_on_kind_and_key() -> None:
    assert merge_sql("delta.`/s`", UPSERT).startswith(
        "MERGE INTO delta.`/s` AS t USING {src} AS s ON t.kind = s.kind AND t.key = s.key "
        "WHEN MATCHED THEN UPDATE SET t.kind = s.kind, t.key = s.key,"
    )
    assert "expected_key" not in UPSERT.split("INSERT")[1]


def test_cas_swaps_on_expected_or_a_replay_and_inserts_only_from_none() -> None:
    assert CAS.startswith(
        "WHEN MATCHED AND (t.cas_key <=> s.expected_key OR (t.cas_key = s.cas_key AND "
        "t.run_id = s.run_id)) THEN UPDATE SET "
    )
    assert "WHEN NOT MATCHED AND s.expected_key IS NULL THEN INSERT (kind, key," in CAS


def test_lock_takes_only_an_expired_lease() -> None:
    assert TAKE_LOCK.startswith("WHEN MATCHED AND t.expires_us <= s.acquired_us THEN UPDATE")
    assert "WHEN NOT MATCHED THEN INSERT" in TAKE_LOCK


def test_create_is_partitioned_and_versioned() -> None:
    text = create_sql("ops.state")
    assert text.startswith("CREATE TABLE IF NOT EXISTS ops.state (`kind` STRING, `key` STRING,")
    assert text.endswith(
        "USING DELTA PARTITIONED BY (kind) TBLPROPERTIES ('pipeloom.state.schema' = '1')"
    )


def test_release_filters_on_the_owner() -> None:
    key = '["run"]'
    assert release_sql("t", key, "h:1:ab") == (
        f"DELETE FROM t WHERE kind = {literal('lock', 'string')} AND "
        f"key = {literal(key, 'string')} AND owner = {literal('h:1:ab', 'string')}"
    )


@pytest.mark.parametrize("kwargs", [{}, {"table": "a.b", "path": "/p"}])
def test_options_need_exactly_one_location(kwargs: dict[str, str]) -> None:
    with pytest.raises(ValidationError, match="exactly one"):
        TableStateOptions(**kwargs)
