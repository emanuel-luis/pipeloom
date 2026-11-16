"""scd2 statement text: pure strings, no Spark."""

from __future__ import annotations

from datetime import UTC, datetime

from pipeloom.engine.types import DeleteSpec, WriteConfig
from pipeloom_spark.sqltext import literal
from pipeloom_spark.write import scd2_counts_query, scd2_merge_statement, scd2_source

DTYPES = {"id": "int64", "label": "string", "f": "float64"}
TS = literal(datetime(2026, 2, 1, 12, tzinfo=UTC), "timestamp[us, UTC]")


def _cfg(deletes: DeleteSpec | None = None) -> WriteConfig:
    return WriteConfig("scd2", unique_key=("id",), deletes=deletes, tracked_columns=("label", "f"))


def test_scd2_source_stages_match_keys_and_changed_rows() -> None:
    nan = "CASE WHEN isnan({0}.`f`) THEN NULL ELSE {0}.`f` END"
    same = f"c.`label` <=> s.`label` AND {nan.format('c')} <=> {nan.format('s')}"
    assert scd2_source("t", ("id",), _cfg(), DTYPES) == (
        "SELECT s.*, s.`id` AS `__pipeloom_mk_id`, false AS `__pipeloom_del` FROM {src} AS s\n"
        "UNION ALL\n"
        "SELECT s.*, NULL, false FROM {src} AS s JOIN t AS c ON c.`id` = s.`id` AND "
        "c.`is_current`\n"
        f"WHERE NOT false AND NOT ({same})"
    )


def test_scd2_source_flags_deletes() -> None:
    text = scd2_source("t", ("id",), _cfg(DeleteSpec("label", "gone", "hard")), DTYPES)
    flag = f"coalesce(s.`label` = {literal('gone', 'string')}, false)"
    assert f", {flag} AS `__pipeloom_del` FROM" in text
    assert f"WHERE NOT {flag} AND NOT (" in text


def test_scd2_merge_closes_and_inserts_at_the_run_start() -> None:
    cfg = WriteConfig("scd2", unique_key=("id", "k"), tracked_columns=("label",))
    dtypes = {"id": "int64", "k": "string", "label": "string"}
    text = scd2_merge_statement("t", ("id", "k"), ("id", "k", "label"), cfg, dtypes, TS)
    head, source, tail = text.partition(scd2_source("t", ("id", "k"), cfg, dtypes))
    assert head == "MERGE INTO t AS target USING (\n"
    assert tail == (
        "\n) AS source\n"
        "ON target.`id` = source.`__pipeloom_mk_id` AND target.`k` = source.`__pipeloom_mk_k` "
        "AND target.`is_current`\n"
        "WHEN MATCHED AND (source.`__pipeloom_del` OR NOT (target.`label` <=> source.`label`)) "
        f"THEN UPDATE SET `valid_to` = {TS}, `is_current` = false\n"
        "WHEN NOT MATCHED AND NOT source.`__pipeloom_del` THEN INSERT "
        "(`id`, `k`, `label`, `valid_from`, `valid_to`, `is_current`) "
        f"VALUES (source.`id`, source.`k`, source.`label`, {TS}, NULL, true)"
    )
    assert "NULL, NULL, false FROM" in source


def test_scd2_without_tracked_columns_never_changes() -> None:
    cfg = WriteConfig("scd2", unique_key=("id",))
    assert scd2_source("t", ("id",), cfg, DTYPES).endswith("NOT (true)")


def test_scd2_counts_query() -> None:
    assert scd2_counts_query("t", ("id",), TS) == (
        "SELECT count_if(o) AS inserted, count_if(c) AS closed, count_if(c AND NOT o) AS deleted "
        f"FROM (SELECT bool_or(`valid_from` = {TS}) AS o, "
        f"bool_or(coalesce(`valid_to` = {TS}, false)) AS c FROM t "
        f"WHERE `valid_from` = {TS} OR `valid_to` = {TS} GROUP BY `id`)"
    )
