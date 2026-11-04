"""Metrics, token metadata and MERGE text: pure, no Spark."""

from __future__ import annotations

from pipeloom.engine.types import DeleteSpec, WriteConfig, WriteResult
from pipeloom_spark.delta import Commit, metrics_to_result, parse_token, token_metadata
from pipeloom_spark.write import app_id, merge_statement


def test_write_metrics_of_an_append() -> None:
    commit = Commit(
        3, "WRITE", {"numOutputRows": "5", "numOutputBytes": "900"}, None, {"mode": "Append"}
    )
    assert metrics_to_result([commit]) == WriteResult(
        rows_written=5, rows_inserted=5, bytes_written=900, target_version=3
    )


def test_overwrite_is_not_an_insert_and_replace_where_reports_deletes() -> None:
    commit = Commit(
        4,
        "WRITE",
        {"numOutputRows": "2", "numOutputBytes": "10", "numDeletedRows": "3"},
        None,
        {"mode": "Overwrite"},
    )
    got = metrics_to_result([commit])
    assert (got.rows_written, got.rows_inserted, got.rows_deleted) == (2, None, 3)


def test_merge_metrics_sum_over_commits() -> None:
    merge = Commit(
        7,
        "MERGE",
        {
            "numTargetRowsInserted": "1",
            "numTargetRowsUpdated": "2",
            "numTargetRowsDeleted": "0",
            "numTargetBytesAdded": "50",
        },
        None,
    )
    token = Commit(
        8, "WRITE", {"numOutputRows": "0", "numOutputBytes": "0"}, None, {"mode": "Append"}
    )
    got = metrics_to_result([merge, token])
    assert (got.rows_inserted, got.rows_updated, got.rows_deleted) == (1, 2, 0)
    assert (got.rows_written, got.bytes_written, got.target_version) == (3, 50, 8)


def test_missing_metrics_stay_none_and_no_commits_is_empty() -> None:
    assert metrics_to_result([Commit(1, "OPTIMIZE", {}, None)]) == WriteResult(target_version=1)
    assert metrics_to_result([]) == WriteResult()


def test_token_metadata_round_trips() -> None:
    result = WriteResult(rows_written=3, rows_inserted=1, target_version=9)
    text = token_metadata("r1:m", {"watermark": "w"}, result)
    assert parse_token(text) == ("r1:m", {"watermark": "w"}, result)
    assert parse_token(token_metadata("r1:m", {}, None)) == ("r1:m", {}, None)
    assert parse_token('{"pipeloom":{"migration_for":"r1:m"}}') is None
    assert parse_token("not json") is None and parse_token(None) is None


def test_app_id_suffixes() -> None:
    assert app_id("shop", "20260101T000000Z-abc123:orders") == "pipeloom/shop/orders"
    assert app_id("shop", "r:orders#quarantine") == "pipeloom/shop/orders#quarantine"


def test_merge_statement_shapes() -> None:
    plain = merge_statement("t", ("id",), WriteConfig("merge", unique_key=("id",)), None)
    assert "WHEN MATCHED THEN UPDATE SET *" in plain and "WHEN NOT MATCHED THEN INSERT *" in plain
    cfg = WriteConfig(
        "merge",
        unique_key=("id", "k"),
        update_condition="source.v > target.v",
        deletes=DeleteSpec("label", "gone", "hard"),
    )
    text = merge_statement("t", ("id", "k"), cfg, "string")
    assert "target.`id` = source.`id` AND target.`k` = source.`k`" in text
    assert "THEN DELETE" in text and "NOT MATCHED AND NOT coalesce" in text
    assert "coalesce(source.v > target.v, false)" in text
    soft = merge_statement(
        "t", ("id",), WriteConfig("merge", deletes=DeleteSpec("l", 1, "soft")), "int64"
    )
    assert "THEN DELETE" not in soft
