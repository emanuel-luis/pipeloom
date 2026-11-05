"""Delta commit history: idempotency tokens in `userMetadata`, and write metrics read back from
the commits a write made. SQL text only (`DESCRIBE HISTORY`), so it works on classic, Connect and
Databricks alike."""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from typing import Any, Final

from pipeloom.diagnostics import JsonValue
from pipeloom.engine.types import WriteResult
from pipeloom_spark import errors

TOKEN_SCAN: Final = 100
"""Commits `applied_token` and the metrics window look through."""
_RESULT_FIELDS: Final = (
    "rows_written",
    "rows_inserted",
    "rows_updated",
    "rows_deleted",
    "bytes_written",
    "target_version",
)


@dataclass(frozen=True)
class Commit:
    version: int
    operation: str
    metrics: Mapping[str, str]
    user_metadata: str | None
    parameters: Mapping[str, str] = field(default_factory=dict)
    """`operationParameters`: `mode` tells an append from an overwrite."""


def history(spark: Any, ident: str, limit: int | None) -> list[Commit]:
    """The table's commits, newest first (`DESCRIBE HISTORY`)."""
    text = f"DESCRIBE HISTORY {ident}" + ("" if limit is None else f" LIMIT {int(limit)}")
    rows = (
        spark.sql(text)
        .select("version", "operation", "operationParameters", "operationMetrics", "userMetadata")
        .collect()
    )
    return [
        Commit(
            int(r["version"]),
            str(r["operation"]),
            dict(r["operationMetrics"] or {}),
            r["userMetadata"],
            dict(r["operationParameters"] or {}),
        )
        for r in rows
    ]


def _history_or_none(spark: Any, ident: str, limit: int | None) -> list[Commit] | None:
    try:
        return history(spark, ident, limit)
    except Exception as exc:
        if errors.condition(exc) in errors.MISSING:
            return None
        raise


def token_metadata(
    token: str, commit_meta: Mapping[str, JsonValue], result: WriteResult | None
) -> str:
    """The `userMetadata` text of a commit that carries `token`."""
    body: dict[str, Any] = {"v": 1, "token": token, "commit_meta": dict(commit_meta)}
    if result is not None:
        body["result"] = {f: getattr(result, f) for f in _RESULT_FIELDS}
    return json.dumps({"pipeloom": body}, sort_keys=True, separators=(",", ":"))


def migration_metadata(token: str) -> str:
    return json.dumps({"pipeloom": {"migration_for": token}}, sort_keys=True, separators=(",", ":"))


def claim_metadata(token: str) -> str:
    """The `userMetadata` of a claim commit; it has no `token` key, so `parse_token` ignores it."""
    return json.dumps({"pipeloom": {"v": 1, "claim": token}}, sort_keys=True, separators=(",", ":"))


def _owner(user_metadata: str | None) -> tuple[str, bool] | None:
    """The token a pipeloom commit carries and whether the commit is a claim; None otherwise."""
    try:
        body = json.loads(user_metadata or "")["pipeloom"]
    except (ValueError, KeyError, TypeError):
        return None
    if not isinstance(body, dict):
        return None
    is_claim = "token" not in body
    owner = body.get("claim") if is_claim else body.get("token")
    return (owner, is_claim) if isinstance(owner, str) else None


def claimed(spark: Any, ident: str, token: str, same_writer: Callable[[str], bool]) -> bool:
    """Whether a claim commit for `token` exists that no other token of the same writer
    (`same_writer`) committed after: a later run that wrote the model in between voids it.
    The scan window first, then the full history, newest first."""
    for limit in (TOKEN_SCAN, None):
        for commit in _history_or_none(spark, ident, limit) or ():
            found = _owner(commit.user_metadata)
            if found is None:
                continue
            owner, is_claim = found
            if owner == token:
                if is_claim:
                    return True
            elif same_writer(owner):
                return False
    return False


def parse_token(
    user_metadata: str | None,
) -> tuple[str, Mapping[str, JsonValue], WriteResult | None] | None:
    """The token, commit meta and stored result of a commit; None for any other commit."""
    if not user_metadata:
        return None
    try:
        body = json.loads(user_metadata)["pipeloom"]
        token = body["token"]
    except (ValueError, KeyError, TypeError):
        return None
    if not isinstance(token, str):
        return None
    meta = body.get("commit_meta")
    stored = body.get("result")
    result = _stored(stored) if isinstance(stored, dict) else None
    return token, meta if isinstance(meta, dict) else {}, result


def _stored(stored: Mapping[str, Any]) -> WriteResult:
    return WriteResult(
        rows_written=stored.get("rows_written"),
        rows_inserted=stored.get("rows_inserted"),
        rows_updated=stored.get("rows_updated"),
        rows_deleted=stored.get("rows_deleted"),
        bytes_written=stored.get("bytes_written"),
        target_version=stored.get("target_version"),
    )


def _num(metrics: Mapping[str, str], key: str) -> int | None:
    value = metrics.get(key)
    try:
        return None if value is None else int(value)
    except ValueError:
        return None


def _one(commit: Commit) -> dict[str, int | None]:
    m = commit.metrics
    if commit.operation == "MERGE":
        ins, upd = _num(m, "numTargetRowsInserted"), _num(m, "numTargetRowsUpdated")
        return {
            "rows_written": None if ins is None and upd is None else (ins or 0) + (upd or 0),
            "rows_inserted": ins,
            "rows_updated": upd,
            "rows_deleted": _num(m, "numTargetRowsDeleted"),
            "bytes_written": _num(m, "numTargetBytesAdded"),
        }
    out = _num(m, "numOutputRows")
    appended = (
        commit.operation == "CREATE TABLE AS SELECT" or commit.parameters.get("mode") == "Append"
    )
    return {
        "rows_written": out,
        "rows_inserted": out if appended else None,
        "rows_updated": None,
        "rows_deleted": _num(m, "numDeletedRows"),
        "bytes_written": _num(m, "numOutputBytes"),
    }


def metrics_to_result(commits: Sequence[Commit]) -> WriteResult:
    """The write result of `commits`: each metric summed over them, None when no commit has it.
    `target_version` is the newest version."""
    totals: dict[str, int | None] = {}
    for commit in commits:
        for key, value in _one(commit).items():
            if value is not None:
                totals[key] = (totals.get(key) or 0) + value
    version = max((c.version for c in commits), default=None)
    return WriteResult(
        rows_written=totals.get("rows_written"),
        rows_inserted=totals.get("rows_inserted"),
        rows_updated=totals.get("rows_updated"),
        rows_deleted=totals.get("rows_deleted"),
        bytes_written=totals.get("bytes_written"),
        target_version=version,
    )


def applied_result(
    spark: Any, ident: str, token: str, limit: int | None = TOKEN_SCAN
) -> WriteResult | None:
    """The result (with `skipped=True`) of the newest commit that carries `token`, else None."""
    commits = _history_or_none(spark, ident, limit)
    for commit in commits or ():
        parsed = parse_token(commit.user_metadata)
        if parsed is not None and parsed[0] == token:
            _, meta, stored = parsed
            base = stored if stored is not None else metrics_to_result([commit])
            return replace(base, skipped=True, commit_meta=dict(meta))
    return None


class DeltaMetrics:
    """`before_write` and `after_write` bracket a write; the result comes from the commits made
    in between, so no session conf and no `DeltaTable` API is needed."""

    def __init__(self, spark: Any) -> None:
        self._spark = spark

    def before_write(self, ident: str) -> int | None:
        """The latest version, None when the table does not exist."""
        commits = _history_or_none(self._spark, ident, 1)
        return commits[0].version if commits else None

    def after_write(
        self, ident: str, before: int | None, token: str, *, merge: bool
    ) -> WriteResult | None:
        """The summed metrics of the commits after `before` that carry `token`. With `merge`, the
        newest MERGE commit counts too (a SQL MERGE carries no token; a concurrent MERGE on the
        same table inside the window can be mistaken for ours). None when no commit carries the
        token and the write is not a merge."""
        floor = -1 if before is None else before
        window = [c for c in history(self._spark, ident, TOKEN_SCAN) if c.version > floor]
        mine = [c for c in window if (parse_token(c.user_metadata) or ("",))[0] == token]
        if not merge:
            return metrics_to_result(mine) if mine else None
        merges = [c for c in window if c.operation == "MERGE"][:1]
        if not merges and not mine:  # a MERGE that changed nothing may not commit
            return WriteResult(0, 0, 0, 0, 0, before)
        return metrics_to_result([*merges, *mine])

    def token_committed(self, ident: str, before: int | None, token: str) -> bool:
        return self.after_write(ident, before, token, merge=False) is not None
