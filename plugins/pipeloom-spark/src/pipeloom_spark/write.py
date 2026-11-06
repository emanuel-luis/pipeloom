"""Delta writes: append, overwrite, merge, replace_partitions and scd2, each under an idempotency
token.

Every per-write setting is a DataFrameWriter option; no Spark conf is touched. A MERGE is plain
SQL followed by an empty append that carries the token (Delta applies `txnAppId` to a MERGE only
through session confs, which would leak into concurrent units).
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from functools import reduce
from typing import Any, Final, Protocol

from pipeloom.diagnostics import JsonValue
from pipeloom.engine.errors import WriteKeyError, WriteRefusedError
from pipeloom.engine.portable import scd2_time
from pipeloom.engine.types import (
    SCD2_COLUMNS,
    DeleteSpec,
    Relation,
    Schema,
    TargetRef,
    WriteConfig,
    WriteResult,
    diff_schemas,
)
from pipeloom_spark import delta, sqltext
from pipeloom_spark.catalog import DeltaPathCatalog, SparkSessionCatalog, schema_of
from pipeloom_spark.delta import DeltaMetrics

MAX_PARTITION_TUPLES: Final = 10_000
_EPOCH: Final = datetime(1970, 1, 1, tzinfo=UTC)
_UTC_TS: Final = "timestamp[us, UTC]"


class Host(Protocol):
    """What the writer needs from the engine session."""

    spark: Any
    catalog: DeltaPathCatalog | SparkSessionCatalog

    def align(self, rel: Relation, schema: Schema) -> Relation: ...

    def persist(self, rel: Relation) -> Relation: ...


def _braces(text: str) -> str:
    return text.replace("{", "{{").replace("}", "}}")


def merge_statement(
    ident: str, keys: Sequence[str], cfg: WriteConfig, delete_dtype: str | None
) -> str:
    """The MERGE for `cfg`; the batch is the `{src}` field. A null key never matches, so such a
    row inserts. A soft delete is a plain update of the matched row."""
    on = " AND ".join(
        f"target.{sqltext.quote_ident(k)} = source.{sqltext.quote_ident(k)}" for k in keys
    )
    cond = (
        ""
        if cfg.update_condition is None
        else f" AND coalesce({_braces(cfg.update_condition)}, false)"
    )
    clauses = []
    is_delete = None
    if cfg.deletes is not None and delete_dtype is not None:
        col = f"source.{sqltext.quote_ident(cfg.deletes.column)}"
        is_delete = f"coalesce({col} = {sqltext.literal(cfg.deletes.value, delete_dtype)}, false)"
        if cfg.deletes.mode == "hard":
            clauses.append(f"WHEN MATCHED{cond} AND {is_delete} THEN DELETE")
    clauses.append(f"WHEN MATCHED{cond} THEN UPDATE SET *")
    clauses.append(
        "WHEN NOT MATCHED"
        + ("" if is_delete is None else f" AND NOT {is_delete}")
        + " THEN INSERT *"
    )
    return f"MERGE INTO {ident} AS target USING {{src}} AS source ON {on}\n" + "\n".join(clauses)


_SYSTEM: Final = frozenset(c.name for c in SCD2_COLUMNS)
_DEL: Final = "`__pipeloom_del`"


def _mk(key: str) -> str:
    return sqltext.quote_ident(f"__pipeloom_mk_{key}")


def _value(alias: str, column: str, dtype: str) -> str:
    ref = f"{alias}.{sqltext.quote_ident(column)}"
    return f"CASE WHEN isnan({ref}) THEN NULL ELSE {ref} END" if dtype.startswith("float") else ref


def _unchanged(a: str, b: str, tracked: Sequence[str], dtypes: Mapping[str, str]) -> str:
    """Null-safe equality of every tracked column (NaN counts as null); `true` with none."""
    terms = [f"{_value(a, c, dtypes[c])} <=> {_value(b, c, dtypes[c])}" for c in tracked]
    return " AND ".join(terms) or "true"


def _is_delete(alias: str, cfg: WriteConfig, dtypes: Mapping[str, str]) -> str:
    if cfg.deletes is None:
        return "false"
    lit = sqltext.literal(cfg.deletes.value, dtypes[cfg.deletes.column])
    return f"coalesce({alias}{sqltext.quote_ident(cfg.deletes.column)} = {lit}, false)"


def scd2_source(
    ident: str, keys: Sequence[str], cfg: WriteConfig, dtypes: Mapping[str, str]
) -> str:
    """The scd2 MERGE source over the batch `{src}`: every batch row with its key copied to the
    match keys `__pipeloom_mk_<k>` and its delete flag, then again with null match keys for each
    row that changes a current row, so one MERGE closes the old version and inserts the new."""
    q = sqltext.quote_ident
    is_del = _is_delete("s.", cfg, dtypes)
    marks = ", ".join(f"s.{q(k)} AS {_mk(k)}" for k in keys)
    nulls = ", ".join("NULL" for _ in keys)
    on = " AND ".join(f"c.{q(k)} = s.{q(k)}" for k in keys)
    same = _unchanged("c", "s", cfg.tracked_columns, dtypes)
    return (
        f"SELECT s.*, {marks}, {is_del} AS {_DEL} FROM {{src}} AS s\n"
        "UNION ALL\n"
        f"SELECT s.*, {nulls}, false FROM {{src}} AS s "
        f"JOIN {ident} AS c ON {on} AND c.`is_current`\n"
        f"WHERE NOT {is_del} AND NOT ({same})"
    )


def scd2_merge_statement(
    ident: str,
    keys: Sequence[str],
    business: Sequence[str],
    cfg: WriteConfig,
    dtypes: Mapping[str, str],
    ts: str,
) -> str:
    """The scd2 MERGE: close the current row of a deleted or changed key at `ts` (a SQL literal)
    and insert new keys and changed rows as current from `ts`."""
    q = sqltext.quote_ident
    on = " AND ".join(f"target.{q(k)} = source.{_mk(k)}" for k in keys)
    same = _unchanged("target", "source", cfg.tracked_columns, dtypes)
    cols = ", ".join(q(c) for c in business)
    values = ", ".join(f"source.{q(c)}" for c in business)
    return (
        f"MERGE INTO {ident} AS target USING (\n{scd2_source(ident, keys, cfg, dtypes)}\n) AS source\n"
        f"ON {on} AND target.`is_current`\n"
        f"WHEN MATCHED AND (source.{_DEL} OR NOT ({same})) THEN UPDATE SET "
        f"`valid_to` = {ts}, `is_current` = false\n"
        f"WHEN NOT MATCHED AND NOT source.{_DEL} THEN INSERT "
        f"({cols}, `valid_from`, `valid_to`, `is_current`) VALUES ({values}, {ts}, NULL, true)"
    )


def scd2_counts_query(ident: str, keys: Sequence[str], ts: str) -> str:
    """Per key, whether a row opened or closed at `ts`: opened keys are inserts, closed and
    reopened keys updates, closed only keys deletes. Rerunning the MERGE leaves it unchanged."""
    group = ", ".join(sqltext.quote_ident(k) for k in keys)
    return (
        "SELECT count_if(o) AS inserted, count_if(c) AS closed, count_if(c AND NOT o) AS deleted "
        f"FROM (SELECT bool_or(`valid_from` = {ts}) AS o, "
        f"bool_or(coalesce(`valid_to` = {ts}, false)) AS c FROM {ident} "
        f"WHERE `valid_from` = {ts} OR `valid_to` = {ts} GROUP BY {group})"
    )


def _opened(frame: Any, ts: str) -> Any:
    """`frame` plus the system columns of a current row opened at `ts`."""
    return frame.selectExpr(
        "*",
        f"{ts} AS `valid_from`",
        "CAST(NULL AS TIMESTAMP_LTZ) AS `valid_to`",
        "true AS `is_current`",
    )


def _epoch_ms(value: datetime) -> int:
    return (value.astimezone(UTC) - _EPOCH) // timedelta(milliseconds=1)


def app_id(project: str, token: str) -> str:
    """`pipeloom/<project>/<model>` with the token's `#quarantine` or `#intermediate` suffix."""
    base, sep, suffix = token.partition("#")
    model = base.rsplit(":", 1)[-1]
    return f"pipeloom/{project}/{model}{sep}{suffix}"


def _refused(ident: str) -> WriteRefusedError:
    return WriteRefusedError(
        f"Delta skipped the write to {ident}: its transaction version is not newer than the "
        "last one committed for this model"
    )


class DeltaWriter:
    def __init__(self, host: Host, project: str, *, txn_guard: bool) -> None:
        self._host = host
        self._project = project
        self._guard = txn_guard
        self._metrics = DeltaMetrics(host.spark)

    @property
    def _spark(self) -> Any:
        return self._host.spark

    # -- shared plumbing ------------------------------------------------------------------------

    def applied(self, ident: str, token: str) -> WriteResult | None:
        return delta.applied_result(self._spark, ident, token)

    def _txn(
        self, app: str, started: datetime | None, force: str | None = None, *, claimed: bool = False
    ) -> dict[str, str]:
        """The guard options. A commit with no data adds nothing, so Delta skips it unless it
        carries a transaction: `force` (a token) gives such a commit an app id of its own. Once
        a claim commit took the run's version (`claimed`), the write uses that app id too."""
        if self._guard and started is not None and not claimed:
            return {"txnAppId": app, "txnVersion": str(_epoch_ms(started))}
        if force is not None:
            return {"txnAppId": f"{app}@{force}", "txnVersion": "1"}
        return {}

    def _writer(self, frame: Any, mode: str, options: Mapping[str, str]) -> Any:
        return frame.write.format("delta").mode(mode).options(**options)

    def _resolve_skipped(self, ident: str, token: str) -> WriteResult:
        """No commit carries the token. A racing attempt may have won; else Delta refused it."""
        for limit in (delta.TOKEN_SCAN, None):
            prior = delta.applied_result(self._spark, ident, token, limit)
            if prior is not None:
                return prior
        raise _refused(ident)

    def _claim(self, target: TargetRef, ident: str, cfg: WriteConfig, token: str) -> bool:
        """Take the run's transaction version in an empty commit before a change the guard cannot
        cover (a migration, a `mergeSchema` commit, a MERGE). Raise PLM207 when Delta refuses it
        and no earlier attempt of this token claimed it. False when the guard is off."""
        if not self._guard or cfg.run_started_at is None:
            return False
        app = app_id(self._project, token)
        opts = {
            "userMetadata": delta.claim_metadata(token),
            **self._txn(app, cfg.run_started_at),
        }
        empty = self._host.catalog.read(target).limit(0)
        self._save(self._writer(empty, "append", opts), target)
        if not delta.claimed(
            self._spark, ident, token, lambda other: app_id(self._project, other) == app
        ):
            raise _refused(ident)
        return True

    def _create_empty(self, frame: Any, target: TargetRef, cfg: WriteConfig) -> Schema | None:
        """A table created by saveAsTable drops the writer's userMetadata: on the session catalog,
        create a missing table empty first so the token rides on an ordinary write. Returns the
        new table's schema, or None when nothing was created."""
        if not isinstance(self._host.catalog, SparkSessionCatalog):
            return None
        create = self._writer(frame.limit(0), "append", {})
        if cfg.partition_by:
            create = create.partitionBy(*cfg.partition_by)
        self._save(create, target)
        return schema_of(frame)

    def _run(
        self, ident: str, token: str, meta: Mapping[str, JsonValue], write: Callable[[], None]
    ) -> WriteResult:
        """Run `write` between the metrics brackets; the caller has checked the token already."""
        before = self._metrics.before_write(ident)
        write()
        result = self._metrics.after_write(ident, before, token, merge=False)
        if result is None:
            result = self._resolve_skipped(ident, token)
        return replace(result, commit_meta=dict(meta))

    def _options(
        self,
        token: str,
        meta: Mapping[str, JsonValue],
        app: str,
        started: datetime | None,
        *,
        empty: bool = False,
        claimed: bool = False,
        result: WriteResult | None = None,
        **more: str,
    ) -> dict[str, str]:
        return {
            "userMetadata": delta.token_metadata(token, meta, result),
            **self._txn(app, started, token if empty or claimed else None, claimed=claimed),
            **more,
        }

    def _save(self, writer: Any, target: TargetRef) -> None:
        self._host.catalog.save(writer, target)

    # -- schema ---------------------------------------------------------------------------------

    def _prepare_schema(
        self, frame: Any, target: TargetRef, cfg: WriteConfig, token: str, *, claim: bool = False
    ) -> tuple[Schema | None, bool, bool]:
        """Run the migration commit when columns were removed or widened, after a claim commit
        (always claimed with `claim`, when the table exists). Return the table's schema before
        the write, whether the write adds columns, and whether the run's version was claimed."""
        existing = self._host.catalog.table_schema(target)
        if existing is None:
            return None, False, False
        wanted = cfg.columns if cfg.columns is not None else schema_of(frame)
        kinds = {c.kind for c in diff_schemas(existing, wanted).changes}
        migrate = bool(kinds & {"removed", "type_changed"})
        claimed = (migrate or claim) and self._claim(
            target, self._host.catalog.ident(target), cfg, token
        )
        if migrate:
            self._migrate(target, wanted, token)
            return existing, True, claimed
        return existing, "added" in kinds, claimed

    def _migrate(self, target: TargetRef, columns: Schema, token: str) -> None:
        catalog = self._host.catalog
        old = Relation(catalog.read(target), "spark", lazy=True)
        aligned = self._host.align(old, columns).native
        parts = self._partition_columns(catalog.ident(target))
        writer = self._writer(
            aligned,
            "overwrite",
            {"overwriteSchema": "true", "userMetadata": delta.migration_metadata(token)},
        )
        if parts:
            writer = writer.partitionBy(*parts)
        self._save(writer, target)

    def _partition_columns(self, ident: str) -> list[str]:
        row = self._spark.sql(f"DESCRIBE DETAIL {ident}").select("partitionColumns").first()
        return list(row[0]) if row is not None and row[0] else []

    # -- strategies -----------------------------------------------------------------------------

    def materialize(
        self, strategy: str, rel: Relation, target: TargetRef, cfg: WriteConfig, token: str
    ) -> WriteResult:
        ident = self._host.catalog.ident(target)
        prior = self.applied(ident, token)
        if prior is not None:
            return prior
        if strategy == "overwrite":
            return self._overwrite(rel.native, target, ident, cfg, token)
        if strategy == "append":
            return self._append(rel.native, target, ident, cfg, token)
        if strategy == "replace_partitions":
            return self._replace_partitions(rel, target, ident, cfg, token)
        if strategy == "merge":
            return self._merge(rel, target, ident, cfg, token)
        if strategy == "scd2":
            return self._scd2(rel, target, ident, cfg, token)
        raise ValueError(f"spark engine does not support strategy '{strategy}'")

    def _overwrite(
        self,
        frame: Any,
        target: TargetRef,
        ident: str,
        cfg: WriteConfig,
        token: str,
        result: WriteResult | None = None,
    ) -> WriteResult:
        opts = self._options(
            token,
            cfg.commit_meta,
            app_id(self._project, token),
            cfg.run_started_at,
            result=result,
            overwriteSchema="true",
        )

        def write() -> None:
            writer = self._writer(frame, "overwrite", opts)
            if cfg.partition_by:
                writer = writer.partitionBy(*cfg.partition_by)
            self._save(writer, target)

        return self._run(ident, token, cfg.commit_meta, write)

    def _append(
        self,
        frame: Any,
        target: TargetRef,
        ident: str,
        cfg: WriteConfig,
        token: str,
        result: WriteResult | None = None,
    ) -> WriteResult:
        existing, adds, claimed = self._prepare_schema(frame, target, cfg, token)
        if existing is None:
            existing = self._create_empty(frame, target, cfg)
        more = {"mergeSchema": "true"} if adds else {}
        opts = self._options(
            token,
            cfg.commit_meta,
            app_id(self._project, token),
            cfg.run_started_at,
            empty=(not self._guard or cfg.run_started_at is None) and frame.isEmpty(),
            claimed=claimed,
            result=result,
            **more,
        )

        def write() -> None:
            writer = self._writer(frame, "append", opts)
            if existing is None and cfg.partition_by:
                writer = writer.partitionBy(*cfg.partition_by)
            self._save(writer, target)

        return self._run(ident, token, cfg.commit_meta, write)

    def _check_keys(self, frame: Any, columns: Sequence[str], what: str) -> None:
        have = set(frame.columns)
        for col in columns:
            if col not in have:
                raise WriteKeyError(f"{what} column '{col}' is missing")

    def _partition_values(self, frame: Any, columns: Sequence[str]) -> list[tuple[Any, ...]]:
        """Distinct partition tuples. Timestamps travel as epoch microseconds: collect() would
        convert them to the Python process time zone."""
        from pyspark.sql import functions as F

        types = {c.name: c.dtype for c in schema_of(frame)}
        exprs = [
            F.expr(f"unix_micros({sqltext.quote_ident(c)})").alias(c)
            if types[c] == _UTC_TS
            else F.col(sqltext.quote_ident(c))
            for c in columns
        ]
        rows = frame.select(*exprs).distinct().limit(MAX_PARTITION_TUPLES + 1).collect()
        if len(rows) > MAX_PARTITION_TUPLES:
            raise ValueError(
                f"replace_partitions touches more than {MAX_PARTITION_TUPLES} partitions; "
                "use the merge strategy"
            )
        out = []
        for row in rows:
            values = [
                None
                if v is None
                else _EPOCH + timedelta(microseconds=v)
                if types[c] == _UTC_TS
                else v
                for c, v in zip(columns, row, strict=True)
            ]
            out.append(tuple(values))
        return out

    def _replace_partitions(
        self, rel: Relation, target: TargetRef, ident: str, cfg: WriteConfig, token: str
    ) -> WriteResult:
        if not cfg.partition_by:
            raise WriteKeyError("replace_partitions needs partition_by")
        batch = self._host.persist(rel).native
        self._check_keys(batch, cfg.partition_by, "partition_by")
        existing, adds, claimed = self._prepare_schema(batch, target, cfg, token)
        if existing is None:
            existing = self._create_empty(batch, target, cfg)
        types = {c.name: c.dtype for c in schema_of(batch)}
        tuples = self._partition_values(batch, cfg.partition_by)
        base = self._options(
            token,
            cfg.commit_meta,
            app_id(self._project, token),
            cfg.run_started_at,
            empty=not tuples,
            claimed=claimed,
        )
        if adds:
            base["mergeSchema"] = "true"

        def write() -> None:
            if not tuples:  # an empty batch replaces nothing: only the token commit
                writer = self._writer(batch.limit(0), "append", base)
            else:
                predicate = sqltext.partition_predicate(
                    cfg.partition_by, tuples, [types[c] for c in cfg.partition_by]
                )
                writer = self._writer(batch, "overwrite", {**base, "replaceWhere": predicate})
            if existing is None:
                writer = writer.partitionBy(*cfg.partition_by)
            self._save(writer, target)

        return self._run(ident, token, cfg.commit_meta, write)

    def _merge(
        self, rel: Relation, target: TargetRef, ident: str, cfg: WriteConfig, token: str
    ) -> WriteResult:
        keys = cfg.unique_key
        if not keys:
            raise WriteKeyError("merge needs a unique_key")
        batch = self._host.persist(rel).native
        self._check_keys(batch, keys, "unique_key")
        self._reject_duplicates(batch, keys)
        dtypes = {c.name: c.dtype for c in schema_of(batch)}
        # Nothing data-changing runs before the claim: a MERGE carries no transaction.
        before = self._metrics.before_write(ident)
        existing, adds, claimed = self._prepare_schema(batch, target, cfg, token, claim=True)
        if existing is None:  # nothing to merge into: the first load appends, minus the deletes
            if cfg.deletes is not None and cfg.deletes.column in dtypes:
                lit = sqltext.literal(cfg.deletes.value, dtypes[cfg.deletes.column])
                col = sqltext.quote_ident(cfg.deletes.column)
                batch = batch.where(f"NOT coalesce({col} = {lit}, false)")
            return self._append(batch, target, ident, cfg, token)
        if adds:  # new columns first, in a commit of their own that carries no token
            self._save(self._writer(batch.limit(0), "append", {"mergeSchema": "true"}), target)
        soft = 0
        if cfg.deletes is not None and cfg.deletes.mode == "soft":
            soft = self._count_soft_deletes(batch, ident, keys, cfg, dtypes)
        dtype = None if cfg.deletes is None else dtypes.get(cfg.deletes.column)
        self._spark.sql(merge_statement(ident, keys, cfg, dtype), src=batch)
        result = self._metrics.after_write(ident, before, token, merge=True)
        assert result is not None  # merge=True always reports
        if soft:
            updated = max(0, (result.rows_updated or 0) - soft)
            result = replace(
                result, rows_updated=updated, rows_deleted=(result.rows_deleted or 0) + soft
            )
        return self._commit_token(batch, target, ident, cfg, token, result, before, claimed)

    def _commit_token(
        self,
        frame: Any,
        target: TargetRef,
        ident: str,
        cfg: WriteConfig,
        token: str,
        result: WriteResult,
        before: int | None,
        claimed: bool,
    ) -> WriteResult:
        """After a MERGE: an empty append that carries the token and `result`."""
        opts = {
            "userMetadata": delta.token_metadata(token, cfg.commit_meta, result),
            # After the claim, `<app>@<token>` still guards two attempts of the same token.
            **self._txn(app_id(self._project, token), cfg.run_started_at, token, claimed=claimed),
        }
        self._save(self._writer(frame.limit(0), "append", opts), target)
        if not self._metrics.token_committed(ident, before, token):
            return self._resolve_skipped(ident, token)
        return replace(result, commit_meta=dict(cfg.commit_meta))

    def _scd2(
        self, rel: Relation, target: TargetRef, ident: str, cfg: WriteConfig, token: str
    ) -> WriteResult:
        """Close and open versions with one MERGE over a staged source (ADR-0008 amendment)."""
        keys = cfg.unique_key
        if not keys:
            raise WriteKeyError("scd2 needs a unique_key")
        batch = self._host.persist(rel).native
        clash = [c for c in batch.columns if c in _SYSTEM]
        if clash:
            raise WriteKeyError(f"the batch has scd2 system column(s) {', '.join(clash)}")
        self._check_keys(batch, keys, "unique_key")
        self._check_keys(batch, cfg.tracked_columns, "tracked_columns")
        if cfg.deletes is not None:
            if cfg.deletes.mode != "hard":
                raise ValueError("scd2 supports hard deletes only")
            self._check_keys(batch, (cfg.deletes.column,), "deletes")
        nulls = " OR ".join(f"{sqltext.quote_ident(k)} IS NULL" for k in keys)
        if batch.where(nulls).limit(1).count():
            raise WriteKeyError("the batch has a null unique_key")
        self._reject_duplicates(batch, keys)
        dtypes = {c.name: c.dtype for c in schema_of(batch)}
        ts = sqltext.literal(scd2_time(cfg), _UTC_TS)
        claim_cfg = replace(
            cfg, columns=None if cfg.columns is None else (*cfg.columns, *SCD2_COLUMNS)
        )
        exists = self._host.catalog.table_schema(target) is not None
        if cfg.full_refresh or not exists:  # a first load: every live row opens a version
            live = _opened(batch.where(f"NOT {_is_delete('', cfg, dtypes)}"), ts)
            n = int(live.count())
            counts = WriteResult(n, n, 0, 0)
            write = self._overwrite if exists else self._append
            loaded = write(live, target, ident, claim_cfg, token, counts)
            return replace(loaded, rows_written=n, rows_inserted=n, rows_updated=0, rows_deleted=0)
        full = _opened(batch, ts)
        before = self._metrics.before_write(ident)
        _, adds, claimed = self._prepare_schema(full, target, claim_cfg, token, claim=True)
        if adds:  # new columns first, in a commit of their own that carries no token
            self._save(self._writer(full.limit(0), "append", {"mergeSchema": "true"}), target)
        business = list(batch.columns)
        self._spark.sql(scd2_merge_statement(ident, keys, business, cfg, dtypes, ts), src=batch)
        merged = self._metrics.after_write(ident, before, token, merge=True)
        assert merged is not None  # merge=True always reports
        row = self._spark.sql(scd2_counts_query(ident, keys, ts)).first()
        inserted, closed, deleted = int(row["inserted"]), int(row["closed"]), int(row["deleted"])
        result = replace(
            merged,
            rows_written=inserted + closed,
            rows_inserted=inserted,
            rows_updated=closed - deleted,
            rows_deleted=deleted,
        )
        return self._commit_token(full, target, ident, cfg, token, result, before, claimed)

    def _reject_duplicates(self, batch: Any, keys: Sequence[str]) -> None:
        from pyspark.sql import functions as F

        cols = [F.col(sqltext.quote_ident(k)) for k in keys]
        keyed = batch.where(reduce(lambda a, b: a & b, [c.isNotNull() for c in cols]))
        if keyed.groupBy(*cols).count().where("count > 1").limit(1).count():
            raise WriteKeyError("the batch repeats a unique_key value")

    def _count_soft_deletes(
        self,
        batch: Any,
        ident: str,
        keys: Sequence[str],
        cfg: WriteConfig,
        dtypes: Mapping[str, str],
    ) -> int:
        deletes: DeleteSpec = cfg.deletes  # type: ignore[assignment]
        col = f"source.{sqltext.quote_ident(deletes.column)}"
        lit = sqltext.literal(deletes.value, dtypes[deletes.column])
        on = " AND ".join(
            f"target.{sqltext.quote_ident(k)} = source.{sqltext.quote_ident(k)}" for k in keys
        )
        cond = "true" if cfg.update_condition is None else _braces(cfg.update_condition)
        text = (
            f"SELECT count(*) AS n FROM {{src}} AS source JOIN {ident} AS target ON {on} "
            f"WHERE coalesce({col} = {lit}, false) AND coalesce({cond}, false)"
        )
        return int(self._spark.sql(text, src=batch).first()["n"])

    # -- intermediates --------------------------------------------------------------------------

    def intermediate(
        self, rel: Relation, location: str, model: str, token: str, started: datetime | None
    ) -> WriteResult:
        ident = f"delta.{sqltext.quote_ident(location)}"
        app = f"pipeloom/{self._project}/{model}#intermediate"
        opts = self._options(token, {}, app, started, overwriteSchema="true")

        def write() -> None:
            self._writer(rel.native, "overwrite", opts).save(location)

        prior = self.applied(ident, token)
        return prior if prior is not None else self._run(ident, token, {}, write)
