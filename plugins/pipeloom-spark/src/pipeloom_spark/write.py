"""Delta writes: append, overwrite, merge and replace_partitions, each under an idempotency token.

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
from pipeloom.engine.types import (
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


def _epoch_ms(value: datetime) -> int:
    return (value.astimezone(UTC) - _EPOCH) // timedelta(milliseconds=1)


def app_id(project: str, token: str) -> str:
    """`pipeloom/<project>/<model>` with the token's `#quarantine` or `#intermediate` suffix."""
    base, sep, suffix = token.partition("#")
    model = base.rsplit(":", 1)[-1]
    return f"pipeloom/{project}/{model}{sep}{suffix}"


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

    def _txn(self, app: str, started: datetime | None, force: str | None = None) -> dict[str, str]:
        """The guard options. A commit with no data adds nothing, so Delta skips it unless it
        carries a transaction: `force` (a token) gives such a commit an app id of its own."""
        if self._guard and started is not None:
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
        raise WriteRefusedError(
            f"Delta skipped the write to {ident}: its transaction version is not newer than the "
            "last one committed for this model"
        )

    def _run(
        self, ident: str, token: str, meta: Mapping[str, JsonValue], write: Callable[[], None]
    ) -> WriteResult:
        prior = self.applied(ident, token)
        if prior is not None:
            return prior
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
        **more: str,
    ) -> dict[str, str]:
        return {
            "userMetadata": delta.token_metadata(token, meta, None),
            **self._txn(app, started, token if empty else None),
            **more,
        }

    def _save(self, writer: Any, target: TargetRef) -> None:
        self._host.catalog.save(writer, target)

    # -- schema ---------------------------------------------------------------------------------

    def _prepare_schema(
        self, frame: Any, target: TargetRef, cfg: WriteConfig, token: str
    ) -> tuple[Schema | None, bool]:
        """Run the migration commit when columns were removed or widened; return the table's
        schema before the write and whether the write adds columns."""
        existing = self._host.catalog.table_schema(target)
        if existing is None:
            return None, False
        wanted = cfg.columns if cfg.columns is not None else schema_of(frame)
        kinds = {c.kind for c in diff_schemas(existing, wanted).changes}
        if kinds & {"removed", "type_changed"}:
            self._migrate(target, wanted, token)
            return existing, True
        return existing, "added" in kinds

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
        raise ValueError(f"spark engine does not support strategy '{strategy}'")

    def _overwrite(
        self, frame: Any, target: TargetRef, ident: str, cfg: WriteConfig, token: str
    ) -> WriteResult:
        opts = self._options(
            token,
            cfg.commit_meta,
            app_id(self._project, token),
            cfg.run_started_at,
            overwriteSchema="true",
        )

        def write() -> None:
            writer = self._writer(frame, "overwrite", opts)
            if cfg.partition_by:
                writer = writer.partitionBy(*cfg.partition_by)
            self._save(writer, target)

        return self._run(ident, token, cfg.commit_meta, write)

    def _append(
        self, frame: Any, target: TargetRef, ident: str, cfg: WriteConfig, token: str
    ) -> WriteResult:
        existing, adds = self._prepare_schema(frame, target, cfg, token)
        if existing is None and isinstance(self._host.catalog, SparkSessionCatalog):
            # A table created by saveAsTable drops the writer's userMetadata: create it empty
            # first, so the token rides on an ordinary append.
            create = self._writer(frame.limit(0), "append", {})
            if cfg.partition_by:
                create = create.partitionBy(*cfg.partition_by)
            self._save(create, target)
            existing = schema_of(frame)
        more = {"mergeSchema": "true"} if adds else {}
        opts = self._options(
            token,
            cfg.commit_meta,
            app_id(self._project, token),
            cfg.run_started_at,
            empty=(not self._guard or cfg.run_started_at is None) and frame.isEmpty(),
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
        existing, adds = self._prepare_schema(batch, target, cfg, token)
        types = {c.name: c.dtype for c in schema_of(batch)}
        tuples = self._partition_values(batch, cfg.partition_by)
        base = self._options(
            token,
            cfg.commit_meta,
            app_id(self._project, token),
            cfg.run_started_at,
            empty=not tuples,
            **({"mergeSchema": "true"} if adds else {}),
        )

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
        existing, adds = self._prepare_schema(batch, target, cfg, token)
        if existing is None:  # nothing to merge into: the first load creates the table
            return self._append(batch, target, ident, cfg, token)
        before = self._metrics.before_write(ident)
        if adds:  # new columns first, in a commit of their own that carries no token
            self._save(self._writer(batch.limit(0), "append", {"mergeSchema": "true"}), target)
        dtypes = {c.name: c.dtype for c in schema_of(batch)}
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
        opts = {
            "userMetadata": delta.token_metadata(token, cfg.commit_meta, result),
            **self._txn(app_id(self._project, token), cfg.run_started_at, token),
        }
        self._save(self._writer(batch.limit(0), "append", opts), target)
        if not self._metrics.token_committed(ident, before, token):
            return replace(self._resolve_skipped(ident, token))
        return replace(result, commit_meta=dict(cfg.commit_meta))

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
