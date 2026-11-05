"""Opening Spark sessions. The engine never sets conf on a session it did not build."""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from contextlib import AbstractContextManager, contextmanager
from dataclasses import dataclass
from typing import Any, Literal

from pipeloom_spark.spec import SparkEngineOptions

Mode = Literal["local", "connect", "active"]

DELTA_CONF = {
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
}


@dataclass
class SessionHandle:
    spark: Any
    mode: Mode
    owned: bool
    """True when this engine built the session; only then does it stop it."""


def active_session() -> Any | None:
    """The session `SparkSession.getActiveSession()` returns, or None when there is none (or no
    pyspark)."""
    try:
        from pyspark.sql import SparkSession

        return SparkSession.getActiveSession()
    except Exception:  # noqa: BLE001 - no pyspark, or no active session: both mean none
        return None


def resolve_mode(opts: SparkEngineOptions, env: Mapping[str, str]) -> Mode:
    """`auto` picks active (Databricks runtime or an existing session), then connect (a remote
    URL in the options or SPARK_REMOTE), then local."""
    if opts.mode != "auto":
        return opts.mode
    if env.get("DATABRICKS_RUNTIME_VERSION") or active_session() is not None:
        return "active"
    if opts.remote or env.get("SPARK_REMOTE"):
        return "connect"
    return "local"


def _build(opts: SparkEngineOptions, mode: Mode, env: Mapping[str, str]) -> Any:
    from pyspark.sql import SparkSession

    if active_session() is not None:
        raise RuntimeError(
            "a Spark session already exists in this process; set mode: active to use it"
        )
    conf = dict(opts.conf)
    builder = SparkSession.builder.appName(opts.app_name)
    url = ""
    if mode == "connect":
        url = opts.remote or env.get("SPARK_REMOTE", "")
        if not url:
            raise ValueError("mode connect needs `remote` or the SPARK_REMOTE variable")
    local = mode == "local" or url.startswith("local")
    if local and opts.delta:
        conf = {**DELTA_CONF, **conf}
    for key, value in conf.items():
        builder = builder.config(key, value)
    if local and opts.delta:
        from delta import configure_spark_with_delta_pip

        builder = configure_spark_with_delta_pip(builder)
    builder = builder.remote(url) if mode == "connect" else builder.master(opts.master)
    return builder.getOrCreate()


@contextmanager
def _open(opts: SparkEngineOptions, env: Mapping[str, str]) -> Iterator[SessionHandle]:
    mode = resolve_mode(opts, env)
    if mode == "active":
        if opts.conf:
            raise ValueError("`conf` cannot be set with mode active: that session is not ours")
        spark = active_session()
        if spark is None and env.get("DATABRICKS_RUNTIME_VERSION"):
            # A wheel task starts without an active session. The runtime owns the context, so
            # this attaches to it: no conf, and it is never stopped here.
            from pyspark.sql import SparkSession

            spark = SparkSession.builder.getOrCreate()
        if spark is None:
            raise RuntimeError("mode active needs a Spark session, and none is active")
        yield SessionHandle(spark, "active", False)
        return
    spark = _build(opts, mode, env)
    try:
        yield SessionHandle(spark, mode, True)
    finally:
        spark.stop()


def open_session(
    opts: SparkEngineOptions, env: Mapping[str, str]
) -> AbstractContextManager[SessionHandle]:
    """Open a session for one invocation. Only a session built here is stopped on exit."""
    return _open(opts, env)
