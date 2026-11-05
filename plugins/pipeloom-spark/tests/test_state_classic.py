"""The `table` state store passes the state store contract on classic local Spark."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, ClassVar

import pytest

from pipeloom.platform import Clock
from pipeloom.state.types import StateStore
from pipeloom.testing.contract.state import StateStoreContract, StoreFactory
from pipeloom_spark.spec import TableStateOptions
from pipeloom_spark.state import TAKE_LOCK, DeltaStateStore

pytestmark = [pytest.mark.spark, pytest.mark.timeout(300)]

HEAVY = os.environ.get("PIPELOOM_TEST_SPARK_HEAVY") == "1"
"""The cross-process lock test starts a second JVM; CI sets this, local runs skip it."""


class TestClassicPathTable(StateStoreContract):
    subprocess_target: ClassVar[tuple[str, str] | None] = (
        ("pipeloom_spark.state:path_store", "state") if HEAVY else None
    )

    @pytest.fixture
    def store_factory(self, classic_spark: Any) -> StoreFactory:
        def build(tmp_path: Path, clock: Clock) -> StateStore:
            opts = TableStateOptions(path=(tmp_path / "state").as_posix())
            return DeltaStateStore(opts, clock=clock, env={})

        return build


def test_a_newer_schema_is_refused(classic_spark: Any, tmp_path: Path) -> None:
    location = (tmp_path / "state").as_posix()
    store = DeltaStateStore(TableStateOptions(path=location), env={})
    assert store.get_run("x") is None
    classic_spark.sql(
        f"ALTER TABLE delta.`{location}` SET TBLPROPERTIES ('pipeloom.state.schema' = '2')"
    )
    with pytest.raises(RuntimeError, match="state schema 2"):
        DeltaStateStore(TableStateOptions(path=location), env={}).get_run("x")


def test_a_replayed_lock_merge_takes_its_own_row(classic_spark: Any, tmp_path: Path) -> None:
    """A retry after a lost reply re-runs the MERGE that already committed: same owner wins."""
    store = DeltaStateStore(TableStateOptions(path=(tmp_path / "state").as_posix()), env={})
    row = {
        "kind": "lock",
        "key": '["run"]',
        "owner": "h:1:a",
        "acquired_us": 0,
        "expires_us": 10**18,
    }
    assert store._merge(TAKE_LOCK, row) == 1
    assert store._merge(TAKE_LOCK, row) == 1
    assert store._merge(TAKE_LOCK, {**row, "owner": "h:2:b"}) == 0
