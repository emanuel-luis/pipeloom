"""UnityCatalog against a fake session that records SQL and catalog calls. No Spark starts."""

from __future__ import annotations

from typing import Any

import pytest

from pipeloom.engine.types import Namespace, TargetRef
from pipeloom_databricks.spec import UNITY_SPEC, UnityOptions
from pipeloom_databricks.unity import UnityCatalog


class FakeCatalogApi:
    def __init__(self, databases: set[str], tables: set[str]) -> None:
        self.databases, self.tables = databases, tables
        self.calls: list[tuple[str, str]] = []

    def databaseExists(self, name: str) -> bool:
        self.calls.append(("databaseExists", name))
        return name in self.databases

    def tableExists(self, name: str) -> bool:
        self.calls.append(("tableExists", name))
        return name in self.tables


class FakeSpark:
    def __init__(self, databases: set[str] | None = None, tables: set[str] | None = None) -> None:
        self.catalog = FakeCatalogApi(databases or set(), tables or set())
        self.statements: list[str] = []
        self.read_tables: list[str] = []

    def sql(self, text: str) -> None:
        self.statements.append(text)

    def table(self, name: str) -> str:
        self.read_tables.append(name)
        return name


class FakeWriter:
    def __init__(self) -> None:
        self.saved: list[str] = []

    def saveAsTable(self, name: str) -> None:
        self.saved.append(name)


def attached(spark: FakeSpark) -> UnityCatalog:
    catalog = UnityCatalog("main")
    catalog._spark = spark  # what `attach` binds for the length of a run
    return catalog


def test_ident_is_three_part_and_backticked_with_the_option_catalog() -> None:
    catalog = attached(FakeSpark())
    assert catalog.ident(TargetRef(None, "sales", "orders")) == "`main`.`sales`.`orders`"
    assert catalog.ident(TargetRef("dev", "sa`les", "o")) == "`dev`.`sa``les`.`o`"


def test_read_save_and_exists_use_the_identifier() -> None:
    spark = FakeSpark(tables={"`main`.`s`.`t`"})
    catalog = attached(spark)
    ref = TargetRef(None, "s", "t")
    assert catalog.read(ref) == "`main`.`s`.`t`"
    writer = FakeWriter()
    catalog.save(writer, ref)
    assert writer.saved == ["`main`.`s`.`t`"]
    assert catalog.exists(ref) and not catalog.exists(TargetRef(None, "s", "u"))


def test_ensure_namespace_creates_only_an_absent_schema() -> None:
    spark = FakeSpark(databases={"`main`.`there`"})
    catalog = attached(spark)
    catalog.ensure_namespace(Namespace(None, "there"))
    assert spark.statements == []
    catalog.ensure_namespace(Namespace(None, "new"))
    assert spark.statements == ["CREATE SCHEMA IF NOT EXISTS `main`.`new`"]
    assert spark.catalog.calls == [
        ("databaseExists", "`main`.`there`"),
        ("databaseExists", "`main`.`new`"),
    ]


def test_it_works_only_inside_a_session() -> None:
    with pytest.raises(RuntimeError):
        UnityCatalog("main").ensure_namespace(Namespace(None, "s"))


def test_spec_builds_a_catalog_for_spark_only_with_views() -> None:
    assert (UNITY_SPEC.group, UNITY_SPEC.kind) == ("catalogs", "unity")
    assert UNITY_SPEC.compatible_engines == frozenset({"spark"})
    assert UNITY_SPEC.capabilities.views is True
    built: Any = UNITY_SPEC.factory(UnityOptions(catalog="prod"))
    assert isinstance(built, UnityCatalog) and built.catalog == "prod"


def test_the_catalog_option_is_required() -> None:
    with pytest.raises(ValueError, match="catalog"):
        UnityOptions.model_validate({})
