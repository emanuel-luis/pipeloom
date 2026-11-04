from __future__ import annotations

import pytest

from pipeloom.builtins.filesystem import SPEC as FILESYSTEM
from pipeloom.builtins.naming import NONE_SPEC, PER_USER_SPEC, PREFIX_SPEC
from pipeloom.builtins.secrets_env import SPEC as ENV
from pipeloom.builtins.secrets_env import EnvSecretProvider
from pipeloom.diagnostics import Report
from pipeloom.engine.protocols import NamingStrategy, SecretProvider
from pipeloom.engine.types import TargetRef
from pipeloom.platform import Identity
from pipeloom.plugins import PluginRegistry
from pipeloom.plugins.builtin import BUILTINS

ME = Identity("Ana.Souza-Lima", "host")


def test_every_builtin_loads_and_matches_its_table_entry() -> None:
    report = Report()
    specs = PluginRegistry.discover(autoload=False).load_all(report)
    assert report.sorted() == ()
    got = {(g, s.kind) for g, ss in specs.items() for s in ss}
    assert got == {(g, k) for g, t in BUILTINS.items() for k in t}


def test_naming_none() -> None:
    n = NONE_SPEC.factory(NONE_SPEC.config_model())
    assert isinstance(n, NamingStrategy)
    assert n.relation("orders", None, ME) == TargetRef(None, "main", "orders")
    assert n.relation("orders", "mart", ME).relation_name == "mart.orders"


def test_naming_prefix_and_catalog() -> None:
    opts = PREFIX_SPEC.config_model.model_validate(
        {"schema": "core", "schema_prefix": "ci_", "catalog": "c"}
    )
    n = PREFIX_SPEC.factory(opts)
    assert n.relation("t", None, ME) == TargetRef("c", "ci_core", "t")
    assert n.relation("t", "mart", ME).relation_name == "c.ci_mart.t"


def test_naming_per_user_sanitizes_the_user() -> None:
    n = PER_USER_SPEC.factory(PER_USER_SPEC.config_model.model_validate({"schema": "dev"}))
    assert n.relation("t", None, ME) == TargetRef(None, "dev_ana_souza_lima", "t")
    assert n.relation("t", "mart", ME).schema == "mart_ana_souza_lima"


def test_naming_options_reject_unknown_keys() -> None:
    with pytest.raises(ValueError, match="strategy"):
        NONE_SPEC.config_model.model_validate({"strategy": "x"})


def test_env_secret_provider() -> None:
    p = EnvSecretProvider({"TOKEN": "abc"})
    assert isinstance(p, SecretProvider) and p.resolve("TOKEN") == "abc"
    with pytest.raises(KeyError, match="NOPE"):
        p.resolve("NOPE")
    assert isinstance(ENV.factory(ENV.config_model()), SecretProvider)


def test_filesystem_catalog_spec() -> None:
    assert FILESYSTEM.group == "catalogs" and FILESYSTEM.config_model(root="data").root == "data"
    assert FILESYSTEM.config_model().format == "parquet"
    with pytest.raises(ValueError):
        FILESYSTEM.config_model(format="avro")
