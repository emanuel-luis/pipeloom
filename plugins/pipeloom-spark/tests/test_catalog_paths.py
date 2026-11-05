"""delta_path roots: a URI is kept, a local path is resolved (no Spark started)."""

from __future__ import annotations

from pathlib import Path

import pytest

from pipeloom.engine.types import TargetRef
from pipeloom_spark.catalog import DeltaPathCatalog, SparkSessionCatalog
from pipeloom_spark.spec import DeltaPathOptions

URIS = ["s3://bucket/wh", "abfss://c@a.dfs.core.windows.net/wh", "dbfs:/wh", "gs://b/wh"]


@pytest.mark.parametrize("uri", URIS)
def test_a_uri_root_is_not_joined_to_the_project(uri: str, tmp_path: Path) -> None:
    opts = DeltaPathOptions.model_validate({"root": uri}, context={"project_dir": str(tmp_path)})
    assert opts.root == uri
    cat = DeltaPathCatalog(opts.root + "/")
    assert cat.path(TargetRef(None, "main", "t")) == f"{uri}/main/t"
    assert cat.ident(TargetRef(None, "main", "t")) == f"delta.`{uri}/main/t`"


def test_a_local_relative_root_resolves_against_the_project(tmp_path: Path) -> None:
    opts = DeltaPathOptions.model_validate({"root": "wh"}, context={"project_dir": str(tmp_path)})
    assert Path(opts.root) == tmp_path / "wh"
    assert DeltaPathCatalog(opts.root).root == (tmp_path / "wh").resolve().as_posix()


def test_delta_path_maps_a_catalog_to_a_leading_directory() -> None:
    cat = DeltaPathCatalog("s3://b/wh")
    assert cat.path(TargetRef("c", "s", "t")) == "s3://b/wh/c/s/t"
    assert cat.path(TargetRef(None, "s", "t")) == "s3://b/wh/s/t"
    with pytest.raises(ValueError, match="directory name"):
        cat.path(TargetRef("..", "s", "t"))


def test_session_catalog_ident_prefers_the_target_catalog() -> None:
    cat = SparkSessionCatalog("base")
    assert cat.ident(TargetRef("gold", "s", "t")) == "`gold`.`s`.`t`"
    assert cat.ident(TargetRef(None, "s", "t")) == "`base`.`s`.`t`"
    assert SparkSessionCatalog(None).ident(TargetRef(None, "s", "t")) == "`s`.`t`"
