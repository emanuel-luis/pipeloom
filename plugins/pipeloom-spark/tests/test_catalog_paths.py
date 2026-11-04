"""delta_path roots: a URI is kept, a local path is resolved (no Spark started)."""

from __future__ import annotations

from pathlib import Path

import pytest

from pipeloom.engine.types import TargetRef
from pipeloom_spark.catalog import DeltaPathCatalog
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
