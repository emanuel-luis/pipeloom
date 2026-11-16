"""Which source locations the runner resolves against the project directory."""

from __future__ import annotations

import pytest

from pipeloom.runtime.runner import _ABSOLUTE


@pytest.mark.parametrize(
    "location",
    [
        "/data/nation.csv",
        "/Volumes/cat/schema/vol/nation.csv",
        "dbfs:/Volumes/cat/schema/vol/nation.csv",
        "file:/tmp/nation.csv",
        "file:///tmp/nation.csv",
        "s3://bucket/nation.csv",
        "C:/data/nation.csv",
    ],
)
def test_kept_as_written(location: str) -> None:
    assert _ABSOLUTE.match(location)


@pytest.mark.parametrize("location", ["data/nation.csv", "./data/nation.csv", "nation.csv"])
def test_resolved_against_the_project(location: str) -> None:
    assert not _ABSOLUTE.match(location)
