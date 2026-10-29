from importlib.metadata import version

import pipeloom


def test_version_matches_metadata() -> None:
    assert pipeloom.__version__ == version("pipeloom")
