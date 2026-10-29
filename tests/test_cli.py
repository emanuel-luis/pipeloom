import subprocess
import sys

from click.testing import CliRunner

import pipeloom
from pipeloom.cli import main


def test_version_flag() -> None:
    result = CliRunner().invoke(main, ["--version"])
    assert result.exit_code == 0
    assert result.output.strip() == f"pipeloom, version {pipeloom.__version__}"


def test_python_dash_m() -> None:
    out = subprocess.run(
        [sys.executable, "-m", "pipeloom", "--version"],
        capture_output=True,
        text=True,
        check=True,
    )
    assert out.stdout.strip() == f"pipeloom, version {pipeloom.__version__}"
