#!/usr/bin/env bash
# Run the Spark plugin tests on Linux or WSL: wsl-spark-tests.sh classic|connect [pytest args...]
# PIPELOOM_WSL_VENV picks the virtualenv, kept outside the repo so a Windows .venv is left alone.
set -euo pipefail

mode="${1:?usage: wsl-spark-tests.sh classic|connect [pytest args...]}"
shift
cd "$(dirname "$0")/.."
export UV_PROJECT_ENVIRONMENT="${PIPELOOM_WSL_VENV:-$HOME/.venvs/pipeloom-spark}"
uv sync --locked --all-packages --group spark
extra=()
if [ "$mode" = "connect" ]; then
    extra=(-m spark_connect)
fi
PIPELOOM_TEST_SPARK="$mode" uv run pytest -q -p no:cacheprovider plugins/pipeloom-spark/tests \
    "${extra[@]}" "$@"
