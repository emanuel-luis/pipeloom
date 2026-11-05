#!/usr/bin/env bash
# Run the Airflow plugin tests on Linux or WSL: wsl-airflow-tests.sh [pytest args...]
# PIPELOOM_WSL_VENV picks the virtualenv, kept outside the repo so a Windows .venv is left alone.
# Airflow gets a throwaway AIRFLOW_HOME and a sqlite database under it, and no connection or
# secrets backend from the calling environment, so no running Airflow is ever reachable.
set -euo pipefail

cd "$(dirname "$0")/.."
# A non-login shell (wsl -e bash) misses the user's uv install directories.
export PATH="$HOME/.local/bin:$HOME/.cargo/bin:$PATH"
export UV_PROJECT_ENVIRONMENT="${PIPELOOM_WSL_VENV:-$HOME/.venvs/pipeloom-airflow}"
AIRFLOW_HOME="$(mktemp -d)"
export AIRFLOW_HOME
trap 'rm -rf "$AIRFLOW_HOME"' EXIT
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="sqlite:///$AIRFLOW_HOME/airflow.db"
export AIRFLOW__CORE__LOAD_EXAMPLES=false
export PIPELOOM_TEST_AIRFLOW=1
for name in $(env | sed -n 's/^\(AIRFLOW_CONN_[^=]*\|AIRFLOW__SECRETS__[^=]*\)=.*/\1/p'); do
    unset "$name"
done
# Sources on /mnt/<drive> import several times slower than on a Linux file system, so the parse
# budgets get a factor here; CI runs them unscaled.
export PIPELOOM_TEST_PARSE_BUDGET_SCALE="${PIPELOOM_TEST_PARSE_BUDGET_SCALE:-3}"
uv sync --locked --all-packages --group airflow
# PIPELOOM_AIRFLOW_PIN (for example 3.2.2) installs that Airflow over the locked one, to test the
# floor; give it its own PIPELOOM_WSL_VENV so the locked environment stays as it is.
if [ -n "${PIPELOOM_AIRFLOW_PIN:-}" ]; then
    uv pip install --python "$UV_PROJECT_ENVIRONMENT" "apache-airflow==$PIPELOOM_AIRFLOW_PIN"
fi
uv run --no-sync pytest -q -p no:cacheprovider plugins/pipeloom-airflow/tests "$@"
