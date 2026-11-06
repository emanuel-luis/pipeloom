"""`pipeloom new model`: kinds, folders, usage errors, and models that validate."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
import yaml
from click.testing import CliRunner
from jsonschema import Draft202012Validator

from pipeloom.cli import main
from pipeloom.config.schema import SCHEMA_BASE_URL
from tests.test_init_editor import MODELINE, installed_schemas


def cli(*args: str) -> Any:
    return CliRunner().invoke(main, list(args), env={})


def make_project(tmp_path: Path, engine: str) -> Path:
    out = tmp_path / engine / "shop"
    assert cli("init", str(out), "--engine", engine, "--no-input").exit_code == 0
    return out


@pytest.fixture
def polars(tmp_path: Path) -> Path:
    return make_project(tmp_path, "polars")


@pytest.fixture
def pandas(tmp_path: Path) -> Path:
    return make_project(tmp_path, "pandas")


def usage_error(result: Any, text: str) -> None:
    assert result.exit_code == 2, result.output
    assert text in result.stderr


def check_new_yaml(path: Path) -> dict[str, Any]:
    text = path.read_text(encoding="utf-8")
    assert text.startswith(f"{MODELINE}{SCHEMA_BASE_URL}models.json\n")
    assert "{" not in text
    data: dict[str, Any] = yaml.safe_load(text)
    Draft202012Validator(installed_schemas()["models.json"]).validate(data)
    return data


def test_sql_model_on_polars(polars: Path) -> None:
    result = cli("new", "model", "extra", "--sql", "--project-dir", str(polars))
    assert result.exit_code == 0, result.stderr
    assert result.stdout == "models/extra.yml\n"
    entry = check_new_yaml(polars / "models" / "extra.yml")["models"][0]
    assert entry == {"name": "extra", "sql": "select 1 as id\n"}
    assert "#     ref: orders" in (polars / "models" / "extra.yml").read_text(encoding="utf-8")
    result = cli("validate", "--project-dir", str(polars))
    assert result.exit_code == 0, result.stderr
    assert "PLK602" not in result.stderr
    assert "extra" in cli("ls", "--project-dir", str(polars)).stdout.split()


def test_python_model_on_pandas(pandas: Path) -> None:
    result = cli("new", "model", "extra", "--python", "--project-dir", str(pandas))
    assert result.exit_code == 0, result.stderr
    assert result.stdout == "src/shop_models/extra.py\nmodels/extra.yml\n"
    entry = check_new_yaml(pandas / "models" / "extra.yml")["models"][0]
    assert entry == {"name": "extra", "python": "shop_models.extra:build"}
    stub = (pandas / "src" / "shop_models" / "extra.py").read_text(encoding="utf-8")
    assert "def build(ctx: Any, **inputs: Any) -> Any:" in stub
    assert "NotImplementedError" in stub and "src/shop_models/extra.py" in stub
    result = cli("validate", "--project-dir", str(pandas))
    assert result.exit_code == 0, result.stderr
    assert "PLK602" not in result.stderr
    assert "extra" in cli("ls", "--project-dir", str(pandas)).stdout.split()


def test_default_kind_follows_the_engine(polars: Path, pandas: Path) -> None:
    assert "sql:" in cli("new", "model", "a", "--project-dir", str(polars)).output + (
        polars / "models" / "a.yml"
    ).read_text(encoding="utf-8")
    result = cli("new", "model", "a", "--project-dir", str(pandas))
    assert result.exit_code == 0, result.stderr
    assert "python: shop_models.a:build" in (pandas / "models" / "a.yml").read_text(
        encoding="utf-8"
    )


def test_explicit_flag_beats_the_default(pandas: Path) -> None:
    # pandas has no SQL, but the flag is the user's call: validation reports it (PLC502).
    assert cli("new", "model", "a", "--sql", "--project-dir", str(pandas)).exit_code == 0
    assert "sql:" in (pandas / "models" / "a.yml").read_text(encoding="utf-8")


def test_python_on_polars_needs_python_paths(polars: Path) -> None:
    result = cli("new", "model", "a", "--python", "--project-dir", str(polars))
    usage_error(result, "python_paths: [src]")
    assert not (polars / "models" / "a.yml").exists()


def test_python_package_init_is_created_once(pandas: Path) -> None:
    init = pandas / "src" / "shop_models" / "__init__.py"
    init.unlink()
    assert cli("new", "model", "a", "--python", "--project-dir", str(pandas)).exit_code == 0
    assert init.is_file()
    init.write_text("# mine\n", encoding="utf-8")
    result = cli("new", "model", "b", "--python", "--project-dir", str(pandas))
    assert "__init__.py" not in result.stdout
    assert init.read_text(encoding="utf-8") == "# mine\n"


def test_folder(polars: Path) -> None:
    result = cli("new", "model", "a", "--folder", "gold/sub", "--project-dir", str(polars))
    assert result.exit_code == 0, result.stderr
    assert result.stdout == "models/gold/sub/a.yml\n"
    assert cli("validate", "--project-dir", str(polars)).exit_code == 0


def test_unknown_default_target_falls_back_to_sql(polars: Path) -> None:
    # default_target is checked when a target is bound, not at load: no traceback here.
    project = polars / "pipeloom.yml"
    project.write_text(
        project.read_text(encoding="utf-8") + "default_target: nope\n", encoding="utf-8"
    )
    result = cli("new", "model", "a", "--project-dir", str(polars))
    assert result.exit_code == 0, result.output
    assert "sql:" in (polars / "models" / "a.yml").read_text(encoding="utf-8")


@pytest.mark.parametrize("folder", ["..", "../x", "a/../../x", "/abs", "a\\..\\..\\x", "C:/x"])
def test_folder_must_stay_inside_models(polars: Path, folder: str) -> None:
    usage_error(
        cli("new", "model", "a", "--folder", folder, "--project-dir", str(polars)),
        "must stay inside models",
    )


def test_sql_and_python_conflict(polars: Path) -> None:
    result = cli("new", "model", "a", "--sql", "--python", "--project-dir", str(polars))
    usage_error(result, "cannot be used together")


@pytest.mark.parametrize("name", ["1x", "a-b", "a b"])
def test_bad_name(polars: Path, name: str) -> None:
    usage_error(cli("new", "model", name, "--project-dir", str(polars)), "not a valid model name")


def test_existing_model_name(polars: Path) -> None:
    usage_error(cli("new", "model", "orders_clean", "--project-dir", str(polars)), "already exists")


def test_existing_target_file(polars: Path) -> None:
    (polars / "models" / "a.yml").write_text("sources: []\n", encoding="utf-8")
    usage_error(cli("new", "model", "a", "--project-dir", str(polars)), "models/a.yml already")
    assert (polars / "models" / "a.yml").read_text(encoding="utf-8") == "sources: []\n"


def test_existing_stub_file(pandas: Path) -> None:
    (pandas / "src" / "shop_models" / "a.py").write_text("x = 1\n", encoding="utf-8")
    usage_error(cli("new", "model", "a", "--project-dir", str(pandas)), "a.py already exists")
    assert not (pandas / "models" / "a.yml").exists()


def test_project_that_does_not_load(tmp_path: Path) -> None:
    result = cli("new", "model", "a", "--project-dir", str(tmp_path))
    assert result.exit_code == 1
    assert "PLY004" in result.stderr


def test_log_format_json(tmp_path: Path) -> None:
    result = cli("new", "model", "a", "--project-dir", str(tmp_path), "--log-format", "json")
    assert result.exit_code == 1
    assert json.loads(result.stderr.splitlines()[0])


def test_project_dir_env(polars: Path) -> None:
    result = CliRunner().invoke(
        main, ["new", "model", "a"], env={"PIPELOOM_PROJECT_DIR": str(polars)}
    )
    assert result.exit_code == 0, result.stderr
    assert (polars / "models" / "a.yml").is_file()


def test_vars_option(polars: Path) -> None:
    result = cli("new", "model", "a", "--vars", "{x: 1}", "--project-dir", str(polars))
    assert result.exit_code == 0, result.stderr
