"""`pipeloom new model`: kinds, folders, usage errors, and models that validate."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from click.testing import CliRunner

from pipeloom.cli import main


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


def read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def test_sql_model_on_polars(polars: Path) -> None:
    result = cli("new", "model", "extra", "--sql", "--project-dir", str(polars))
    assert result.exit_code == 0, result.stderr
    assert result.stdout == "models/extra.py\n"
    text = read(polars / "models" / "extra.py")
    assert "@model\ndef extra(ctx: ModelContext):" in text
    assert 'return ctx.sql("select 1 as id")' in text
    result = cli("validate", "--project-dir", str(polars))
    assert result.exit_code == 0, result.stderr
    assert "extra" in cli("ls", "--project-dir", str(polars)).stdout.split()
    result = cli("run", "--project-dir", str(polars), "--select", "extra")
    assert result.exit_code == 0, result.stderr


def test_python_model_on_pandas(pandas: Path) -> None:
    result = cli("new", "model", "extra", "--python", "--project-dir", str(pandas))
    assert result.exit_code == 0, result.stderr
    assert result.stdout == "models/extra.py\n"
    text = read(pandas / "models" / "extra.py")
    assert "@model\ndef extra(ctx: ModelContext):" in text
    assert "NotImplementedError" in text and "models/extra.py" in text
    assert "ctx.sql" not in text
    result = cli("validate", "--project-dir", str(pandas))
    assert result.exit_code == 0, result.stderr
    assert "extra" in cli("ls", "--project-dir", str(pandas)).stdout.split()


def test_python_on_polars_needs_no_python_paths(polars: Path) -> None:
    result = cli("new", "model", "a", "--python", "--project-dir", str(polars))
    assert result.exit_code == 0, result.stderr
    assert "NotImplementedError" in read(polars / "models" / "a.py")
    assert cli("validate", "--project-dir", str(polars)).exit_code == 0


def test_default_kind_follows_the_engine(polars: Path, pandas: Path) -> None:
    assert cli("new", "model", "a", "--project-dir", str(polars)).exit_code == 0
    assert "ctx.sql(" in read(polars / "models" / "a.py")
    assert cli("new", "model", "a", "--project-dir", str(pandas)).exit_code == 0
    assert "NotImplementedError" in read(pandas / "models" / "a.py")


def test_explicit_flag_beats_the_default(pandas: Path) -> None:
    # The default for pandas is a frame stub, but the flag is the user's call. pandas runs SQL
    # through DuckDB when it is installed (ADR-0068), and only warns (PLK606) without it.
    from pipeloom_pandas.spec import CAPABILITIES

    assert cli("new", "model", "a", "--sql", "--project-dir", str(pandas)).exit_code == 0
    assert "ctx.sql(" in read(pandas / "models" / "a.py")
    warned = "PLK606" in cli("validate", "--project-dir", str(pandas)).stderr
    assert warned is (CAPABILITIES.sql_dialect is None)


def test_folder(polars: Path) -> None:
    result = cli("new", "model", "a", "--folder", "gold/sub", "--project-dir", str(polars))
    assert result.exit_code == 0, result.stderr
    assert result.stdout == "models/gold/sub/a.py\n"
    assert cli("validate", "--project-dir", str(polars)).exit_code == 0


def test_unknown_default_target_falls_back_to_sql(polars: Path) -> None:
    # default_target is checked when a target is bound, not at load: no traceback here.
    project = polars / "pipeloom.yml"
    project.write_text(read(project) + "default_target: nope\n", encoding="utf-8")
    result = cli("new", "model", "a", "--project-dir", str(polars))
    assert result.exit_code == 0, result.output
    assert "ctx.sql(" in read(polars / "models" / "a.py")


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


@pytest.mark.parametrize("name", ["class", "ctx", "model"])
def test_name_that_cannot_be_a_function(polars: Path, name: str) -> None:
    usage_error(cli("new", "model", name, "--project-dir", str(polars)), "cannot name a model")


def test_existing_decorated_model_name(polars: Path) -> None:
    result = cli("new", "model", "orders_clean", "--folder", "x", "--project-dir", str(polars))
    usage_error(result, "a model named 'orders_clean' already exists")
    assert not (polars / "models" / "x").exists()


def test_name_of_an_existing_folder(polars: Path) -> None:
    usage_error(
        cli("new", "model", "bronze", "--project-dir", str(polars)),
        "'bronze' is already a folder in models",
    )
    assert not (polars / "models" / "bronze.py").exists()


def test_file_outside_every_python_paths_root(polars: Path) -> None:
    project = polars / "pipeloom.yml"
    text = read(project)
    project.write_text(
        text.replace("name: shop\n", "name: shop\npython_paths:\n  - lib\n"), encoding="utf-8"
    )
    assert "python_paths:\n  - lib\n" in read(project)
    usage_error(
        cli("new", "model", "a", "--project-dir", str(polars)),
        "models/a.py is outside every python_paths root (lib)",
    )
    assert not (polars / "models" / "a.py").exists()


def test_existing_target_file(polars: Path) -> None:
    (polars / "models" / "a.py").write_text("x = 1\n", encoding="utf-8")
    usage_error(cli("new", "model", "a", "--project-dir", str(polars)), "models/a.py already")
    assert read(polars / "models" / "a.py") == "x = 1\n"


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
    assert (polars / "models" / "a.py").is_file()


def test_vars_option(polars: Path) -> None:
    result = cli("new", "model", "a", "--vars", "{x: 1}", "--project-dir", str(polars))
    assert result.exit_code == 0, result.stderr
