"""Runtime secret resolution: only when executing, masked, never in the manifest."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from click.testing import CliRunner

from pipeloom.api import Project, ProjectOptions
from pipeloom.cli import main
from pipeloom.diagnostics import JsonValue
from pipeloom.plan.manifest import manifest_bytes
from pipeloom.runtime.run import RunOptions
from pipeloom.secrets.redaction import Redactor
from pipeloom.secrets.resolve import SECRET_REF, SecretError, contains_secret, resolve_secrets
from pipeloom.testing.clock import FakeClock
from tests.conftest import make_project
from tests.plugins.fakes import VAULT, Vault

REF = "${secret:vault/db/pass}"
VALUE = "s3cr3t-value"
YML = f"""config_version: 1
name: shop
python_paths: [src]
targets:
  dev:
    engine:
      kind: memory
      tables: {{t: [{{token: "{REF}"}}]}}
    catalog: {{kind: memory}}
"""
MODELS = "models:\n  - {name: a, python: 'm:body'}\n"
BODY = "import os\n\n\ndef body(ctx):\n    raise RuntimeError('leaked ' + os.environ['PL_TOKEN'])\n"
ENV = {"PIPELOOM_TARGET": "", "PIPELOOM_DISABLE_PLUGIN_AUTOLOAD": "", "PIPELOOM_LOG_FORMAT": ""}


def test_grammar() -> None:
    m = SECRET_REF.fullmatch(REF)
    assert m is not None and m.groups() == ("vault", "db/pass")
    assert contains_secret({"a": [f"x {REF}"]}) and not contains_secret({"a": "${env:X}"})


def test_resolves_nested_values_and_registers_them() -> None:
    redactor = Redactor()
    value: JsonValue = {"a": [f"u:{REF}", 3], "b": REF, "c": None}
    out = resolve_secrets(value, lambda _name: Vault(), redactor)
    assert out == {"a": [f"u:{VALUE}", 3], "b": VALUE, "c": None}
    assert redactor.mask(f"x {VALUE} y") == "x *** y"
    assert value == {"a": [f"u:{REF}", 3], "b": REF, "c": None}  # the input is not changed


def test_failure_names_provider_and_path_not_a_value() -> None:
    with pytest.raises(SecretError) as err:
        resolve_secrets("${secret:vault/nope}", lambda _n: Vault(), Redactor())
    assert err.value.provider == "vault" and err.value.path == "nope"
    assert VALUE not in str(err.value)


def project(tmp_path: Path, yml: str = YML) -> Project:
    root = make_project(tmp_path, {"pipeloom.yml": yml, "models/m.yml": MODELS, "src/m.py": BODY})
    opts = ProjectOptions(
        project_dir=root, plugins=("tests.plugins.fakes:VAULT",), env={"PL_TOKEN": VALUE}
    )
    return Project(opts, clock=FakeClock())


def test_compile_keeps_the_reference_and_the_manifest_has_no_value(tmp_path: Path) -> None:
    proj = project(tmp_path)
    plan = proj.compile().value
    assert plan is not None
    text = manifest_bytes(plan).decode("utf-8")
    assert VALUE not in text
    bound = proj.bind().value
    assert bound is not None
    assert bound.plugins["engines"].raw["tables"] == {"t": [{"token": REF}]}
    assert bound.plugins["engines"].options.model_dump()["tables"] == {"t": [{"token": REF}]}
    assert proj.redactor.mask(VALUE) == VALUE  # nothing registered yet


def test_run_resolves_with_a_fake_provider(tmp_path: Path) -> None:
    proj = project(tmp_path)
    outcome = proj.run(RunOptions(run_id="r", workers=1))
    assert outcome.exit_code == 1  # the model raised, which proves the engine was built
    assert not [d for d in outcome.diagnostics if d.code == "PLX109"]
    assert proj.redactor.mask(VALUE) == "***"


def test_unknown_provider_is_plx109(tmp_path: Path) -> None:
    proj = project(tmp_path, YML.replace("vault/db/pass", "nowhere/x"))
    outcome = proj.run(RunOptions(run_id="r", workers=1))
    (d,) = [d for d in outcome.diagnostics if d.code == "PLX109"]
    assert d.context == {"provider": "nowhere", "path": ""}
    assert outcome.exit_code == 1


def test_cli_run_masks_events_and_keeps_the_manifest_clean(tmp_path: Path) -> None:
    yml = YML.replace(REF, "${secret:env/PL_TOKEN}")
    root = make_project(tmp_path, {"pipeloom.yml": yml, "models/m.yml": MODELS, "src/m.py": BODY})
    env = {**ENV, "PL_TOKEN": VALUE}
    args = ["--project-dir", str(root), "--target", "dev", "--log-format", "json"]
    run = CliRunner().invoke(main, ["run", *args], env=env)
    assert run.exit_code == 1
    assert "RuntimeError" in run.stderr and VALUE not in run.stderr and "leaked ***" in run.stderr
    for path in (root / "target").rglob("manifest.json"):
        assert VALUE not in path.read_text(encoding="utf-8")
    compiled = CliRunner().invoke(main, ["compile", *args[:4]], env=env)
    assert VALUE not in compiled.output and VALUE not in compiled.stderr
    for path in (root / "target").rglob("*.jsonl"):
        assert VALUE not in path.read_text(encoding="utf-8")
    assert json.loads(next((root / "target").rglob("manifest.json")).read_text("utf-8"))


def test_cli_run_with_an_unresolvable_reference_exits_1(tmp_path: Path) -> None:
    yml = YML.replace(REF, "${secret:env/PL_MISSING}")
    root = make_project(tmp_path, {"pipeloom.yml": yml, "models/m.yml": MODELS, "src/m.py": BODY})
    args = ["run", "--project-dir", str(root), "--target", "dev", "--log-format", "json"]
    result = CliRunner().invoke(main, args, env=ENV)
    assert result.exit_code == 1 and "PLX109" in result.stderr
    assert "PL_MISSING" in result.stderr


def test_vault_spec_is_a_secret_provider() -> None:
    assert VAULT.group == "secret_providers"
