"""The Databricks secret provider through FakeDatabricks, alone and in a `pipeloom run`."""

from __future__ import annotations

from pathlib import Path

import pytest
from click.testing import CliRunner

from pipeloom.cli import main
from pipeloom.secrets.resolve import SecretError
from pipeloom_databricks import secrets as secrets_module
from pipeloom_databricks.client import ApiError, ConnectionNotConfigured
from pipeloom_databricks.secrets import DatabricksSecretProvider
from pipeloom_databricks.spec import SECRETS_SPEC
from pipeloom_databricks.testing.fake import FakeDatabricks

VALUE = "s3cr3t-välue"


@pytest.fixture
def fake(monkeypatch: pytest.MonkeyPatch) -> FakeDatabricks:
    client = FakeDatabricks()
    client.secrets[("scope", "key")] = VALUE
    client.secrets[("scope", "nested/key")] = "nested"
    built: list[object] = []

    def connect(*, profile: str | None, env: object, factory: str | None = None) -> FakeDatabricks:
        assert profile is None and factory is None  # no profile is ever chosen
        built.append(env)
        return client

    monkeypatch.setattr(secrets_module, "connect", connect)
    client.built = built  # type: ignore[attr-defined]
    return client


def test_resolves_base64_utf8_and_connects_once(fake: FakeDatabricks) -> None:
    provider = DatabricksSecretProvider({})
    assert fake.built == []  # type: ignore[attr-defined]
    assert provider.resolve("scope/key") == VALUE
    assert provider.resolve("scope/nested/key") == "nested"
    assert len(fake.built) == 1  # type: ignore[attr-defined]
    assert fake.called("get_secret") == ["scope/key", "scope/nested/key"]


@pytest.mark.parametrize("path", ["", "scope", "scope/", "/key"])
def test_a_bad_path_is_a_secret_error_naming_provider_and_path(
    fake: FakeDatabricks, path: str
) -> None:
    with pytest.raises(SecretError) as err:
        DatabricksSecretProvider({}).resolve(path)
    assert (err.value.provider, err.value.path) == ("databricks", path)
    assert fake.called("get_secret") == []


def test_an_api_error_names_provider_and_path_not_the_value(fake: FakeDatabricks) -> None:
    with pytest.raises(SecretError) as err:
        DatabricksSecretProvider({}).resolve("scope/missing")
    assert (err.value.provider, err.value.path) == ("databricks", "scope/missing")
    assert "404" in str(err.value) and VALUE not in str(err.value)
    fake.fail["get_secret"] = ApiError(403, "no permission")
    with pytest.raises(SecretError, match="403"):
        DatabricksSecretProvider({}).resolve("scope/key")


def test_a_value_that_is_not_base64_is_an_error_without_the_value(fake: FakeDatabricks) -> None:
    fake.get_secret = lambda scope, key: "not base64 !"  # type: ignore[method-assign]
    with pytest.raises(SecretError, match="base64") as err:
        DatabricksSecretProvider({}).resolve("scope/key")
    assert "not base64 !" not in str(err.value)


def test_no_connection_is_a_secret_error(monkeypatch: pytest.MonkeyPatch) -> None:
    def refuse(**_: object) -> None:
        raise ConnectionNotConfigured("no Databricks connection: nothing set")

    monkeypatch.setattr(secrets_module, "connect", refuse)
    with pytest.raises(SecretError, match="no Databricks connection"):
        DatabricksSecretProvider({}).resolve("scope/key")


def test_the_spec_builds_the_provider() -> None:
    assert (SECRETS_SPEC.group, SECRETS_SPEC.kind) == ("secret_providers", "databricks")
    assert isinstance(SECRETS_SPEC.factory(SECRETS_SPEC.config_model()), DatabricksSecretProvider)


YML = """config_version: 1
name: shop
python_paths: [src]
targets:
  dev:
    engine:
      kind: memory
      tables: {t: [{token: "${secret:databricks/scope/key}"}]}
    catalog: {kind: memory}
"""
MODELS = "models:\n  - {name: a, python: 'm:body'}\n"
BODY = "import os\n\n\ndef body(ctx):\n    raise RuntimeError('leaked ' + os.environ['PL_TOKEN'])\n"
ENV = {"PIPELOOM_TARGET": "", "PIPELOOM_DISABLE_PLUGIN_AUTOLOAD": "", "PIPELOOM_LOG_FORMAT": ""}


def write_project(root: Path, yml: str) -> None:
    for rel, text in {"pipeloom.yml": yml, "models/m.yml": MODELS, "src/m.py": BODY}.items():
        path = root / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(text.encode("utf-8"))


def run_args(root: Path) -> list[str]:
    return ["run", "--project-dir", str(root), "--target", "dev", "--log-format", "json"]


def test_a_run_resolves_through_the_provider_and_masks_the_value(
    tmp_path: Path, fake: FakeDatabricks
) -> None:
    write_project(tmp_path, YML)
    result = CliRunner().invoke(main, run_args(tmp_path), env={**ENV, "PL_TOKEN": VALUE})
    assert result.exit_code == 1  # the model raised, so the engine was built with the secret
    assert fake.called("get_secret") == ["scope/key"]
    assert "leaked ***" in result.stderr and VALUE not in result.stderr
    assert VALUE not in result.stdout
    leaks = [p for p in tmp_path.rglob("*") if p.is_file() and VALUE.encode() in p.read_bytes()]
    assert leaks == []


def test_a_run_with_a_missing_secret_is_plx109_naming_the_path(
    tmp_path: Path, fake: FakeDatabricks
) -> None:
    write_project(tmp_path, YML.replace("scope/key", "scope/nope"))
    result = CliRunner().invoke(main, run_args(tmp_path), env={**ENV, "PL_TOKEN": VALUE})
    assert result.exit_code == 1
    assert "PLX109" in result.stderr and "scope/nope" in result.stderr
    assert VALUE not in result.stderr
