"""Discovery end to end: the import fallback, the load summary, isolation, the cache and reload."""

from __future__ import annotations

import json
import os
import sys
import textwrap
from pathlib import Path
from typing import Any

import pytest

import pipeloom.discovery as disc
from pipeloom.config import models as cfg
from pipeloom.discovery import Discovery, DiscoveryResult, ModuleLoader, discover
from pipeloom.discovery import cache as cache_mod
from pipeloom.discovery.ast_resolver import parse_file

STATIC = """
from pipeloom import Write, model, var

@model(write=Write(strategy="merge", unique_key=["k"]), retries=var("r", default=1))
def orders(customers, n=1):
    return customers

@model
def customers():
    return 1
"""

DYNAMIC = """
from pipeloom import model

TAGS = ["t" + "1"]

@model(tags=TAGS, schema="s")
def payments():
    return 1

def make(name):
    def built(orders):
        return orders
    return model(name=name)(built)

refunds = make("refunds")
"""


def write(root: Path, files: dict[str, str]) -> Path:
    for rel, body in files.items():
        p = root / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(textwrap.dedent(body).lstrip("\n"), encoding="utf-8")
    return root


def names(result: DiscoveryResult) -> list[str]:
    return [m.name for m in result.models]


def test_static_and_dynamic_models(tmp_path: Path) -> None:
    write(tmp_path, {"models/static.py": STATIC, "models/dyn.py": DYNAMIC, "models/x.txt": "-"})
    result = discover(tmp_path, ["models"])
    assert result.failures == ()
    by = {m.name: m for m in result.models}
    assert sorted(by) == ["customers", "orders", "payments", "refunds"]

    orders = by["orders"]
    assert (orders.module, orders.attr, orders.file, orders.dynamic) == (
        "models.static",
        "orders",
        "models/static.py",
        False,
    )
    assert orders.kwargs["write"] == cfg.WriteConfig(strategy="merge", unique_key=["k"])
    assert orders.callable_ref == "models.static:orders"

    pay = by["payments"]
    assert pay.dynamic and pay.kwargs == {"tags": ["t1"], "schema": "s"}
    assert (pay.line, pay.col) == (5, 2)
    assert pay.kwarg_locations == {"tags": (5, 8), "schema": (5, 19)}

    refunds = by["refunds"]  # factory-built: the function's first line, column 1
    assert (refunds.attr, refunds.line, refunds.col, refunds.kwarg_locations) == (
        "refunds",
        10,
        1,
        {},
    )
    assert [p.name for p in refunds.params] == ["orders"]
    assert "models" not in sys.modules and "models.dyn" not in sys.modules


def test_every_failure_is_reported(tmp_path: Path) -> None:
    write(
        tmp_path,
        {
            "models/good.py": DYNAMIC,
            "models/syntax.py": "import pipeloom\ndef a(:\n",
            "models/boom.py": "import pipeloom as pl\nX = []\n\n@pl.model(tags=X)\ndef a(): ...\n"
            "raise RuntimeError('boom')\n",
            "models/missing.py": "import pipeloom as pl\nimport no_such_module_xyz\n"
            "@pl.model(tags=list())\ndef b(): ...\n",
            "models/exits.py": "import sys\nfrom pipeloom import model\nmodel(sys.exit(3))\n",
            "models/unknown.py": "from pipeloom import model\nX = 1\n@model(retries=X, colour=1)\n"
            "def c(): ...\n",
        },
    )
    result = discover(tmp_path, ["models"], use_cache=False)
    assert names(result) == ["payments", "refunds"]
    got = {(f.file, f.kind, f.line) for f in result.failures}
    assert got == {
        ("models/boom.py", "import_error", 6),
        ("models/exits.py", "import_error", 3),
        ("models/missing.py", "import_error", 2),
        ("models/syntax.py", "syntax", 2),
        ("models/unknown.py", "unknown_keyword", 3),
    }
    boom = next(f for f in result.failures if f.file == "models/boom.py")
    assert boom.message == "RuntimeError: boom" and boom.traceback and "boom.py" in boom.traceback


def test_two_projects_with_the_same_module_names(tmp_path: Path) -> None:
    body = "from pipeloom import model\nV = {v!r}\n@model(meta={{'v': V}})\ndef orders(): ...\n"
    a = write(tmp_path / "a", {"models/orders.py": body.format(v="a")})
    b = write(tmp_path / "b", {"models/orders.py": body.format(v="b")})
    for project, value in [(a, "a"), (b, "b"), (a, "a")]:
        (m,) = discover(project, ["models"]).models
        assert m.kwargs["meta"] == {"v": value}
    assert not any(k == "models" or k.startswith("models.") for k in sys.modules)


def test_namespace_packages_are_purged(tmp_path: Path) -> None:
    write(tmp_path, {"nsmodels/sub/dyn.py": DYNAMIC})
    assert len(discover(tmp_path, ["nsmodels"]).models) == 2
    assert not any(k.startswith("nsmodels") for k in sys.modules)


def test_reexported_functions_are_kept_in_their_own_module(tmp_path: Path) -> None:
    write(
        tmp_path,
        {
            "models/a.py": DYNAMIC,
            "models/b.py": "from models.a import payments, refunds\nfrom models.s import orders\n"
            "import pipeloom\nX = pipeloom.model\n",
            "models/s.py": STATIC,
        },
    )
    result = discover(tmp_path, ["models"])
    assert result.failures == ()
    assert sorted((m.name, m.module) for m in result.models) == [
        ("customers", "models.s"),
        ("orders", "models.s"),
        ("payments", "models.a"),
        ("refunds", "models.a"),
    ]


def test_outside_roots_and_shadowed_modules(tmp_path: Path) -> None:
    write(tmp_path, {"models/a.py": DYNAMIC, "elsewhere/b.py": STATIC})
    result = discover(tmp_path, ["models", "elsewhere"], python_paths=["models"])
    assert names(result) == ["payments", "refunds"]
    assert [(f.file, f.kind) for f in result.failures] == [("elsewhere/b.py", "outside_roots")]

    write(tmp_path, {"other/models/a.py": "x = 1\n"})
    result = discover(tmp_path, ["models"], python_paths=["other", "."])
    assert [(f.file, f.kind) for f in result.failures] == [("models/a.py", "name_mismatch")]


def test_globs_packages_and_skipped_directories(tmp_path: Path) -> None:
    write(
        tmp_path,
        {
            "models/one.py": STATIC,
            "models/.hidden/x.py": STATIC,
            "models/__pycache__/x.py": STATIC,
            "models/venv/pyvenv.cfg": "",
            "models/venv/lib/x.py": STATIC,
            "extra/two.py": DYNAMIC,
            "src/mypkg/__init__.py": "",
            "src/mypkg/layer/three.py": STATIC.replace("orders", "o3").replace("customers", "c3"),
        },
    )
    result = discover(
        tmp_path, ["models", "extra/*.py"], ["mypkg.layer", "nope_pkg_xyz"], [".", "src"]
    )
    assert sorted({m.file for m in result.models}) == [
        "extra/two.py",
        "models/one.py",
        "src/mypkg/layer/three.py",
    ]
    assert {m.module for m in result.models if m.file.startswith("src")} == {"mypkg.layer.three"}
    assert [(f.file, f.kind) for f in result.failures] == [("nope_pkg_xyz", "not_found")]


def test_load_one_callable(tmp_path: Path) -> None:
    write(tmp_path, {"models/dyn.py": DYNAMIC})
    with ModuleLoader(tmp_path, ["."]) as loader:
        fn = loader.load("models.dyn:payments")
        assert fn() == 1 and loader.load("models.dyn:payments") is fn
        with pytest.raises(TypeError, match="not callable"):
            loader.load("models.dyn:TAGS")
        assert str(tmp_path.resolve()) == sys.path[0]
    assert str(tmp_path.resolve()) not in sys.path and "models.dyn" not in sys.modules


# --- cache -------------------------------------------------------------------------------------


def _no_parse(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    calls: list[str] = []
    real = parse_file

    def spy(source: bytes, path: str) -> Any:
        calls.append(path)
        return real(source, path)

    monkeypatch.setattr(disc, "parse_file", spy)
    return calls


def test_cache_hit_round_trips_exactly(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    write(tmp_path, {"models/s.py": STATIC, "models/d.py": DYNAMIC, "models/n.py": "x = 1\n"})
    cold = discover(tmp_path, ["models"])
    data = json.loads((tmp_path / cache_mod.CACHE_FILE).read_text(encoding="utf-8"))
    assert {p: e["kind"] for p, e in data["files"].items()} == {
        "models/d.py": "dynamic",
        "models/n.py": "none",
        "models/s.py": "static",
    }
    calls = _no_parse(monkeypatch)
    warm = discover(tmp_path, ["models"])
    assert calls == []
    assert warm == cold
    w = next(m for m in warm.models if m.name == "orders").kwargs["write"]
    assert w.model_fields_set == {"strategy", "unique_key"}


def test_cache_invalidation(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    write(tmp_path, {"models/s.py": STATIC, "models/gone.py": STATIC.replace("orders", "o2")})
    discover(tmp_path, ["models"])
    calls = _no_parse(monkeypatch)

    path = tmp_path / "models/s.py"
    st = path.stat()
    os.utime(path, ns=(st.st_atime_ns, st.st_mtime_ns + 10**9))  # mtime only
    (tmp_path / "models/gone.py").unlink()
    discover(tmp_path, ["models"])
    assert calls == ["models/s.py"]
    data = json.loads((tmp_path / cache_mod.CACHE_FILE).read_text(encoding="utf-8"))
    assert list(data["files"]) == ["models/s.py"]

    calls.clear()
    path.write_text(path.read_text(encoding="utf-8") + "\n", encoding="utf-8")
    os.utime(path, ns=(st.st_atime_ns, st.st_mtime_ns + 10**9))  # same mtime, new size
    discover(tmp_path, ["models"])
    assert calls == ["models/s.py"]

    calls.clear()
    monkeypatch.setattr(cache_mod, "__version__", "0.0.0-other")
    discover(tmp_path, ["models"])
    assert calls == ["models/s.py"]


@pytest.mark.parametrize("garbage", ["", "{", "[]", '{"header": 1}', "null"])
def test_corrupt_cache_means_a_cold_run(tmp_path: Path, garbage: str) -> None:
    write(tmp_path, {"models/s.py": STATIC})
    expected = discover(tmp_path, ["models"], use_cache=False)
    (tmp_path / cache_mod.CACHE_FILE).parent.mkdir(parents=True)
    (tmp_path / cache_mod.CACHE_FILE).write_text(garbage, encoding="utf-8")
    assert discover(tmp_path, ["models"]) == expected


def test_unwritable_cache_is_ignored(tmp_path: Path) -> None:
    write(tmp_path, {"models/s.py": STATIC, ".pipeloom/cache": "a file where a directory goes"})
    assert names(discover(tmp_path, ["models"])) == ["orders", "customers"]
    assert not (tmp_path / cache_mod.CACHE_FILE).exists()


def test_cache_is_not_used_when_disabled(tmp_path: Path) -> None:
    write(tmp_path, {"models/s.py": STATIC})
    discover(tmp_path, ["models"], use_cache=False)
    assert not (tmp_path / ".pipeloom").exists()


# --- reload ------------------------------------------------------------------------------------


def test_refresh_picks_up_edits(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    helper = "TAGS = ['{}']\n"
    dyn = "from pipeloom import model\nfrom models.helper import TAGS\n@model(tags=TAGS)\ndef d(): ...\n"
    write(
        tmp_path,
        {"models/s.py": STATIC, "models/d.py": dyn, "models/helper.py": helper.format("v1")},
    )
    d = Discovery(tmp_path, ["models"])
    first = d.discover()
    assert next(m for m in first.models if m.name == "d").kwargs == {"tags": ["v1"]}

    calls = _no_parse(monkeypatch)
    write(tmp_path, {"models/helper.py": helper.format("v22")})
    second = d.refresh()
    assert calls == ["models/helper.py"]
    assert next(m for m in second.models if m.name == "d").kwargs == {"tags": ["v22"]}

    calls.clear()
    write(tmp_path, {"models/s.py": STATIC.replace("def customers", "def clients")})
    assert "clients" in names(d.refresh()) and calls == ["models/s.py"]
