from __future__ import annotations

from pipeloom.config.yaml import MAX_NODES, parse_yaml
from pipeloom.diagnostics import Report


def load(text: str | bytes) -> tuple[object, Report]:
    report = Report()
    raw = text if isinstance(text, bytes) else text.encode("utf-8")
    doc = parse_yaml(raw, "f.yml", report)
    return doc, report


def codes(report: Report) -> list[str]:
    return [d.code for d in report.sorted()]


def test_marks_are_one_based_and_pointer_keyed() -> None:
    doc, report = load("a:\n  b: 1\n  c: [x, y]\n")
    assert doc is not None and not report.has_errors
    loc = doc.locate("/a/c/1")  # type: ignore[attr-defined]
    assert (loc.line, loc.col, loc.json_pointer) == (3, 10, "/a/c/1")
    key = doc.locate("/a/b", key=True)  # type: ignore[attr-defined]
    assert (key.line, key.col) == (2, 3)


def test_locate_falls_back_to_ancestor() -> None:
    doc, _ = load("a:\n  b: 1\n")
    loc = doc.locate("/a/missing/deeper")  # type: ignore[attr-defined]
    assert loc.line == 2 and loc.json_pointer == "/a/missing/deeper"


def test_pointer_escapes() -> None:
    doc, _ = load("'a/b~c': 1\n")
    assert doc is not None and "/a~1b~0c" in doc.value_marks  # type: ignore[attr-defined]


def test_merge_keys_explicit_wins_and_no_duplicate_error() -> None:
    doc, report = load("base: &b {x: 1, y: 2}\nuse:\n  <<: *b\n  y: 3\n")
    assert doc is not None and not report.has_errors
    assert doc.data["use"] == {"x": 1, "y": 3}  # type: ignore[attr-defined]


def test_merge_sequence_earlier_source_wins() -> None:
    doc, _ = load("a: &a {k: 1}\nb: &b {k: 2}\nc:\n  <<: [*a, *b]\n")
    assert doc is not None and doc.data["c"] == {"k": 1}  # type: ignore[attr-defined]


def test_duplicate_key() -> None:
    _, report = load("a: 1\na: 2\n")
    assert codes(report) == ["PLY002"]


def test_yaml12_scalars() -> None:
    doc, _ = load("country: NO\nflag: yes\nok: true\nn: 010\nh: 0x1f\no: 0o17\nnull_: ~\nf: 1e3\n")
    assert doc is not None
    data = doc.data  # type: ignore[attr-defined]
    assert data == {
        "country": "NO",
        "flag": "yes",
        "ok": True,
        "n": 10,
        "h": 31,
        "o": 15,
        "null_": None,
        "f": 1000.0,
    }


def test_no_implicit_timestamp() -> None:
    doc, _ = load("d: 2026-11-04\n")
    assert doc is not None and doc.data == {"d": "2026-11-04"}  # type: ignore[attr-defined]


def test_empty_document_is_empty_mapping() -> None:
    for text in ("", "# only a comment\n"):
        doc, report = load(text)
        assert doc is not None and doc.data == {} and not report.has_errors  # type: ignore[attr-defined]


def test_crlf_and_bom_accepted() -> None:
    doc, report = load(b"\xef\xbb\xbfa: 1\r\nb: [1,\r\n 2]\r\n")
    assert doc is not None and doc.data == {"a": 1, "b": [1, 2]}  # type: ignore[attr-defined]
    assert not report.has_errors


def test_syntax_error_and_multiple_documents() -> None:
    _, report = load("a: [1\n")
    assert codes(report) == ["PLY001"]
    _, report = load("a: 1\n---\nb: 2\n")
    assert codes(report) == ["PLY001"]
    assert "more than one" in report.sorted()[0].message


def test_recursive_alias() -> None:
    _, report = load("a: &x\n  b: *x\n")
    assert codes(report) == ["PLY001"]
    assert "recursive" in report.sorted()[0].message


def test_alias_bomb_is_limited() -> None:
    lines = ["l0: &l0 [x, x, x, x, x, x, x, x, x, x]"]
    for i in range(1, 8):
        prev = f"*l{i - 1}"
        lines.append(f"l{i}: &l{i} [{', '.join([prev] * 10)}]")
    _, report = load("\n".join(lines) + "\n")
    assert codes(report) == ["PLY001"]
    assert str(MAX_NODES) in report.sorted()[0].message


def test_non_string_key_rejected() -> None:
    _, report = load("1: a\n")
    assert codes(report) == ["PLY001"]


def test_invalid_bytes_and_utf16() -> None:
    _, report = load(b"a: \xff\n")
    assert codes(report) == ["PLY003"]
    _, report = load("a: 1\n".encode("utf-16"))
    assert codes(report) == ["PLY003"]


def test_control_character_is_reader_error() -> None:
    _, report = load("a: \x01\n")
    assert codes(report) == ["PLY003"]
