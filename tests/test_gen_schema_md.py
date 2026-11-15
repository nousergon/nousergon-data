"""Behaviour tests for features/gen_schema_md.py (SCHEMA.md §3 table render/rewrite)."""

from __future__ import annotations

from pathlib import Path

import pytest

from features import gen_schema_md as gsm
from features.registry import PRIVATE_PACK_COMPUTE, FeatureEntry

_HEADER = "| Field | Units | Compute | Consumers |"
_SEP = "|---|---|---|---|"


def _entries() -> list[FeatureEntry]:
    return [
        FeatureEntry(
            name="b_ratio", group="technical", description="d", units="ratio",
            formula="B formula", consumers="scorer", display_order=2,
        ),
        FeatureEntry(
            name="a_pct", group="technical", description="d", units="percent",
            formula="A formula", consumers="gate", display_order=1,
        ),
        FeatureEntry(
            name="p_ratio", group="technical", description="d", units="ratio",
            formula="SECRET", consumers="pack", display_order=3,
            compute=PRIVATE_PACK_COMPUTE,
        ),
        FeatureEntry(name="m_ratio", group="macro", description="d", units="ratio",
                     formula="M", consumers="x", per_ticker=False),
    ]


@pytest.fixture
def small_catalog(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(gsm, "CATALOG", _entries())


def test_render_group_table_sorted_by_display_order(small_catalog: None) -> None:
    assert gsm.render_group_table("technical").split("\n") == [
        _HEADER,
        _SEP,
        "| `a_pct` | percent | A formula | gate |",
        "| `b_ratio` | ratio | B formula | scorer |",
        "| `p_ratio` | ratio | private pack | pack |",
    ]


def test_private_pack_formula_never_disclosed(small_catalog: None) -> None:
    # Mutation guarded: rendering entry.formula for private-pack rows.
    assert "SECRET" not in gsm.render_group_table("technical")


def test_render_group_table_unknown_group_is_header_only(small_catalog: None) -> None:
    assert gsm.render_group_table("nope") == f"{_HEADER}\n{_SEP}"


def test_render_all_tables_keys_follow_group_sections(small_catalog: None) -> None:
    tables = gsm.render_all_tables()
    assert list(tables) == [h for h, _ in gsm.GROUP_SECTIONS]
    assert len(tables) == 6
    assert "`m_ratio`" in tables["### Macro (one row per date — `per_ticker=False`)"]


def _doc(rows_by_header: dict[str, str]) -> str:
    parts = ["# Schema\n\n## 3. Fields\n\nLead-in prose.\n\n"]
    for header, _group in gsm.GROUP_SECTIONS:
        parts.append(f"{header}\n\nProse for group.\n\n{_HEADER}\n{_SEP}\n")
        parts.append(rows_by_header.get(header, "| `old` | u | f | c |\n"))
        parts.append("\nTrailing prose.\n\n")
    parts.append("### 3b. Private-pack columns\n\n| Hand | written |\n|---|---|\n| x | y |\n")
    return "".join(parts)


def test_rewrite_replaces_only_table_blocks(small_catalog: None) -> None:
    text = _doc({})
    out = gsm.rewrite_schema_md(text)
    assert "| `old` | u | f | c |" not in out
    assert "| `a_pct` | percent | A formula | gate |" in out
    # prose, headings and 3b preserved byte-for-byte
    assert out.count("Prose for group.") == 6
    assert out.count("Trailing prose.") == 6
    assert out.endswith("| Hand | written |\n|---|---|\n| x | y |\n")
    assert out.startswith("# Schema\n\n## 3. Fields\n\nLead-in prose.\n\n")


def test_rewrite_is_idempotent(small_catalog: None) -> None:
    once = gsm.rewrite_schema_md(_doc({}))
    assert gsm.rewrite_schema_md(once) == once


def test_find_table_span_consumes_only_consecutive_rows() -> None:
    header = "### Technical (per-ticker, daily refresh)"
    text = f"{header}\n\nprose\n\n{_HEADER}\n{_SEP}\n| a | b | c | d |\n| e | f | g | h |\n\nafter\n| z |\n"
    start, end = gsm._find_table_span(text, header)  # pyright: ignore[reportPrivateUsage]
    assert text[start:].startswith(_HEADER)
    assert text[start:end].endswith("| e | f | g | h |\n")
    assert text[end:].startswith("\nafter")


def test_find_table_span_missing_heading_raises() -> None:
    with pytest.raises(ValueError):
        gsm._find_table_span("no headings here", "### Technical (per-ticker, daily refresh)")  # pyright: ignore[reportPrivateUsage]


def test_rewrite_missing_section_raises(small_catalog: None) -> None:
    with pytest.raises(ValueError):
        gsm.rewrite_schema_md("# nothing\n")


def test_main_write_then_check(
    small_catalog: None,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    target = tmp_path / "SCHEMA.md"
    target.write_text(_doc({}), encoding="utf-8")
    monkeypatch.setattr(gsm, "SCHEMA_MD", target)
    assert gsm.main(["--check"]) == 1  # stale before write
    assert capsys.readouterr().err.startswith("DRIFT:")
    assert gsm.main(["--write"]) == 0
    assert "§3 table blocks only" in capsys.readouterr().out
    assert gsm.main(["--check"]) == 0
    assert "OK." in capsys.readouterr().out


def test_main_check_does_not_modify_file(
    small_catalog: None, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    target = tmp_path / "SCHEMA.md"
    original = _doc({})
    target.write_text(original, encoding="utf-8")
    monkeypatch.setattr(gsm, "SCHEMA_MD", target)
    gsm.main(["--check"])
    assert target.read_text(encoding="utf-8") == original


def test_main_default_prints_every_group(
    small_catalog: None, capsys: pytest.CaptureFixture[str]
) -> None:
    assert gsm.main([]) == 0
    out = capsys.readouterr().out
    for header, _group in gsm.GROUP_SECTIONS:
        assert header in out
    assert out.count(_HEADER) == 6
