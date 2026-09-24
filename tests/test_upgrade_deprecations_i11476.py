"""alpha-engine-config-I11476 — library deprecations that change behaviour on
upgrade, fixed where they originate in this repo.

* pandas: ``pct_change()``'s default ``fill_method="pad"`` is deprecated and
  goes away in pandas 3, which would silently change returns across gaps. The
  weekly rehearsal logged it at ``collectors/alternative.py`` and
  ``features/feature_engineer.py``. Every call site now forward-fills
  explicitly and passes ``fill_method=None``, which is exactly the old default,
  so the numbers do not move today or after the upgrade.
* bs4: an XML document handed to the HTML parser raises
  ``XMLParsedAsHTMLWarning`` (RAG ingestion). ``rag/pipelines/_markup.py``
  now picks the XML parser for those documents.
"""

from __future__ import annotations

import ast
import warnings
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
from bs4 import XMLParsedAsHTMLWarning

from features.factor_momentum import compute_daily_factor_returns
from rag.pipelines._markup import is_non_xhtml_xml, parse_filing_markup

_REPO = Path(__file__).resolve().parent.parent
_SKIP_DIRS = {"tests", ".venv", ".git", "__pycache__", "node_modules"}


def _production_py_files():
    for path in _REPO.rglob("*.py"):
        rel = path.relative_to(_REPO)
        if any(part in _SKIP_DIRS for part in rel.parts):
            continue
        yield path


def _pct_change_calls_without_explicit_none(path: Path) -> list[int]:
    tree = ast.parse(path.read_text(), filename=str(path))
    bad: list[int] = []
    for node in ast.walk(tree):
        if not (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)):
            continue
        if node.func.attr != "pct_change":
            continue
        fm = next((kw for kw in node.keywords if kw.arg == "fill_method"), None)
        if fm is None or not (isinstance(fm.value, ast.Constant) and fm.value.value is None):
            bad.append(node.lineno)
    return bad


def test_every_pct_change_call_passes_fill_method_none():
    offenders = {
        str(p.relative_to(_REPO)): lines
        for p in _production_py_files()
        if (lines := _pct_change_calls_without_explicit_none(p))
    }
    assert not offenders, (
        "pct_change() without fill_method=None relies on pandas' deprecated "
        "'pad' default, which pandas 3 removes. Forward-fill explicitly "
        f"(.ffill()) and pass fill_method=None: {offenders}"
    )


def _series_with_gaps() -> pd.Series:
    idx = pd.bdate_range("2026-01-01", periods=10)
    return pd.Series([10.0, 11.0, np.nan, np.nan, 12.0, 12.5, np.nan, 13.0, 12.0, 12.2], index=idx)


def test_explicit_ffill_matches_the_legacy_pad_default():
    s = _series_with_gaps()
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", FutureWarning)
        legacy = s.pct_change()  # pandas<3 default: fill_method="pad"
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        explicit = s.ffill().pct_change(fill_method=None)
    pd.testing.assert_series_equal(explicit, legacy)


def test_factor_returns_emit_no_deprecation_on_gappy_closes():
    """The groupby form is the one non-mechanical rewrite; it must stay
    warning-free on NaN closes and keep the per-ticker pad semantics."""
    dates = pd.bdate_range("2026-01-01", periods=6)
    rows = []
    for t_i, ticker in enumerate(["AAA", "BBB", "CCC", "DDD"]):
        for d_i, d in enumerate(dates):
            close = np.nan if (t_i == 1 and d_i == 2) else 100.0 + t_i + d_i
            rows.append({"ticker": ticker, "date": d, "close": close, "f1": float(t_i)})
    panel = pd.DataFrame(rows)

    with warnings.catch_warnings():
        warnings.simplefilter("error")
        out = compute_daily_factor_returns(panel, ["f1"], quantile=0.25, min_names=2)
    assert list(out.columns) == ["f1"]

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", FutureWarning)
        legacy = panel.sort_values(["ticker", "date"]).groupby(
            "ticker", sort=False
        )["close"].pct_change()
    ff = panel.sort_values(["ticker", "date"])
    explicit = ff.groupby("ticker", sort=False)["close"].ffill().groupby(
        ff["ticker"], sort=False
    ).pct_change(fill_method=None)
    pd.testing.assert_series_equal(explicit, legacy)


_PLAIN_XML = (
    '<?xml version="1.0"?>\n<ownershipDocument><issuer>'
    "<issuerName>ACME</issuerName></issuer></ownershipDocument>"
)
_INLINE_XBRL = (
    "<?xml version='1.0' encoding='ASCII'?>\n<!-- generated -->\n"
    '<html xmlns="http://www.w3.org/1999/xhtml"><body><p>Item 7. Management</p></body></html>'
)
_PLAIN_HTML = "<!DOCTYPE html><html><body><p>Item 1.01 Entry</p></body></html>"


@pytest.mark.parametrize(
    "markup, expect_xml",
    [(_PLAIN_XML, True), (_INLINE_XBRL, False), (_PLAIN_HTML, False)],
)
def test_parser_choice(markup, expect_xml):
    assert is_non_xhtml_xml(markup) is expect_xml


@pytest.mark.parametrize("markup", [_PLAIN_XML, _INLINE_XBRL, _PLAIN_HTML])
def test_parse_filing_markup_raises_no_xml_as_html_warning(markup):
    with warnings.catch_warnings():
        warnings.simplefilter("error", XMLParsedAsHTMLWarning)
        soup = parse_filing_markup(markup)
    assert soup.get_text(strip=True)


def test_rag_pipelines_do_not_silence_the_warning_globally():
    for rel in ("rag/pipelines/ingest_8k_filings.py", "rag/pipelines/ingest_sec_filings.py"):
        src = (_REPO / rel).read_text()
        assert "filterwarnings" not in src, f"{rel} suppresses warnings process-wide"
        assert 'BeautifulSoup(' not in src, f"{rel} bypasses parse_filing_markup"
