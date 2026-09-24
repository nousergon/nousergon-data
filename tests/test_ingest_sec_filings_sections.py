"""alpha-engine-config-I11472: SEC filing section extraction and form coverage.

The 2026-09-23 weekly rehearsal logged ``No sections extracted`` for 33 filings
(28 of them 10-Ks) and ``Found 0 ['10-K', '10-Q'] filings`` for every foreign
filer in the RAG scope. Root causes, each pinned below by a fixture shaped like
the real documents it stands for (the shapes were read off public EDGAR 10-K /
10-Q / 20-F HTML for PG, UNH, WMT, MSFT, HUBS and two 20-F filers):

* **First-match extraction.** The extractor took the FIRST occurrence of each
  section title. In a 10-K that is the table of contents, whose "Item 1A. Risk
  Factors" entry runs one line to "Item 1B"; it measured under the 200-char
  floor and was dropped, and the real heading further down was never tried.
* **Headings split across inline spans.** ``get_text(separator="\\n")`` put a
  newline inside drop-capped headings ("RI" / "SK FACTORS").
* **Typography.** No-break spaces and ``’`` in "Management’s".
* **Form list.** Only 10-K and 10-Q were requested; 20-F / 40-F filers
  returned nothing at all.

(The ``XMLParsedAsHTMLWarning`` the same log shows is parser selection,
``rag/pipelines/_markup.py`` from alpha-engine-config-I11476. That fix still
warned on every REAL inline-XBRL filing — bs4 decides from the first 500
characters, and a real filing's declaration + generator comments + ``<html``
tag never match its ``<[^ +]html`` pattern — so the inline-XBRL test below
uses a document long enough to reproduce that, which a short fixture hides.)

The old extractor returns ``{}`` for the TOC fixture below — that is the
rehearsal's failure, reproduced.
"""

from __future__ import annotations

import sys
import warnings
from types import ModuleType
from unittest.mock import MagicMock

import pytest

from rag.pipelines import _cik_lookup, ingest_sec_filings
from rag.pipelines.ingest_sec_filings import (
    DEFAULT_FORM_TYPES,
    _extract_sections,
    _html_to_text,
)
from rag.pipelines.source_yield import SourceYield

RISK = "Risk Factors"
MDA = "Management's Discussion and Analysis"
BUSINESS = "Business"
MARKET = "Quantitative and Qualitative Disclosures About Market Risk"


def _para(topic: str, n: int = 6) -> str:
    sentence = f"This paragraph discusses {topic} in enough detail to be a real section of the filing. "
    return f"<p>{sentence * n}</p>"


_TOC = """
<table>
  <tr><td>Item 1.</td><td>Business</td><td>3</td></tr>
  <tr><td>Item 1A.</td><td>Risk Factors</td><td>12</td></tr>
  <tr><td>Item 1B.</td><td>Unresolved Staff Comments</td><td>30</td></tr>
  <tr><td>Item 7.</td><td>Management&#8217;s Discussion and Analysis of Financial Condition</td><td>35</td></tr>
  <tr><td>Item 7A.</td><td>Quantitative and Qualitative Disclosures About Market Risk</td><td>50</td></tr>
  <tr><td>Item 8.</td><td>Financial Statements and Supplementary Data</td><td>52</td></tr>
</table>
"""

# A 10-K in the shape that failed: cover page mentioning "business day", a
# tabular TOC, then body headings typeset with &nbsp;, a drop-capped heading
# split across spans, and a typographic apostrophe.
_TEN_K = f"""
<html><body>
<p>Aggregate market value as of the last business day of the registrant's second fiscal quarter.</p>
{_TOC}
<div><span>Item&#160;1.</span><span> Business</span></div>
{_para("our products and customers")}
<div><span>Item&#160;1A.&#160;&#160;RI</span><span>SK FACTORS</span></div>
{_para("the risks to our business", 12)}
<div>Item 1B. Unresolved Staff Comments</div>
<p>None.</p>
<div>Item 7. Management&#8217;s Discussion and Analysis of Financial Condition and Results of Operations</div>
{_para("results of operations", 10)}
<div>Item 7A. Quantitative and Qualitative Disclosures About Market Risk</div>
{_para("interest rate and currency exposure")}
<div>Item 8. Financial Statements and Supplementary Data</div>
{_para("the balance sheet")}
</body></html>
"""


def test_toc_first_filing_yields_every_section_from_the_body():
    sections = _extract_sections(_TEN_K, form_type="10-K")

    assert set(sections) == {RISK, MDA, BUSINESS, MARKET}
    # The body, not the TOC line and not the cover page's "business day".
    assert sections[RISK].startswith("Item 1A. RISK FACTORS")
    assert "the risks to our business" in sections[RISK]
    assert "Unresolved Staff Comments" not in sections[RISK]
    assert sections[BUSINESS].startswith("Item 1. Business")
    assert "our products and customers" in sections[BUSINESS]
    assert "last business day" not in sections[BUSINESS]
    assert "results of operations" in sections[MDA]
    assert "the balance sheet" not in sections[MARKET]


def test_a_drop_capped_heading_reads_as_one_word():
    text = _html_to_text(_TEN_K)
    assert "Item 1A. RISK FACTORS" in text
    assert " " not in text


def test_a_cross_reference_opening_a_line_does_not_steal_the_section():
    """WMT 10-K shape: '"Item 1A. Risk Factors"' quoted at the start of a line
    inside Business runs on into the real section, so it is the longest span
    while starting in the wrong place."""
    html = f"""
    <html><body>
    <div>Item 1. Business</div>
    {_para("stores and e-commerce", 4)}
    <p>For more information see the risk factors in "</p><p>Item 1A. Risk Factors</p><p>" below.</p>
    {_para("our omni-channel strategy", 4)}
    <div>ITEM 1A. RISK FACTORS</div>
    {_para("risks that could adversely affect us", 12)}
    <div>ITEM 1B. UNRESOLVED STAFF COMMENTS</div>
    </body></html>
    """
    risk = _extract_sections(html, form_type="10-K")[RISK]
    assert risk.startswith("ITEM 1A. RISK FACTORS")
    assert "omni-channel" not in risk


def test_a_same_item_running_header_does_not_end_the_section():
    """MSFT 10-K shape: every page of the risk factors carries a 'PART I /
    Item 1A' running header followed by a capitalised sub-heading."""
    html = f"""
    <html><body>
    <div>ITEM 1A. RISK FACTORS</div>
    {_para("competition risk", 6)}
    <div>PART I</div><div>Item 1A</div><div>Business model competition</div>
    {_para("pricing pressure risk", 6)}
    <div>Item 1A</div><p>. With the interconnected components of this strategy, risks compound.</p>
    {_para("cybersecurity risk", 6)}
    <div>ITEM 1B. UNRESOLVED STAFF COMMENTS</div>
    </body></html>
    """
    risk = _extract_sections(html, form_type="10-K")[RISK]
    assert "competition risk" in risk
    assert "pricing pressure risk" in risk
    assert "cybersecurity risk" in risk
    assert "UNRESOLVED" not in risk


def test_a_10q_has_no_business_section_even_when_the_text_says_business():
    html = f"""
    <html><body>
    <p>as of the close of business on the last business day</p>
    <div>PART I. FINANCIAL INFORMATION</div>
    <div>Item 2. Management's Discussion and Analysis of Financial Condition</div>
    {_para("the quarter", 8)}
    <div>Item 3. Quantitative and Qualitative Disclosures About Market Risk</div>
    {_para("market risk in the quarter")}
    <div>Item 4. Controls and Procedures</div>
    </body></html>
    """
    sections = _extract_sections(html, form_type="10-Q")
    assert set(sections) == {MDA, MARKET}


def test_a_20f_maps_its_own_item_names_onto_the_corpus_labels():
    """20-F shape: Item 3.D Risk Factors is a lettered sub-heading on its own
    line; Items 4 and 5 are the Business and MD&A equivalents."""
    html = f"""
    <html><body>
    <div>ITEM 3. KEY INFORMATION</div>
    <div>A. [Reserved]</div>
    <div>D. Risk Factors</div>
    {_para("risks of a foreign private issuer", 10)}
    <div>ITEM 4. INFORMATION ON THE COMPANY</div>
    {_para("history and development of the company", 6)}
    <div>ITEM 4A. UNRESOLVED STAFF COMMENTS</div>
    <p>None.</p>
    <div>ITEM 5. OPERATING AND FINANCIAL REVIEW AND PROSPECTS</div>
    {_para("operating results", 8)}
    <div>ITEM 6. DIRECTORS, SENIOR MANAGEMENT AND EMPLOYEES</div>
    {_para("the board")}
    <div>ITEM 11. QUANTITATIVE AND QUALITATIVE DISCLOSURES ABOUT MARKET RISK</div>
    {_para("foreign exchange exposure")}
    <div>ITEM 12. DESCRIPTION OF SECURITIES OTHER THAN EQUITY SECURITIES</div>
    </body></html>
    """
    sections = _extract_sections(html, form_type="20-F")
    assert set(sections) == {RISK, MDA, BUSINESS, MARKET}
    assert sections[BUSINESS].startswith("ITEM 4. INFORMATION ON THE COMPANY")
    assert sections[MDA].startswith("ITEM 5. OPERATING AND FINANCIAL REVIEW")
    assert "risks of a foreign private issuer" in sections[RISK]
    assert "history and development" not in sections[RISK]


def test_inline_xbrl_parses_without_the_xml_as_html_warning():
    from bs4 import XMLParsedAsHTMLWarning

    # The Workiva prolog real filings carry pushes "</html>" (the only thing
    # bs4's check recognises) far past its 500-character window.
    ixbrl = (
        "<?xml version='1.0' encoding='ASCII'?>\n"
        "<!--XBRL Document Created with the Workiva Platform-->\n"
        '<html xmlns="http://www.w3.org/1999/xhtml" xmlns:ix="http://www.xbrl.org/2013/inlineXBRL">'
        '<body><div style="display:none"><ix:header><ix:hidden>'
        "<ix:nonNumeric name=\"dei:DocumentType\">Item 1A. Risk Factors hidden fact</ix:nonNumeric>"
        "</ix:hidden></ix:header></div>"
        f"{_TEN_K.split('<body>', 1)[1]}"
    )
    with warnings.catch_warnings():
        warnings.simplefilter("error", XMLParsedAsHTMLWarning)
        sections = _extract_sections(ixbrl, form_type="10-K")
    assert "hidden fact" not in _html_to_text(ixbrl)
    assert RISK in sections


def test_foreign_annual_forms_are_requested_by_default():
    assert {"10-K", "10-Q", "20-F", "40-F"} <= set(DEFAULT_FORM_TYPES)


# ── ingest_ticker: forms requested, form passed through, failures counted ──


@pytest.fixture
def _stub_rag(monkeypatch, tmp_path):
    monkeypatch.setattr(_cik_lookup, "DEFAULT_CACHE_PATH", str(tmp_path / "cik.json"))
    monkeypatch.setattr(ingest_sec_filings, "_CIK_CACHE", {})
    monkeypatch.setattr(ingest_sec_filings.time, "sleep", lambda s: None)
    retrieval = ModuleType("nousergon_lib.rag.retrieval")
    retrieval.document_exists = MagicMock(return_value=False)
    retrieval.ingest_document = MagicMock(return_value="doc-id")
    embeddings = ModuleType("nousergon_lib.rag.embeddings")
    embeddings.embed_texts = MagicMock(side_effect=lambda texts: [[0.0] for _ in texts])
    monkeypatch.setitem(sys.modules, "nousergon_lib.rag.retrieval", retrieval)
    monkeypatch.setitem(sys.modules, "nousergon_lib.rag.embeddings", embeddings)
    return retrieval


def _edgar(monkeypatch, *, forms, docs):
    """Fake EDGAR: one filer, the given recent forms, documents by filename."""
    from datetime import date, timedelta

    filed = (date.today() - timedelta(days=30)).isoformat()

    def get(url, headers=None, timeout=None):
        resp = MagicMock(status_code=200)
        if "company_tickers.json" in url:
            resp.json.return_value = {"0": {"ticker": "TSM", "cik_str": 1046179}}
        elif "submissions/CIK" in url:
            resp.json.return_value = {"filings": {"recent": {
                "form": [f for f, _ in forms],
                "filingDate": [filed] * len(forms),
                "accessionNumber": [f"0001046179-26-00000{i}" for i in range(len(forms))],
                "primaryDocument": [d for _, d in forms],
            }}}
        else:
            name = url.rsplit("/", 1)[-1]
            resp.text = docs.get(name, "")
            resp.status_code = 200 if name in docs else 404
        return resp

    fake = MagicMock()
    fake.get.side_effect = get
    monkeypatch.setattr(ingest_sec_filings, "requests", fake)


def test_a_20f_filer_is_found_and_ingested(monkeypatch, _stub_rag):
    _edgar(monkeypatch, forms=[("20-F", "tsm-20f.htm"), ("6-K", "tsm-6k.htm")],
           docs={"tsm-20f.htm": _TEN_K.replace("Item 7.", "Item 5.")})
    stats = SourceYield(source="sec_filings", scope=1)

    n = ingest_sec_filings.ingest_ticker("TSM", stats=stats)

    assert n == 1
    assert stats.discovered == 1  # the 6-K is not requested
    _, kwargs = _stub_rag.ingest_document.call_args
    assert kwargs["doc_type"] == "20-F"


def test_a_filing_with_no_sections_is_counted_not_silent(monkeypatch, _stub_rag):
    empty = "<html><body>" + _para("nothing that looks like an item", 20) + "</body></html>"
    _edgar(monkeypatch, forms=[("10-K", "a-10k.htm")], docs={"a-10k.htm": empty})
    stats = SourceYield(source="sec_filings", scope=1)

    assert ingest_sec_filings.ingest_ticker("TSM", stats=stats) == 0
    assert stats.failures == {"no_sections": 1}
    assert stats.discovered == 1 and stats.ingested == 0
