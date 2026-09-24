"""Ingest SEC 10-K / 10-Q (and foreign 20-F / 40-F) filings into the RAG vector store.

Downloads filing HTML from SEC EDGAR, extracts key sections (Risk Factors,
MD&A, Business Description), chunks the text, embeds via Voyage, and stores
in Neon pgvector.

Uses the EDGAR full-text search API (efts.sec.gov/LATEST/search-index) for
filing discovery and the EDGAR Archives for document download.

Usage:
    # Ingest recent filings for a list of tickers
    .venv/bin/python -m rag.pipelines.ingest_sec_filings --tickers AAPL,MSFT,GOOG

    # Backfill last 2 years for all tickers in latest signals
    .venv/bin/python -m rag.pipelines.ingest_sec_filings --from-signals --lookback-years 2
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import time
from datetime import date, timedelta

import requests

from rag.pipelines._markup import parse_filing_markup

logger = logging.getLogger(__name__)

_SEC_HEADERS = {
    "User-Agent": "AlphaEngine research@nousergon.ai",
    "Accept-Encoding": "gzip, deflate",
}

# ── Forms and the sections extracted from each ──────────────────────────────
#
# alpha-engine-config-I11472: domestic issuers file 10-K / 10-Q; foreign
# private issuers file the annual 20-F, and Canadian MJDS issuers the annual
# 40-F, and neither files a 10-Q. With only ["10-K", "10-Q"] requested, every
# foreign filer in the RAG scope (ASML, TSM, RACE, RIO, BN, CCJ, SU, ...) was
# "Found 0 filings" every week — silently, because 0 is also what a domestic
# filer with nothing new returns.
DEFAULT_FORM_TYPES: tuple[str, ...] = ("10-K", "10-Q", "20-F", "40-F")

# Section label -> title patterns that head that section, per form. The
# LABELS are the corpus contract (``rag.chunks.section_label``, and
# ``filing_change_detection`` compares sections by label), so a 20-F's
# "Operating and Financial Review and Prospects" lands under the same MD&A
# label a 10-K's Item 7 does: it is the same section under the form's own name.
_RISK = "Risk Factors"
_MDA = "Management's Discussion and Analysis"
_BUSINESS = "Business"
_MARKET_RISK = "Quantitative and Qualitative Disclosures About Market Risk"

_TITLE_RISK = r"risk\s+factors"
_TITLE_MDA = r"management'?s\s+discussion\s+(?:and|&)\s+analysis"
_TITLE_MARKET_RISK = (
    r"quantitative\s+and\s+qualitative\s+disclosures?\s+"
    r"(?:about|of|on|regarding)\s+market\s+risks?"
)

_SECTION_TITLES: dict[str, dict[str, tuple[str, ...]]] = {
    "10-K": {
        _RISK: (_TITLE_RISK,),
        _MDA: (_TITLE_MDA,),
        # "Items 1 and 2. Business and Properties" is matched by the item
        # prefix; the title only has to START with "Business".
        _BUSINESS: (r"business\b",),
        _MARKET_RISK: (_TITLE_MARKET_RISK,),
    },
    # A 10-Q has no Business item.
    "10-Q": {
        _RISK: (_TITLE_RISK,),
        _MDA: (_TITLE_MDA,),
        _MARKET_RISK: (_TITLE_MARKET_RISK,),
    },
    # 20-F: Item 3.D Risk Factors, Item 4 Information on the Company, Item 5
    # Operating and Financial Review and Prospects, Item 11 market risk.
    "20-F": {
        _RISK: (_TITLE_RISK,),
        _MDA: (r"operating\s+and\s+financial\s+review\s+and\s+prospects", _TITLE_MDA),
        _BUSINESS: (r"information\s+on\s+the\s+company",),
        _MARKET_RISK: (_TITLE_MARKET_RISK,),
    },
    # 40-F (MJDS): the Annual Information Form and the MD&A are usually
    # EXHIBITS to the 40-F rather than its primary document, so only what the
    # primary document itself carries can be extracted here.
    "40-F": {
        _RISK: (_TITLE_RISK,),
        _MDA: (_TITLE_MDA,),
        _BUSINESS: (r"description\s+of\s+(?:the\s+)?business", r"business\b"),
        _MARKET_RISK: (_TITLE_MARKET_RISK,),
    },
}

# Kept for callers that read the label list; the per-form map above is the
# source of truth.
_TARGET_SECTIONS = [_RISK, _MDA, _BUSINESS, _MARKET_RISK]

_MIN_SECTION_CHARS = 200
_MAX_SECTION_CHARS = 50000

_CHUNK_SIZE = 400
_CHUNK_OVERLAP = 50


# ── CIK lookup ───────────────────────────────────────────────────────────────
#
# config#2956: the process-level ``_CIK_CACHE`` dict below only lives for
# ONE pipeline step (each is a separate ``python -m`` invocation), so a
# cold cache used to always re-download the ~10k-entry company_tickers.json
# from EDGAR. ``_cik_lookup.load_cik_map`` backs a cold in-memory cache
# with a shared ``/tmp`` file cache (mtime TTL) so only the FIRST step in a
# run (or day) actually hits EDGAR.

from rag.pipelines._cik_lookup import load_cik_map  # noqa: E402
from rag.pipelines.source_yield import SourceYield, write_yield  # noqa: E402

_CIK_CACHE: dict[str, str] = {}


def _get_cik(ticker: str) -> str | None:
    """Look up a company's CIK number from ticker via EDGAR company tickers JSON."""
    if ticker in _CIK_CACHE:
        return _CIK_CACHE[ticker]

    _CIK_CACHE.update(load_cik_map(http=requests, headers=_SEC_HEADERS))
    return _CIK_CACHE.get(ticker.upper())


# ── Filing search via EDGAR submissions API ──────────────────────────────────

def _search_filings(ticker: str, form_types: list[str], lookback_days: int = 730) -> list[dict]:
    """Search SEC EDGAR for recent filings using the submissions API.

    Uses https://data.sec.gov/submissions/CIK{cik}.json which returns
    all recent filings for a company with proper form types.
    """
    cik = _get_cik(ticker)
    if not cik:
        logger.warning("No CIK found for %s", ticker)
        return []

    cik_padded = cik.zfill(10)
    url = f"https://data.sec.gov/submissions/CIK{cik_padded}.json"

    try:
        time.sleep(0.12)  # SEC rate limit: 10 req/sec
        resp = requests.get(url, headers=_SEC_HEADERS, timeout=15)
        if resp.status_code != 200:
            logger.warning("EDGAR submissions API returned %d for %s", resp.status_code, ticker)
            return []
        data = resp.json()
    except Exception as e:
        logger.warning("EDGAR submissions API failed for %s: %s", ticker, e)
        return []

    cutoff = date.today() - timedelta(days=lookback_days)
    form_type_set = set(f.upper() for f in form_types)

    results = []
    recent = data.get("filings", {}).get("recent", {})
    forms = recent.get("form", [])
    dates = recent.get("filingDate", [])
    accessions = recent.get("accessionNumber", [])
    primary_docs = recent.get("primaryDocument", [])

    for i, form in enumerate(forms):
        if form.upper() not in form_type_set:
            continue
        filed_str = dates[i] if i < len(dates) else ""
        if not filed_str:
            continue
        try:
            filed_date = date.fromisoformat(filed_str)
        except ValueError:
            continue
        if filed_date < cutoff:
            continue

        accession = accessions[i] if i < len(accessions) else ""
        primary_doc = primary_docs[i] if i < len(primary_docs) else ""
        accession_path = accession.replace("-", "")

        doc_url = (
            f"https://www.sec.gov/Archives/edgar/data/{cik}/{accession_path}/{primary_doc}"
            if primary_doc
            else f"https://www.sec.gov/Archives/edgar/data/{cik}/{accession_path}/"
        )

        results.append({
            "form_type": form.upper(),
            "filed_date": filed_str,
            "accession_number": accession,
            "cik": cik,
            "primary_doc": primary_doc,
            "url": doc_url,
        })

    logger.info("Found %d %s filings for %s (since %s)", len(results), form_types, ticker, cutoff)
    return results


# ── Filing download and section extraction ───────────────────────────────────

def _download_filing_html(url: str) -> str | None:
    """Download filing document from EDGAR Archives."""
    try:
        time.sleep(0.12)
        resp = requests.get(url, headers=_SEC_HEADERS, timeout=60)
        if resp.status_code == 200 and len(resp.text) > 1000:
            return resp.text
        logger.debug("Filing download returned %d (%d bytes) from %s", resp.status_code, len(resp.text), url)
        return None
    except Exception as e:
        logger.warning("Failed to download filing from %s: %s", url, e)
        return None


# Text normalisation before any heading match: filings set headings with
# no-break / thin spaces and typographic apostrophes, so "Item\xa01A." and
# "Management\u2019s" must read as the ASCII the patterns are written in.
_SPACE_CHARS = dict.fromkeys(map(ord, "\xa0\u2002\u2003\u2007\u2009\u200a\u202f"), " ")
_SPACE_CHARS.update(dict.fromkeys(map(ord, "\u200b\ufeff"), None))
_SPACE_CHARS.update({ord("\u2019"): "'", ord("\u2018"): "'", ord("\u201c"): '"', ord("\u201d"): '"'})

# "Item 1A." / "ITEM 7A:" / "Items 1 and 2." / "Part II, Item 1A." at the start
# of a line. The capture is the item id.
_ITEM_PREFIX = (
    r"^[ \t]*(?:part\s+[ivx]+\s*[,.:\-\u2013\u2014]?\s*)?items?\s*"
    r"(\d{1,2}[a-d]?)\b(?:\s*(?:and|&|,|-|\u2013)\s*\d{1,2}[a-d]?\b)*"
)

# Any item HEADING — an item prefix followed directly by a title that starts
# with a capital (or "[Reserved]"). This is the section boundary. The case of
# the title's first letter is checked case-SENSITIVELY on purpose: it is what
# separates the heading "Item 1B. Unresolved Staff Comments" from running prose
# that merely starts a line with "Item 1A. With the interconnected ...".
_ANY_ITEM_HEADING = re.compile(
    r"(?im:" + _ITEM_PREFIX + r")(?:\.|:|[ \t]*[\-\u2013\u2014])?\s*(?=[A-Z\[])",
)


# A title set on its own line may carry a sub-item letter ("D. Risk Factors",
# 20-F Item 3.D) or a bare item number ("3.D Risk Factors").
_BARE_PREFIX = r"[ \t]*(?:(?:item\s*)?\d{1,2}\s*\.?\s*)?(?:[A-Z]\s*\.\s*)?"


# Block-level elements: each one ends a line of text. Inline elements (the
# <span>s a heading is typeset in) are joined with NO separator, because
# filers split heading words across spans for drop-caps and letter-spacing —
# "Item 1A. RI</span><span>SK FACTORS" (HUBS 10-K 2025) must read as ONE word,
# and bs4's ``get_text(separator="\n")`` put a newline inside it.
_BLOCK_TAGS = (
    "p", "div", "br", "tr", "td", "th", "li", "table", "section",
    "h1", "h2", "h3", "h4", "h5", "h6", "center", "blockquote", "pre", "hr",
)


def _html_to_text(html: str) -> str:
    """Filing HTML -> normalised plain text, one block element per line.

    Parsed by ``parse_filing_markup`` (the parser that matches what the
    document is, alpha-engine-config-I11476). The hidden inline-XBRL
    ``ix:header`` block — machine-readable XBRL context, not filing text — is
    dropped before the text is read so it cannot be mistaken for a heading.
    """
    soup = parse_filing_markup(html)
    for hidden in soup.find_all(["ix:header", "script", "style"]):
        hidden.decompose()
    for block in soup.find_all(_BLOCK_TAGS):
        block.append("\n")
    raw = soup.get_text().translate(_SPACE_CHARS)
    lines = (re.sub(r"[ \t\r\f\v]+", " ", line).strip() for line in raw.split("\n"))
    return "\n".join(line for line in lines if line)


def _pick_section(
    text: str,
    starts: list[tuple[int, int, str | None]],
    boundaries: list[tuple[int, str | None]],
) -> str:
    """Choose the span that IS the section among every heading for it.

    ``starts`` is (heading start, heading end, item id). Each runs to the next
    boundary heading of a DIFFERENT item — a same-item heading is a running
    page header ("PART I / Item 1A" atop every page of MSFT's risk factors),
    not the end of the section.

    A section's heading appears in the table of contents and in
    cross-references as well as over the section itself. The table-of-contents
    entry runs one line to the next entry, so the longest span is the section,
    not the first match — taking the first match is what left 33 filings with
    no sections at all in the 2026-09-23 rehearsal
    (alpha-engine-config-I11472): the TOC line was found, measured under the
    minimum length, and the real heading further down was never looked at.

    One refinement: a cross-reference that starts a line ("Item 1A. Risk
    Factors" quoted inside Business) runs on INTO the real section, so it is
    the longest span while starting in the wrong place. When the longest span
    contains another heading whose own span is at least half its length, that
    inner heading is the section and the outer one was the reference.
    """
    spans: list[tuple[int, int]] = []
    for start, heading_end, item_id in starts:
        end = len(text)
        for pos, b_id in boundaries:
            if pos >= heading_end and (item_id is None or b_id != item_id):
                end = pos
                break
        spans.append((start, end))
    if not spans:
        return ""

    best = max(spans, key=lambda s: s[1] - s[0])
    while True:
        inner = [
            s for s in spans
            if best[0] < s[0] < best[1] and (s[1] - s[0]) * 2 >= (best[1] - best[0])
        ]
        if not inner:
            break
        best = max(inner, key=lambda s: s[1] - s[0])
    return text[best[0]:best[1]].strip()


def _extract_sections(html: str, form_type: str = "10-K") -> dict[str, str]:
    """Extract the target sections of one filing, keyed by section label.

    For every target section, every line-start heading for it is a candidate:
    an item heading first ("Item 1A. Risk Factors", "ITEM 7. MANAGEMENT'S
    DISCUSSION ..."), and — only when no item heading yields a section — a
    line that IS the section title ("Risk Factors" on its own line, as filers
    that use a cross-reference index and 20-F sub-items like "D. Risk Factors"
    set it). :func:`_pick_section` chooses among them.
    """
    titles_by_label = _SECTION_TITLES.get(form_type.upper(), _SECTION_TITLES["10-K"])
    text = _html_to_text(html)

    item_boundaries = [(m.start(), m.group(1).upper()) for m in _ANY_ITEM_HEADING.finditer(text)]
    all_titles = "|".join(t for titles in titles_by_label.values() for t in titles)
    bare_title_line = re.compile(
        r"(?im)^" + _BARE_PREFIX + r"(?:" + all_titles + r")[^\n]{0,40}$",
    )
    bare_boundaries = sorted(
        item_boundaries + [(m.start(), None) for m in bare_title_line.finditer(text)],
        key=lambda b: b[0],
    )

    sections: dict[str, str] = {}
    for label, titles in titles_by_label.items():
        title_alt = "|".join(titles)
        item_heading = re.compile(
            r"(?im:" + _ITEM_PREFIX + r")(?:\.|:|[ \t]*[\-\u2013\u2014])?\s*(?i:" + title_alt + r")",
        )
        starts = [(m.start(), m.end(), m.group(1).upper()) for m in item_heading.finditer(text)]
        best = _pick_section(text, starts, item_boundaries)

        if len(best) <= _MIN_SECTION_CHARS:
            bare_heading = re.compile(
                r"(?im)^" + _BARE_PREFIX + r"(?:" + title_alt + r")[ \t]*\.?[ \t]*$",
            )
            starts = [(m.start(), m.end(), None) for m in bare_heading.finditer(text)]
            best = _pick_section(text, starts, bare_boundaries)

        if len(best) > _MIN_SECTION_CHARS:
            sections[label] = best[:_MAX_SECTION_CHARS]

    return sections


def _chunk_text(text: str, chunk_size: int = _CHUNK_SIZE, overlap: int = _CHUNK_OVERLAP) -> list[str]:
    """Split text into overlapping chunks by approximate token count."""
    words = text.split()
    words_per_chunk = int(chunk_size / 1.3)
    overlap_words = int(overlap / 1.3)

    chunks = []
    start = 0
    while start < len(words):
        end = start + words_per_chunk
        chunk = " ".join(words[start:end])
        if chunk.strip():
            chunks.append(chunk)
        start = end - overlap_words
        if start >= len(words):
            break

    return chunks


# ── Per-ticker ingestion ─────────────────────────────────────────────────────

def ingest_ticker(
    ticker: str,
    sector: str | None = None,
    form_types: list[str] | None = None,
    lookback_days: int = 730,
    dry_run: bool = False,
    stats: SourceYield | None = None,
) -> int:
    """Ingest SEC filings for a single ticker. Returns count ingested.

    ``stats``, when given, accumulates what the source did (documents
    offered, already held, stored, and named failures) for the run's
    source-yield verdict.
    """
    from nousergon_lib.rag.embeddings import embed_texts
    from nousergon_lib.rag.retrieval import ingest_document, document_exists

    if form_types is None:
        form_types = list(DEFAULT_FORM_TYPES)
    if stats is None:
        stats = SourceYield(source="sec_filings")

    filings = _search_filings(ticker, form_types, lookback_days)
    stats.discovered += len(filings)

    ingested = 0
    for filing in filings:
        filed_date_str = filing.get("filed_date", "")
        try:
            filed_date = date.fromisoformat(filed_date_str[:10])
        except ValueError:
            continue

        form_type = filing["form_type"]
        if document_exists(ticker, form_type, filed_date, "sec_edgar"):
            logger.debug("Already ingested: %s %s %s", ticker, form_type, filed_date)
            stats.already_held += 1
            continue

        if dry_run:
            logger.info("[DRY RUN] Would ingest %s %s %s", ticker, form_type, filed_date)
            ingested += 1
            continue

        html = _download_filing_html(filing["url"])
        if not html:
            logger.warning("Could not download %s %s %s", ticker, form_type, filed_date)
            stats.fail("download_failed")
            continue

        sections = _extract_sections(html, form_type=form_type)
        if not sections:
            logger.warning(
                "No sections extracted from %s %s %s (%s)",
                ticker, form_type, filed_date, filing.get("url"),
            )
            stats.fail("no_sections")
            continue

        all_chunks = []
        for section_label, section_text in sections.items():
            for chunk_text in _chunk_text(section_text):
                all_chunks.append({
                    "content": chunk_text,
                    "section_label": section_label,
                })

        if not all_chunks:
            continue

        embeddings = embed_texts([c["content"] for c in all_chunks])
        for chunk, emb in zip(all_chunks, embeddings):
            chunk["embedding"] = emb

        doc_id = ingest_document(
            ticker=ticker,
            sector=sector,
            doc_type=form_type,
            source="sec_edgar",
            filed_date=filed_date,
            title=f"{ticker} {form_type} ({filed_date})",
            url=filing.get("url"),
            chunks=all_chunks,
        )
        if doc_id:
            ingested += 1
            stats.ingested += 1

    return ingested


# ── CLI ──────────────────────────────────────────────────────────────────────

def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    parser = argparse.ArgumentParser(description="Ingest SEC filings into RAG store")
    parser.add_argument("--tickers", type=str, help="Comma-separated ticker list")
    parser.add_argument("--from-signals", action="store_true", help="Load tickers from the scanner decision set (universe-membership cuts.attractiveness_top_60)")
    parser.add_argument("--lookback-years", type=int, default=2, help="Years of filings to backfill")
    parser.add_argument("--dry-run", action="store_true", help="Print what would be ingested without writing")
    args = parser.parse_args()

    if args.tickers:
        tickers = [t.strip().upper() for t in args.tickers.split(",")]
    elif args.from_signals:
        # config-I5700: the corpus scope is the scanner decision set, not
        # signals.json::universe (a 903-row SIZING envelope). One resolver
        # for every pipeline — this used to be an inline copy.
        from rag.pipelines._rag_scope import load_rag_scope_tickers
        tickers = load_rag_scope_tickers()
    else:
        parser.error("Provide --tickers or --from-signals")
        return

    lookback_days = args.lookback_years * 365
    stats = SourceYield(source="sec_filings", scope=len(tickers))
    total = 0
    for ticker in tickers:
        n = ingest_ticker(ticker, lookback_days=lookback_days, dry_run=args.dry_run, stats=stats)
        total += n

    logger.info(
        "Total: %d filings ingested for %d tickers (%d offered, %d already held, failures=%s)",
        total, len(tickers), stats.discovered, stats.already_held, stats.failures or "{}",
    )
    write_yield(stats)


if __name__ == "__main__":
    main()
