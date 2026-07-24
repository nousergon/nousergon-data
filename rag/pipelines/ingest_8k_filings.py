"""Ingest SEC 8-K material event filings into the RAG vector store.

8-K filings report material corporate events: executive changes, M&A,
financial results, material agreements, and Regulation FD disclosures.
These are the highest-signal SEC filings for short-term trading.

Uses SEC EDGAR submissions API for discovery and Archives for download.

Usage:
    # Ingest recent 8-Ks for specific tickers
    python -m rag.pipelines.ingest_8k_filings --tickers AAPL,MSFT

    # Backfill from the corpus scope (holdings ∪ active candidates ∪
    # top-60 signals board — config#2943)
    python -m rag.pipelines.ingest_8k_filings --scope holdings+candidates+board60 --lookback-days 365

    # Daily mode: only last 7 days (for cron/Lambda)
    python -m rag.pipelines.ingest_8k_filings --scope holdings+candidates+board60 --lookback-days 7

config#2943: the old ``--from-signals`` (whole ~900-ticker signals.json
universe) is retired — replaced by ``--scope holdings+candidates+board60``,
resolved via the shared ``rag.pipelines._corpus_scope`` module.
"""

from __future__ import annotations

import argparse
import logging
import os
import re
import time
from datetime import date, timedelta

import warnings

import requests
from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning

warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

logger = logging.getLogger(__name__)

_SEC_HEADERS = {
    "User-Agent": "AlphaEngine research@nousergon.ai",
    "Accept-Encoding": "gzip, deflate",
}

# Material 8-K items worth embedding
_MATERIAL_ITEMS = {
    "1.01": "Entry into a Material Definitive Agreement",
    "1.02": "Termination of a Material Definitive Agreement",
    "2.01": "Completion of Acquisition or Disposition of Assets",
    "2.02": "Results of Operations and Financial Condition",
    "2.05": "Costs Associated with Exit or Disposal Activities",
    "2.06": "Material Impairments",
    "4.01": "Changes in Registrant's Certifying Accountant",
    "4.02": "Non-Reliance on Previously Issued Financial Statements",
    "5.02": "Departure/Election of Directors or Principal Officers",
    "7.01": "Regulation FD Disclosure",
    "8.01": "Other Events",
}

_CHUNK_SIZE = 400
_CHUNK_OVERLAP = 50

# ── CIK lookup (shared with ingest_sec_filings) ────────────────────────────
#
# config#2956: backed by the shared ``/tmp`` file cache (see
# ``_cik_lookup.load_cik_map``) so a cold in-memory cache in THIS process
# doesn't re-download company_tickers.json if another pipeline step
# already fetched it this run/day.

from rag.pipelines._cik_lookup import load_cik_map  # noqa: E402
from rag.pipelines._corpus_scope import (  # noqa: E402
    DEFAULT_BUCKET,
    add_scope_arg,
    resolve_tickers_from_args,
)

_CIK_CACHE: dict[str, str] = {}


def _get_cik(ticker: str) -> str | None:
    if ticker in _CIK_CACHE:
        return _CIK_CACHE[ticker]
    _CIK_CACHE.update(load_cik_map(http=requests, headers=_SEC_HEADERS))
    return _CIK_CACHE.get(ticker.upper())


def _search_8k_filings(ticker: str, lookback_days: int = 365) -> list[dict]:
    """Search for 8-K filings via EDGAR submissions API."""
    cik = _get_cik(ticker)
    if not cik:
        return []

    cik_padded = cik.zfill(10)
    url = f"https://data.sec.gov/submissions/CIK{cik_padded}.json"

    try:
        time.sleep(0.12)
        resp = requests.get(url, headers=_SEC_HEADERS, timeout=15)
        if resp.status_code != 200:
            return []
        data = resp.json()
    except Exception as e:
        logger.warning("EDGAR API failed for %s: %s", ticker, e)
        return []

    cutoff = date.today() - timedelta(days=lookback_days)
    results = []
    recent = data.get("filings", {}).get("recent", {})
    forms = recent.get("form", [])
    dates = recent.get("filingDate", [])
    accessions = recent.get("accessionNumber", [])
    primary_docs = recent.get("primaryDocument", [])

    for i, form in enumerate(forms):
        if form.upper() != "8-K":
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
        doc_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{accession_path}/{primary_doc}"

        results.append({
            "form_type": "8-K",
            "filed_date": filed_str,
            "accession_number": accession,
            "url": doc_url,
        })

    logger.debug("Found %d 8-K filings for %s (since %s)", len(results), ticker, cutoff)
    return results


def _download_and_extract(url: str) -> str | None:
    """Download 8-K filing and extract text content."""
    try:
        time.sleep(0.12)
        resp = requests.get(url, headers=_SEC_HEADERS, timeout=60)
        if resp.status_code != 200 or len(resp.text) < 200:
            return None
        soup = BeautifulSoup(resp.text, "lxml")
        text = soup.get_text(separator="\n", strip=True)
        # 8-Ks are typically short; cap at 15K chars
        return text[:15000] if len(text) > 15000 else text
    except Exception as e:
        # Per-URL download failure: visible at WARNING so the rate of
        # failures can be monitored in SSM logs. Caller treats None as
        # "skip this filing" and continues; aggregated across all 8-Ks
        # the caller already reports counts, so there's no hidden drift.
        logger.warning("8-K download failed from %s: %s", url, e)
        return None


def _detect_items(text: str) -> list[str]:
    """Detect which material items are reported in the 8-K."""
    found = []
    for item_num, item_name in _MATERIAL_ITEMS.items():
        pattern = rf"Item\s+{re.escape(item_num)}"
        if re.search(pattern, text, re.IGNORECASE):
            found.append(f"Item {item_num}: {item_name}")
    return found


def _chunk_text(text: str) -> list[str]:
    words = text.split()
    words_per_chunk = int(_CHUNK_SIZE / 1.3)
    overlap_words = int(_CHUNK_OVERLAP / 1.3)
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


def ingest_ticker(
    ticker: str,
    sector: str | None = None,
    lookback_days: int = 365,
    dry_run: bool = False,
) -> int:
    """Ingest 8-K filings for a single ticker. Returns count ingested."""
    from nousergon_lib.rag.embeddings import embed_texts
    from nousergon_lib.rag.retrieval import ingest_document, document_exists

    filings = _search_8k_filings(ticker, lookback_days)
    ingested = 0

    for filing in filings:
        filed_str = filing.get("filed_date", "")
        try:
            filed_date = date.fromisoformat(filed_str[:10])
        except ValueError:
            continue

        if document_exists(ticker, "8-K", filed_date, "sec_edgar"):
            continue

        if dry_run:
            logger.info("[DRY RUN] Would ingest %s 8-K %s", ticker, filed_date)
            ingested += 1
            continue

        text = _download_and_extract(filing["url"])
        if not text or len(text) < 200:
            continue

        # Detect material items for section labeling
        items = _detect_items(text)
        section_label = "; ".join(items) if items else "8-K"
        if len(section_label) > 100:
            section_label = section_label[:97] + "..."

        chunks_text = _chunk_text(text)
        if not chunks_text:
            continue

        all_chunks = [{"content": c, "section_label": section_label} for c in chunks_text]

        embeddings = embed_texts([c["content"] for c in all_chunks])
        for chunk, emb in zip(all_chunks, embeddings):
            chunk["embedding"] = emb

        title = f"{ticker} 8-K ({filed_date})"
        if items:
            title += f" — {items[0]}"

        doc_id = ingest_document(
            ticker=ticker,
            sector=sector,
            doc_type="8-K",
            source="sec_edgar",
            filed_date=filed_date,
            title=title,
            url=filing.get("url"),
            chunks=all_chunks,
        )
        if doc_id:
            ingested += 1

    return ingested


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    parser = argparse.ArgumentParser(description="Ingest 8-K filings into RAG store")
    parser.add_argument("--tickers", type=str, help="Comma-separated ticker list")
    add_scope_arg(parser)
    parser.add_argument("--bucket", type=str, default=DEFAULT_BUCKET)
    parser.add_argument("--lookback-days", type=int, default=365, help="Days of filings to backfill")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    tickers = resolve_tickers_from_args(args)
    if not tickers:
        parser.error("Provide --tickers or --scope holdings+candidates+board60")
        return
    logger.info("Resolved %d tickers for 8-K ingestion", len(tickers))

    total = 0
    for ticker in tickers:
        n = ingest_ticker(ticker, lookback_days=args.lookback_days, dry_run=args.dry_run)
        total += n

    logger.info("Total: %d 8-K filings ingested for %d tickers", total, len(tickers))


if __name__ == "__main__":
    main()
