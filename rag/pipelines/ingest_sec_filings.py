"""Ingest SEC 10-K and 10-Q filings into the RAG vector store.

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
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

_SEC_HEADERS = {
    "User-Agent": "AlphaEngine research@nousergon.ai",
    "Accept-Encoding": "gzip, deflate",
}

# Sections to extract from 10-K / 10-Q
_TARGET_SECTIONS = [
    "Risk Factors",
    "Management's Discussion and Analysis",
    "Business",
    "Quantitative and Qualitative Disclosures About Market Risk",
]

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


def _extract_sections(html: str) -> dict[str, str]:
    """Extract target sections from filing HTML."""
    soup = BeautifulSoup(html, "lxml")
    text = soup.get_text(separator="\n", strip=True)

    sections = {}
    for section_name in _TARGET_SECTIONS:
        pattern = re.compile(
            rf"(?:Item\s+\d+[A-Z]?\.?\s*)?{re.escape(section_name)}",
            re.IGNORECASE,
        )
        match = pattern.search(text)
        if match:
            start = match.start()
            next_item = re.search(r"\nItem\s+\d+[A-Z]?\.?\s+[A-Z]", text[start + len(match.group()):])
            end = start + len(match.group()) + next_item.start() if next_item else start + 50000
            section_text = text[start:end].strip()
            if len(section_text) > 50000:
                section_text = section_text[:50000]
            if len(section_text) > 200:
                sections[section_name] = section_text

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
) -> int:
    """Ingest SEC filings for a single ticker. Returns count ingested."""
    from nousergon_lib.rag.embeddings import embed_texts
    from nousergon_lib.rag.retrieval import ingest_document, document_exists

    if form_types is None:
        form_types = ["10-K", "10-Q"]

    filings = _search_filings(ticker, form_types, lookback_days)

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
            continue

        if dry_run:
            logger.info("[DRY RUN] Would ingest %s %s %s", ticker, form_type, filed_date)
            ingested += 1
            continue

        html = _download_filing_html(filing["url"])
        if not html:
            logger.warning("Could not download %s %s %s", ticker, form_type, filed_date)
            continue

        sections = _extract_sections(html)
        if not sections:
            logger.warning("No sections extracted from %s %s %s", ticker, form_type, filed_date)
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
            logger.info("Ingested %s %s %s: %d chunks", ticker, form_type, filed_date, len(all_chunks))

    return ingested


# ── CLI ──────────────────────────────────────────────────────────────────────

def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    parser = argparse.ArgumentParser(description="Ingest SEC filings into RAG store")
    parser.add_argument("--tickers", type=str, help="Comma-separated ticker list")
    parser.add_argument("--from-signals", action="store_true", help="Load tickers from latest signals.json on S3")
    parser.add_argument("--lookback-years", type=int, default=2, help="Years of filings to backfill")
    parser.add_argument("--dry-run", action="store_true", help="Print what would be ingested without writing")
    args = parser.parse_args()

    if args.tickers:
        tickers = [t.strip().upper() for t in args.tickers.split(",")]
    elif args.from_signals:
        import boto3
        s3 = boto3.client("s3")
        resp = s3.list_objects_v2(Bucket="alpha-engine-research", Prefix="signals/", Delimiter="/")
        prefixes = sorted([p["Prefix"] for p in resp.get("CommonPrefixes", [])])
        if not prefixes:
            logger.error("No signals found on S3")
            return
        obj = s3.get_object(Bucket="alpha-engine-research", Key=f"{prefixes[-1]}signals.json")
        data = json.loads(obj["Body"].read())
        tickers = [s["ticker"] for s in data.get("universe", []) if s.get("ticker")]
        logger.info("Loaded %d tickers from signals", len(tickers))
    else:
        parser.error("Provide --tickers or --from-signals")
        return

    lookback_days = args.lookback_years * 365
    total = 0
    for ticker in tickers:
        n = ingest_ticker(ticker, lookback_days=lookback_days, dry_run=args.dry_run)
        total += n

    logger.info("Total: %d filings ingested for %d tickers", total, len(tickers))


if __name__ == "__main__":
    main()
