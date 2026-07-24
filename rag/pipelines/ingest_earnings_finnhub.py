"""Ingest earnings call transcripts from Finnhub into the RAG vector store.

Finnhub free tier provides speaker-labeled transcripts at 60 req/min.
Higher quality than FMP (pre-split by speaker with roles).

Requires: FINNHUB_API_KEY environment variable (free at finnhub.io).

Usage:
    # Ingest recent transcripts for specific tickers
    python -m rag.pipelines.ingest_earnings_finnhub --tickers AAPL,MSFT

    # Backfill from signals universe
    python -m rag.pipelines.ingest_earnings_finnhub --from-signals

    # Dry run
    python -m rag.pipelines.ingest_earnings_finnhub --tickers AAPL --dry-run
"""

from __future__ import annotations

import argparse
import json
import logging
import time
from datetime import date

import requests

from nousergon_lib.secrets import get_secret

logger = logging.getLogger(__name__)

_FINNHUB_BASE = "https://finnhub.io/api/v1"
_CHUNK_SIZE = 400
_CHUNK_OVERLAP = 50


def _get_api_key() -> str:
    key = get_secret("FINNHUB_API_KEY", required=False, default="")
    if not key:
        raise RuntimeError("FINNHUB_API_KEY not set — sign up free at finnhub.io")
    return key


def _list_transcripts(ticker: str) -> list[dict]:
    """List available earnings call transcripts for a ticker.

    Returns list of {id, title, time, year, quarter} dicts.
    """
    key = _get_api_key()
    url = f"{_FINNHUB_BASE}/stock/transcripts/list"
    try:
        time.sleep(1.1)  # 60 req/min = 1 req/sec
        resp = requests.get(url, params={"symbol": ticker, "token": key}, timeout=15)
        if resp.status_code != 200:
            logger.debug("Finnhub transcript list %d for %s", resp.status_code, ticker)
            return []
        data = resp.json()
        return data.get("transcripts", [])
    except Exception as e:
        logger.warning("Finnhub list failed for %s: %s", ticker, e)
        return []


def _fetch_transcript(transcript_id: str) -> dict | None:
    """Fetch a single transcript by ID.

    Returns {symbol, title, time, transcript: [{name, role, speech}]}.
    """
    key = _get_api_key()
    url = f"{_FINNHUB_BASE}/stock/transcripts"
    try:
        time.sleep(1.1)
        resp = requests.get(url, params={"id": transcript_id, "token": key}, timeout=15)
        if resp.status_code != 200:
            return None
        data = resp.json()
        if not data.get("transcript"):
            return None
        return data
    except Exception as e:
        logger.warning("Finnhub transcript fetch failed for %s: %s", transcript_id, e)
        return None


def _transcript_to_sections(transcript_data: dict) -> dict[str, str]:
    """Split speaker-labeled transcript into prepared remarks and Q&A.

    Finnhub returns [{name, role, speech}] entries. We split by detecting
    the transition to analyst questions.
    """
    entries = transcript_data.get("transcript", [])
    if not entries:
        return {}

    prepared = []
    qa = []
    in_qa = False

    for entry in entries:
        name = entry.get("name", "")
        role = (entry.get("role", "") or "").lower()
        speech = entry.get("speech", "")
        if not speech:
            continue

        # Detect Q&A transition: first analyst/external question
        if not in_qa and role in ("analyst", ""):
            # Check if this looks like a question (short, from non-company person)
            if "?" in speech or role == "analyst":
                in_qa = True

        speaker_line = f"**{name}** ({role}):" if role else f"**{name}**:"
        text = f"{speaker_line} {speech}"

        if in_qa:
            qa.append(text)
        else:
            prepared.append(text)

    sections = {}
    if prepared:
        sections["prepared_remarks"] = "\n\n".join(prepared)
    if qa:
        sections["qa_session"] = "\n\n".join(qa)

    # If we couldn't split, put everything in prepared_remarks
    if not sections and entries:
        all_text = "\n\n".join(
            f"**{e.get('name', '')}**: {e.get('speech', '')}"
            for e in entries if e.get("speech")
        )
        if all_text:
            sections["prepared_remarks"] = all_text

    # Truncate very long sections
    for key in sections:
        if len(sections[key]) > 60000:
            sections[key] = sections[key][:60000]

    return {k: v for k, v in sections.items() if len(v) > 100}


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
    max_transcripts: int = 8,
    dry_run: bool = False,
) -> int:
    """Ingest earnings transcripts for a single ticker via Finnhub.

    Returns number of transcripts ingested.
    """
    from nousergon_lib.rag.embeddings import embed_texts
    from nousergon_lib.rag.retrieval import ingest_document, document_exists

    available = _list_transcripts(ticker)
    if not available:
        return 0

    # Take most recent N transcripts
    available = available[:max_transcripts]
    ingested = 0

    for t_meta in available:
        transcript_id = t_meta.get("id", "")
        year = t_meta.get("year", 0)
        quarter = t_meta.get("quarter", 0)
        time_str = t_meta.get("time", "")

        # Parse filed date from the time field or approximate from year/quarter
        try:
            filed_date = date.fromisoformat(time_str[:10])
        except (ValueError, IndexError):
            q_month = quarter * 3 if quarter else 12
            filed_date = date(year, min(q_month, 12), 28) if year else date.today()

        if document_exists(ticker, "earnings_transcript", filed_date, "finnhub"):
            continue

        if dry_run:
            logger.info("[DRY RUN] Would ingest %s Q%d %d transcript", ticker, quarter, year)
            ingested += 1
            continue

        transcript_data = _fetch_transcript(transcript_id)
        if not transcript_data:
            continue

        sections = _transcript_to_sections(transcript_data)
        if not sections:
            continue

        all_chunks = []
        for section_label, section_text in sections.items():
            for chunk_text in _chunk_text(section_text):
                all_chunks.append({"content": chunk_text, "section_label": section_label})

        if not all_chunks:
            continue

        embeddings = embed_texts([c["content"] for c in all_chunks])
        for chunk, emb in zip(all_chunks, embeddings):
            chunk["embedding"] = emb

        doc_id = ingest_document(
            ticker=ticker,
            sector=sector,
            doc_type="earnings_transcript",
            source="finnhub",
            filed_date=filed_date,
            title=t_meta.get("title", f"{ticker} Q{quarter} {year} Earnings Call"),
            url=None,
            chunks=all_chunks,
        )
        if doc_id:
            ingested += 1

    return ingested


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    parser = argparse.ArgumentParser(description="Ingest earnings transcripts from Finnhub into RAG")
    parser.add_argument("--tickers", type=str, help="Comma-separated ticker list")
    parser.add_argument("--from-signals", action="store_true", help="Load tickers from latest signals.json")
    parser.add_argument("--max-per-ticker", type=int, default=8, help="Max transcripts per ticker")
    parser.add_argument("--dry-run", action="store_true")
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

    total = 0
    for ticker in tickers:
        n = ingest_ticker(ticker, max_transcripts=args.max_per_ticker, dry_run=args.dry_run)
        total += n

    logger.info("Total: %d transcripts ingested for %d tickers", total, len(tickers))


if __name__ == "__main__":
    main()
