"""Ingest 13F institutional-ownership snapshots into the RAG vector store.

config#2428 ("13F institutional-ownership fetcher queries wrong CIK —
always-empty signal, unwired from scoring/RAG"). A prior groom pass fixed
the CIK bug by replacing the old per-ticker EDGAR fetcher
(``collectors/alternative.py::_fetch_institutional``, now deprecated)
with ``data/derived/inst_ownership.py`` — a derived table built from SEC
quarterly bulk Form 13F data (CUSIP→ticker crosswalk + QoQ deltas), one
row per (ticker, quarter).

This module is the last missing piece: it doesn't re-fetch or re-parse
any SEC data. It reads the ALREADY-BUILT ``inst_ownership`` derived table
(via ``read_inst_ownership_parquet``) and converts each per-ticker QoQ
summary row into one RAG-ingestible document — e.g.:

    "Q2 2026: 12 funds increased AAPL holdings, 3 decreased, net +2.1M
    shares (+4.3% QoQ), top-5 funds now hold 8.2% of shares outstanding."

Why RAG (not just the structured parquet the scoring pipeline reads
directly): the qual analyst's ``search_filings`` tool (crucible-research
``rag_retrieval_tools.py``) documents ``doc_type="13F"`` as part of its
default filings set for narrative-style queries ("has institutional
sentiment shifted on this name?") — before this module, no producer ever
wrote ``doc_type="13F"`` documents, so those queries silently returned
nothing (config#2428 gap #1).

Why structured + also RAG (unlike Form 4, which stays structured-only
per its module docstring): the institutional QoQ summary IS the
narrative here — there's no separate long-form filing text to prefer
over the aggregate. One short chunk per (ticker, quarter) is the right
granularity, mirroring ``ingest_news.py``'s "one chunk per short article"
convention rather than ``ingest_earnings_finnhub.py``'s multi-chunk
transcript splitting.

Idempotency: ``document_exists`` keyed on (ticker, "13F", filed_date,
source) — same convention as every other ingest pipeline. ``filed_date``
is the last day of the covered quarter (e.g. 2026Q2 -> 2026-06-30) so a
re-run against the same quarter's data is a no-op.
"""

from __future__ import annotations

import argparse
import logging
from datetime import date

import pandas as pd

logger = logging.getLogger(__name__)

_RAG_DOC_TYPE = "13F"
_RAG_SOURCE = "sec_13f_bulk"

# Below this many funds moving (increasing + decreasing combined) in
# either direction, the QoQ signal is too thin a sample to narrate with
# confidence — mirrors the institutional_min_funds gate crucible-research's
# stewardship_score institutional component uses for the same reason.
_MIN_FUNDS_MOVING_FOR_NARRATIVE = 1


def _quarter_end_date(quarter: str) -> date:
    """Convert a ``"2026Q2"``-style quarter string to its calendar end
    date (e.g. ``date(2026, 6, 30)``). Used as the RAG ``filed_date``."""
    year = int(quarter[:4])
    q = int(quarter[5])
    month = q * 3
    day = 31 if month in (3, 12) else 30
    return date(year, month, day)


def _fmt_shares(n: float | None, *, signed: bool = False) -> str:
    """Human-scale a share count (e.g. 2_100_000 -> '2.1M').

    ``signed=True`` prepends '+' for non-negative values (used for QoQ
    deltas, where the sign is meaningful); totals (always non-negative)
    render without a sign.
    """
    if n is None or pd.isna(n):
        return "an unknown number of"
    sign = "+" if (signed and n >= 0) else ""
    if abs(n) >= 1_000_000:
        return f"{sign}{n / 1_000_000:.1f}M"
    if abs(n) >= 1_000:
        return f"{sign}{n / 1_000:.1f}K"
    return f"{sign}{n:.0f}"


def _fmt_pct(n: float | None) -> str | None:
    if n is None or pd.isna(n):
        return None
    sign = "+" if n >= 0 else ""
    return f"{sign}{n:.1f}%"


def row_to_chunk_text(row: pd.Series) -> str:
    """Build the narrative chunk body for one (ticker, quarter) row.

    Example output::

        "Q2 2026 13F institutional ownership for AAPL: 12 funds increased
        holdings, 3 decreased, 1 opened new positions, 0 fully exited.
        Net change: +2.1M shares (+4.3% QoQ). 18 funds hold a combined
        450.2M shares. Top-5 funds hold 8.2% of shares outstanding."
    """
    ticker = row.get("ticker", "")
    quarter = row.get("quarter", "")
    q_label = f"Q{quarter[5]} {quarter[:4]}" if quarter and len(quarter) >= 6 else quarter

    n_holding = int(row.get("n_funds_holding", 0) or 0)
    n_inc = int(row.get("n_funds_increasing", 0) or 0)
    n_dec = int(row.get("n_funds_decreasing", 0) or 0)
    n_new = int(row.get("n_funds_new", 0) or 0)
    n_exited = int(row.get("n_funds_exited", 0) or 0)

    shares_qoq = row.get("shares_qoq_change")
    total_shares = row.get("total_shares_held")
    top5_pct = row.get("top5_concentration_pct")

    pct_change = None
    if pd.notna(shares_qoq) and pd.notna(total_shares):
        prior_shares = total_shares - shares_qoq
        if prior_shares:
            pct_change = (shares_qoq / prior_shares) * 100.0

    pieces = [
        f"{q_label} 13F institutional ownership for {ticker}: "
        f"{n_inc} funds increased holdings, {n_dec} decreased, "
        f"{n_new} opened new positions, {n_exited} fully exited."
    ]

    if pd.notna(shares_qoq):
        change_str = _fmt_shares(shares_qoq, signed=True) + " shares"
        pct_str = _fmt_pct(pct_change)
        if pct_str:
            change_str += f" ({pct_str} QoQ)"
        pieces.append(f"Net change: {change_str}.")

    if n_holding:
        shares_str = _fmt_shares(total_shares) if pd.notna(total_shares) else "an unreported number of"
        pieces.append(f"{n_holding} funds hold a combined {shares_str} shares.")

    if pd.notna(top5_pct):
        pieces.append(f"Top-5 funds hold {top5_pct:.1f}% of shares outstanding.")

    return " ".join(pieces)


def _title_for_row(row: pd.Series) -> str:
    ticker = row.get("ticker", "")
    quarter = row.get("quarter", "")
    q_label = f"Q{quarter[5]} {quarter[:4]}" if quarter and len(quarter) >= 6 else quarter
    return f"{ticker} {q_label} 13F Institutional Ownership Summary"


def ingest_inst_ownership(
    inst_ownership_df: pd.DataFrame,
    *,
    tickers: list[str] | None = None,
    ticker_to_sector: dict[str, str] | None = None,
    embed_texts_fn=None,
    document_exists_fn=None,
    ingest_document_fn=None,
    dry_run: bool = False,
) -> dict[str, int]:
    """Ingest the inst_ownership derived table into the RAG corpus.

    One document per (ticker, quarter) row. Rows with zero funds holding
    the ticker (or missing quarter/ticker) are skipped — nothing to
    narrate.

    Returns a stats dict::

        {
            "n_rows_input": int,
            "n_documents_attempted": int,
            "n_documents_skipped_exists": int,
            "n_documents_skipped_no_data": int,
            "n_documents_ingested": int,
            "n_failures": int,
        }

    Args:
        inst_ownership_df: output of
            ``data.derived.inst_ownership.read_inst_ownership_parquet``
            (or an equivalent DataFrame with the same columns).
        tickers: optional allowlist — when given, only these tickers are
            ingested (narrows a full-universe read to the signals
            universe, mirroring the other pipelines' ``--from-signals``).
            ``None`` ingests every row in ``inst_ownership_df``.
        ticker_to_sector: optional ticker -> GICS sector map, same as
            ``ingest_news.ingest_articles``.
        embed_texts_fn / document_exists_fn / ingest_document_fn:
            injectable for testing. Production callers pass None and we
            lazy-import from ``nousergon_lib.rag``.
        dry_run: log the would-be ingest without calling the embedder or
            the DB writer.
    """
    if embed_texts_fn is None:
        from nousergon_lib.rag import embed_texts
        embed_texts_fn = embed_texts
    if document_exists_fn is None:
        from nousergon_lib.rag import document_exists
        document_exists_fn = document_exists
    if ingest_document_fn is None:
        from nousergon_lib.rag import ingest_document
        ingest_document_fn = ingest_document
    ticker_to_sector = ticker_to_sector or {}

    stats = {
        "n_rows_input": 0 if inst_ownership_df is None else len(inst_ownership_df),
        "n_documents_attempted": 0,
        "n_documents_skipped_exists": 0,
        "n_documents_skipped_no_data": 0,
        "n_documents_ingested": 0,
        "n_failures": 0,
    }

    if inst_ownership_df is None or len(inst_ownership_df) == 0:
        logger.info("[ingest_13f] no inst_ownership rows to ingest")
        return stats

    ticker_allowlist = {t.upper() for t in tickers} if tickers else None

    for _, row in inst_ownership_df.iterrows():
        ticker = str(row.get("ticker") or "").upper()
        quarter = row.get("quarter")
        n_holding = row.get("n_funds_holding", 0) or 0

        if not ticker or not quarter or not n_holding:
            stats["n_documents_skipped_no_data"] += 1
            continue
        if ticker_allowlist is not None and ticker not in ticker_allowlist:
            continue

        stats["n_documents_attempted"] += 1
        filed_date = _quarter_end_date(str(quarter))

        if document_exists_fn(ticker, _RAG_DOC_TYPE, filed_date, _RAG_SOURCE):
            stats["n_documents_skipped_exists"] += 1
            continue

        body = row_to_chunk_text(row)
        title = _title_for_row(row)

        if dry_run:
            logger.info(
                "[DRY RUN] Would ingest %s 13F %s: %s",
                ticker, quarter, body[:120],
            )
            stats["n_documents_ingested"] += 1
            continue

        try:
            chunks = [{
                "content": body,
                "section_label": "13f_ownership_summary",
            }]
            embeddings = embed_texts_fn([chunks[0]["content"]])
            chunks[0]["embedding"] = embeddings[0]

            doc_id = ingest_document_fn(
                ticker=ticker,
                sector=ticker_to_sector.get(ticker),
                doc_type=_RAG_DOC_TYPE,
                source=_RAG_SOURCE,
                filed_date=filed_date,
                title=title,
                url=None,
                chunks=chunks,
            )
            if doc_id:
                stats["n_documents_ingested"] += 1
                logger.info("Ingested 13F summary for %s %s", ticker, quarter)
            else:
                stats["n_failures"] += 1
        except Exception as e:
            stats["n_failures"] += 1
            logger.warning("[ingest_13f] failed for %s %s: %s", ticker, quarter, e)

    logger.info("[ingest_13f] complete: %s", stats)
    return stats


# ── CLI ────────────────────────────────────────────────────────────────


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    parser = argparse.ArgumentParser(
        description="Ingest 13F institutional-ownership summaries into RAG",
    )
    grp = parser.add_mutually_exclusive_group(required=False)
    grp.add_argument(
        "--tickers", type=str,
        help="Comma-separated ticker allowlist (default: all tickers in "
             "the inst_ownership table).",
    )
    grp.add_argument(
        "--from-signals", action="store_true",
        help="Restrict to tickers in the latest signals.json on S3.",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Discover + format but don't write to S3/RAG.",
    )
    parser.add_argument(
        "--bucket", type=str, default="alpha-engine-research",
    )
    args = parser.parse_args()

    import boto3
    from data.derived.inst_ownership import read_inst_ownership_parquet

    s3 = boto3.client("s3")

    tickers: list[str] | None = None
    if args.tickers:
        tickers = [t.strip().upper() for t in args.tickers.split(",") if t.strip()]
    elif args.from_signals:
        from rag.pipelines._signals_universe import load_signals_tickers
        tickers = load_signals_tickers(bucket=args.bucket, s3_client=s3)
        if not tickers:
            logger.error("[ingest_13f] --from-signals requested but no tickers found — aborting")
            return

    inst_df = read_inst_ownership_parquet(s3_client=s3, bucket=args.bucket)
    if len(inst_df) == 0:
        logger.warning(
            "[ingest_13f] inst_ownership table empty/unavailable — nothing to ingest "
            "(has compute_and_write_inst_ownership run this quarter?)",
        )

    stats = ingest_inst_ownership(
        inst_df,
        tickers=tickers,
        dry_run=args.dry_run,
    )
    print(stats)


if __name__ == "__main__":
    main()
