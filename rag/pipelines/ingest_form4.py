"""Ingest SEC Form 4 (insider transactions) into structured S3 parquet.

Wave 1 PR B of the institutional data-revamp arc (plan doc:
``~/Development/alpha-engine-docs/private/data-revamp-260513.md``).

Form 4 is the Section 16 filing every officer, director, and 10%
beneficial owner files within 2 business days of each transaction in
the issuer's securities. Net insider activity (buy vs sell pressure)
is a real institutional alpha signal, especially:

- Cluster buying by multiple insiders → strong bullish
- CEO/CFO buys (vs RSU vests) → distinguished from forced exercises
- Sales right after earnings → may signal weakness
- 10b5-1 plan disclosures → reduce noise from scheduled sales

This module fetches recent Form 4 filings from EDGAR, parses the
strict XML schema, and emits structured per-transaction rows to S3
parquet at ``s3://alpha-engine-research/data/insider_transactions/
{filed_date}.parquet`` — one parquet per filing-date.

Why structured + not RAG: Form 4 data is fundamentally tabular. The
alpha signal is in the aggregates (sum of insider $ flow over 90d
per ticker), not in the document narrative. Downstream rollup
(per-(ticker, quarter) aggregates with net_dollar_flow, n_insiders,
n_buys, n_sells) is a follow-up sub-PR.

Discovery: EDGAR ``data.sec.gov/submissions/CIK{padded}.json`` —
shared with the 8-K + 10-K/Q pipelines.

Download: ``www.sec.gov/Archives/edgar/data/{cik}/{accession_no_dashes}/
{primary_doc}`` — Form 4 ``primaryDocument`` is the XML file directly.
"""

from __future__ import annotations

import argparse
import logging
import re
import time
import xml.etree.ElementTree as ET
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta, timezone
from io import BytesIO
from typing import Any

import pandas as pd
import requests

logger = logging.getLogger(__name__)


_SEC_HEADERS = {
    "User-Agent": "AlphaEngine research@nousergon.ai",
    "Accept-Encoding": "gzip, deflate",
}

# EDGAR rate limit: 10 req/sec per IP. We use 0.12s between requests
# (~8 req/sec) for safety margin.
_INTER_REQUEST_SLEEP_SECONDS = 0.12

DEFAULT_S3_BUCKET = "alpha-engine-research"
DEFAULT_S3_PREFIX = "data/insider_transactions"

SCHEMA_VERSION = 1


# ── CIK lookup (mirror of ingest_8k_filings._get_cik) ──────────────────


_CIK_CACHE: dict[str, str] = {}


def _get_cik(ticker: str, *, http=requests) -> str | None:
    """Look up the EDGAR CIK for a ticker symbol. Cached per-process."""
    if ticker in _CIK_CACHE:
        return _CIK_CACHE[ticker]
    try:
        resp = http.get(
            "https://www.sec.gov/files/company_tickers.json",
            headers=_SEC_HEADERS, timeout=10,
        )
        if resp.status_code == 200:
            for entry in resp.json().values():
                _CIK_CACHE[entry.get("ticker", "").upper()] = str(
                    entry.get("cik_str", "")
                )
            return _CIK_CACHE.get(ticker.upper())
    except Exception as e:
        logger.warning("[form4] CIK lookup failed for %s: %s", ticker, e)
    return None


# ── Structured row shape ──────────────────────────────────────────────


@dataclass(frozen=True)
class Form4Transaction:
    """One insider transaction row.

    Schema is per-transaction (one Form 4 may report multiple
    transactions in the same filing). All amount fields are nullable
    when the filing doesn't disclose (e.g. derivatives without a
    cash transaction).
    """

    ticker: str
    issuer_cik: str
    accession_number: str
    filed_date: date
    schema_version: int
    transaction_date: date | None
    reporting_owner_name: str
    reporting_owner_cik: str | None
    is_director: bool
    is_officer: bool
    is_ten_percent_owner: bool
    officer_title: str
    security_title: str
    transaction_code: str
    """SEC transaction type code:
    A=grant, D=disposition, M=exercise, S=sale, P=purchase, F=tax-withholding,
    G=gift, J=other, ..."""
    transaction_shares: float | None
    transaction_price_per_share: float | None
    acquired_disposed_code: str
    """'A' = acquired, 'D' = disposed."""
    transaction_value_usd: float | None
    shares_owned_after: float | None
    direct_or_indirect: str
    """'D' = direct, 'I' = indirect."""
    is_derivative: bool
    fetched_at: datetime


# ── XML parser ─────────────────────────────────────────────────────────


def _text_in(elem: ET.Element | None, path: str) -> str:
    """Safely extract trimmed text at ``path`` under ``elem``. Returns
    empty string if missing."""
    if elem is None:
        return ""
    sub = elem.find(path)
    if sub is None or sub.text is None:
        return ""
    return sub.text.strip()


def _value_at(elem: ET.Element | None, path: str) -> str:
    """Form 4 wraps most field values inside a ``<value>`` child. This
    helper unwraps. Returns empty string if missing."""
    return _text_in(elem, f"{path}/value")


def _parse_bool(s: str) -> bool:
    return s.strip().lower() in {"true", "1", "yes"}


def _parse_float(s: str) -> float | None:
    if not s:
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _parse_date(s: str) -> date | None:
    if not s:
        return None
    try:
        return date.fromisoformat(s[:10])
    except ValueError:
        return None


def parse_form4_xml(
    xml: str,
    *,
    accession_number: str,
    filed_date: date,
) -> list[Form4Transaction]:
    """Parse one Form 4 XML document into a list of structured
    transaction rows.

    Handles both non-derivative and derivative tables. Multi-transaction
    filings emit multiple rows; single-transaction filings emit one
    row. Empty filings emit zero rows.

    Returns ``[]`` on parse failure (logged) — caller should skip the
    filing rather than crash the batch.
    """
    try:
        root = ET.fromstring(xml)
    except ET.ParseError as e:
        logger.warning(
            "[form4] XML parse failed for %s: %s", accession_number, e,
        )
        return []

    issuer = root.find("issuer")
    issuer_cik = _text_in(issuer, "issuerCik")
    ticker = _text_in(issuer, "issuerTradingSymbol").upper()

    owner = root.find("reportingOwner")
    owner_name = _text_in(owner, "reportingOwnerId/rptOwnerName")
    owner_cik = _text_in(owner, "reportingOwnerId/rptOwnerCik") or None
    relationship = owner.find("reportingOwnerRelationship") if owner is not None else None
    is_director = _parse_bool(_text_in(relationship, "isDirector"))
    is_officer = _parse_bool(_text_in(relationship, "isOfficer"))
    is_ten_pct = _parse_bool(_text_in(relationship, "isTenPercentOwner"))
    officer_title = _text_in(relationship, "officerTitle")

    fetched_at = datetime.now(timezone.utc)
    out: list[Form4Transaction] = []

    # Non-derivative table
    for tx in root.findall("nonDerivativeTable/nonDerivativeTransaction"):
        row = _build_row(
            tx,
            ticker=ticker,
            issuer_cik=issuer_cik,
            accession_number=accession_number,
            filed_date=filed_date,
            owner_name=owner_name,
            owner_cik=owner_cik,
            is_director=is_director,
            is_officer=is_officer,
            is_ten_pct=is_ten_pct,
            officer_title=officer_title,
            is_derivative=False,
            fetched_at=fetched_at,
        )
        if row is not None:
            out.append(row)

    # Derivative table (options, warrants, etc.)
    for tx in root.findall("derivativeTable/derivativeTransaction"):
        row = _build_row(
            tx,
            ticker=ticker,
            issuer_cik=issuer_cik,
            accession_number=accession_number,
            filed_date=filed_date,
            owner_name=owner_name,
            owner_cik=owner_cik,
            is_director=is_director,
            is_officer=is_officer,
            is_ten_pct=is_ten_pct,
            officer_title=officer_title,
            is_derivative=True,
            fetched_at=fetched_at,
        )
        if row is not None:
            out.append(row)

    return out


def _build_row(
    tx: ET.Element,
    *,
    ticker: str,
    issuer_cik: str,
    accession_number: str,
    filed_date: date,
    owner_name: str,
    owner_cik: str | None,
    is_director: bool,
    is_officer: bool,
    is_ten_pct: bool,
    officer_title: str,
    is_derivative: bool,
    fetched_at: datetime,
) -> Form4Transaction | None:
    """Build one Form4Transaction from a transaction element."""
    security_title = _value_at(tx, "securityTitle")
    transaction_date = _parse_date(_value_at(tx, "transactionDate"))
    coding = tx.find("transactionCoding")
    # transactionCode is direct text in older filings, wrapped in
    # <value> in newer ones. Try unwrapped first, fall back to <value>.
    if coding is not None:
        direct = _text_in(coding, "transactionCode")
        transaction_code = direct or _value_at(coding, "transactionCode")
    else:
        transaction_code = ""
    amounts = tx.find("transactionAmounts")
    shares = _parse_float(_value_at(amounts, "transactionShares"))
    price = _parse_float(_value_at(amounts, "transactionPricePerShare"))
    acquired_disposed = _value_at(amounts, "transactionAcquiredDisposedCode")
    post = tx.find("postTransactionAmounts")
    shares_owned_after = _parse_float(
        _value_at(post, "sharesOwnedFollowingTransaction")
    )
    ownership = tx.find("ownershipNature")
    direct_or_indirect = _value_at(ownership, "directOrIndirectOwnership")

    transaction_value = None
    if shares is not None and price is not None:
        transaction_value = round(shares * price, 2)

    return Form4Transaction(
        ticker=ticker,
        issuer_cik=issuer_cik,
        accession_number=accession_number,
        filed_date=filed_date,
        schema_version=SCHEMA_VERSION,
        transaction_date=transaction_date,
        reporting_owner_name=owner_name,
        reporting_owner_cik=owner_cik,
        is_director=is_director,
        is_officer=is_officer,
        is_ten_percent_owner=is_ten_pct,
        officer_title=officer_title,
        security_title=security_title,
        transaction_code=transaction_code,
        transaction_shares=shares,
        transaction_price_per_share=price,
        acquired_disposed_code=acquired_disposed,
        transaction_value_usd=transaction_value,
        shares_owned_after=shares_owned_after,
        direct_or_indirect=direct_or_indirect,
        is_derivative=is_derivative,
        fetched_at=fetched_at,
    )


# ── EDGAR discovery + download ────────────────────────────────────────


def _search_form4_filings(
    ticker: str,
    *,
    lookback_days: int = 90,
    http=requests,
) -> list[dict]:
    """List recent Form 4 filings for ``ticker`` via the EDGAR
    submissions API.

    Each result has: ``accession_number``, ``filed_date``,
    ``primary_doc`` (XML file name), ``url`` (composed Archives URL).
    Returns empty list if CIK lookup fails or no filings in window.
    """
    cik = _get_cik(ticker, http=http)
    if not cik:
        return []

    cik_padded = cik.zfill(10)
    url = f"https://data.sec.gov/submissions/CIK{cik_padded}.json"
    try:
        time.sleep(_INTER_REQUEST_SLEEP_SECONDS)
        resp = http.get(url, headers=_SEC_HEADERS, timeout=15)
        if resp.status_code != 200:
            return []
        data = resp.json()
    except Exception as e:
        logger.warning(
            "[form4] EDGAR submissions API failed for %s: %s", ticker, e,
        )
        return []

    recent = (data.get("filings") or {}).get("recent") or {}
    forms = recent.get("form") or []
    accessions = recent.get("accessionNumber") or []
    filed_dates = recent.get("filingDate") or []
    primary_docs = recent.get("primaryDocument") or []

    cutoff = date.today() - timedelta(days=lookback_days)
    results: list[dict] = []
    for form, acc, filed_str, primary in zip(
        forms, accessions, filed_dates, primary_docs,
    ):
        if form.upper() != "4":
            continue
        try:
            filed = date.fromisoformat(filed_str)
        except ValueError:
            continue
        if filed < cutoff:
            continue
        acc_nodash = acc.replace("-", "")
        results.append({
            "accession_number": acc,
            "filed_date": filed,
            "primary_doc": primary,
            "url": f"https://www.sec.gov/Archives/edgar/data/{cik}/"
                   f"{acc_nodash}/{primary}",
        })
    return results


def _download_xml(url: str, *, http=requests) -> str | None:
    try:
        time.sleep(_INTER_REQUEST_SLEEP_SECONDS)
        resp = http.get(url, headers=_SEC_HEADERS, timeout=15)
        if resp.status_code != 200:
            return None
        return resp.text
    except Exception as e:
        logger.warning("[form4] download failed for %s: %s", url, e)
        return None


# ── S3 parquet writer ──────────────────────────────────────────────────


def s3_key_for_filed_date(
    filed_date: date, *, prefix: str = DEFAULT_S3_PREFIX,
) -> str:
    """Canonical S3 key. One parquet file per filed_date holds all
    insider transactions filed that day across all tickers."""
    return f"{prefix}/{filed_date.isoformat()}.parquet"


def transactions_to_dataframe(
    transactions: list[Form4Transaction],
) -> pd.DataFrame:
    """Convert a list of Form4Transaction records to a DataFrame ready
    for parquet. Returns an empty DataFrame with the canonical schema
    if the input is empty.
    """
    if not transactions:
        cols = list(Form4Transaction.__dataclass_fields__.keys())
        return pd.DataFrame(columns=cols)
    return pd.DataFrame([asdict(tx) for tx in transactions])


def write_form4_parquet(
    transactions: list[Form4Transaction],
    *,
    filed_date: date,
    s3_client: Any,
    bucket: str = DEFAULT_S3_BUCKET,
    prefix: str = DEFAULT_S3_PREFIX,
) -> str:
    """Write transactions for one filed_date to S3 parquet. Returns
    the S3 key. Idempotent overwrite."""
    df = transactions_to_dataframe(transactions)
    key = s3_key_for_filed_date(filed_date, prefix=prefix)
    buf = BytesIO()
    df.to_parquet(buf, engine="pyarrow", index=False)
    buf.seek(0)
    s3_client.put_object(
        Bucket=bucket, Key=key, Body=buf.getvalue(),
        ContentType="application/octet-stream",
    )
    logger.info(
        "[form4] wrote %d rows to s3://%s/%s", len(df), bucket, key,
    )
    return key


# ── Orchestrator ───────────────────────────────────────────────────────


def ingest_for_tickers(
    tickers: list[str],
    *,
    lookback_days: int = 90,
    s3_client: Any,
    bucket: str = DEFAULT_S3_BUCKET,
    prefix: str = DEFAULT_S3_PREFIX,
    http=requests,
    dry_run: bool = False,
) -> dict[str, int]:
    """End-to-end: for each ticker, fetch recent Form 4 filings,
    parse, and write per-filed_date parquet to S3.

    Returns stats dict::

        {
            "n_tickers": int,
            "n_filings_discovered": int,
            "n_filings_downloaded": int,
            "n_transactions_parsed": int,
            "n_parquet_writes": int,
            "n_failures": int,
        }
    """
    stats = {
        "n_tickers": len(tickers),
        "n_filings_discovered": 0,
        "n_filings_downloaded": 0,
        "n_transactions_parsed": 0,
        "n_parquet_writes": 0,
        "n_failures": 0,
    }
    # Group transactions by filed_date so we write one parquet per day
    by_filed_date: dict[date, list[Form4Transaction]] = {}

    for ticker in tickers:
        filings = _search_form4_filings(
            ticker, lookback_days=lookback_days, http=http,
        )
        stats["n_filings_discovered"] += len(filings)
        for filing in filings:
            xml = _download_xml(filing["url"], http=http)
            if xml is None:
                stats["n_failures"] += 1
                continue
            stats["n_filings_downloaded"] += 1
            transactions = parse_form4_xml(
                xml,
                accession_number=filing["accession_number"],
                filed_date=filing["filed_date"],
            )
            stats["n_transactions_parsed"] += len(transactions)
            by_filed_date.setdefault(
                filing["filed_date"], [],
            ).extend(transactions)

    if dry_run:
        logger.info(
            "[form4][DRY RUN] would write %d parquet files",
            len(by_filed_date),
        )
        stats["n_parquet_writes"] = len(by_filed_date)
        return stats

    for filed_date, transactions in by_filed_date.items():
        try:
            write_form4_parquet(
                transactions,
                filed_date=filed_date,
                s3_client=s3_client,
                bucket=bucket,
                prefix=prefix,
            )
            stats["n_parquet_writes"] += 1
        except Exception as e:
            stats["n_failures"] += 1
            logger.warning(
                "[form4] parquet write failed for %s: %s", filed_date, e,
            )

    logger.info("[form4] complete: %s", stats)
    return stats


# ── CLI ────────────────────────────────────────────────────────────────


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    parser = argparse.ArgumentParser(
        description="Ingest SEC Form 4 (insider transactions) to S3 parquet",
    )
    parser.add_argument(
        "--tickers", type=str, required=True,
        help="Comma-separated ticker list.",
    )
    parser.add_argument(
        "--lookback-days", type=int, default=90,
        help="Lookback window in days (default 90).",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Discover + parse but don't write to S3.",
    )
    parser.add_argument(
        "--bucket", type=str, default=DEFAULT_S3_BUCKET,
    )
    args = parser.parse_args()

    import boto3
    s3 = boto3.client("s3")
    tickers = [t.strip().upper() for t in args.tickers.split(",") if t.strip()]
    stats = ingest_for_tickers(
        tickers,
        lookback_days=args.lookback_days,
        s3_client=s3,
        bucket=args.bucket,
        dry_run=args.dry_run,
    )
    print(stats)


if __name__ == "__main__":
    main()
