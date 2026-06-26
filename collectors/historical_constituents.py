"""
historical_constituents.py — Point-in-time S&P 500 index membership (G12).

Survivorship-bias mitigation, Phase 1 (research memo
``nousergon-docs/survivorship-bias-research.md``): the backtester/predictor
today see only *currently-listed* constituents, so 10y synthetic backtests
silently exclude every name that was delisted, acquired, or index-dropped —
an upward (survivor) bias on backtest credibility (~1-4%/yr overstatement
class).

This module reconstructs as-of-date membership by **replaying the Wikipedia
"Selected changes to the list" table backward from today's roster**: starting
from the current constituents, each change (ticker *added* on date D, ticker
*removed* on date D) is undone walking from newest to oldest, so the membership
set immediately *before* date D is recovered. The output is a
``{date: [tickers]}`` map the backtester reads to define the point-in-time
universe for each backtest date.

Two layers, deliberately separated so the risky parsing is unit-tested with no
network:
  * ``parse_changes_table(df)`` — Wikipedia changes DataFrame -> list of
    structured ``ConstituentChange`` events (pure).
  * ``build_pit_membership(current_tickers, changes)`` -> ``{date: [tickers]}``
    point-in-time map (pure).
  * ``collect(...)`` — fetch + build + write to S3 (the I/O shell).

S&P 500 only (the changes table on the S&P 400 page is sparser); the memo
flags S&P 400 mid-cap as a follow-on. Delisted-ticker *prices* are memo
Phase 2 — out of scope here; this ships the membership list.
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from io import StringIO

import boto3
import pandas as pd
import requests

logger = logging.getLogger(__name__)

_SP500_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
_HEADERS = {"User-Agent": "alpha-engine-data/1.0 (historical-constituents)"}

ADDED = "added"
REMOVED = "removed"


@dataclass(frozen=True)
class ConstituentChange:
    """One index-membership change event from the Wikipedia changes table."""

    date: str  # ISO YYYY-MM-DD
    ticker: str
    action: str  # ADDED or REMOVED


def _normalize_ticker(raw: object) -> str | None:
    """Wikipedia uses BRK.B etc.; strip footnote markers + whitespace.

    Returns ``None`` for empty / placeholder cells (the changes table leaves
    the added or removed cell blank when only one side changed)."""
    if raw is None:
        return None
    s = str(raw).strip()
    if not s or s.lower() in {"nan", "—", "-", "none"}:
        return None
    # Drop bracketed footnote refs like "ABC[1]" and trailing notes.
    s = re.sub(r"\[.*?\]", "", s).strip()
    # Keep the symbol token only (uppercase letters, digits, dot, dash).
    m = re.match(r"[A-Z0-9.\-]+", s.upper())
    return m.group(0) if m else None


def _parse_date(raw: object) -> str | None:
    """Parse a changes-table date cell to ISO ``YYYY-MM-DD`` (or None)."""
    if raw is None:
        return None
    s = re.sub(r"\[.*?\]", "", str(raw)).strip()
    if not s or s.lower() == "nan":
        return None
    for fmt in ("%B %d, %Y", "%b %d, %Y", "%Y-%m-%d", "%d %B %Y"):
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def _flatten_columns(df: pd.DataFrame) -> pd.DataFrame:
    if isinstance(df.columns, pd.MultiIndex):
        df = df.copy()
        df.columns = [" ".join(str(c) for c in col).strip() for col in df.columns]
    return df


def select_changes_table(tables: list[pd.DataFrame]) -> pd.DataFrame:
    """Pick the "Selected changes to the list" table from read_html output.

    Identified by columns: a Date column plus *Added* and *Removed* groups
    (each typically a Ticker sub-column). Mirrors the column-based selection
    in ``constituents._select_constituents_table`` (position is unstable —
    Wikipedia inserts banner tables without notice)."""
    for df in tables:
        flat = _flatten_columns(df)
        cols = [str(c).lower() for c in flat.columns]
        has_date = any("date" in c for c in cols)
        has_added = any("added" in c for c in cols)
        has_removed = any("removed" in c for c in cols)
        if has_date and has_added and has_removed:
            return flat
    raise RuntimeError(
        "No 'Selected changes to the list' table found on the S&P 500 "
        "Wikipedia page (need columns matching date + added + removed). "
        "Wikipedia layout drift — extractor needs update."
    )


def _pick_col(cols: list[str], *, contains: str, prefer: str) -> str | None:
    """Find the column whose lowercased name contains ``contains`` (and,
    when several match, the one also containing ``prefer`` — e.g. the
    'Added Ticker' rather than 'Added Security')."""
    matches = [c for c in cols if contains in c.lower()]
    if not matches:
        return None
    preferred = [c for c in matches if prefer in c.lower()]
    return (preferred or matches)[0]


def parse_changes_table(df: pd.DataFrame) -> list[ConstituentChange]:
    """Wikipedia changes DataFrame -> ordered list of ``ConstituentChange``.

    Each row may carry an addition, a removal, or both. Rows with an
    unparseable date or no valid ticker on either side are skipped. The
    returned list is sorted oldest-first (deterministic replay order)."""
    df = _flatten_columns(df)
    cols = list(df.columns)
    date_col = _pick_col([str(c) for c in cols], contains="date", prefer="date")
    added_ticker_col = _pick_col(
        [str(c) for c in cols], contains="added", prefer="ticker"
    )
    removed_ticker_col = _pick_col(
        [str(c) for c in cols], contains="removed", prefer="ticker"
    )
    if not date_col or not (added_ticker_col or removed_ticker_col):
        raise RuntimeError(
            "Changes table missing a usable date/added/removed column set; "
            f"saw columns {cols}."
        )

    changes: list[ConstituentChange] = []
    for _, row in df.iterrows():
        iso = _parse_date(row.get(date_col))
        if iso is None:
            continue
        if added_ticker_col:
            t = _normalize_ticker(row.get(added_ticker_col))
            if t:
                changes.append(ConstituentChange(iso, t, ADDED))
        if removed_ticker_col:
            t = _normalize_ticker(row.get(removed_ticker_col))
            if t:
                changes.append(ConstituentChange(iso, t, REMOVED))

    changes.sort(key=lambda c: (c.date, c.ticker, c.action))
    return changes


def build_pit_membership(
    current_tickers: list[str],
    changes: list[ConstituentChange],
) -> dict[str, list[str]]:
    """Replay ``changes`` backward from ``current_tickers`` to a PIT map.

    Returns ``{change_date: sorted_tickers_immediately_before_that_date}``.
    The membership *after* the most recent change equals the current roster;
    walking each change date from newest to oldest, undo it to recover the
    set that held just before that date:
      * undo an ADDED ticker -> it was NOT a member before that date -> remove
      * undo a REMOVED ticker -> it WAS a member before that date -> add

    Same-date changes are applied as a group so the snapshot for date D is the
    membership the instant before D's changes took effect.
    """
    members = set(current_tickers)
    # Group changes by date, newest first.
    by_date: dict[str, list[ConstituentChange]] = {}
    for c in changes:
        by_date.setdefault(c.date, []).append(c)

    pit: dict[str, list[str]] = {}
    for date in sorted(by_date, reverse=True):
        for c in by_date[date]:
            if c.action == ADDED:
                members.discard(c.ticker)  # wasn't a member before D
            elif c.action == REMOVED:
                members.add(c.ticker)  # was a member before D
        pit[date] = sorted(members)
    return pit


def _fetch_changes_table() -> pd.DataFrame:
    resp = requests.get(_SP500_URL, headers=_HEADERS, timeout=15)
    resp.raise_for_status()
    tables = pd.read_html(StringIO(resp.text))
    return select_changes_table(tables)


def collect(
    bucket: str,
    current_tickers: list[str],
    s3_prefix: str = "market_data/",
    dry_run: bool = False,
) -> dict:
    """Build the point-in-time S&P 500 membership map and write to S3.

    ``current_tickers`` is today's roster (the caller already has it from
    ``constituents.collect``); this avoids a second live fetch of the live
    roster and keeps the two collectors' rosters consistent. Writes
    ``{s3_prefix}historical_constituents.json`` per the memo's recommended
    path."""
    changes = parse_changes_table(_fetch_changes_table())
    pit = build_pit_membership(current_tickers, changes)

    result = {
        "schema_version": 1,
        "source": _SP500_URL,
        "index": "S&P 500",
        "current_count": len(current_tickers),
        "n_changes": len(changes),
        "n_snapshots": len(pit),
        "membership": pit,  # {date: [tickers as-of just before that date]}
        "built_at": datetime.now(timezone.utc).isoformat(),
    }

    if dry_run:
        logger.info(
            "[dry-run] historical_constituents: %d changes -> %d PIT snapshots "
            "(current roster %d)",
            len(changes), len(pit), len(current_tickers),
        )
        return {"status": "ok_dry_run", "n_changes": len(changes), "n_snapshots": len(pit)}

    s3 = boto3.client("s3")
    key = f"{s3_prefix}historical_constituents.json"
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(result, indent=2),
        ContentType="application/json",
    )
    logger.info(
        "Wrote historical_constituents.json to s3://%s/%s (%d changes, %d snapshots)",
        bucket, key, len(changes), len(pit),
    )
    return {"status": "ok", "n_changes": len(changes), "n_snapshots": len(pit)}
