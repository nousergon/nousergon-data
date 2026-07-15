"""
backfill_delisted_history.py — recover ALREADY-pruned delisted tickers into the
``delisted_history`` ArcticDB retention library (config#1943 Leg 3, backfill clause).

Context
-------
Leg 3's *going-forward* retention shipped in nousergon-data#696: the pruner now
MOVES a confirmed-delisted ticker's history into ``delisted_history`` (see
``store.arctic_store.get_delisted_history_lib``) BEFORE deleting it from the live
``universe`` library. But every name hard-deleted BEFORE #696 landed is gone —
``prune_delisted_tickers.py`` historically called ``universe_lib.delete(ticker)``
with no retention, so that OHLCV was destroyed and ArcticDB has no record to
reconstruct it from. Leg 3's closes-when requires, beyond the schema decision and
the stop-destroying change, "a defensible backfill covers a documented fraction of
already-pruned names" — this builder is that backfill.

Target set
----------
The PIT membership producer (config#1942, ``market_data/historical_constituents.json``)
enumerates every ticker that was EVER an S&P 500 member. The backfill target is::

    ever_members  -  currently_tracked(universe)  -  already_retained(delisted_history)

i.e. names that were index members at some point but are absent from BOTH the live
``universe`` library (tradable today) and the ``delisted_history`` retention store
(already retained going-forward). Each target's OHLCV is re-fetched from a free
historical source (yfinance, which still carries most delisted US tickers' daily
bars) over the ticker's index-membership window and written into ``delisted_history``
under the SAME schema the pruner's ``_retain_delisted`` writes, so the
survivorship-free backtester (config#1942 Leg 2 as-of universe wiring) can include
these names for the dates they were index members.

Provenance & coverage
---------------------
Backfilled bars are stamped ``source = "yfinance-backfill"`` (the canonical
PROVENANCE_COL) so they are auditable and distinguishable from polygon-sourced
live bars, and the record metadata carries ``price_source="yfinance"`` +
``origin="backfill_delisted_history"``. Free-source coverage of delisted tickers
is PARTIAL — some old / foreign / OTC-migrated names return nothing — so the run
reports the documented recovered fraction and NEVER claims 100% (per the issue's
closes-when: "100% recovery is not guaranteed").

Safety
------
* Dry-run by default (``--apply`` to write), mirroring ``prune_delisted_tickers``.
* Idempotent: names already present in ``delisted_history`` are skipped; writes use
  ``prune_previous_versions=True`` so a re-run overwrites in place.
* Per-ticker isolation: one ticker's fetch/parse failure is recorded and the run
  continues — a free-source hiccup never aborts the whole backfill.
* NEVER touches the live ``universe`` library — this is a pure add into the
  retention store; there is no delete path here.

Usage
-----
    python -m builders.backfill_delisted_history                 # dry-run (no writes)
    python -m builders.backfill_delisted_history --apply         # actually backfill
    python -m builders.backfill_delisted_history --apply --limit 50
    python -m builders.backfill_delisted_history --apply --tickers ABMD,CTXS
"""

from __future__ import annotations

import argparse
import json
import logging
from datetime import datetime, timezone

import numpy as np
import pandas as pd

log = logging.getLogger(__name__)

# Canonical universe-library schema. Redefined locally (import-light) per the
# established builder convention — cf. ``daily_append.OHLCV_COLS``,
# ``migrate_universe_vwap.OHLCV_COLS_CANONICAL``,
# ``promote_ohlcv_only_schema.OHLCV_COLS`` — and MUST stay equal to
# ``store.arctic_store.OHLCV_COLS`` / ``PROVENANCE_COL``.
OHLCV_COLS: list[str] = ["Open", "High", "Low", "Close", "Volume", "VWAP"]
PROVENANCE_COL: str = "source"

# Provenance value stamped on every backfilled row so the backtester / audits can
# distinguish free-source recovered bars from polygon-sourced live bars.
BACKFILL_PROVENANCE: str = "yfinance-backfill"

# MUST equal ``builders.prune_delisted_tickers.DELISTED_HISTORY_SCHEMA_VERSION``
# (the delisted_history record contract readers gate on). Kept in sync with the
# pruner; a breaking layout change bumps both.
DELISTED_HISTORY_SCHEMA_VERSION: int = 1

DEFAULT_BUCKET = "alpha-engine-research"
DEFAULT_S3_PREFIX = "market_data/"
HISTORICAL_CONSTITUENTS_KEY = "historical_constituents.json"
BACKFILL_AUDIT_PREFIX = "builders/backfill_delisted_audit/"

# A recovered frame with fewer than this many rows is treated as no-coverage —
# a 1-2 row yfinance stub is not usable backtest history and pollutes the store.
DEFAULT_MIN_ROWS = 20

# Extend the fetch window past the last membership snapshot so the ticker's final
# index-member trading days are covered (membership dates are keyed "as-of just
# before the change date").
_MEMBERSHIP_END_BUFFER = pd.Timedelta(days=7)


# ──────────────────────────────────────────────────────────────────────────────
# Seams — lazily import the heavy deps so this module imports with only pandas,
# and unit tests can patch these without arcticdb / boto3 / yfinance installed.
# ──────────────────────────────────────────────────────────────────────────────
def _s3_client():
    import boto3

    return boto3.client("s3")


def _get_universe_lib(bucket: str):
    from store.arctic_store import get_universe_lib

    return get_universe_lib(bucket)


def _get_delisted_history_lib(bucket: str):
    from store.arctic_store import get_delisted_history_lib

    return get_delisted_history_lib(bucket)


def _yf_download(ticker: str, start: str, end: str) -> pd.DataFrame:
    """Fetch daily OHLCV for ``ticker`` over ``[start, end)`` from yfinance.

    Mirrors ``weekly_collector``'s heal fetch: ``auto_adjust=True``, bounded
    ``timeout``, ``progress=False``. A network / not-found failure raises; the
    caller isolates it per-ticker.
    """
    import yfinance as yf

    return yf.download(
        ticker,
        start=start,
        end=end,
        progress=False,
        auto_adjust=True,
        timeout=30,
    )


# ──────────────────────────────────────────────────────────────────────────────
# Pure helpers (no I/O — the unit-tested core).
# ──────────────────────────────────────────────────────────────────────────────
def _read_historical_membership(
    s3, bucket: str, s3_prefix: str = DEFAULT_S3_PREFIX,
) -> dict[str, list[str]]:
    """Return the ``{date: [tickers-as-of]}`` membership map from
    ``{s3_prefix}historical_constituents.json`` (config#1942 producer)."""
    key = f"{s3_prefix}{HISTORICAL_CONSTITUENTS_KEY}"
    obj = s3.get_object(Bucket=bucket, Key=key)
    payload = json.loads(obj["Body"].read())
    membership = payload.get("membership")
    if not isinstance(membership, dict):
        raise ValueError(
            f"s3://{bucket}/{key} has no 'membership' map (got "
            f"{type(membership).__name__}) — is the config#1942 producer wired?"
        )
    return membership


def compute_membership_windows(
    membership: dict[str, list[str]],
) -> dict[str, tuple[str, str]]:
    """Collapse a ``{date: [tickers]}`` map into ``{ticker: (first, last)}`` —
    the first and last snapshot date each ticker appears as a member. That window
    bounds the OHLCV fetch (we only want prices for dates the name was in-index)."""
    first: dict[str, str] = {}
    last: dict[str, str] = {}
    for date_str, tickers in membership.items():
        for t in tickers:
            t = t.strip().upper()
            if not t:
                continue
            if t not in first or date_str < first[t]:
                first[t] = date_str
            if t not in last or date_str > last[t]:
                last[t] = date_str
    return {t: (first[t], last[t]) for t in first}


def compute_backfill_targets(
    ever_members: set[str],
    universe_symbols: set[str],
    retained_symbols: set[str],
) -> list[str]:
    """``ever_members - universe - retained``, sorted for deterministic runs."""
    norm = lambda s: {x.strip().upper() for x in s if x and x.strip()}  # noqa: E731
    return sorted(norm(ever_members) - norm(universe_symbols) - norm(retained_symbols))


def _normalize_bars(yf_df: pd.DataFrame) -> pd.DataFrame:
    """Coerce a yfinance frame into the canonical universe schema: OHLCV_COLS
    (VWAP absent from yfinance → NaN) + a ``source`` provenance column, DatetimeIndex
    normalized to naive dates. Returns an empty frame if nothing usable."""
    if yf_df is None or yf_df.empty:
        return pd.DataFrame(columns=[*OHLCV_COLS, PROVENANCE_COL])

    df = yf_df.copy()
    # yfinance returns a column MultiIndex for some calls — flatten to the field level.
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)

    out = pd.DataFrame(index=pd.DatetimeIndex(df.index).normalize())
    for col in OHLCV_COLS:
        # np.nan (not pd.NA) keeps the column float64 — pd.NA broadcasts to
        # object dtype, which ArcticDB's write() refuses to normalize.
        out[col] = df[col].to_numpy() if col in df.columns else np.nan
    # VWAP is not provided by yfinance daily bars (matches weekly_collector's
    # daily pass, which also writes OHLCV with no VWAP).
    out["VWAP"] = np.nan
    out[PROVENANCE_COL] = BACKFILL_PROVENANCE
    out = out[~out.index.duplicated(keep="last")].sort_index()
    # Drop rows with no usable close (a delisted stub can carry all-NaN tails).
    out = out[out["Close"].notna()]
    return out


def _build_metadata(
    ticker: str,
    df: pd.DataFrame,
    *,
    membership_first: str,
    membership_last: str,
    price_source: str = "yfinance",
) -> dict:
    """Metadata parallel to ``prune_delisted_tickers._retain_delisted``'s contract,
    with backfill provenance added so the two write paths are readable identically."""
    return {
        "schema_version": DELISTED_HISTORY_SCHEMA_VERSION,
        "symbol": ticker,
        "first_active_date": pd.Timestamp(df.index[0]).strftime("%Y-%m-%d"),
        "last_active_date": pd.Timestamp(df.index[-1]).strftime("%Y-%m-%d"),
        "rows": int(len(df)),
        "membership_first_date": membership_first,
        "membership_last_date": membership_last,
        "origin": "backfill_delisted_history",
        "price_source": price_source,
        "source": BACKFILL_PROVENANCE,
        "backfilled_at": datetime.now(timezone.utc).isoformat(),
    }


# ──────────────────────────────────────────────────────────────────────────────
# Orchestrator + CLI.
# ──────────────────────────────────────────────────────────────────────────────
def backfill_delisted_history(
    *,
    bucket: str = DEFAULT_BUCKET,
    apply: bool = False,
    limit: int | None = None,
    tickers_override: list[str] | None = None,
    s3_prefix: str = DEFAULT_S3_PREFIX,
    min_rows: int = DEFAULT_MIN_ROWS,
    today: pd.Timestamp | None = None,
) -> dict:
    """Recover already-pruned delisted names into ``delisted_history``.

    Returns a summary dict (also written to S3 as an audit record when ``apply``).
    """
    today = (today or pd.Timestamp.now(tz="UTC").normalize().tz_localize(None))
    s3 = _s3_client()

    membership = _read_historical_membership(s3, bucket, s3_prefix)
    windows = compute_membership_windows(membership)
    ever_members = set(windows)

    universe_lib = _get_universe_lib(bucket)
    delisted_lib = _get_delisted_history_lib(bucket)
    universe_syms = set(universe_lib.list_symbols())
    retained_syms = set(delisted_lib.list_symbols())

    targets = compute_backfill_targets(ever_members, universe_syms, retained_syms)

    if tickers_override:
        override = {t.strip().upper() for t in tickers_override if t.strip()}
        skipped = sorted(override - set(targets))
        if skipped:
            log.warning(
                "override tickers already tracked/retained (skipping): %s",
                ", ".join(skipped),
            )
        targets = [t for t in targets if t in override]

    if limit is not None:
        targets = targets[:limit]

    recovered: list[dict] = []
    no_data: list[str] = []
    errors: list[dict] = []

    for ticker in targets:
        try:
            m_first, m_last = windows[ticker]
            start = pd.Timestamp(m_first).strftime("%Y-%m-%d")
            end = (pd.Timestamp(m_last) + _MEMBERSHIP_END_BUFFER).strftime("%Y-%m-%d")

            raw = _yf_download(ticker, start, end)
            df = _normalize_bars(raw)
            if len(df) < min_rows:
                no_data.append(ticker)
                log.info(
                    "NO-DATA ticker=%s rows=%d (< min_rows=%d) window=%s..%s",
                    ticker, len(df), min_rows, m_first, m_last,
                )
                continue

            metadata = _build_metadata(
                ticker, df, membership_first=m_first, membership_last=m_last,
            )
            if apply:
                delisted_lib.write(
                    ticker, df, metadata=metadata, prune_previous_versions=True,
                )
            recovered.append(metadata)
            log.warning(
                "%s ticker=%s -> delisted_history rows=%d window=%s..%s",
                "BACKFILLED" if apply else "WOULD-BACKFILL",
                ticker, metadata["rows"], metadata["first_active_date"],
                metadata["last_active_date"],
            )
        except Exception as exc:  # noqa: BLE001 - per-ticker isolation, never abort the run
            errors.append({"ticker": ticker, "error": str(exc)})
            log.warning("ERROR ticker=%s — %s", ticker, exc)

    n_targets = len(targets)
    summary = {
        "today": today.strftime("%Y-%m-%d"),
        "apply": apply,
        "bucket": bucket,
        "n_ever_members": len(ever_members),
        "n_universe": len(universe_syms),
        "n_retained_before": len(retained_syms),
        "n_targets": n_targets,
        "n_recovered": len(recovered),
        "n_no_data": len(no_data),
        "n_errors": len(errors),
        "recovered_fraction": round(len(recovered) / n_targets, 4) if n_targets else 0.0,
        "recovered": recovered,
        "no_data": no_data,
        "errors": errors,
    }

    if apply:
        _write_audit(s3, bucket, summary)

    log.warning(
        "backfill_delisted_history: targets=%d recovered=%d (%.1f%%) no_data=%d errors=%d "
        "(apply=%s)",
        n_targets, len(recovered), 100.0 * summary["recovered_fraction"],
        len(no_data), len(errors), apply,
    )
    return summary


def _write_audit(s3, bucket: str, summary: dict) -> None:
    """Persist the run summary to S3, mirroring ``prune_delisted_tickers``'s audit."""
    ts = datetime.now(timezone.utc).strftime("%H%M%SZ")
    key = f"{BACKFILL_AUDIT_PREFIX}{summary['today']}-{ts}.json"
    try:
        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=json.dumps(summary, indent=2, default=str).encode(),
            ContentType="application/json",
        )
        log.info("wrote backfill audit to s3://%s/%s", bucket, key)
    except Exception as exc:  # noqa: BLE001 - audit is best-effort, never fail the run on it
        log.warning("could not write backfill audit (%s)", exc)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--apply", action="store_true",
        help="Actually write to delisted_history. Default is dry-run.",
    )
    parser.add_argument(
        "--bucket", default=DEFAULT_BUCKET,
        help=f"S3 bucket (default: {DEFAULT_BUCKET}).",
    )
    parser.add_argument(
        "--limit", type=int, default=None,
        help="Cap the number of tickers backfilled this run (default: all).",
    )
    parser.add_argument(
        "--tickers", type=str, default=None,
        help="Comma-separated ticker override (still filtered to genuine "
             "backfill targets — cannot re-fetch a currently-tracked name).",
    )
    parser.add_argument(
        "--min-rows", type=int, default=DEFAULT_MIN_ROWS,
        help=f"Minimum recovered rows to keep a name (default {DEFAULT_MIN_ROWS}).",
    )
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
        datefmt="%H:%M:%S",
    )

    tickers_override = None
    if args.tickers:
        tickers_override = [t.strip().upper() for t in args.tickers.split(",") if t.strip()]

    summary = backfill_delisted_history(
        bucket=args.bucket,
        apply=args.apply,
        limit=args.limit,
        tickers_override=tickers_override,
        min_rows=args.min_rows,
    )
    print(json.dumps(summary, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
