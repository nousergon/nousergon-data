"""builders/migrate_universe_crsp_basis.py — OFFLINE CRSP-basis universe build.

Corporate-actions program PR7, step 7a (epic config#1433 / config#1434).
=========================================================================

This is the **offline, build-the-evidence** step of the CRSP/Barra basis
migration. It reconstructs every universe ticker on ONE clean,
polygon-authoritative basis and writes the result to a **SCRATCH** ArcticDB
library (default ``universe_crsp``) — it NEVER touches the live ``universe``
library, the live champion, or any consumer. The live basis flip
(``feature_engineer.py`` close basis + ne-data/predictor labels), the
dual-writer wiring, and ``prices.py auto_adjust=False`` are all GATED to
PR7-7c, after the shadow-retrain + backtest gate of 7b.

Target representation (per ticker), per the approved plan §7a:
    Close              = split-adjusted price LEVEL (polygon-authoritative;
                         changes only on splits).
    total_return_close = NEW column — the split-adjusted series further
                         dividend-back-adjusted via the registry dividend
                         events (``corporate_actions.total_return_series``);
                         the SEPARATE total-return axis (does NOT mutate Close).
    53 feature columns = recomputed on ``total_return_close`` (via
                         ``compute_features(..., close_col="total_return_close")``)
                         for the scratch build only.

Per ticker the script:
  1. Re-pulls RAW (unadjusted) prices over the full history — yfinance
     ``auto_adjust=False`` (mirrors ``collectors/prices.py`` but raw).
  2. Applies polygon SPLITS to get the split-adjusted ``Close`` LEVEL
     (``corporate_actions.apply`` → ``_split_math.restate_series_for_splits``).
  3. Derives ``total_return_close`` from polygon DIVIDENDS
     (``corporate_actions.get_dividends`` + ``total_return_series``).
  4. **Reconciles** the derived ``total_return_close`` against the retiring
     yfinance total-return ``Close`` (read from the LIVE ``universe`` library)
     per ticker: max relative deviation, classify within-tol vs OUT-OF-TOL.
     FAILS LOUD (raises / non-zero exit) on any ticker whose OUT-OF-TOL
     residual is not an operator-acknowledged known divergence — no silent
     skip, no "unscoreable" sentinel (Brian standing feedback). A missing /
     doubled / mis-ratio'd split or dividend surfaces here before it can reach
     the model.
  5. ``--apply`` only: recomputes the 53 features on ``total_return_close`` and
     writes the full per-ticker series to the SCRATCH library via
     ``to_arctic_canonical`` (+ a factor-momentum second pass).

The reconciliation report is emitted to S3 (audit JSON) and summarised in the
log. Both series are total-return and anchored at the latest split-adjusted
price, so within tolerance they should be identical up to feed rounding; the
NEW one is polygon-authoritative.

NOTE — this PR ships the SCRIPT + tests only. The actual 10y / ~900-ticker
scratch-library build is a separate OPERATIONAL (spot) run; do NOT run this
against a live/large ArcticDB from a dev box.

Template / mirrored design: ``builders/migrate_universe_feature_order.py``
(dry-run → ThreadPool → S3 audit, per-ticker error capture, idempotent skip)
and ``builders/backfill.py`` (the universe-library write shape).

Usage::

    python -m builders.migrate_universe_crsp_basis                      # dry-run reconcile + report
    python -m builders.migrate_universe_crsp_basis --apply              # write scratch lib
    python -m builders.migrate_universe_crsp_basis --tickers AAPL,MMM   # subset (testing)
    python -m builders.migrate_universe_crsp_basis --scratch-lib universe_crsp_v2
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timezone

import boto3
import numpy as np
import pandas as pd

import corporate_actions as ca
from features.compute import DEFAULT_BUCKET
from store.arctic_store import (
    TOTAL_RETURN_COL,
    get_scratch_universe_lib,
    get_universe_lib,
    to_arctic_canonical,
)

log = logging.getLogger(__name__)

AUDIT_PREFIX = "builders/migrate_universe_crsp_basis_audit/"
DEFAULT_SCRATCH_LIB = "universe_crsp"
DEFAULT_WORKERS = 8

# Both the derived total_return_close and the retiring yfinance Close are
# TOTAL-RETURN series anchored at the latest (post-action) split-adjusted
# price, so within feed-rounding they should be identical. 2% is a generous
# band that absorbs provider close-price rounding + a stray T+1 print without
# masking a missing/doubled action (which moves the boundary by the action
# factor — a dividend ~0.5–3%/event compounding, a split 50%+).
DEFAULT_RECONCILE_REL_TOL = 0.02

# Window (calendar days) for attributing an out-of-tol residual's worst date
# to the nearest registered corporate action — diagnostic only (the report
# names the likely culprit action); it does NOT gate the fail-loud decision.
_ACTION_ATTRIBUTION_WINDOW_DAYS = 5

# The logical store passed to corporate_actions.apply for the scratch
# split restatement. A DISTINCT store name (not STORE_ARCTICDB_UNIVERSE) so
# the offline build's applied-markers can never collide with / pollute the
# live universe restatement markers. We pass registry=None anyway (structural
# idempotency: we always restate from the freshly re-pulled raw source), so no
# marker is actually written — but the distinct name is defense in depth.
SCRATCH_RESTATE_STORE = "crsp_scratch_build"


# ── reconciliation record ────────────────────────────────────────────────────


@dataclass
class ReconcileRecord:
    """Per-ticker reconciliation of the NEW total_return_close vs the retiring
    yfinance total-return Close."""

    ticker: str
    status: str  # "within_tol" | "out_of_tol" | "no_overlap"
    n_common_dates: int
    max_rel_dev: float
    max_dev_date: str | None
    explained: bool
    explanation: str
    nearest_action: str | None = None

    def to_dict(self) -> dict:
        return {
            "ticker": self.ticker,
            "status": self.status,
            "n_common_dates": self.n_common_dates,
            "max_rel_dev": self.max_rel_dev,
            "max_dev_date": self.max_dev_date,
            "explained": self.explained,
            "explanation": self.explanation,
            "nearest_action": self.nearest_action,
        }


@dataclass
class TickerOutcome:
    """Per-ticker pipeline outcome carried back from the worker."""

    ticker: str
    outcome: str  # "ok" | "fetch_empty" | "no_old_close" | "error"
    reconcile: ReconcileRecord | None = None
    n_rows: int = 0
    n_splits: int = 0
    n_dividends: int = 0
    error: str | None = None
    written: bool = False
    new_df: pd.DataFrame | None = field(default=None, repr=False)


# ── raw price fetch (yfinance auto_adjust=False) ──────────────────────────────


def fetch_raw_prices(ticker: str, *, period: str = "max") -> pd.DataFrame:
    """Re-pull RAW (unadjusted) daily OHLCV for one ticker via yfinance
    ``auto_adjust=False`` — so polygon is the SINGLE corporate-action authority
    (we apply polygon splits/dividends to this raw series ourselves).

    Mirrors ``collectors/prices.py`` (same index normalization) but with
    ``auto_adjust=False`` and drops the yfinance ``Adj Close`` column (we do not
    use yfinance's adjustment — that is exactly the basis we are retiring).
    Returns a DatetimeIndex-sorted frame with ``Open/High/Low/Close/Volume``;
    raises on an empty/failed pull (fail-loud — a silent empty would corrupt the
    reconstruction).
    """
    import yfinance as yf

    raw = yf.download(
        tickers=ticker,
        period=period,
        interval="1d",
        auto_adjust=False,  # RAW: do NOT let yfinance adjust — polygon is authority
        progress=False,
        group_by="ticker",
        threads=False,
    )
    if raw is None or len(raw) == 0:
        raise RuntimeError(f"yfinance returned no rows for {ticker} (auto_adjust=False)")

    # yfinance returns a MultiIndex (ticker, field) when group_by="ticker".
    if isinstance(raw.columns, pd.MultiIndex):
        if ticker in raw.columns.get_level_values(0):
            raw = raw[ticker].copy()
        else:
            raw = raw.droplevel(0, axis=1).copy()

    if "Close" not in raw.columns:
        raise RuntimeError(f"yfinance frame for {ticker} missing Close column")

    raw = raw.dropna(subset=["Close"])
    if raw.empty:
        raise RuntimeError(f"yfinance frame for {ticker} empty after dropna(Close)")

    idx = pd.to_datetime(raw.index)
    if idx.tz is not None:
        idx = idx.tz_convert("UTC").tz_localize(None)
    raw.index = idx
    raw = raw.sort_index()

    keep = [c for c in ("Open", "High", "Low", "Close", "Volume") if c in raw.columns]
    return raw[keep]


# ── core: per-ticker basis reconstruction (pure, network-free) ────────────────


def reconstruct_basis(
    ticker: str,
    raw_df: pd.DataFrame,
    split_actions: list,
    dividend_actions: list,
) -> tuple[pd.DataFrame, list[dict]]:
    """Reconstruct one ticker on the CRSP basis from a RAW price frame.

    Returns ``(df, applied_split_results)`` where ``df`` is the raw frame with
    ``Close`` replaced by the split-adjusted price LEVEL and a NEW
    ``total_return_close`` column (split-adjusted + dividend-back-adjusted).

    Steps:
      * SPLIT restatement → ``Close`` (price LEVEL) via ``corporate_actions.apply``
        (routes through ``_split_math.restate_series_for_splits``). registry=None:
        structural idempotency (we always restate from the freshly-pulled raw).
      * ``total_return_close`` via ``corporate_actions.total_return_series`` over
        the dividend events — the SEPARATE total-return axis; it does NOT mutate
        ``Close``.

    Pure: no network / S3 / ArcticDB — the migration's network fetch + I/O live
    in the orchestration, so this is unit-testable with hand-built fakes.
    """
    if raw_df is None or raw_df.empty:
        return raw_df, []

    # 1. SPLIT-adjusted Close LEVEL. apply() filters to splits, raises on a
    #    stray dividend/rename, and delegates the factor math.
    split_adj, applied = ca.apply(
        raw_df,
        split_actions,
        store=SCRATCH_RESTATE_STORE,
        registry=None,
    )

    # 2. SEPARATE total-return axis (does NOT mutate split_adj["Close"]).
    tr_close = ca.total_return_series(split_adj, dividend_actions)

    out = split_adj.copy()
    out[TOTAL_RETURN_COL] = tr_close.reindex(out.index)
    return out, applied


def recompute_features_on_tr(
    df: pd.DataFrame,
    *,
    spy_series: pd.Series | None = None,
    vix_series: pd.Series | None = None,
    sector_etf_series: pd.Series | None = None,
    tnx_series: pd.Series | None = None,
    irx_series: pd.Series | None = None,
    gld_series: pd.Series | None = None,
    uso_series: pd.Series | None = None,
    vix3m_series: pd.Series | None = None,
    earnings_data: dict | None = None,
    revision_data: dict | None = None,
    options_data: dict | None = None,
    fundamental_data: dict | None = None,
) -> pd.DataFrame:
    """Recompute the 53 features with ``total_return_close`` as the close basis.

    Thin wrapper over ``features.feature_engineer.compute_features`` pinning
    ``close_col=TOTAL_RETURN_COL`` — the single basis chokepoint the live flip
    (PR7-7c) will set as the default. Open/High/Low stay raw (split-adjusted
    level), matching the plan's single-chokepoint design.
    """
    from features.feature_engineer import compute_features

    return compute_features(
        df,
        spy_series=spy_series,
        vix_series=vix_series,
        sector_etf_series=sector_etf_series,
        tnx_series=tnx_series,
        irx_series=irx_series,
        gld_series=gld_series,
        uso_series=uso_series,
        vix3m_series=vix3m_series,
        earnings_data=earnings_data,
        revision_data=revision_data,
        options_data=options_data,
        fundamental_data=fundamental_data,
        close_col=TOTAL_RETURN_COL,
    )


def _nearest_action(date: pd.Timestamp, actions: list, window_days: int) -> object | None:
    """Return the registered action whose ex_date is closest to ``date`` within
    ``window_days`` (diagnostic attribution), or ``None``."""
    best = None
    best_gap = None
    for a in actions or []:
        try:
            ex = pd.Timestamp(a.ex_date).normalize()
        except Exception:  # noqa: BLE001 - malformed ex_date, skip candidate
            continue
        gap = abs((ex - date).days)
        if gap <= window_days and (best_gap is None or gap < best_gap):
            best, best_gap = a, gap
    return best


def reconcile_total_return(
    ticker: str,
    new_tr_close: pd.Series,
    old_close: pd.Series,
    *,
    split_actions: list | None = None,
    dividend_actions: list | None = None,
    rel_tol: float = DEFAULT_RECONCILE_REL_TOL,
    known_divergence: bool = False,
) -> ReconcileRecord:
    """Compare the NEW ``total_return_close`` against the retiring yfinance
    total-return ``Close`` for one ticker.

    Aligns the two series on their common dates, computes the per-date relative
    deviation ``|new - old| / |old|`` and takes its max. Classifies:

      * ``within_tol`` — ``max_rel_dev <= rel_tol``: the expected case (both are
        total-return, identical up to feed rounding; the new one is
        polygon-authoritative). ``explained=True``.
      * ``out_of_tol`` — ``max_rel_dev > rel_tol``: a missing / doubled /
        mis-ratio'd split or dividend, OR a real polygon-vs-yfinance data
        divergence. ``explained`` is ``True`` ONLY if the operator passed
        ``known_divergence=True`` for this ticker (an acknowledged, documented
        divergence) — otherwise ``False`` (the fail-loud case).
      * ``no_overlap`` — no common dates to compare. Treated as UNEXPLAINED
        (``explained=False``): a ticker we cannot reconcile is a fail-loud
        condition, not a silent skip.

    Attributes the worst-deviation date to the nearest registered action (within
    ``_ACTION_ATTRIBUTION_WINDOW_DAYS``) for the report — diagnostic only.
    """
    new_tr_close = new_tr_close.dropna()
    old_close = old_close.dropna()
    common = new_tr_close.index.intersection(old_close.index)
    if len(common) == 0:
        return ReconcileRecord(
            ticker=ticker,
            status="no_overlap",
            n_common_dates=0,
            max_rel_dev=float("inf"),
            max_dev_date=None,
            explained=False,
            explanation=(
                "no common dates between new total_return_close and old "
                "yfinance Close — cannot reconcile (fail-loud, not skipped)"
            ),
        )

    a = new_tr_close.reindex(common).to_numpy(dtype="float64")
    b = old_close.reindex(common).to_numpy(dtype="float64")
    denom = np.where(np.abs(b) > 1e-12, np.abs(b), np.nan)
    rel_dev = np.abs(a - b) / denom
    # NaN denom (old close ~0) → ignore that date rather than emit inf.
    rel_dev = np.where(np.isfinite(rel_dev), rel_dev, 0.0)
    worst_pos = int(np.argmax(rel_dev))
    max_rel_dev = float(rel_dev[worst_pos])
    max_dev_date = pd.Timestamp(common[worst_pos]).strftime("%Y-%m-%d")

    all_actions = list(split_actions or []) + list(dividend_actions or [])
    near = _nearest_action(
        pd.Timestamp(common[worst_pos]).normalize(),
        all_actions,
        _ACTION_ATTRIBUTION_WINDOW_DAYS,
    )
    nearest_action = near.human() if near is not None else None

    if max_rel_dev <= rel_tol:
        return ReconcileRecord(
            ticker=ticker,
            status="within_tol",
            n_common_dates=len(common),
            max_rel_dev=max_rel_dev,
            max_dev_date=max_dev_date,
            explained=True,
            explanation=f"within tol ({max_rel_dev:.4%} <= {rel_tol:.2%})",
            nearest_action=nearest_action,
        )

    if known_divergence:
        explanation = (
            f"OUT-OF-TOL ({max_rel_dev:.4%} > {rel_tol:.2%}) at {max_dev_date} "
            f"but operator-acknowledged known divergence"
            + (f" (nearest action: {nearest_action})" if nearest_action else "")
        )
        return ReconcileRecord(
            ticker=ticker,
            status="out_of_tol",
            n_common_dates=len(common),
            max_rel_dev=max_rel_dev,
            max_dev_date=max_dev_date,
            explained=True,
            explanation=explanation,
            nearest_action=nearest_action,
        )

    explanation = (
        f"OUT-OF-TOL ({max_rel_dev:.4%} > {rel_tol:.2%}) at {max_dev_date} — "
        f"likely a missing/doubled/mis-ratio'd split or dividend, or a real "
        f"polygon-vs-yfinance divergence"
        + (f"; nearest registered action: {nearest_action}" if nearest_action
           else "; NO registered action near this date")
    )
    return ReconcileRecord(
        ticker=ticker,
        status="out_of_tol",
        n_common_dates=len(common),
        max_rel_dev=max_rel_dev,
        max_dev_date=max_dev_date,
        explained=False,
        explanation=explanation,
        nearest_action=nearest_action,
    )


# ── orchestration ─────────────────────────────────────────────────────────────


def _read_old_close(universe_lib, ticker: str) -> pd.Series | None:
    """Read the retiring yfinance total-return Close for ``ticker`` from the
    LIVE universe library (READ ONLY — the live lib is never written here)."""
    try:
        df = universe_lib.read(ticker).data
    except Exception:  # noqa: BLE001 - missing symbol / read failure
        return None
    if df is None or df.empty or "Close" not in df.columns:
        return None
    s = df["Close"]
    s.index = pd.to_datetime(s.index)
    return s


def _write_audit(s3, bucket: str, summary: dict) -> None:
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    key = f"{AUDIT_PREFIX}{ts}.json"
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(summary, indent=2, default=str).encode("utf-8"),
        ContentType="application/json",
    )
    log.info("Wrote reconciliation audit to s3://%s/%s", bucket, key)


def migrate_universe_crsp_basis(
    *,
    bucket: str = DEFAULT_BUCKET,
    scratch_lib: str = DEFAULT_SCRATCH_LIB,
    apply: bool = False,
    tickers_override: list[str] | None = None,
    rel_tol: float = DEFAULT_RECONCILE_REL_TOL,
    known_divergence_tickers: frozenset[str] | None = None,
    workers: int | None = None,
    raw_fetch=None,
    client=None,
    macro: dict | None = None,
    sector_map: dict | None = None,
    fundamentals: dict | None = None,
    alt_data: dict | None = None,
) -> dict:
    """Reconstruct the universe on the CRSP basis into a SCRATCH library and
    emit a per-ticker reconciliation report. FAILS LOUD on any unexplained
    out-of-tolerance residual.

    NEVER writes the live ``universe`` library: scratch writes go through
    ``get_scratch_universe_lib`` (which refuses the live names), and the live
    universe is opened READ-ONLY for the retiring-Close reconciliation baseline.

    Parameters
    ----------
    scratch_lib
        SCRATCH ArcticDB library name (default ``universe_crsp``). Must not be a
        live name — enforced by ``get_scratch_universe_lib``.
    apply
        If True, recompute features and WRITE the reconstructed series to the
        scratch library. Default False (dry-run: reconcile + report only).
    rel_tol
        Reconciliation relative-deviation tolerance.
    known_divergence_tickers
        Operator-acknowledged documented divergences — these tickers' out-of-tol
        residuals are reported but do NOT fail the run. Default: none (every
        out-of-tol residual fails loud).
    raw_fetch / client
        Injectable RAW-price fetcher ``(ticker) -> DataFrame`` and polygon client
        (for tests / alternate sources). Default: yfinance ``auto_adjust=False``
        + the polygon singleton.
    macro / sector_map / fundamentals / alt_data
        Feature-recompute inputs (apply path only). Loaded from S3/ArcticDB when
        not injected.

    Returns
    -------
    summary dict with the reconciliation report + write outcome.
    """
    known_divergence_tickers = known_divergence_tickers or frozenset()
    workers = workers or int(
        os.environ.get("MIGRATE_UNIVERSE_CRSP_WORKERS", str(DEFAULT_WORKERS))
    )
    raw_fetch = raw_fetch or fetch_raw_prices

    s3 = boto3.client("s3")
    universe_lib = get_universe_lib(bucket)  # READ-ONLY baseline source

    arctic_symbols = sorted(universe_lib.list_symbols())
    log.info("Live universe holds %d symbols (read-only reconciliation baseline)",
             len(arctic_symbols))

    if tickers_override is not None:
        targets = sorted(set(tickers_override) & set(arctic_symbols))
        ignored = sorted(set(tickers_override) - set(arctic_symbols))
        if ignored:
            log.warning(
                "Skipping %d --tickers not in the live universe: %s",
                len(ignored), ignored,
            )
    else:
        targets = arctic_symbols

    # Open the scratch lib eagerly even on dry-run so a bad --scratch-lib name
    # fails immediately (the guard refuses live names), not after the work.
    scratch = get_scratch_universe_lib(scratch_lib, bucket)
    if apply:
        # Feature-recompute inputs (apply path only). Lazy-loaded so dry-run +
        # tests don't pay the cost.
        if macro is None:
            macro = _load_macro_series(s3, bucket)
        if sector_map is None:
            from features.compute import _load_sector_map

            sector_map = _load_sector_map(s3, bucket)
        if fundamentals is None:
            from features.compute import _load_cached_fundamentals

            today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            fundamentals = _load_cached_fundamentals(s3, bucket, today)
        if alt_data is None:
            from features.compute import _load_cached_alternative

            alt_data = _load_cached_alternative(s3, bucket)
    macro = macro or {}
    sector_map = sector_map or {}
    fundamentals = fundamentals or {}
    alt_data = alt_data or {}

    def _process_one(ticker: str) -> TickerOutcome:
        try:
            old_close = _read_old_close(universe_lib, ticker)
            if old_close is None:
                return TickerOutcome(ticker, "no_old_close")

            raw_df = raw_fetch(ticker)
            if raw_df is None or raw_df.empty:
                return TickerOutcome(ticker, "fetch_empty")

            split_actions = ca.get_splits(ticker, client=client)
            dividend_actions = ca.get_dividends(ticker, client=client)

            new_df, _applied = reconstruct_basis(
                ticker, raw_df, split_actions, dividend_actions,
            )

            rec = reconcile_total_return(
                ticker,
                new_df[TOTAL_RETURN_COL],
                old_close,
                split_actions=split_actions,
                dividend_actions=dividend_actions,
                rel_tol=rel_tol,
                known_divergence=ticker in known_divergence_tickers,
            )

            out = TickerOutcome(
                ticker, "ok",
                reconcile=rec,
                n_rows=len(new_df),
                n_splits=len(split_actions),
                n_dividends=len(dividend_actions),
            )

            if apply:
                sector_etf_sym = sector_map.get(ticker)
                featured = recompute_features_on_tr(
                    new_df,
                    spy_series=macro.get("SPY"),
                    vix_series=macro.get("VIX"),
                    sector_etf_series=(macro.get(sector_etf_sym) if sector_etf_sym else None),
                    tnx_series=macro.get("TNX"),
                    irx_series=macro.get("IRX"),
                    gld_series=macro.get("GLD"),
                    uso_series=macro.get("USO"),
                    vix3m_series=macro.get("VIX3M"),
                    earnings_data=(alt_data.get(ticker, {}) or {}).get("earnings"),
                    revision_data=(alt_data.get(ticker, {}) or {}).get("revisions"),
                    options_data=(alt_data.get(ticker, {}) or {}).get("options"),
                    fundamental_data=fundamentals.get(ticker),
                )
                out.new_df = featured
            return out
        except Exception as exc:  # noqa: BLE001 - capture per-ticker, surface in report
            return TickerOutcome(ticker, "error", error=str(exc))

    t0 = time.time()
    with ThreadPoolExecutor(max_workers=workers) as pool:
        outcomes = list(pool.map(_process_one, targets))
    elapsed = time.time() - t0

    # ── partition outcomes ────────────────────────────────────────────────────
    records: list[ReconcileRecord] = []
    errors: list[dict] = []
    no_old_close: list[str] = []
    fetch_empty: list[str] = []
    written = 0

    for o in outcomes:
        if o.outcome == "ok" and o.reconcile is not None:
            records.append(o.reconcile)
        elif o.outcome == "no_old_close":
            no_old_close.append(o.ticker)
        elif o.outcome == "fetch_empty":
            fetch_empty.append(o.ticker)
        elif o.outcome == "error":
            errors.append({"ticker": o.ticker, "error": o.error})
            log.error("CRSP migration error for %s: %s", o.ticker, o.error)

    # ── WRITE scratch (apply) — only after all reconciliations computed ───────
    if apply:
        for o in outcomes:
            if o.outcome == "ok" and o.new_df is not None and not o.new_df.empty:
                try:
                    scratch.write(o.ticker, to_arctic_canonical(o.new_df))
                    o.written = True
                    written += 1
                except Exception as exc:  # noqa: BLE001
                    errors.append({"ticker": o.ticker, "stage": "scratch_write",
                                   "error": str(exc)})
                    log.error("Scratch write failed for %s: %s", o.ticker, exc)

    within_tol = [r for r in records if r.status == "within_tol"]
    out_of_tol = [r for r in records if r.status == "out_of_tol"]
    no_overlap = [r for r in records if r.status == "no_overlap"]
    # FAIL-LOUD set: every out-of-tol or no-overlap record not explained.
    unexplained = [r for r in records if not r.explained]

    for r in out_of_tol + no_overlap:
        lvl = log.warning if r.explained else log.error
        lvl("RECONCILE %s status=%s max_rel_dev=%.4f date=%s — %s",
            r.ticker, r.status, r.max_rel_dev, r.max_dev_date, r.explanation)

    summary = {
        "status": "ok" if (not unexplained and not errors) else "fail",
        "applied": apply,
        "scratch_lib": scratch_lib,
        "rel_tol": rel_tol,
        "live_universe_size": len(arctic_symbols),
        "targets_count": len(targets),
        "reconciled_count": len(records),
        "within_tol_count": len(within_tol),
        "out_of_tol_count": len(out_of_tol),
        "no_overlap_count": len(no_overlap),
        "unexplained_count": len(unexplained),
        "no_old_close_count": len(no_old_close),
        "fetch_empty_count": len(fetch_empty),
        "errors_count": len(errors),
        "written_count": written,
        "elapsed_seconds": round(elapsed, 1),
        "workers": workers,
        "reconciliations": [r.to_dict() for r in records],
        "unexplained": [r.to_dict() for r in unexplained],
        "no_old_close": no_old_close,
        "fetch_empty": fetch_empty,
        "errors": errors,
        "known_divergence_tickers": sorted(known_divergence_tickers),
    }

    # Persist the report BEFORE raising so the evidence survives a fail-loud.
    _write_audit(s3, bucket, summary)

    log.info(
        "migrate_universe_crsp_basis: applied=%s targets=%d reconciled=%d "
        "within_tol=%d out_of_tol=%d no_overlap=%d unexplained=%d errors=%d "
        "written=%d elapsed=%.1fs",
        apply, len(targets), len(records), len(within_tol), len(out_of_tol),
        len(no_overlap), len(unexplained), len(errors), written, elapsed,
    )

    # ── FAIL LOUD ─────────────────────────────────────────────────────────────
    # No silent skip, no "unscoreable" sentinel (Brian standing feedback): an
    # unexplained out-of-tol / no-overlap residual means a missing/doubled
    # action or a real basis divergence reached the reconstruction — it MUST
    # halt the build before the scratch basis is trusted for the shadow retrain.
    if unexplained:
        raise RuntimeError(
            f"CRSP reconciliation FAILED LOUD: {len(unexplained)} ticker(s) "
            f"carry an unexplained out-of-tolerance residual (rel_tol={rel_tol:.2%}). "
            f"Each is a missing/doubled/mis-ratio'd split or dividend, an "
            f"un-reconcilable ticker, or a real polygon-vs-yfinance divergence — "
            f"localize and fix the action (or acknowledge via "
            f"--known-divergence) before trusting the scratch basis. First 20: "
            f"{[r.to_dict() for r in unexplained[:20]]}"
        )
    if errors:
        raise RuntimeError(
            f"CRSP migration had {len(errors)} per-ticker error(s) — see audit. "
            f"First 20: {errors[:20]}"
        )

    return summary


def _load_macro_series(s3, bucket: str) -> dict[str, pd.Series]:
    """Load the macro/SPY/sector-ETF Close series for the feature recompute.

    Reuses ``features.compute`` loaders (ArcticDB universe+macro) so the scratch
    feature recompute sees the same macro context the live build does. READ
    ONLY. Degrades to ``{}`` (features fall back to defaults) on failure rather
    than blocking the reconciliation, which needs no macro.
    """
    try:
        from features.compute import _extract_macro, _load_price_source

        source = _load_price_source(s3, bucket)
        if not source:
            return {}
        return _extract_macro(source, source)
    except Exception as exc:  # noqa: BLE001
        log.warning("macro load for CRSP feature recompute failed (%s) — "
                    "features will use defaults", exc)
        return {}


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument(
        "--apply", action="store_true",
        help="Recompute features + WRITE the scratch library. Default: dry-run "
             "(reconcile + report only, no writes).",
    )
    parser.add_argument(
        "--scratch-lib", default=DEFAULT_SCRATCH_LIB,
        help=f"Scratch ArcticDB library name (default: {DEFAULT_SCRATCH_LIB}). "
             "Must NOT be a live name (universe/macro).",
    )
    parser.add_argument(
        "--tickers",
        help="Comma-separated subset of tickers (default: all live-universe symbols).",
    )
    parser.add_argument(
        "--rel-tol", type=float, default=DEFAULT_RECONCILE_REL_TOL,
        help=f"Reconciliation relative-deviation tolerance (default: {DEFAULT_RECONCILE_REL_TOL}).",
    )
    parser.add_argument(
        "--known-divergence",
        help="Comma-separated tickers whose out-of-tol residual is an "
             "operator-acknowledged known divergence (reported, not fatal).",
    )
    parser.add_argument(
        "--bucket", default=DEFAULT_BUCKET,
        help=f"S3 bucket (default: {DEFAULT_BUCKET})",
    )
    args = parser.parse_args()

    tickers_override = (
        [t.strip() for t in args.tickers.split(",") if t.strip()]
        if args.tickers else None
    )
    known = (
        frozenset(t.strip() for t in args.known_divergence.split(",") if t.strip())
        if args.known_divergence else frozenset()
    )

    result = migrate_universe_crsp_basis(
        bucket=args.bucket,
        scratch_lib=args.scratch_lib,
        apply=args.apply,
        tickers_override=tickers_override,
        rel_tol=args.rel_tol,
        known_divergence_tickers=known,
    )
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
