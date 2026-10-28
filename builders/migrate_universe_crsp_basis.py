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
  1. Re-pulls RAW (unadjusted) prices over the full history — polygon
     ``adjusted=false`` aggregates over polygon's reach (~2 years, free tier),
     and before that yfinance ``auto_adjust=False`` UN-adjusted for the splits
     on/after polygon's first bar (yfinance's ``Close`` is split-adjusted, so
     it is not raw on its own; alpha-engine-config-I12674). Splits yfinance
     folded in before that bar are marked in-basis and never applied again.
  2. Applies polygon SPLITS to get the split-adjusted ``Close`` LEVEL
     (``corporate_actions.apply`` → ``_split_math.restate_series_for_splits``).
  3. Derives ``total_return_close`` from polygon DIVIDENDS
     (``corporate_actions.get_dividends`` + ``total_return_series``), queried
     under polygon's own symbol for a class share (``BF-B`` -> ``BF.B``). When
     the current symbol's dividends do not reach back to the window start, the
     dividends the entity paid under its PRIOR tickers (polygon ticker events
     plus ``collectors/data/sp500_known_retickers.json``) are folded in
     (``fetch_prior_ticker_dividends``; alpha-engine-config-I12436).
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
log. Both series are total-return, but each is constructed on its own
normalization epoch (polygon-authoritative back-adjustment vs yfinance's
auto_adjust convention) — for split/spinoff names those epochs diverge even
when the two feeds agree on every return. `reconcile_total_return` rebases
both series to 1.0 at the first in-window date before diffing, so within
tolerance they should be identical up to feed rounding regardless of their
raw levels; the NEW one is polygon-authoritative (config#1455).

Scoping the gate to the training window (``--reconcile-start``)
---------------------------------------------------------------
Polygon's corporate-action history is SPARSE before ~2019 — for split /
restructuring names (e.g. DD, HON, KLAC) a full-10y reconciliation fails at
~2016 dates with huge residuals because the deep-history split/dividend events
simply aren't in polygon, while the SAME names reconcile cleanly inside the
recent window where polygon coverage is complete. The predictor only trains on
a clamped recent window (``TRAIN_START_DATE``), so per Brian's decision
(2026-06-30, after the subset dry-run surfaced the coverage gap) the
reconciliation + basis-trust GATE is scoped to the predictor training window;
deep pre-window history stays yfinance-backed best-effort.

``--reconcile-start YYYY-MM-DD`` scopes the pass/fail GATE: residuals on common
dates strictly BEFORE ``reconcile_start`` are EXCLUDED from the gate (deep
pre-window history the model never trains on), while every in-window residual
(``>= reconcile_start``) STILL fails loud. **The full history is STILL WRITTEN**
to the scratch library (pre-window rows included, best-effort basis) — only the
GATE is scoped, and the audit report makes the scope explicit (per-ticker
in-window vs excluded date counts + a best-effort flag). Default
(``--reconcile-start`` unset) is UNCHANGED: full-history reconciliation, so the
operator must OPT IN to scoping.

CRITICAL — warmup buffer. The operator MUST set ``--reconcile-start`` to the
predictor ``TRAIN_START_DATE`` MINUS the maximum feature lookback (~252 trading
days ≈ 1 calendar year; e.g. ``mom_12_1`` uses ``close.shift(252)``), NOT to
``TRAIN_START_DATE`` itself. Warmup features at the FIRST training date are
computed from prices up to ~252 trading days earlier, so those earlier rows must
also be reconciled (clean) for the gate to actually protect every value the
model trains on. Using ``train_start`` directly would leave the warmup window
unguarded — a silent hole exactly where the model's first labels are built.

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
    # scope the GATE to the training window + ~1y warmup buffer (TRAIN_START_DATE
    # minus ~252 trading days); deep pre-window history stays best-effort:
    python -m builders.migrate_universe_crsp_basis --reconcile-start 2020-01-01
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import shutil
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor
import dataclasses
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
#: The pass receipt for one scratch library (alpha-engine-config-I12684). Written
#: only by an ``--apply`` build that passed the reconcile gate AND wrote every name;
#: a consumer of a non-live library (``collectors/edgar_pit_fundamentals.py
#: backfill``) refuses the library without it. It sits under ``AUDIT_PREFIX``
#: because that is the one prefix the build box may PUT under (nous-ergon-ops#1842).
VERIFIED_RECEIPT_PREFIX = f"{AUDIT_PREFIX}verified/"
#: ``status`` values a receipt can carry. ``writing`` is stamped BEFORE the write
#: pass touches the library, so a build killed mid-write leaves a receipt that
#: refuses rather than a stale ``verified`` one vouching for half a library.
RECEIPT_VERIFIED = "verified"
RECEIPT_WRITING = "writing"


def verified_receipt_key(scratch_lib: str) -> str:
    """The S3 key of ``scratch_lib``'s pass receipt."""
    return f"{VERIFIED_RECEIPT_PREFIX}{scratch_lib}.json"
DEFAULT_SCRATCH_LIB = "universe_crsp"
# 2, not 8 (alpha-engine-config-I12436). Each name costs three polygon calls
# (splits, dividends, and since alpha-engine-config-I12674 its raw daily bars)
# against a 5-calls/minute key the fleet shares, so the polygon budget — not
# CPU — sets the build's pace (~909 names x 3 calls / 5 per min ~= 9 h) and
# more workers buy no speed. What they did buy on 2026-10-25 was 2,230 429s and
# 478 corporate-action fetches abandoned. Two keeps one name's yfinance pull and
# feature recompute overlapping the other's polygon wait.
DEFAULT_WORKERS = 2

# Corporate-action fetch patience (alpha-engine-config-I12436). The polygon
# client already retries a 429 four times over ~105 s, which is one rate-limit
# window; on a key other fleet jobs share, that is not enough. These are OUTER
# retries around a whole get_splits/get_dividends call, after the client gave
# up: waits 60, 120, 240 s (capped at 300) — ~7 min more per call before the
# name is declared incomplete and the build stops. The build NEVER proceeds on
# an empty action list it could not confirm is empty.
CA_FETCH_ATTEMPTS = 4
CA_FETCH_BACKOFF_BASE_SECONDS = 60.0
CA_FETCH_BACKOFF_CAP_SECONDS = 300.0

# Both the derived total_return_close and the retiring yfinance Close are
# TOTAL-RETURN series anchored at the latest (post-action) split-adjusted
# price, so within feed-rounding they should be identical. 2% is a generous
# band that absorbs provider close-price rounding + a stray T+1 print without
# masking a missing/doubled action (which moves the boundary by the action
# factor — a dividend ~0.5–3%/event compounding, a split 50%+).
DEFAULT_RECONCILE_REL_TOL = 0.02

# ── which axis GATES the build (alpha-engine-config-I12436, "Gate on close") ──
#
# The build reconciles two axes per name:
#   * ``close``: the split-only ``Close`` LEVEL against yfinance's
#     ``auto_adjust=False`` ``Close`` (split-adjusted, NOT dividend-adjusted),
#     kept by ``fetch_raw_prices`` as ``REFERENCE_CLOSE_ATTR``. This is the
#     column the EDGAR PIT rebuild reads (option A, Brian 2026-10-23).
#   * ``total_return``: ``total_return_close`` against the live ``universe``
#     ``Close`` (dividend back-adjusted yfinance).
# The GATE axis decides pass/fail, the scratch write and the receipt. The other
# axis is still reconciled, still logged at ERROR for every unexplained name,
# and recorded in the audit and the receipt (``total_return_axis_trusted``), so
# a consumer of ``total_return_close`` or the features on it can refuse an
# untrusted build. Gating on ``close`` checks what the rebuild uses; it does not
# loosen the ``close`` check, which had no gate before.
GATE_AXES = ("close", "total_return")
DEFAULT_GATE_AXIS = "close"
REFERENCE_CLOSE_ATTR = "reference_split_adjusted_close"

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
    n_common_dates: int  # dates that GATE the pass/fail decision (in-window when scoped)
    max_rel_dev: float
    max_dev_date: str | None
    explained: bool
    explanation: str
    nearest_action: str | None = None
    # ── reconcile-window scoping (--reconcile-start) ──────────────────────────
    # The gate considers ONLY common dates >= reconcile_start (the predictor
    # training window + warmup buffer); residuals strictly before it are
    # EXCLUDED from the gate (deep pre-window history the model never trains on,
    # where polygon corporate-action coverage is sparse). Made explicit here so
    # the audit report shows the scope — no silent loosening.
    reconcile_start: str | None = None  # None ⇒ full-history (default, unchanged)
    n_common_in_window: int = 0  # common dates >= reconcile_start (gate these)
    n_common_excluded: int = 0  # common dates < reconcile_start (best-effort, ungated)

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
            "reconcile_start": self.reconcile_start,
            "n_common_in_window": self.n_common_in_window,
            "n_common_excluded": self.n_common_excluded,
            "pre_window_best_effort": self.n_common_excluded > 0,
        }


@dataclass
class TickerOutcome:
    """Per-ticker pipeline outcome carried back from the worker."""

    ticker: str
    outcome: str  # "ok" | "fetch_empty" | "no_old_close" | "error"
    reconcile: ReconcileRecord | None = None
    # The split-only Close against its reference (gate on close).
    close_reconcile: ReconcileRecord | None = None
    n_rows: int = 0
    n_splits: int = 0
    n_dividends: int = 0
    error: str | None = None
    written: bool = False
    # --apply: the featured frame is SPILLED to local disk (``spill_path``),
    # never held in this outcome. Holding ~909 full-history 60-column frames
    # until the write pass is what OOM-killed the 2026-10-25 build on a 4 GiB
    # box (exit 137 two-thirds through the alphabet, alpha-engine-config-I12436).
    spill_path: str | None = None
    # Splits the feed reported but ``corporate_actions.apply`` declined to
    # apply because the raw close shows no corroborating price move
    # (config#1455 price-evidence gate) — human-readable ``"TICKER ratio
    # (ex_date)"`` strings. Surfaced explicitly in the audit report so an
    # out-of-tol reconciliation residual caused by a feed-reported-but-
    # unconfirmed split is diagnosed as exactly that, not a generic
    # "missing/doubled/mis-ratio'd" guess.
    unconfirmed_splits: list[str] = field(default_factory=list)
    # Prior symbols whose dividends were folded in (alpha-engine-config-I12436):
    # ``[{"symbol", "start", "end", "source", "dividends_added"}]``. Empty when
    # the current symbol's dividend history had no gap.
    prior_tickers: list[dict] = field(default_factory=list)
    # True when the dividend history showed a gap and polygon was asked for the
    # name's prior tickers (one ticker-events call, plus one per prior symbol).
    prior_ticker_lookup: bool = False


# ── raw price fetch (polygon adjusted=false, yfinance before polygon's reach) ──

# Free-tier polygon serves ~2 years of daily aggregates; asking further back
# risks a plan refusal for the whole call. 720 days stays inside it with a
# margin. Before polygon's first bar the history comes from yfinance, un-adjusted
# for every split on/after that bar (alpha-engine-config-I12674).
POLYGON_RAW_LOOKBACK_DAYS = 720

# How the yfinance and polygon halves are joined. Over the first
# ``_SEAM_CHECK_SESSIONS`` sessions both feeds price, the median ratio of the
# un-adjusted yfinance close to polygon's raw close must sit within
# ``_SEAM_REL_TOL`` of 1.0. A split yfinance folded in but did not list (or listed
# with another ratio) moves that ratio by the split factor (>= 1.45x), so it fails
# here instead of silently shifting every pre-seam row.
_SEAM_CHECK_SESSIONS = 10
_SEAM_REL_TOL = 0.02

# yfinance's own split events, matched to a feed-reported split when deciding
# that the split is ALREADY in the returned frame's basis. Same window/tolerance
# as the price-evidence gate (config#1455).
_IN_BASIS_WINDOW_DAYS = 4
_IN_BASIS_REL_TOL = 0.15

#: ``DataFrame.attrs`` key: ``[(ex_date: Timestamp, multiplier: float), ...]`` —
#: splits ALREADY folded into the frame ``fetch_raw_prices`` returned (yfinance
#: splits before polygon's first bar), as the multiplier yfinance applied to the
#: rows before ``ex_date``. ``reconstruct_basis`` never applies these again.
SPLITS_IN_BASIS_ATTR = "splits_in_basis"
#: ``DataFrame.attrs`` key: ISO date of polygon's first raw bar (the seam).
POLYGON_RAW_FROM_ATTR = "polygon_raw_from"

_PRICE_COLS = ("Open", "High", "Low", "Close")
_OHLCV = ("Open", "High", "Low", "Close", "Volume")


class RawPriceFetchError(RuntimeError):
    """A ticker's raw daily bars could not be fetched or joined.

    Raised instead of falling back to a split-adjusted close: that fallback is
    the defect this fetch exists to remove (alpha-engine-config-I12674)."""


def _yfinance_frame(ticker: str, period: str) -> pd.DataFrame:
    """yfinance ``auto_adjust=False`` OHLCV plus its ``Stock Splits`` column.

    ``auto_adjust=False`` turns off the DIVIDEND adjustment only: yfinance's
    ``Close`` is still SPLIT-adjusted over the whole history, which is why it is
    not raw and why ``Stock Splits`` is pulled alongside it."""
    import yfinance as yf

    raw = yf.download(
        tickers=ticker,
        period=period,
        interval="1d",
        auto_adjust=False,
        actions=True,  # the split events yfinance folded into Close
        progress=False,
        group_by="ticker",
        threads=False,
    )
    if raw is None or len(raw) == 0:
        raise RawPriceFetchError(f"yfinance returned no rows for {ticker} (auto_adjust=False)")

    # yfinance returns a MultiIndex (ticker, field) when group_by="ticker".
    if isinstance(raw.columns, pd.MultiIndex):
        if ticker in raw.columns.get_level_values(0):
            raw = raw[ticker].copy()
        else:
            raw = raw.droplevel(0, axis=1).copy()

    if "Close" not in raw.columns:
        raise RawPriceFetchError(f"yfinance frame for {ticker} missing Close column")
    raw = raw.dropna(subset=["Close"])
    if raw.empty:
        raise RawPriceFetchError(f"yfinance frame for {ticker} empty after dropna(Close)")

    idx = pd.to_datetime(raw.index)
    if idx.tz is not None:
        idx = idx.tz_convert("UTC").tz_localize(None)
    raw.index = idx.normalize()
    return raw.sort_index()


def _yfinance_splits(frame: pd.DataFrame) -> list[tuple[pd.Timestamp, float]]:
    """``[(date, ratio)]`` from yfinance's ``Stock Splits`` (2.0 = 2-for-1)."""
    if "Stock Splits" not in frame.columns:
        return []
    s = pd.to_numeric(frame["Stock Splits"], errors="coerce").fillna(0.0)
    return [(pd.Timestamp(d), float(r)) for d, r in s[s > 0].items()]


def _polygon_raw_bars(
    ticker: str,
    *,
    client,
    start: str,
    end: str,
    attempts: int,
    sleep,
) -> pd.DataFrame:
    """polygon daily aggregates with ``adjusted=false`` — genuinely raw.

    Goes through the client's shared, locked rate limiter (data#2298) and the
    same patient, strict retry the corporate-action fetch uses."""
    from collectors.daily_closes import _polygon_symbol

    symbol = _polygon_symbol(ticker)
    for attempt in range(attempts):
        try:
            return client.get_daily_bars(symbol, start, end, adjusted=False)
        except Exception as exc:  # noqa: BLE001 - classified below
            if not _is_retryable_fetch_error(exc) or attempt == attempts - 1:
                raise RawPriceFetchError(
                    f"{ticker}: polygon raw-bars fetch failed after "
                    f"{attempt + 1} attempt(s) ({_describe_fetch_error(exc)})"
                    " — refusing to fall back to a split-adjusted close"
                ) from None
            wait = min(
                CA_FETCH_BACKOFF_BASE_SECONDS * (2 ** attempt),
                CA_FETCH_BACKOFF_CAP_SECONDS,
            )
            log.warning(
                "%s: polygon raw-bars fetch failed (%s) — waiting %.0fs, attempt %d/%d",
                ticker, _describe_fetch_error(exc), wait, attempt + 1, attempts,
            )
            sleep(wait)
    raise AssertionError("unreachable")  # pragma: no cover


def fetch_raw_prices(
    ticker: str,
    *,
    period: str = "max",
    client=None,
    today: str | None = None,
    polygon_lookback_days: int | None = None,
    attempts: int = CA_FETCH_ATTEMPTS,
    sleep=None,
) -> pd.DataFrame:
    """RAW (unadjusted) daily OHLCV for one ticker, so polygon's splits are
    applied to it exactly once (alpha-engine-config-I12674).

    yfinance ``auto_adjust=False`` is NOT raw: its ``Close`` is split-adjusted,
    so the price-evidence gate judged splits on a series that already carried
    them and could "confirm" one off a coincident move and apply it twice. So:

    * **polygon window** (its first bar onward, ~2 years on the free tier):
      polygon aggregates with ``adjusted=false``.
    * **before polygon's first bar** (the seam): yfinance, UN-adjusted for every
      split it lists on/after the seam, so it joins polygon's raw basis. The join
      is checked on the first overlapping sessions and fails loud on a mismatch.
      Splits yfinance lists BEFORE the seam stay folded into those rows and are
      recorded in ``df.attrs[SPLITS_IN_BASIS_ATTR]``; ``reconstruct_basis``
      never applies them again.
    * after polygon's last bar (its close is T+1): yfinance, likewise un-adjusted.

    Contract unchanged: a tz-naive, sorted ``DatetimeIndex`` with
    ``Open/High/Low/Close`` (float64) and ``Volume`` (int64). Raises
    :class:`RawPriceFetchError` on any empty/failed pull or a bad join — a
    silent fallback would corrupt the reconstruction.
    """
    sleep = sleep or time.sleep
    if client is None:
        from polygon_client import polygon_client

        client = polygon_client()
    lookback = polygon_lookback_days or int(
        os.environ.get("MIGRATE_UNIVERSE_CRSP_POLYGON_RAW_DAYS", POLYGON_RAW_LOOKBACK_DAYS)
    )
    end_ts = pd.Timestamp(today) if today else pd.Timestamp(datetime.now(timezone.utc).date())
    start_ts = end_ts - pd.Timedelta(days=lookback)

    yf_frame = _yfinance_frame(ticker, period)
    poly = _polygon_raw_bars(
        ticker, client=client,
        start=start_ts.strftime("%Y-%m-%d"), end=end_ts.strftime("%Y-%m-%d"),
        attempts=attempts, sleep=sleep,
    )
    if poly is None or poly.empty:
        raise RawPriceFetchError(
            f"{ticker}: polygon returned no raw bars for "
            f"{start_ts.date()}..{end_ts.date()} — refusing to fall back to a "
            "split-adjusted close"
        )
    poly = poly.copy()
    poly.index = pd.to_datetime(poly.index).normalize()
    poly = poly.sort_index()
    seam = poly.index.min()

    splits = _yfinance_splits(yf_frame)
    yf_unadj = yf_frame[[c for c in _OHLCV if c in yf_frame.columns]].astype("float64")
    for when, ratio in splits:
        if when < seam:
            continue  # stays in the basis; recorded below, never re-applied
        before = yf_unadj.index < when
        cols = [c for c in _PRICE_COLS if c in yf_unadj.columns]
        yf_unadj.loc[before, cols] *= ratio
        if "Volume" in yf_unadj.columns:
            yf_unadj.loc[before, "Volume"] /= ratio

    common = yf_unadj.index.intersection(poly.index)
    if (yf_unadj.index < seam).any():
        if len(common) == 0:
            raise RawPriceFetchError(
                f"{ticker}: yfinance and polygon share no session, so the "
                "pre-polygon history cannot be joined to the raw basis"
            )
        head = common[:_SEAM_CHECK_SESSIONS]
        ratio = float((yf_unadj.loc[head, "Close"] / poly.loc[head, "Close"]).median())
        if not abs(ratio - 1.0) <= _SEAM_REL_TOL:
            raise RawPriceFetchError(
                f"{ticker}: un-adjusted yfinance close is {ratio:.4f}x polygon's "
                f"raw close at the seam ({seam.date()}) — a split yfinance folded "
                "in is missing from its split list or mis-ratio'd; refusing to "
                "join the pre-polygon history"
            )

    out = poly[list(_OHLCV)].combine_first(yf_unadj[[c for c in _OHLCV if c in yf_unadj.columns]])
    out = out.dropna(subset=["Close"]).sort_index()
    for c in _PRICE_COLS:
        out[c] = out[c].astype("float64")
    out["Volume"] = out["Volume"].fillna(0).round().astype("int64")
    out = out[list(_OHLCV)]
    out.attrs[SPLITS_IN_BASIS_ATTR] = [(when, 1.0 / ratio) for when, ratio in splits if when < seam]
    out.attrs[POLYGON_RAW_FROM_ATTR] = seam.strftime("%Y-%m-%d")
    # The close axis's reference: yfinance's own split-adjusted, dividend-
    # unadjusted Close, untouched (alpha-engine-config-I12436, gate on close).
    # A Series in ``attrs`` must not ride into pandas ops: the build pops it
    # before reconstructing (see ``_process_one``).
    out.attrs[REFERENCE_CLOSE_ATTR] = yf_frame["Close"].astype("float64").copy()
    return out


def split_actions_in_source_basis(raw_df: pd.DataFrame, split_actions: list) -> set:
    """``action_id``s of feed splits ALREADY folded into ``raw_df``.

    Matches each split in ``raw_df.attrs[SPLITS_IN_BASIS_ATTR]`` (set by
    :func:`fetch_raw_prices`) to a feed split within ±4 days whose factor, or its
    inverse (an inverted feed record is the same event), agrees within 15%. A
    frame without the attribute (an injected fetcher) has nothing in basis."""
    in_basis = list((getattr(raw_df, "attrs", None) or {}).get(SPLITS_IN_BASIS_ATTR) or [])
    if not in_basis:
        return set()
    out: set = set()
    window = pd.Timedelta(days=_IN_BASIS_WINDOW_DAYS)
    for a in split_actions:
        try:
            factor = ca.expected_factor(a)
        except Exception:  # noqa: BLE001 - malformed ratio: let apply() guard it
            continue
        if factor <= 0:
            continue
        ex = pd.Timestamp(a.ex_date).normalize()
        for when, mult in in_basis:
            if abs(pd.Timestamp(when) - ex) > window:
                continue
            if any(abs(f - mult) <= _IN_BASIS_REL_TOL * mult for f in (factor, 1.0 / factor)):
                out.add(a.action_id)
                break
    return out


# ── corporate actions: strict, patient fetch ─────────────────────────────────


class CorporateActionFetchError(RuntimeError):
    """A ticker's polygon split/dividend history could not be fetched.

    Raised instead of building the name on an empty action list: "could not
    fetch" is not "never split / never paid", and a series reconstructed from
    the second when the truth was the first is silently wrong in every row
    before the missed action (alpha-engine-config-I12436)."""


def _is_retryable_fetch_error(exc: BaseException) -> bool:
    """Rate-limit exhaustion, a 5xx, or a network error: worth waiting out.
    Anything else (a malformed ratio, a programming error) is not."""
    import requests

    try:
        from polygon_client import PolygonRateLimitError
    except Exception:  # noqa: BLE001 - client module unimportable
        PolygonRateLimitError = ()  # type: ignore[assignment]
    if PolygonRateLimitError and isinstance(exc, PolygonRateLimitError):
        return True
    if isinstance(exc, (requests.ConnectionError, requests.Timeout)):
        return True
    if isinstance(exc, requests.HTTPError):
        resp = getattr(exc, "response", None)
        return resp is not None and getattr(resp, "status_code", 0) >= 500
    return False


def _scrub(exc: BaseException) -> str:
    try:
        from polygon_client import _scrub_api_key

        return _scrub_api_key(exc)
    except Exception:  # noqa: BLE001 - scrubber unavailable; drop the text
        return type(exc).__name__


def _describe_fetch_error(exc: BaseException) -> str:
    """A FIXED description of a fetch failure: class name plus, for an HTTP
    error, its status code. Never the exception's message: a requests error
    message embeds the effective URL, and the polygon URL carries ``apiKey``.
    Redacting with a regex is not enough to rely on here, so the text is
    simply not used (CodeQL py/clear-text-logging-sensitive-data on #2298)."""
    import requests

    name = type(exc).__name__
    if isinstance(exc, requests.HTTPError):
        code = getattr(getattr(exc, "response", None), "status_code", None)
        return f"{name} (HTTP {code})" if code is not None else name
    return name


def _strict_call(
    ticker: str,
    kind: str,
    fn,
    *,
    attempts: int,
    sleep,
    abort: threading.Event | None,
):
    """Call ``fn()`` STRICTLY: retry a retryable failure (429 the client could
    not clear, 5xx, network) up to ``attempts`` times with exponential backoff,
    and raise :class:`CorporateActionFetchError` when that is exhausted, on a
    non-retryable failure, or once ``abort`` is set. Never returns a degraded
    empty answer."""
    for attempt in range(attempts):
        try:
            return fn()
        except Exception as exc:  # noqa: BLE001 - classified below
            last = attempt == attempts - 1
            if not _is_retryable_fetch_error(exc) or last or (
                abort is not None and abort.is_set()
            ):
                raise CorporateActionFetchError(
                    f"{ticker}: polygon {kind} fetch failed after "
                    f"{attempt + 1} attempt(s) ({_describe_fetch_error(exc)})"
                    f" — the name cannot be built without its "
                    f"full {kind} history"
                ) from None
            wait = min(
                CA_FETCH_BACKOFF_BASE_SECONDS * (2 ** attempt),
                CA_FETCH_BACKOFF_CAP_SECONDS,
            )
            log.warning(
                "%s: polygon %s fetch failed (%s) — waiting %.0fs, "
                "attempt %d/%d",
                ticker, kind, _describe_fetch_error(exc), wait,
                attempt + 1, attempts,
            )
            sleep(wait)
    raise AssertionError("unreachable")  # pragma: no cover


class _PolygonSymbolClient:
    """Sends polygon ITS symbol for a class-share store ticker.

    The store keys class shares with a dash (``BF-B``, ``MOG-A``); polygon's
    reference endpoints key them with a dot (``BF.B``). The raw-bars fetch
    already maps through ``collectors.daily_closes._polygon_symbol``; the split,
    dividend and ticker-event queries did not, so the 2026-10-26 build asked
    polygon for ``BF-B`` dividends, got none, and BF-B and MOG-A drifted out of
    tolerance by their whole cumulative yield (alpha-engine-config-I12436,
    class 3). Every event is still keyed back to the STORE ticker by the
    caller."""

    def __init__(self, client):
        self._client = client

    def get_splits(self, ticker: str):
        from collectors.daily_closes import _polygon_symbol

        return self._client.get_splits(_polygon_symbol(ticker))

    def get_dividends(self, ticker: str):
        from collectors.daily_closes import _polygon_symbol

        return self._client.get_dividends(_polygon_symbol(ticker))

    def get_ticker_events(self, ticker: str):
        from collectors.daily_closes import _polygon_symbol

        return self._client.get_ticker_events(_polygon_symbol(ticker))


def _strict_client(ticker: str, client):
    """The polygon client to fetch with, built lazily; a client that cannot be
    built fails the name (strict), never yields an empty history."""
    if client is not None:
        return client
    try:
        from polygon_client import polygon_client

        return polygon_client()
    except Exception as exc:  # noqa: BLE001 - construction failure: strict
        raise CorporateActionFetchError(
            f"{ticker}: polygon client could not be built "
            f"({_describe_fetch_error(exc)}) — the name cannot be built without "
            "its corporate-action history"
        ) from None


def fetch_corporate_actions(
    ticker: str,
    *,
    client=None,
    attempts: int = CA_FETCH_ATTEMPTS,
    sleep=None,
    abort: threading.Event | None = None,
) -> tuple[list, list]:
    """Fetch ``(split_actions, dividend_actions)`` for one ticker, STRICTLY.

    ``corporate_actions.get_splits`` / ``get_dividends`` are called with
    ``strict=True`` so a failed fetch raises rather than returning ``[]``. A
    retryable failure (429 the client could not clear, 5xx, network) is retried
    up to ``attempts`` times with exponential backoff; when that is exhausted,
    or on a non-retryable failure, :class:`CorporateActionFetchError` is raised
    and the caller must not build the name. ``abort`` (set once any name has
    failed this way) cuts the wait short: the build is already lost.

    Polygon is queried with its own symbol for a class-share ticker
    (:class:`_PolygonSymbolClient`); the actions are keyed to ``ticker``.
    """
    sleep = sleep or time.sleep
    query = _PolygonSymbolClient(_strict_client(ticker, client))
    results = []
    for kind, fn in (("splits", ca.get_splits), ("dividends", ca.get_dividends)):
        results.append(_strict_call(
            ticker, kind,
            lambda fn=fn: fn(ticker, client=query, strict=True),
            attempts=attempts, sleep=sleep, abort=abort,
        ))
    return results[0], results[1]


# ── prior tickers: dividends paid under an earlier symbol ─────────────────────
#
# alpha-engine-config-I12436, class 1 of the 2026-10-26 build (24 of its 46
# out-of-tolerance names): polygon's ``/v3/reference/dividends?ticker=`` answers
# for the symbol asked, so a name that changed ticker (EQR -> VMRK, BK -> BNY,
# PSKY -> SKYD, a holdco reorg) loses every dividend paid before the change and
# its total-return series drifts by the old symbol's cumulative yield. The build
# now also asks polygon for the name's PRIOR tickers and folds in the dividends
# each one paid while the entity held it.
#
# Budget: the free tier is 5 calls/min and the build already spends three per
# name (splits, dividends, raw bars): ~9.4 h of the dispatcher's 12 h cap. A
# fourth call for all ~909 names (~3 h) would not fit, so the prior-ticker
# lookup runs only where the dividend history shows the gap a rename leaves:
# no dividends at all, or a first ex-date more than ``PRIOR_TICKER_GAP_DAYS``
# after the start of the window that matters (the later of the first price row
# and ``--reconcile-start``). A name that has paid under one symbol through the
# whole window is never queried, and a rename always opens such a gap.

#: The curated renames polygon does not report as ``ticker_change`` (it returns
#: no events for BK, verified 2026-08-12): consulted beside polygon's events.
KNOWN_RETICKERS_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "collectors", "data", "sp500_known_retickers.json",
)
#: Longer than any regular payer's gap between ex-dates (annual: ~365 days).
PRIOR_TICKER_GAP_DAYS = 400


@dataclass(frozen=True)
class PriorTicker:
    """A symbol the entity traded under before ``end``, its rename date.

    Dividends polygon lists under ``symbol`` count only when they went ex in
    ``[start, end)``: before ``start`` (or on/after ``end``) the symbol belonged
    to someone else, or the entity already traded under its next symbol.
    ``start`` is ``None`` when the source does not say when the entity took
    the symbol."""

    symbol: str
    start: str | None
    end: str
    source: str  # "polygon_ticker_events" | "known_retickers"


def load_known_retickers(path: str = KNOWN_RETICKERS_PATH) -> list[dict]:
    """The curated ``{"old", "new", "effective"}`` rename rows. A file that is
    missing or malformed RAISES: silently reading it as "no renames" would put
    the dividend gap it exists to close back into the build."""
    with open(path, encoding="utf-8") as fh:
        rows = json.load(fh)["retickers"]
    out = []
    for r in rows:
        old, new, eff = r["old"], r["new"], r["effective"]
        pd.Timestamp(eff)  # a malformed date fails here, loudly
        out.append({"old": str(old), "new": str(new), "effective": str(eff)})
    return out


def _known_prior_tickers(ticker: str, retickers: list[dict]) -> list[PriorTicker]:
    """Walk the curated renames backward from ``ticker`` (A -> B -> ticker)."""
    by_new: dict[str, dict] = {}
    for r in retickers:
        by_new.setdefault(r["new"], r)
    out: list[PriorTicker] = []
    seen = {ticker}
    cur = ticker
    while cur in by_new:
        row = by_new[cur]
        old = row["old"]
        if old in seen:
            break  # a cycle in the curated file: stop, do not loop
        seen.add(old)
        prev = by_new.get(old)
        out.append(PriorTicker(
            symbol=old,
            start=prev["effective"] if prev is not None else None,
            end=row["effective"],
            source="known_retickers",
        ))
        cur = old
    return out


def _polygon_prior_tickers(ticker: str, pairs: list[dict]) -> list[PriorTicker]:
    """Prior symbols from ``PolygonClient.get_ticker_events`` rename pairs
    (``[{"date", "old_ticker", "new_ticker"}]`` ascending): the entity held
    pair i's ``old_ticker`` from pair i-1's date until pair i's date."""
    ordered = sorted(pairs or [], key=lambda p: str(p.get("date") or ""))
    out: list[PriorTicker] = []
    prev_date: str | None = None
    for p in ordered:
        old, date = p.get("old_ticker"), p.get("date")
        if not old or not date:
            continue
        if old != ticker:
            out.append(PriorTicker(
                symbol=str(old), start=prev_date, end=str(date),
                source="polygon_ticker_events",
            ))
        prev_date = str(date)
    return out


def dividend_history_gap(
    dividend_actions: list, *, window_start: pd.Timestamp | None,
) -> bool:
    """True when the dividends under the current symbol do not reach back to
    ``window_start``: none at all, or a first ex-date more than
    ``PRIOR_TICKER_GAP_DAYS`` after it. ``window_start`` ``None`` (no price
    history to measure from) reads only "none at all" as a gap."""
    if not dividend_actions:
        return True
    if window_start is None:
        return False
    first = min(pd.Timestamp(a.ex_date).normalize() for a in dividend_actions)
    return (first - pd.Timestamp(window_start).normalize()).days > PRIOR_TICKER_GAP_DAYS


def fetch_prior_ticker_dividends(
    ticker: str,
    dividend_actions: list,
    *,
    client=None,
    window_start: pd.Timestamp | None = None,
    retickers: list[dict] | None = None,
    attempts: int = CA_FETCH_ATTEMPTS,
    sleep=None,
    abort: threading.Event | None = None,
) -> tuple[list, list[dict]]:
    """Fold in the dividends ``ticker``'s entity paid under its PRIOR symbols.

    Returns ``(dividend_actions, prior_report)``. When the current symbol's
    dividend history has no gap (:func:`dividend_history_gap`) nothing is
    fetched and the input comes back unchanged with an empty report. Otherwise
    polygon's ticker events for ``ticker`` and the curated renames name the
    prior symbols; each one's dividends inside its ``[start, end)`` span are
    added, keyed to ``ticker``, skipping any ex-date the list already has (so a
    dividend polygon already files under the new symbol is never counted
    twice). Every polygon call is STRICT (:func:`_strict_call`): a lookup that
    cannot be answered fails the name, never reads as "no prior tickers".
    """
    if not dividend_history_gap(dividend_actions, window_start=window_start):
        return list(dividend_actions), []
    sleep = sleep or time.sleep
    query = _PolygonSymbolClient(_strict_client(ticker, client))
    retickers = load_known_retickers() if retickers is None else retickers

    pairs = _strict_call(
        ticker, "ticker-events", lambda: query.get_ticker_events(ticker),
        attempts=attempts, sleep=sleep, abort=abort,
    )
    priors: dict[str, PriorTicker] = {}
    # The curated file first, then polygon's events, which win for a symbol
    # both name: polygon dates the span the entity held it at both ends.
    for pt in _known_prior_tickers(ticker, retickers) + _polygon_prior_tickers(ticker, pairs):
        priors[pt.symbol] = pt

    merged = list(dividend_actions)
    have = {pd.Timestamp(a.ex_date).normalize() for a in merged}
    report: list[dict] = []
    for pt in sorted(priors.values(), key=lambda p: p.end):
        events = _strict_call(
            ticker, f"dividends (prior ticker {pt.symbol})",
            lambda pt=pt: query.get_dividends(pt.symbol),
            attempts=attempts, sleep=sleep, abort=abort,
        )
        lo = pd.Timestamp(pt.start).normalize() if pt.start else None
        hi = pd.Timestamp(pt.end).normalize()
        keep = []
        for ev in events or []:
            ex = pd.Timestamp(ev["ex_dividend_date"]).normalize()
            if ex >= hi or (lo is not None and ex < lo) or ex in have:
                continue
            have.add(ex)
            keep.append({**ev, "ticker": ticker})
        added = ca.dividends_from_events(keep)
        merged.extend(added)
        report.append({
            "symbol": pt.symbol, "start": pt.start, "end": pt.end,
            "source": pt.source, "dividends_added": len(added),
        })
    merged.sort(key=lambda a: pd.Timestamp(a.ex_date))
    if report:
        log.info(
            "%s: dividend history gap — folded in %d dividend(s) from prior "
            "ticker(s) %s",
            ticker, sum(r["dividends_added"] for r in report),
            [r["symbol"] for r in report],
        )
    return merged, report


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
        ``Close``. ``split_actions`` is also passed to ``total_return_series`` so
        a dividend declared before a later split has its nominal ``cash_amount``
        scaled onto ``Close``'s post-split basis (config#1462) — without this a
        pre-split dividend's declared amount overstates the back-adjust and the
        error compounds across every subsequent pre-split dividend.

    Pure: no network / S3 / ArcticDB — the migration's network fetch + I/O live
    in the orchestration, so this is unit-testable with hand-built fakes.
    """
    if raw_df is None or raw_df.empty:
        return raw_df, []

    # 1. SPLIT-adjusted Close LEVEL. apply() filters to splits, raises on a
    #    stray dividend/rename, and delegates the factor math. A split the RAW
    #    fetch reports as already folded into its rows (yfinance history before
    #    polygon's first raw bar) is never applied a second time
    #    (alpha-engine-config-I12674).
    in_basis = split_actions_in_source_basis(raw_df, split_actions)
    split_adj, applied = ca.apply(
        raw_df,
        [a for a in split_actions if a.action_id not in in_basis],
        store=SCRATCH_RESTATE_STORE,
        registry=None,
    )
    applied = list(applied) + [
        {
            "action_id": a.action_id,
            "store": SCRATCH_RESTATE_STORE,
            "n_rows_adjusted": 0,
            "factor": ca.expected_factor(a),
            "status": "in_source_basis",
        }
        for a in split_actions
        if a.action_id in in_basis
    ]

    # 2. SEPARATE total-return axis (does NOT mutate split_adj["Close"]).
    # split_actions is passed through so a dividend declared BEFORE a later
    # split has its feed-nominal cash_amount scaled onto the SAME post-split
    # share basis split_adj["Close"] is already on (config#1462) — otherwise
    # a pre-split dividend's cash amount overstates the back-adjust by the
    # intervening cumulative split factor, whether or not that split was
    # itself confirmed-applied to Close (see total_return_series docstring).
    tr_close = ca.total_return_series(
        split_adj, dividend_actions, split_actions=split_actions,
    )

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
    reconcile_start: str | pd.Timestamp | None = None,
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

    ``reconcile_start`` (the predictor training window start MINUS the feature
    warmup buffer — see the module docstring) scopes the GATE: only common dates
    ``>= reconcile_start`` are compared for the pass/fail decision (max_rel_dev,
    status, attribution are all computed over the IN-WINDOW subset); common dates
    strictly before it are EXCLUDED from the gate (counted, reported, flagged
    best-effort — deep pre-window history the model never trains on). When
    ``None`` (default) the full common history gates, behavior is UNCHANGED. If
    scoping leaves ZERO in-window common dates, that is ``no_overlap`` /
    fail-loud (the training-window data is unreconcilable, not a silent skip).

    Attributes the worst-deviation date to the nearest registered action (within
    ``_ACTION_ATTRIBUTION_WINDOW_DAYS``) for the report — diagnostic only.
    """
    new_tr_close = new_tr_close.dropna()
    old_close = old_close.dropna()
    common = new_tr_close.index.intersection(old_close.index)

    reconcile_start_ts = (
        pd.Timestamp(reconcile_start).normalize() if reconcile_start is not None else None
    )
    reconcile_start_str = (
        reconcile_start_ts.strftime("%Y-%m-%d") if reconcile_start_ts is not None else None
    )

    # GATE scope: only common dates >= reconcile_start gate the decision; dates
    # strictly before it are deep pre-window history (best-effort, ungated).
    if reconcile_start_ts is not None:
        in_window = common[common.normalize() >= reconcile_start_ts]
    else:
        in_window = common
    n_total = len(common)
    n_in_window = len(in_window)
    n_excluded = n_total - n_in_window

    if n_in_window == 0:
        if reconcile_start_ts is not None and n_total > 0:
            explanation = (
                f"no common dates >= reconcile_start {reconcile_start_str} "
                f"({n_excluded} pre-window common date(s) excluded) — the "
                f"training-window data is unreconcilable (fail-loud, not skipped)"
            )
        else:
            explanation = (
                "no common dates between new total_return_close and old "
                "yfinance Close — cannot reconcile (fail-loud, not skipped)"
            )
        return ReconcileRecord(
            ticker=ticker,
            status="no_overlap",
            n_common_dates=0,
            max_rel_dev=float("inf"),
            max_dev_date=None,
            explained=False,
            explanation=explanation,
            reconcile_start=reconcile_start_str,
            n_common_in_window=0,
            n_common_excluded=n_excluded,
        )

    a = new_tr_close.reindex(in_window).to_numpy(dtype="float64")
    b = old_close.reindex(in_window).to_numpy(dtype="float64")

    # Anchor-rebase both series to 1.0 at the first in-window date before
    # comparing. `new_tr_close` and `old_close` are each internally-consistent
    # total-return series, but their ABSOLUTE levels are constructed on
    # different normalization epochs (polygon-authoritative split/dividend
    # back-adjustment vs yfinance's own auto_adjust convention). For names
    # with a stock split or spinoff those epochs diverge, and a raw-level
    # comparison manufactures a false OUT-OF-TOL residual even when the two
    # series agree on every actual return. Rebasing removes the epoch offset
    # and leaves only genuine return disagreement (config#1455).
    anchor_a, anchor_b = a[0], b[0]
    if (
        not np.isfinite(anchor_a) or abs(anchor_a) <= 1e-12
        or not np.isfinite(anchor_b) or abs(anchor_b) <= 1e-12
    ):
        # Degenerate anchor (zero/NaN) — cannot rebase meaningfully; fail loud
        # rather than silently falling back to an un-rebased comparison.
        return ReconcileRecord(
            ticker=ticker,
            status="no_overlap",
            n_common_dates=n_in_window,
            max_rel_dev=float("inf"),
            max_dev_date=None,
            explained=False,
            explanation=(
                f"anchor date {pd.Timestamp(in_window[0]).strftime('%Y-%m-%d')} has a "
                f"zero/non-finite price in one series — cannot anchor-rebase "
                f"(fail-loud, not skipped)"
            ),
            reconcile_start=reconcile_start_str,
            n_common_in_window=n_in_window,
            n_common_excluded=n_excluded,
        )
    a = a / anchor_a
    b = b / anchor_b

    denom = np.where(np.abs(b) > 1e-12, np.abs(b), np.nan)
    rel_dev = np.abs(a - b) / denom
    # NaN denom (old close ~0) → ignore that date rather than emit inf.
    rel_dev = np.where(np.isfinite(rel_dev), rel_dev, 0.0)
    worst_pos = int(np.argmax(rel_dev))
    max_rel_dev = float(rel_dev[worst_pos])
    max_dev_date = pd.Timestamp(in_window[worst_pos]).strftime("%Y-%m-%d")

    all_actions = list(split_actions or []) + list(dividend_actions or [])
    near = _nearest_action(
        pd.Timestamp(in_window[worst_pos]).normalize(),
        all_actions,
        _ACTION_ATTRIBUTION_WINDOW_DAYS,
    )
    nearest_action = near.human() if near is not None else None

    scope_note = (
        f" [gate scoped to >= {reconcile_start_str}: {n_in_window} in-window, "
        f"{n_excluded} pre-window best-effort excluded]"
        if reconcile_start_ts is not None else ""
    )

    if max_rel_dev <= rel_tol:
        return ReconcileRecord(
            ticker=ticker,
            status="within_tol",
            n_common_dates=n_in_window,
            max_rel_dev=max_rel_dev,
            max_dev_date=max_dev_date,
            explained=True,
            explanation=f"within tol ({max_rel_dev:.4%} <= {rel_tol:.2%}){scope_note}",
            nearest_action=nearest_action,
            reconcile_start=reconcile_start_str,
            n_common_in_window=n_in_window,
            n_common_excluded=n_excluded,
        )

    if known_divergence:
        explanation = (
            f"OUT-OF-TOL ({max_rel_dev:.4%} > {rel_tol:.2%}) at {max_dev_date} "
            f"but operator-acknowledged known divergence"
            + (f" (nearest action: {nearest_action})" if nearest_action else "")
            + scope_note
        )
        return ReconcileRecord(
            ticker=ticker,
            status="out_of_tol",
            n_common_dates=n_in_window,
            max_rel_dev=max_rel_dev,
            max_dev_date=max_dev_date,
            explained=True,
            explanation=explanation,
            nearest_action=nearest_action,
            reconcile_start=reconcile_start_str,
            n_common_in_window=n_in_window,
            n_common_excluded=n_excluded,
        )

    explanation = (
        f"OUT-OF-TOL ({max_rel_dev:.4%} > {rel_tol:.2%}) at {max_dev_date} — "
        f"likely a missing/doubled/mis-ratio'd split or dividend, or a real "
        f"polygon-vs-yfinance divergence"
        + (f"; nearest registered action: {nearest_action}" if nearest_action
           else "; NO registered action near this date")
        + scope_note
    )
    return ReconcileRecord(
        ticker=ticker,
        status="out_of_tol",
        n_common_dates=n_in_window,
        max_rel_dev=max_rel_dev,
        max_dev_date=max_dev_date,
        explained=False,
        explanation=explanation,
        nearest_action=nearest_action,
        reconcile_start=reconcile_start_str,
        n_common_in_window=n_in_window,
        n_common_excluded=n_excluded,
    )


def reconcile_close(
    ticker: str,
    new_close: pd.Series,
    reference_close: pd.Series | None,
    *,
    split_actions: list | None = None,
    rel_tol: float = DEFAULT_RECONCILE_REL_TOL,
    known_divergence: bool = False,
    reconcile_start: str | pd.Timestamp | None = None,
) -> ReconcileRecord:
    """The CLOSE axis: the reconstructed split-only ``Close`` against yfinance's
    split-adjusted, dividend-unadjusted ``Close`` (``REFERENCE_CLOSE_ATTR``).

    Same comparison as :func:`reconcile_total_return` (common dates, scoped to
    ``reconcile_start``, anchor-rebased, max relative deviation), so a missing,
    doubled or mis-ratio'd SPLIT surfaces here. Dividends play no part on this
    axis. A name with no reference is ``no_overlap`` and UNEXPLAINED: a close
    that cannot be checked is never passed."""
    if reference_close is None or len(reference_close) == 0:
        return ReconcileRecord(
            ticker=ticker,
            status="no_overlap",
            n_common_dates=0,
            max_rel_dev=float("inf"),
            max_dev_date=None,
            explained=False,
            explanation=(
                "CLOSE axis: no split-adjusted reference close for this name "
                "(fetch_raw_prices keeps yfinance auto_adjust=False Close) — "
                "cannot reconcile (fail-loud, not skipped)"
            ),
            reconcile_start=(
                pd.Timestamp(reconcile_start).strftime("%Y-%m-%d")
                if reconcile_start is not None else None
            ),
        )
    ref = reference_close.copy()
    ref.index = pd.to_datetime(ref.index).normalize()
    rec = reconcile_total_return(
        ticker, new_close, ref,
        split_actions=split_actions, dividend_actions=[],
        rel_tol=rel_tol, known_divergence=known_divergence,
        reconcile_start=reconcile_start,
    )
    return dataclasses.replace(rec, explanation=f"CLOSE axis: {rec.explanation}")


# ── declared classes of known total-return divergence ────────────────────────
#
# alpha-engine-config-I12436, classes 2, 4 and 5 of the 2026-10-26 build: names
# whose total-return residual is a real difference in how the two sides treat
# an event, not a fetch bug (classes 1 and 3, prior tickers and class-share
# symbols, are FIXED above instead of declared). Each row says what the event
# was, when, and the largest deviation it can explain; it expires so it is
# re-read rather than carried.
#
# CLASSIFICATION ONLY. A declared row does NOT make a residual "explained" and
# does NOT pass the gate: ``explained`` still comes only from the operator's
# ``--known-divergence``. The audit records each out-of-tolerance name's class
# (or that it has none), whether its deviation is inside the declared bound, and
# whether the row has expired, so a residual that outgrows its event, or a name
# with no event at all, is told apart from the ones already understood.

DIVERGENCE_CLASSES = ("spin_off", "stock_dividend", "special_distribution")


@dataclass(frozen=True)
class DeclaredDivergence:
    """One name whose total-return residual an identified event explains."""

    ticker: str
    divergence_class: str  # one of DIVERGENCE_CLASSES
    event: str
    event_when: str  # ISO date, or its YYYY-MM / YYYY prefix when only that is sourced
    max_rel_dev_bound: float  # the largest residual the event explains
    evidence: str
    expires: str  # YYYY-MM-DD: re-read on or after this date

    def __post_init__(self):
        if self.divergence_class not in DIVERGENCE_CLASSES:
            raise ValueError(f"{self.ticker}: unknown class {self.divergence_class!r}")
        if not 0.0 < self.max_rel_dev_bound < 1.0:
            raise ValueError(f"{self.ticker}: bound {self.max_rel_dev_bound!r} not in (0, 1)")
        if not self.event.strip() or not self.evidence.strip():
            raise ValueError(f"{self.ticker}: event and evidence are required")
        pd.Timestamp(self.expires)
        if len(self.event_when) not in (4, 7, 10):
            raise ValueError(f"{self.ticker}: event_when {self.event_when!r} is not YYYY[-MM[-DD]]")
        pd.Timestamp(self.event_when)


_I12436 = (
    "alpha-engine-config-I12436, classification of the 2026-10-26 "
    "universe-crsp-basis-build residuals"
)
# The event is public corporate history and the build log puts the name's
# worst deviation on it; the filing itself has not been re-read for the row.
_PUBLIC = f"public corporate-event history, max deviation on the event; {_I12436}"

DECLARED_RECONCILE_DIVERGENCES: tuple[DeclaredDivergence, ...] = (
    # ── class 2: spin-offs the dividend feed carries no record of ──────────────
    DeclaredDivergence(
        "CTVA", "spin_off", "VYLR spin-off, 1 VYLR per CTVA share", "2026-10-01", 0.60,
        f"corporate_actions.declared.DECLARED_SPINOFFS (alpha-engine-config-I11806); {_I12436}",
        "2027-01-31",
    ),
    DeclaredDivergence(
        "HON", "spin_off", "Solstice Advanced Materials spin-off", "2025-10-30", 0.08,
        _PUBLIC, "2027-01-31",
    ),
    DeclaredDivergence(
        "IBM", "spin_off", "Kyndryl spin-off", "2021-11", 0.06,
        _PUBLIC, "2027-01-31",
    ),
    DeclaredDivergence(
        "LEN", "spin_off", "Millrose Properties spin-off", "2025-02", 0.05,
        _PUBLIC, "2027-01-31",
    ),
    DeclaredDivergence(
        "BDX", "spin_off",
        "Biosciences & Diagnostic Solutions separation (Waters Reverse Morris Trust)",
        "2026", 0.04,
        _PUBLIC, "2027-01-31",
    ),
    # ── class 4: a stock dividend is a share distribution, not cash ────────────
    DeclaredDivergence(
        "CBSH", "stock_dividend", "annual 5% stock dividend, each December", "2020-12", 0.40,
        f"Commerce Bancshares' annual 5% stock dividend: 1.05^6 = 1.34 over six "
        f"Decembers matches the residual; {_I12436}", "2027-01-31",
    ),
    # ── class 5: a special or merger distribution one side carries ─────────────
    DeclaredDivergence(
        "HR", "special_distribution", "special cash distribution with the HTA merger",
        "2022-07", 0.10,
        _PUBLIC, "2027-01-31",
    ),
    DeclaredDivergence(
        "PSA", "special_distribution", "special cash dividend", "2023-01", 0.15,
        _PUBLIC, "2027-01-31",
    ),
    DeclaredDivergence(
        "RRX", "special_distribution",
        "distribution recorded around the Rexnord PMC combination", "2021-10", 0.06,
        f"the build log's nearest action is a dividend-feed record at the deal; {_I12436}", "2027-01-31",
    ),
)


def _declared_by_ticker() -> dict[str, DeclaredDivergence]:
    out: dict[str, DeclaredDivergence] = {}
    for d in DECLARED_RECONCILE_DIVERGENCES:
        if d.ticker in out:
            raise ValueError(f"{d.ticker} is declared twice")
        out[d.ticker] = d
    return out


def classify_divergences(
    records: list[ReconcileRecord],
    *,
    today: str | pd.Timestamp | None = None,
    declared: tuple[DeclaredDivergence, ...] | None = None,
) -> dict:
    """Classify every out-of-tolerance / no-overlap record against the declared
    rows. Report only: nothing here changes a record's ``explained``.

    Returns ``{"classified": [...], "unclassified": [tickers],
    "exceeds_bound": [tickers], "expired": [tickers]}``. A classified row
    carries the record's ``max_rel_dev`` beside its declared bound."""
    table = (
        _declared_by_ticker() if declared is None
        else {d.ticker: d for d in declared}
    )
    now = pd.Timestamp(today).normalize() if today is not None else pd.Timestamp(
        datetime.now(timezone.utc).date()
    )
    classified: list[dict] = []
    unclassified: list[str] = []
    exceeds: list[str] = []
    expired: list[str] = []
    for r in records:
        if r.status == "within_tol":
            continue
        d = table.get(r.ticker)
        if d is None:
            unclassified.append(r.ticker)
            continue
        inside = bool(np.isfinite(r.max_rel_dev) and r.max_rel_dev <= d.max_rel_dev_bound)
        is_expired = now >= pd.Timestamp(d.expires).normalize()
        if not inside:
            exceeds.append(r.ticker)
        if is_expired:
            expired.append(r.ticker)
        classified.append({
            "ticker": r.ticker,
            "class": d.divergence_class,
            "event": d.event,
            "event_when": d.event_when,
            "max_rel_dev": r.max_rel_dev,
            "max_rel_dev_bound": d.max_rel_dev_bound,
            "within_bound": inside,
            "expired": is_expired,
            "explained": r.explained,
        })
    return {
        "classified": classified,
        "unclassified": unclassified,
        "exceeds_bound": exceeds,
        "expired": expired,
    }


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


def _put_json(s3, bucket: str, key: str, payload: dict) -> None:
    """The build's one S3 PUT site: the audit and the pass receipt, both under
    ``AUDIT_PREFIX`` (alpha-engine-config-I12684)."""
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(payload, indent=2, default=str).encode("utf-8"),
        ContentType="application/json",
    )


def _write_audit(s3, bucket: str, summary: dict) -> str:
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    key = f"{AUDIT_PREFIX}{ts}.json"
    _put_json(s3, bucket, key, summary)
    log.info("Wrote reconciliation audit to s3://%s/%s", bucket, key)
    return key


def _write_receipt(s3, bucket: str, scratch_lib: str, receipt: dict) -> None:
    key = verified_receipt_key(scratch_lib)
    _put_json(s3, bucket, key, receipt)
    log.info("Wrote %s receipt for %s to s3://%s/%s",
             receipt.get("status"), scratch_lib, bucket, key)


def migrate_universe_crsp_basis(
    *,
    bucket: str = DEFAULT_BUCKET,
    scratch_lib: str = DEFAULT_SCRATCH_LIB,
    apply: bool = False,
    tickers_override: list[str] | None = None,
    rel_tol: float = DEFAULT_RECONCILE_REL_TOL,
    known_divergence_tickers: frozenset[str] | None = None,
    reconcile_start: str | None = None,
    gate_axis: str | None = None,
    workers: int | None = None,
    raw_fetch=None,
    client=None,
    spill_dir: str | None = None,
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
    reconcile_start
        ``YYYY-MM-DD`` lower bound scoping the reconciliation GATE to the
        predictor training window (+ feature warmup buffer — see the module
        docstring). Residuals on common dates ``< reconcile_start`` are EXCLUDED
        from the pass/fail decision (deep pre-window history, best-effort basis,
        polygon coverage sparse); in-window residuals STILL fail loud. The full
        history is still WRITTEN to scratch regardless. Default ``None``:
        full-history reconciliation (UNCHANGED, conservative — operator opts in).
    spill_dir
        ``--apply`` only: where each ticker's featured frame is spilled until
        the write pass (one pickle per name, deleted once written). Default:
        ``$MIGRATE_UNIVERSE_CRSP_SPILL_DIR``, else a fresh directory under the
        CWD — deliberately NOT ``/tmp``, which is RAM-backed tmpfs on AL2023 and
        would put the frames straight back in memory.
    raw_fetch / client
        Injectable RAW-price fetcher ``(ticker) -> DataFrame`` and polygon client
        (for tests / alternate sources). Default: :func:`fetch_raw_prices`
        (polygon ``adjusted=false`` joined to un-adjusted yfinance history) on
        ``client``, else the polygon singleton.
    macro / sector_map / fundamentals / alt_data
        Feature-recompute inputs (apply path only). Loaded from S3/ArcticDB when
        not injected.

    Returns
    -------
    summary dict with the reconciliation report + write outcome.
    """
    known_divergence_tickers = known_divergence_tickers or frozenset()
    gate_axis = gate_axis or DEFAULT_GATE_AXIS
    if gate_axis not in GATE_AXES:
        raise ValueError(f"gate_axis must be one of {GATE_AXES}, got {gate_axis!r}")
    workers = workers or int(
        os.environ.get("MIGRATE_UNIVERSE_CRSP_WORKERS", str(DEFAULT_WORKERS))
    )
    if raw_fetch is None:
        # Same polygon client (and so the same locked limiter) as the
        # corporate-action fetch: the raw bars are one more call per name.
        def raw_fetch(ticker: str) -> pd.DataFrame:
            return fetch_raw_prices(ticker, client=client)

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

    own_spill_dir = False
    if apply and spill_dir is None:
        spill_dir = os.environ.get("MIGRATE_UNIVERSE_CRSP_SPILL_DIR") or None
        if spill_dir is None:
            spill_dir = tempfile.mkdtemp(prefix=".crsp_basis_spill_", dir=os.getcwd())
            own_spill_dir = True
    if apply:
        os.makedirs(spill_dir, exist_ok=True)
        log.info("Spilling featured frames to %s until the write pass", spill_dir)
    slot_of = {t: i for i, t in enumerate(targets)}

    # The curated renames, read once; a missing or malformed file fails the
    # build here, before any polygon budget is spent.
    retickers = load_known_retickers()

    # Set when any name's corporate actions could not be fetched: the build can
    # no longer pass (the audit gate needs zero errors), so the remaining names
    # are skipped instead of spending hours of polygon budget on a lost run.
    abort = threading.Event()

    def _process_one(ticker: str) -> TickerOutcome:
        if abort.is_set():
            return TickerOutcome(ticker, "aborted")
        try:
            old_close = _read_old_close(universe_lib, ticker)
            if old_close is None:
                return TickerOutcome(ticker, "no_old_close")

            try:
                raw_df = raw_fetch(ticker)
            except RawPriceFetchError as exc:
                abort.set()
                log.error(
                    "%s — STOPPING the build: no name is built on a close that is "
                    "not raw (alpha-engine-config-I12674)",
                    exc,
                )
                return TickerOutcome(ticker, "error", error=f"raw_prices: {exc}")
            if raw_df is None or raw_df.empty:
                return TickerOutcome(ticker, "fetch_empty")
            # Take the close axis's reference off the frame before any pandas
            # op sees it: a Series in ``attrs`` breaks attrs propagation
            # ("truth value of a Series is ambiguous") in the feature path.
            reference_close = (getattr(raw_df, "attrs", None) or {}).pop(
                REFERENCE_CLOSE_ATTR, None,
            )

            try:
                split_actions, dividend_actions = fetch_corporate_actions(
                    ticker, client=client, abort=abort,
                )
            except CorporateActionFetchError as exc:
                abort.set()
                log.error(
                    "%s — STOPPING the build: no name is built on corporate "
                    "actions it could not fetch (alpha-engine-config-I12436)",
                    exc,
                )
                return TickerOutcome(
                    ticker, "error", error=f"corporate_actions: {exc}",
                )

            # Dividends paid under an earlier symbol (alpha-engine-config-I12436,
            # class 1). Asked only when the history shows a rename's gap.
            window_start = pd.Timestamp(raw_df.index.min()).normalize()
            if reconcile_start is not None:
                window_start = max(window_start, pd.Timestamp(reconcile_start).normalize())
            lookup = dividend_history_gap(dividend_actions, window_start=window_start)
            try:
                dividend_actions, prior_tickers = fetch_prior_ticker_dividends(
                    ticker, dividend_actions, client=client,
                    window_start=window_start, retickers=retickers, abort=abort,
                )
            except CorporateActionFetchError as exc:
                abort.set()
                log.error(
                    "%s — STOPPING the build: no name is built on a dividend "
                    "history whose prior tickers could not be read "
                    "(alpha-engine-config-I12436)",
                    exc,
                )
                return TickerOutcome(
                    ticker, "error", error=f"corporate_actions: {exc}",
                )

            new_df, applied = reconstruct_basis(
                ticker, raw_df, split_actions, dividend_actions,
            )
            unconfirmed_splits = [
                f"{ticker} {a.human()} ({a.ex_date})"
                for a in split_actions
                if any(
                    r.get("action_id") == a.action_id and r.get("status") == "unconfirmed"
                    for r in applied
                )
            ]
            if unconfirmed_splits:
                log.warning(
                    "%s: %d feed-reported split(s) NOT applied — no "
                    "corroborating price move (config#1455): %s",
                    ticker, len(unconfirmed_splits), unconfirmed_splits,
                )

            rec = reconcile_total_return(
                ticker,
                new_df[TOTAL_RETURN_COL],
                old_close,
                split_actions=split_actions,
                dividend_actions=dividend_actions,
                rel_tol=rel_tol,
                known_divergence=ticker in known_divergence_tickers,
                reconcile_start=reconcile_start,
            )

            close_rec = reconcile_close(
                ticker,
                new_df["Close"],
                reference_close,
                split_actions=split_actions,
                rel_tol=rel_tol,
                known_divergence=ticker in known_divergence_tickers,
                reconcile_start=reconcile_start,
            )

            out = TickerOutcome(
                ticker, "ok",
                reconcile=rec,
                close_reconcile=close_rec,
                n_rows=len(new_df),
                n_splits=len(split_actions),
                n_dividends=len(dividend_actions),
                unconfirmed_splits=unconfirmed_splits,
                prior_tickers=prior_tickers,
                prior_ticker_lookup=lookup,
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
                if featured is not None and not featured.empty:
                    path = os.path.join(spill_dir, f"{slot_of[ticker]:05d}.pkl")
                    featured.to_pickle(path)
                    out.spill_path = path
                del featured
            return out
        except Exception as exc:  # noqa: BLE001 - capture per-ticker, surface in report
            return TickerOutcome(ticker, "error", error=_scrub(exc))

    try:
        summary = _run_and_report(
            targets=targets, workers=workers, process_one=_process_one,
            apply=apply, scratch=scratch, scratch_lib=scratch_lib, s3=s3,
            bucket=bucket, rel_tol=rel_tol, reconcile_start=reconcile_start,
            arctic_symbols=arctic_symbols,
            known_divergence_tickers=known_divergence_tickers,
            gate_axis=gate_axis,
        )
    finally:
        if own_spill_dir:
            shutil.rmtree(spill_dir, ignore_errors=True)
    return summary


def _run_and_report(
    *,
    targets: list[str],
    workers: int,
    process_one,
    apply: bool,
    scratch,
    scratch_lib: str,
    s3,
    bucket: str,
    rel_tol: float,
    reconcile_start: str | None,
    arctic_symbols: list[str],
    known_divergence_tickers: frozenset[str],
    gate_axis: str = DEFAULT_GATE_AXIS,
) -> dict:
    """Run every target, gate, write the scratch library only on a pass, emit the
    audit (and on a pass the receipt), fail loud."""
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=workers) as pool:
        outcomes = list(pool.map(process_one, targets))
    elapsed = time.time() - t0

    # ── partition outcomes ────────────────────────────────────────────────────
    records: list[ReconcileRecord] = []  # total-return axis
    close_records: list[ReconcileRecord] = []
    errors: list[dict] = []
    no_old_close: list[str] = []
    fetch_empty: list[str] = []
    unconfirmed_splits: list[str] = []
    aborted: list[str] = []
    prior_ticker_dividends: list[dict] = []
    prior_ticker_lookups = 0
    written = 0

    for o in outcomes:
        if o.outcome == "ok" and o.reconcile is not None:
            records.append(o.reconcile)
            if o.close_reconcile is not None:
                close_records.append(o.close_reconcile)
            else:  # an outcome built without its close check is never passed
                close_records.append(ReconcileRecord(
                    ticker=o.ticker, status="no_overlap", n_common_dates=0,
                    max_rel_dev=float("inf"), max_dev_date=None, explained=False,
                    explanation="CLOSE axis: not reconciled (fail-loud, not skipped)",
                ))
        elif o.outcome == "no_old_close":
            no_old_close.append(o.ticker)
        elif o.outcome == "fetch_empty":
            fetch_empty.append(o.ticker)
        elif o.outcome == "aborted":
            aborted.append(o.ticker)
        elif o.outcome == "error":
            errors.append({"ticker": o.ticker, "error": o.error})
            log.error("CRSP migration error for %s: %s", o.ticker, o.error)
        if o.unconfirmed_splits:
            unconfirmed_splits.extend(o.unconfirmed_splits)
        if o.prior_ticker_lookup:
            prior_ticker_lookups += 1
        if o.prior_tickers:
            prior_ticker_dividends.append({"ticker": o.ticker, "prior": o.prior_tickers})

    tr_records = records
    tr_unexplained = [r for r in tr_records if not r.explained]
    close_unexplained = [r for r in close_records if not r.explained]
    # The GATE axis's records drive pass/fail, the scratch write, the receipt
    # and every top-level count below (alpha-engine-config-I12436).
    records = close_records if gate_axis == "close" else tr_records
    within_tol = [r for r in records if r.status == "within_tol"]
    out_of_tol = [r for r in records if r.status == "out_of_tol"]
    no_overlap = [r for r in records if r.status == "no_overlap"]
    # FAIL-LOUD set: every out-of-tol or no-overlap record not explained.
    unexplained = [r for r in records if not r.explained]
    # Report-only classification of the total-return residuals (I12436).
    divergence_classes = classify_divergences(tr_records)
    total_return_axis_trusted = not tr_unexplained

    # ── WRITE scratch (apply) — only when the reconcile gate PASSED ───────────
    # alpha-engine-config-I12684: this pass used to run before the gate, so the
    # 2026-10-26 build that failed with 46 unexplained names still wrote all 909
    # into `universe_crsp`, and the library a rebuild reads became exactly the
    # failed build's output. A build that is going to raise below writes NOTHING
    # to the scratch library; it still writes its audit, and still removes every
    # spill file.
    passed_gate = not unexplained and not errors and not aborted
    newest_session: str | None = None
    if apply and passed_gate:
        # Invalidate the previous receipt BEFORE the library changes: a build
        # killed mid-write must leave a receipt that refuses, not one vouching
        # for a library it no longer describes.
        _write_receipt(s3, bucket, scratch_lib, {
            "status": RECEIPT_WRITING,
            "scratch_lib": scratch_lib,
            "started_at": datetime.now(timezone.utc).isoformat(),
        })
        # One frame in memory at a time: read the spill, write, delete it.
        for o in outcomes:
            if o.outcome == "ok" and o.spill_path is not None:
                try:
                    frame = pd.read_pickle(o.spill_path)
                    if len(frame.index):
                        last = pd.Timestamp(pd.DatetimeIndex(frame.index).max()).date().isoformat()
                        newest_session = max(newest_session or last, last)
                    scratch.write(o.ticker, to_arctic_canonical(frame))
                    del frame
                    o.written = True
                    written += 1
                except Exception as exc:  # noqa: BLE001
                    errors.append({"ticker": o.ticker, "stage": "scratch_write",
                                   "error": str(exc)})
                    log.error("Scratch write failed for %s: %s", o.ticker, exc)
    elif apply:
        log.error(
            "Reconcile gate FAILED (unexplained=%d errors=%d aborted=%d): writing "
            "NOTHING to scratch library %s (alpha-engine-config-I12684)",
            len(unexplained), len(errors), len(aborted), scratch_lib,
        )
    # Every spill file goes, on every path (a written one is already consumed).
    for o in outcomes:
        if o.spill_path is not None:
            try:
                os.remove(o.spill_path)
            except OSError:
                pass

    # BOTH axes log every out-of-tolerance name; an unexplained one at ERROR,
    # whether or not its axis gates (the non-gating axis still fails loudly).
    for axis, axis_records in (("close", close_records), ("total_return", tr_records)):
        for r in axis_records:
            if r.status == "within_tol":
                continue
            lvl = log.warning if r.explained else log.error
            lvl("RECONCILE[%s%s] %s status=%s max_rel_dev=%.4f date=%s — %s",
                axis, " gate" if axis == gate_axis else "", r.ticker, r.status,
                r.max_rel_dev, r.max_dev_date, r.explanation)

    # Reconcile-window scope summary: make the gate scope EXPLICIT in the audit —
    # the start used, aggregate in-window vs excluded date counts, and a flag that
    # pre-window rows are written best-effort / unreconciled (no silent loosening).
    total_in_window = sum(r.n_common_in_window for r in records)
    total_excluded = sum(r.n_common_excluded for r in records)
    tickers_with_excluded = sum(1 for r in records if r.n_common_excluded > 0)

    def _axis_report(axis_records: list[ReconcileRecord]) -> dict:
        return {
            "reconciled_count": len(axis_records),
            "within_tol_count": sum(r.status == "within_tol" for r in axis_records),
            "out_of_tol_count": sum(r.status == "out_of_tol" for r in axis_records),
            "no_overlap_count": sum(r.status == "no_overlap" for r in axis_records),
            "unexplained_count": sum(not r.explained for r in axis_records),
            "unexplained": [r.to_dict() for r in axis_records if not r.explained],
        }

    summary = {
        "status": "ok" if (not unexplained and not errors and not aborted) else "fail",
        # alpha-engine-config-I12436 ("Gate on close"): the axis whose records
        # the top-level counts, ``unexplained`` and ``status`` describe.
        "gate_axis": gate_axis,
        "total_return_axis_trusted": total_return_axis_trusted,
        "close_unexplained_count": len(close_unexplained),
        "tr_unexplained_count": len(tr_unexplained),
        "axes": {
            "close": _axis_report(close_records),
            "total_return": _axis_report(tr_records),
        },
        "applied": apply,
        "scratch_lib": scratch_lib,
        "rel_tol": rel_tol,
        "reconcile_start": reconcile_start,
        "reconcile_scope": (
            "full_history" if reconcile_start is None
            else f"gated >= {reconcile_start} (pre-window rows written best-effort/unreconciled)"
        ),
        "in_window_dates_total": total_in_window,
        "excluded_pre_window_dates_total": total_excluded,
        "tickers_with_excluded_pre_window": tickers_with_excluded,
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
        "aborted_count": len(aborted),
        "unconfirmed_splits_count": len(unconfirmed_splits),
        # Names whose dividend history showed a rename's gap, so polygon was
        # asked for their prior tickers; and what each prior symbol added
        # (alpha-engine-config-I12436).
        "prior_ticker_lookups_count": prior_ticker_lookups,
        "prior_ticker_dividends_count": sum(
            p["dividends_added"] for d in prior_ticker_dividends for p in d["prior"]
        ),
        "prior_ticker_dividends": prior_ticker_dividends,
        "written_count": written,
        "elapsed_seconds": round(elapsed, 1),
        "workers": workers,
        "reconciliations": [r.to_dict() for r in records],
        "unexplained": [r.to_dict() for r in unexplained],
        "no_old_close": no_old_close,
        "fetch_empty": fetch_empty,
        # Feed-reported splits corporate_actions.apply declined to apply (no
        # corroborating price move — config#1455). NOT applied to Close/
        # total_return_close for these tickers, so an out-of-tol residual on
        # one of these names is diagnosed as exactly this, not a generic
        # "missing/doubled/mis-ratio'd" guess. Still counted in the ticker's
        # n_splits; does NOT by itself fail the run (the reconciliation gate
        # is the actual pass/fail signal — a name whose feed-reported split
        # was correctly withheld may still reconcile within_tol).
        "unconfirmed_splits": unconfirmed_splits,
        "errors": errors,
        # Names skipped after another name's corporate actions could not be
        # fetched (the build was already lost; alpha-engine-config-I12436).
        "aborted": aborted,
        "known_divergence_tickers": sorted(known_divergence_tickers),
        # Each out-of-tolerance name's declared event class, or none
        # (DECLARED_RECONCILE_DIVERGENCES). Report only: it never passes the
        # gate; ``unexplained_count`` above is unchanged by it.
        "divergence_classes": divergence_classes,
        "classified_out_of_tol_count": len(divergence_classes["classified"]),
        "unclassified_out_of_tol_count": len(divergence_classes["unclassified"]),
    }

    # Persist the report BEFORE raising so the evidence survives a fail-loud.
    audit_key = _write_audit(s3, bucket, summary)

    # The pass receipt: only a build that passed the gate AND wrote every name.
    # A scratch-write error above turns the status to "fail", so the receipt
    # stays at "writing" and the library is refused until a clean rebuild.
    if apply and summary["status"] == "ok":
        _write_receipt(s3, bucket, scratch_lib, {
            "status": RECEIPT_VERIFIED,
            "scratch_lib": scratch_lib,
            "audit_key": audit_key,
            "verified_at": datetime.now(timezone.utc).isoformat(),
            "gate_axis": gate_axis,
            # A consumer of total_return_close (or the features on it) must
            # refuse this library while this is false (I12436).
            "total_return_axis_trusted": total_return_axis_trusted,
            "tr_unexplained_count": len(tr_unexplained),
            "close_unexplained_count": len(close_unexplained),
            "unexplained_count": len(unexplained),
            "errors_count": len(errors),
            "aborted_count": len(aborted),
            "written_count": written,
            "targets_count": len(targets),
            "live_universe_size": len(arctic_symbols),
            "full_universe": len(targets) == len(arctic_symbols),
            "reconcile_start": reconcile_start,
            "newest_session": newest_session,
        })

    log.info(
        "migrate_universe_crsp_basis: applied=%s reconcile_start=%s targets=%d "
        "reconciled=%d within_tol=%d out_of_tol=%d no_overlap=%d unexplained=%d "
        "errors=%d aborted=%d unconfirmed_splits=%d written=%d in_window_dates=%d "
        "excluded_pre_window_dates=%d elapsed=%.1fs",
        apply, reconcile_start, len(targets), len(records), len(within_tol),
        len(out_of_tol), len(no_overlap), len(unexplained), len(errors), len(aborted),
        len(unconfirmed_splits), written, total_in_window, total_excluded, elapsed,
    )

    if tr_unexplained and gate_axis != "total_return":
        log.error(
            "TOTAL-RETURN AXIS UNTRUSTED: %d name(s) carry an unexplained "
            "total-return residual (gate_axis=%s; first 20: %s). total_return_close "
            "and the features on it are NOT trusted; the receipt says "
            "total_return_axis_trusted=false (alpha-engine-config-I12436)",
            len(tr_unexplained), gate_axis, [r.ticker for r in tr_unexplained[:20]],
        )

    # ── FAIL LOUD ─────────────────────────────────────────────────────────────
    # No silent skip, no "unscoreable" sentinel (Brian standing feedback): an
    # unexplained out-of-tol / no-overlap residual means a missing/doubled
    # action or a real basis divergence reached the reconstruction — it MUST
    # halt the build before the scratch basis is trusted for the shadow retrain.
    if unexplained:
        raise RuntimeError(
            f"CRSP reconciliation FAILED LOUD: {len(unexplained)} ticker(s) "
            f"carry an unexplained out-of-tolerance residual on the "
            f"{gate_axis} axis (rel_tol={rel_tol:.2%}). "
            f"Each is a missing/doubled/mis-ratio'd split or dividend, an "
            f"un-reconcilable ticker, or a real polygon-vs-yfinance divergence — "
            f"localize and fix the action (or acknowledge via "
            f"--known-divergence) before trusting the scratch basis. First 20: "
            f"{[r.to_dict() for r in unexplained[:20]]}"
        )
    if errors or aborted:
        raise RuntimeError(
            f"CRSP migration had {len(errors)} per-ticker error(s) and "
            f"{len(aborted)} name(s) skipped after the build was stopped — see "
            f"audit. First 20 errors: {errors[:20]}"
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
        "--reconcile-start",
        help="YYYY-MM-DD lower bound scoping the reconciliation GATE to the "
             "predictor training window. Residuals on common dates BEFORE this "
             "are EXCLUDED from the pass/fail gate (deep pre-window history, "
             "best-effort, where polygon corporate-action coverage is sparse); "
             "in-window residuals STILL fail loud and the full history is still "
             "WRITTEN to scratch. CRITICAL: set this to the predictor "
             "TRAIN_START_DATE MINUS the max feature lookback (~252 trading days "
             "≈ 1 calendar year; e.g. mom_12_1 uses close.shift(252)) — NOT "
             "train_start itself — so warmup features at the first training date "
             "are computed on reconciled data. Default unset: full-history "
             "reconciliation (UNCHANGED; operator opts in to scoping).",
    )
    parser.add_argument(
        "--gate-axis", choices=GATE_AXES, default=DEFAULT_GATE_AXIS,
        help="Which reconcile axis GATES the build (default: close, the split-only "
             "Close the EDGAR rebuild reads; alpha-engine-config-I12436). The other "
             "axis is still reconciled, logged at ERROR and recorded in the receipt.",
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
        reconcile_start=args.reconcile_start,
        gate_axis=args.gate_axis,
    )
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
