"""corporate_actions._split_math — polygon-authoritative split factor math.

Moved here from the top-level ``split_factor.py`` shim (corporate-actions
program PR6, config#1433): the split-restatement math is part of the unified
corporate-actions model, not a stray top-level module. It is RE-EXPORTED from
``corporate_actions`` (``from corporate_actions import cumulative_factor,
restate_series_for_splits, split_events``) so consumers depend on the package,
not on a loose module name. No behavior changed in the move — the factor
convention is byte-for-byte the one validated on the DD 2026-06-24 reverse
split.

WHY THIS EXISTS (data#1298):
    The ArcticDB universe library (predictor TRAINING input) is append-only +
    windowed-reconciled. When a ticker splits, yfinance back-adjusts its ENTIRE
    price history (so ``predictor/price_cache/`` — the INFERENCE store — stays
    split-clean), but ArcticDB only ever got a recent N-day window patched. A
    split restates the FULL adjusted history, so a windowed patch leaves a
    split-boundary discontinuity (verified on DD 2026-06-24: +201% / -66% /
    +200% artificial jumps in a live S&P constituent's return series). That
    corrupts every feature computed across the boundary and — via cross-sectional
    rank normalization — perturbs the whole cohort's ranks on those dates.

    The ROOT-CAUSE fix is a full-history RESTATEMENT on detection: when a split
    is detected, back-adjust EVERY price strictly before the split's execution
    date by the cumulative split factor, so the full series materialized for the
    ArcticDB write is continuous and on ONE adjusted scale (train == serve).

WHY POLYGON, NOT yfinance:
    yfinance ``auto_adjust`` LAGS a fresh split — on the DD split it had adjusted
    only 6/18+ (×3) and left <=6/17 on the old scale for a day or two. So
    yfinance cannot be trusted to restate a *fresh* split. Polygon's
    ``/v3/reference/splits`` endpoint carries the exact effective date + ratio
    the day the split lands and is the authoritative factor + validator.

CONVENTION:
    A forward N-for-1 split (``split_from=1, split_to=N``) divides the adjusted
    price by N for every date BEFORE the execution date. A reverse 1-for-N split
    (``split_from=N, split_to=1``) multiplies by N. The per-event multiplicative
    factor applied to dates strictly before ``execution_date`` is therefore
    ``split_from / split_to``. Multiple splits compound multiplicatively.
"""

from __future__ import annotations

import logging

import pandas as pd

log = logging.getLogger(__name__)

# A factor whose distance from 1.0 is below this is treated as "no split"
# (absorbs feed float noise; real splits are integer ratios >= 2:1).
_FACTOR_NOOP_TOL = 1e-9


def split_events(ticker: str, client=None) -> list[dict]:
    """Return polygon's split events for ``ticker`` (ascending by date).

    Each event is ``{"execution_date": "YYYY-MM-DD", "split_from": int,
    "split_to": int}``. ``client`` defaults to the polygon singleton; pass an
    explicit (or fake) client in tests to avoid a live API call.
    """
    if client is None:
        from polygon_client import polygon_client

        client = polygon_client()
    return client.get_splits(ticker)


def cumulative_factor(
    events: list[dict],
    on_or_before_date,
    *,
    reference_date=None,
) -> float:
    """Cumulative multiplicative price factor to put ``on_or_before_date`` on
    the ``reference_date`` adjusted scale.

    A split with ``execution_date`` E applies factor ``split_from/split_to`` to
    every price on dates strictly before E. ``cumulative_factor`` multiplies the
    factors of every split whose execution date falls in
    ``(on_or_before_date, reference_date]`` — i.e. every split that occurred
    AFTER the given date but on/before the reference (default: the latest event
    date, i.e. "the current scale"). Prices already on or after the most recent
    split return ``1.0``.

    This is the per-row factor used to back-adjust a price series so its whole
    history sits on one continuous adjusted scale.
    """
    d = pd.Timestamp(on_or_before_date).normalize()
    factor = 1.0
    for ev in events:
        e = pd.Timestamp(ev["execution_date"]).normalize()
        if reference_date is not None and e > pd.Timestamp(reference_date).normalize():
            continue
        if e > d:
            factor *= ev["split_from"] / ev["split_to"]
    return factor


def restate_series_for_splits(
    df: pd.DataFrame,
    events: list[dict],
    *,
    price_cols=("Open", "High", "Low", "Close", "VWAP", "Adj_Close"),
    volume_cols=("Volume",),
) -> pd.DataFrame:
    """Back-adjust a price DataFrame so its full history is split-consistent.

    For every split event, every row STRICTLY BEFORE the execution date has its
    price columns multiplied by ``split_from/split_to`` and its volume columns
    divided by the same factor (share count moves inversely to price). Rows on
    or after the latest split are left untouched — they already define the
    current adjusted scale. Returns a new frame (input is not mutated); a frame
    with no in-range splits is returned unchanged (no copy).

    This puts the ENTIRE series on one adjusted scale, eliminating the
    split-boundary discontinuity that corrupts cross-boundary training features
    (data#1298). Idempotent on an already-restated series only insofar as the
    caller restates from a raw/un-restated source — it always applies the FULL
    cumulative factor, so it must be fed the source series, not a previously
    restated one (mirrors yfinance's full back-adjust + the price_cache heal).
    """
    if df.empty or not events:
        return df

    idx = df.index
    if not isinstance(idx, pd.DatetimeIndex):
        idx = pd.to_datetime(idx)

    # Per-row cumulative factor relative to the latest (current) scale.
    factors = pd.Series(
        [cumulative_factor(events, ts) for ts in idx],
        index=df.index,
        dtype="float64",
    )

    if (factors.sub(1.0).abs() <= _FACTOR_NOOP_TOL).all():
        # No row needs restating (all splits predate the series, or none in range).
        return df

    out = df.copy()
    for col in price_cols:
        if col in out.columns:
            out[col] = out[col] * factors
    for col in volume_cols:
        if col in out.columns:
            # Volume scales inversely; keep integer-ish volumes round.
            out[col] = (out[col] / factors).round()
    log.info(
        "Restated %d row(s) across %d split event(s) — full-history "
        "split-consistent (data#1298)",
        int((factors.sub(1.0).abs() > _FACTOR_NOOP_TOL).sum()),
        len(events),
    )
    return out
