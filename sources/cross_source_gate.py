"""sources/cross_source_gate.py — L1 independent cross-source agreement gate.

Part of the market-value integrity framework (alpha-engine-config#1277, Phase 2 /
Layer-1). Phase 1 (config#1276) proved L0 (settled-only store) + L3 (T+1
re-reconcile) on the EOD SPY artifact. THIS module is the next self-contained
slice: an **independent cross-source agreement gate at ingestion**.

Principle (from #1277): *no number a human acts on should be trusted unless it is
agreed by ≥2 independent sources*. A single-source "golden" close is not
institutional — config#1276 showed a silently-wrong single-source SPY close drove
a bad EOD number. So at settled-close ingestion we require ≥2 independent vendors
(e.g. Polygon settled daily aggregate primary + yfinance check) to agree on the
close within a small tolerance (~5-10 bps). On agreement we record the value with
a provenance tag (which sources, the agreement bps); on disagreement beyond
tolerance we **quarantine** the value (flag it provisional, emit a discrepancy
record) rather than silently picking one source.

Design boundaries (deliberate, smallest-slice):

  * This is the GATE LOGIC + a thin two-source fetch helper — pure, deterministic,
    and network-free at its core so it is fully unit-testable on fixture prices.
    It does NOT yet rewire ``collectors.daily_closes.collect`` to route every
    ticker through the gate (that is the wider Phase-2 wiring); it provides the
    institutional primitive the wiring will call, sited in the ingestion/reconcile
    seam (``sources/``) — NOT at each downstream read site.
  * Fail-soft: if one source is unavailable the gate does not crash ingestion; it
    records a *single-source-provisional* decision (flagged), so downstream NAV/PnL
    can see "this is not cross-checked" rather than getting nothing or a hard error.

Relationship to existing infra: ``collectors.daily_closes._log_close_discrepancies``
already *logs* polygon-vs-prior drift when polygon overwrites a yfinance cell, but
it only logs — it neither tags accepted values with cross-source provenance nor
quarantines a disagreement. This module is the missing gate that turns that
observation into an institutional accept/quarantine decision with provenance.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

logger = logging.getLogger(__name__)

# Default agreement tolerance. #1277 / L1 calls for ~5-10 bps; 1 bp = 0.0001.
# Configurable per call (``tolerance_bps=``) and centrally overridable here.
DEFAULT_TOLERANCE_BPS: float = 7.5


class GateStatus(str, Enum):
    """Outcome of the cross-source agreement gate for one settled close."""

    AGREED = "agreed"                                  # >=2 sources within tolerance — accept + provenance
    QUARANTINED = "quarantined"                        # >=2 sources DISAGREE beyond tolerance — flag, do not pick one
    SINGLE_SOURCE_PROVISIONAL = "single_source_provisional"  # only 1 source available — accept-but-flagged
    NO_DATA = "no_data"                                # zero usable sources — nothing to record


# Statuses whose value is safe to publish into a settled artifact unflagged.
# Only AGREED clears the gate cleanly; everything else carries a flag so
# downstream NAV/PnL knows the number is not cross-checked.
_CLEAN_STATUSES = frozenset({GateStatus.AGREED})


@dataclass(frozen=True)
class SourceClose:
    """One independent source's settled close for a single (ticker, date).

    ``price`` is None when the source was queried but returned no usable settled
    value (missing bar, NaN, vendor outage); such entries are treated as "source
    unavailable" by the gate, not as a zero price.
    """

    source: str
    price: Optional[float]

    @property
    def usable(self) -> bool:
        return self.price is not None and self.price > 0


@dataclass(frozen=True)
class GateDecision:
    """Result of the L1 gate: the accepted value (if any) + provenance + flag.

    ``flagged`` is True for everything that is not a clean cross-source agreement,
    so a caller can persist ``provisional=flagged`` exactly as the config#1276
    Phase-1 provisional flag does. ``provenance`` is a compact, human-readable
    audit tag suitable for L4 provenance surfacing
    (e.g. ``"polygon=734.30 yfinance=734.32 agree@0.27bps"``).
    """

    ticker: str
    date: str
    status: GateStatus
    value: Optional[float]                 # the close to record; None for NO_DATA / QUARANTINED
    sources_used: tuple[str, ...]          # the sources that contributed a usable price
    agreement_bps: Optional[float]         # observed max pairwise spread in bps (None if <2 usable)
    tolerance_bps: float
    provenance: str                        # compact audit string for L4 surfacing
    discrepancy: Optional[dict] = field(default=None)  # populated on QUARANTINE for the discrepancy lake

    @property
    def flagged(self) -> bool:
        """True unless this is a clean >=2-source agreement (provisional otherwise)."""
        return self.status not in _CLEAN_STATUSES

    @property
    def accepted(self) -> bool:
        """True when a value is safe to record (clean OR single-source-provisional)."""
        return self.value is not None


def _pairwise_max_bps(prices: list[float]) -> float:
    """Max pairwise spread across ``prices`` in basis points.

    Spread is relative to the smaller price of each pair (a conservative
    denominator — it makes the bps slightly larger, so the gate errs toward
    flagging rather than toward silently trusting). With 2 sources this is just
    the single pair. Caller guarantees len(prices) >= 2 and all > 0.
    """
    worst = 0.0
    for i in range(len(prices)):
        for j in range(i + 1, len(prices)):
            hi, lo = max(prices[i], prices[j]), min(prices[i], prices[j])
            bps = (hi - lo) / lo * 10_000.0
            worst = max(worst, bps)
    return worst


def evaluate(
    ticker: str,
    date: str,
    closes: list[SourceClose],
    *,
    tolerance_bps: float = DEFAULT_TOLERANCE_BPS,
) -> GateDecision:
    """Run the L1 cross-source agreement gate on already-fetched settled closes.

    PURE + network-free — this is the institutional decision logic. Callers fetch
    each source's close (see :func:`gate_settled_close` for a registry-driven
    two-source fetch helper) and hand the results here.

    Semantics:

      * >=2 usable sources AGREE within ``tolerance_bps``  -> AGREED, value = mean,
        ``flagged=False``, provenance lists every source + the observed bps.
      * >=2 usable sources DISAGREE beyond tolerance        -> QUARANTINED,
        ``value=None`` (we refuse to silently pick one), a ``discrepancy`` record
        is attached for the discrepancy lake, ``flagged=True``.
      * exactly 1 usable source                             -> SINGLE_SOURCE_PROVISIONAL,
        value = that source's price, ``flagged=True`` (fail-soft: ingestion is not
        blocked, but the number is marked not-cross-checked).
      * 0 usable sources                                    -> NO_DATA, ``value=None``.

    The mean (not "primary wins") is used on agreement because within tolerance the
    sources are, by construction, indistinguishable to ~<1 bp of NAV impact and the
    mean is the lower-variance estimator; provenance still records both raw values.
    """
    usable = [c for c in closes if c.usable]
    sources_used = tuple(c.source for c in usable)

    if not usable:
        return GateDecision(
            ticker=ticker, date=date, status=GateStatus.NO_DATA, value=None,
            sources_used=(), agreement_bps=None, tolerance_bps=tolerance_bps,
            provenance=f"{ticker}@{date}: no usable source",
        )

    if len(usable) == 1:
        only = usable[0]
        logger.warning(
            "L1 single-source-provisional %s @ %s: only %s available (price=%.4f) "
            "— recorded FLAGGED (not cross-checked); downstream must treat as provisional",
            ticker, date, only.source, only.price,
        )
        return GateDecision(
            ticker=ticker, date=date, status=GateStatus.SINGLE_SOURCE_PROVISIONAL,
            value=only.price, sources_used=sources_used, agreement_bps=None,
            tolerance_bps=tolerance_bps,
            provenance=(
                f"{ticker}@{date}: {only.source}={only.price:.4f} "
                f"single-source PROVISIONAL (no cross-check)"
            ),
        )

    prices = [c.price for c in usable]  # all > 0 by ``usable``
    spread_bps = _pairwise_max_bps(prices)
    detail = " ".join(f"{c.source}={c.price:.4f}" for c in usable)

    if spread_bps <= tolerance_bps:
        value = sum(prices) / len(prices)
        logger.info(
            "L1 cross-source AGREED %s @ %s: %s agree@%.2fbps (tol=%.1fbps) value=%.4f",
            ticker, date, detail, spread_bps, tolerance_bps, value,
        )
        return GateDecision(
            ticker=ticker, date=date, status=GateStatus.AGREED, value=value,
            sources_used=sources_used, agreement_bps=spread_bps,
            tolerance_bps=tolerance_bps,
            provenance=f"{ticker}@{date}: {detail} agree@{spread_bps:.2f}bps",
        )

    # Disagreement beyond tolerance — QUARANTINE. We deliberately do NOT pick a
    # source; emitting None forces the caller to flag/hold rather than build NAV
    # on an unreconciled single-source outlier (the config#1276 failure class).
    discrepancy = {
        "ticker": ticker,
        "date": date,
        "spread_bps": round(spread_bps, 4),
        "tolerance_bps": tolerance_bps,
        "sources": {c.source: c.price for c in usable},
    }
    logger.error(
        "L1 cross-source QUARANTINE %s @ %s: %s disagree@%.2fbps > tol=%.1fbps "
        "— value withheld (NOT recorded), discrepancy emitted; investigate before "
        "downstream NAV/PnL consumes this date",
        ticker, date, detail, spread_bps, tolerance_bps,
    )
    return GateDecision(
        ticker=ticker, date=date, status=GateStatus.QUARANTINED, value=None,
        sources_used=sources_used, agreement_bps=spread_bps,
        tolerance_bps=tolerance_bps,
        provenance=f"{ticker}@{date}: {detail} DISAGREE@{spread_bps:.2f}bps QUARANTINED",
        discrepancy=discrepancy,
    )


def gate_settled_close(
    ticker: str,
    date: str,
    *,
    primary_source: str = "polygon",
    check_source: str = "yfinance",
    tolerance_bps: float = DEFAULT_TOLERANCE_BPS,
) -> GateDecision:
    """Fetch ``ticker``'s settled close from two registered adapters and gate it.

    Thin, registry-driven wrapper over :func:`evaluate` for live use at the
    ingestion seam. The two sources and the tolerance are configurable; both
    default to the #1277 L1 recommendation (Polygon primary + yfinance check,
    ~5-10 bps).

    FAIL-SOFT: each source fetch is independently guarded — a vendor outage,
    auth failure, or empty bar degrades that source to "unavailable" (price=None)
    rather than raising, so one broken vendor yields a single-source-provisional
    decision instead of crashing ingestion. (Network-touching; the pure
    :func:`evaluate` is what the unit tests exercise on fixtures.)
    """
    from .registry import get_adapter

    def _one(source_name: str) -> SourceClose:
        try:
            adapter = get_adapter(source_name)
            bars = adapter.fetch_ohlcv([ticker], date, strict=False)
            for bar in bars:
                if bar.ticker.lstrip("^") == ticker.lstrip("^") and bar.close:
                    return SourceClose(source=source_name, price=float(bar.close))
            return SourceClose(source=source_name, price=None)
        except Exception as exc:  # fail-soft: never let one vendor crash ingestion
            logger.warning(
                "L1 gate: source %s unavailable for %s @ %s (%s) — degrading to "
                "single-source path",
                source_name, ticker, date, exc,
            )
            return SourceClose(source=source_name, price=None)

    closes = [_one(primary_source), _one(check_source)]
    return evaluate(ticker, date, closes, tolerance_bps=tolerance_bps)
