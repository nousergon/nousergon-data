"""Unit tests for the L1 cross-source agreement gate (config#1277, Phase 2 / L1).

Fixture prices only — no live network. Locks the institutional invariants:

  * >=2 sources AGREE within tolerance      -> accepted + provenance, unflagged.
  * >=2 sources DISAGREE beyond tolerance   -> quarantined, value withheld, flagged.
  * exactly 1 source available              -> single-source-provisional, flagged.
  * 0 sources                               -> no_data.
  * tolerance is configurable (bps).
  * the live two-source helper fail-soft when one vendor raises.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest

from sources.cross_source_gate import (
    DEFAULT_TOLERANCE_BPS,
    GateStatus,
    SourceClose,
    evaluate,
    gate_settled_close,
)


# ----------------------------------------------------------------------------
# Agreement: two sources within tolerance -> accepted + provenance, unflagged.
# ----------------------------------------------------------------------------
def test_sources_agree_accepted_with_provenance():
    # 734.30 vs 734.32 -> spread ~0.27 bps, well within default 7.5 bps.
    closes = [
        SourceClose("polygon", 734.30),
        SourceClose("yfinance", 734.32),
    ]
    d = evaluate("SPY", "2026-06-25", closes)

    assert d.status is GateStatus.AGREED
    assert d.flagged is False
    assert d.accepted is True
    # mean of the two agreeing sources
    assert d.value == pytest.approx((734.30 + 734.32) / 2)
    assert set(d.sources_used) == {"polygon", "yfinance"}
    assert d.agreement_bps is not None and d.agreement_bps < DEFAULT_TOLERANCE_BPS
    assert "polygon=734.30" in d.provenance
    assert "yfinance=734.32" in d.provenance
    assert "agree@" in d.provenance


def test_exact_match_is_zero_bps_agreement():
    d = evaluate("AAPL", "2026-06-25", [
        SourceClose("polygon", 200.0),
        SourceClose("yfinance", 200.0),
    ])
    assert d.status is GateStatus.AGREED
    assert d.agreement_bps == pytest.approx(0.0)
    assert d.value == pytest.approx(200.0)


# ----------------------------------------------------------------------------
# Disagreement beyond tolerance -> quarantined, value withheld, flagged.
# ----------------------------------------------------------------------------
def test_sources_disagree_quarantined():
    # 100.00 vs 105.00 -> ~500 bps, far beyond tolerance.
    d = evaluate("XYZ", "2026-06-25", [
        SourceClose("polygon", 100.00),
        SourceClose("yfinance", 105.00),
    ])
    assert d.status is GateStatus.QUARANTINED
    assert d.flagged is True
    assert d.accepted is False
    # we MUST NOT silently pick one source
    assert d.value is None
    # discrepancy record emitted for the discrepancy lake
    assert d.discrepancy is not None
    assert d.discrepancy["ticker"] == "XYZ"
    assert d.discrepancy["sources"] == {"polygon": 100.00, "yfinance": 105.00}
    assert d.discrepancy["spread_bps"] > d.tolerance_bps
    assert "DISAGREE" in d.provenance and "QUARANTINED" in d.provenance


def test_just_over_tolerance_quarantines():
    # 8 bps spread with default 7.5 bps tolerance -> quarantine.
    base = 100.0
    other = base * (1 + 8e-4)  # +8 bps
    d = evaluate("T", "2026-06-25", [
        SourceClose("polygon", base),
        SourceClose("yfinance", other),
    ])
    assert d.status is GateStatus.QUARANTINED
    assert d.value is None


def test_just_under_tolerance_agrees():
    # 7 bps spread with default 7.5 bps tolerance -> agree.
    base = 100.0
    other = base * (1 + 7e-4)  # +7 bps
    d = evaluate("T", "2026-06-25", [
        SourceClose("polygon", base),
        SourceClose("yfinance", other),
    ])
    assert d.status is GateStatus.AGREED
    assert d.value == pytest.approx((base + other) / 2)


# ----------------------------------------------------------------------------
# Single source available -> single-source-provisional, flagged, value kept.
# ----------------------------------------------------------------------------
def test_one_source_missing_single_source_provisional():
    d = evaluate("SPY", "2026-06-25", [
        SourceClose("polygon", 734.30),
        SourceClose("yfinance", None),       # vendor unavailable
    ])
    assert d.status is GateStatus.SINGLE_SOURCE_PROVISIONAL
    assert d.flagged is True
    assert d.accepted is True                # fail-soft: value retained
    assert d.value == pytest.approx(734.30)
    assert d.sources_used == ("polygon",)
    assert d.agreement_bps is None
    assert "PROVISIONAL" in d.provenance


def test_nonpositive_price_treated_as_unavailable():
    # A 0/negative price is NOT a usable source (don't trust a 0 close).
    d = evaluate("SPY", "2026-06-25", [
        SourceClose("polygon", 734.30),
        SourceClose("yfinance", 0.0),
    ])
    assert d.status is GateStatus.SINGLE_SOURCE_PROVISIONAL
    assert d.value == pytest.approx(734.30)


# ----------------------------------------------------------------------------
# No usable source -> no_data.
# ----------------------------------------------------------------------------
def test_no_sources_no_data():
    d = evaluate("SPY", "2026-06-25", [
        SourceClose("polygon", None),
        SourceClose("yfinance", None),
    ])
    assert d.status is GateStatus.NO_DATA
    assert d.value is None
    assert d.accepted is False
    assert d.flagged is True


# ----------------------------------------------------------------------------
# Tolerance is configurable.
# ----------------------------------------------------------------------------
def test_tolerance_is_configurable():
    closes = [SourceClose("polygon", 100.0), SourceClose("yfinance", 100.3)]  # 30 bps
    # tight 10 bps tolerance -> quarantine
    assert evaluate("X", "d", closes, tolerance_bps=10).status is GateStatus.QUARANTINED
    # loose 50 bps tolerance -> agree
    assert evaluate("X", "d", closes, tolerance_bps=50).status is GateStatus.AGREED


def test_three_sources_use_worst_pairwise_spread():
    # Two tight + one outlier: worst pair must drive the quarantine decision.
    d = evaluate("X", "d", [
        SourceClose("polygon", 100.00),
        SourceClose("yfinance", 100.01),
        SourceClose("tiingo", 102.00),
    ], tolerance_bps=10)
    assert d.status is GateStatus.QUARANTINED
    assert len(d.sources_used) == 3


# ----------------------------------------------------------------------------
# Live two-source helper: fail-soft when a vendor raises.
# ----------------------------------------------------------------------------
def test_gate_settled_close_failsoft_on_vendor_error():
    class _Bar:
        def __init__(self, ticker, close):
            self.ticker = ticker
            self.close = close

    class _GoodAdapter:
        def fetch_ohlcv(self, tickers, run_date, *, strict=False):
            return [_Bar("SPY", 734.30)]

    class _BrokenAdapter:
        def fetch_ohlcv(self, tickers, run_date, *, strict=False):
            raise RuntimeError("vendor 503")

    def _fake_get_adapter(name):
        return _GoodAdapter() if name == "polygon" else _BrokenAdapter()

    with patch("sources.registry.get_adapter", side_effect=_fake_get_adapter):
        d = gate_settled_close("SPY", "2026-06-25")

    # broken check source -> single-source-provisional, ingestion not crashed
    assert d.status is GateStatus.SINGLE_SOURCE_PROVISIONAL
    assert d.value == pytest.approx(734.30)
    assert d.flagged is True


def test_gate_settled_close_agrees_via_helper():
    class _Bar:
        def __init__(self, ticker, close):
            self.ticker = ticker
            self.close = close

    class _Adapter:
        def __init__(self, px):
            self.px = px

        def fetch_ohlcv(self, tickers, run_date, *, strict=False):
            return [_Bar("SPY", self.px)]

    def _fake_get_adapter(name):
        return _Adapter(734.30) if name == "polygon" else _Adapter(734.33)

    with patch("sources.registry.get_adapter", side_effect=_fake_get_adapter):
        d = gate_settled_close("SPY", "2026-06-25", tolerance_bps=10)

    assert d.status is GateStatus.AGREED
    assert d.value == pytest.approx((734.30 + 734.33) / 2)
