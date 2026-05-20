"""Tests for collectors/fundamentals.py — Finnhub /stock/metric migration.

Covers:
  * Field mapping from Finnhub schema to the existing 8-field output.
  * TTM-preferred / annual-fallback logic per field.
  * FCF yield computation from raw FCF + market cap.
  * NEUTRAL fallback for missing tickers + malformed payloads.
  * NEUTRAL fallback when both required FCF inputs are missing.
  * ok_ratio hard-fail gate (silent-zeros guard).
  * S3 write only on dry_run=False.
"""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from collectors import fundamentals
from collectors.fundamentals import (
    NEUTRAL,
    _clip,
    _fetch_single_ticker,
    _pick,
    _safe_float,
)


# ── Helpers ─────────────────────────────────────────────────────────────────


def _aapl_payload(**overrides):
    """A representative Finnhub /stock/metric response for AAPL."""
    metric = {
        "peTTM": 28.5,
        "peExclExtraTTM": 27.9,
        "pbAnnual": 35.2,
        "pbQuarterly": 36.1,
        "totalDebt/totalEquityAnnual": 1.95,
        "totalDebt/totalEquityQuarterly": 2.01,
        "revenueGrowthTTMYoy": 0.025,  # 2.5%
        "revenueGrowthQuarterlyYoy": 0.018,
        "freeCashFlowTTM": 100_000_000_000.0,  # $100B
        "marketCapitalization": 3_000_000_000_000.0,  # $3T → 3.33% yield
        "grossMarginTTM": 0.42,
        "grossMarginAnnual": 0.41,
        "roeTTM": 0.62,  # 62% — extreme but exists
        "roeRfy": 0.61,
        "currentRatioAnnual": 0.93,
        "currentRatioQuarterly": 0.95,
        # Phase 3a of attractiveness-pillars-260520 — Growth + Stewardship
        # pillar substrate added to NEUTRAL + _fetch_single_ticker.
        "revenueGrowth3Y": 0.08,           # 8% 3y CAGR
        "epsGrowth3Y": 0.12,                # 12% 3y EPS CAGR
        "payoutRatioTTM": 0.18,             # 18% of NI paid as dividends
        "dividendYieldIndicatedAnnual": 0.005,  # 0.5%
        "capitalSpendingGrowth5Y": 0.10,    # 10% CAPEX growth
    }
    metric.update(overrides)
    return {"metric": metric, "metricType": "all", "symbol": "AAPL"}


# ── _safe_float, _clip, _pick ───────────────────────────────────────────────


class TestSafeFloat:
    def test_none_returns_default(self):
        assert _safe_float(None) == 0.0
        assert _safe_float(None, default=1.0) == 1.0

    def test_string_number_parses(self):
        assert _safe_float("3.14") == 3.14

    def test_invalid_returns_default(self):
        assert _safe_float("nope") == 0.0
        assert _safe_float("nope", default=-1.0) == -1.0


class TestClip:
    def test_clip_high(self):
        assert _clip(5.0, -1.0, 1.0) == 1.0

    def test_clip_low(self):
        assert _clip(-5.0, -1.0, 1.0) == -1.0

    def test_clip_inside(self):
        assert _clip(0.5, -1.0, 1.0) == 0.5


class TestPick:
    def test_first_present_wins(self):
        assert _pick({"a": 1.0, "b": 2.0}, "a", "b") == 1.0

    def test_falls_through_to_next(self):
        assert _pick({"b": 2.0}, "a", "b") == 2.0

    def test_skips_none_values(self):
        assert _pick({"a": None, "b": 2.0}, "a", "b") == 2.0

    def test_returns_default_when_all_missing(self):
        assert _pick({}, "a", "b", default=99.0) == 99.0


# ── _fetch_single_ticker — field mapping ────────────────────────────────────


class TestFetchSingleTicker:
    def test_full_payload_maps_all_fields(self):
        with patch.object(fundamentals, "finnhub_get", return_value=_aapl_payload()):
            data = _fetch_single_ticker("AAPL")
        # Each field is non-zero / non-NEUTRAL.
        assert data["pe_ratio"] != 0.0
        assert data["pb_ratio"] != 0.0
        assert data["debt_to_equity"] != 0.0
        assert data["revenue_growth_yoy"] != 0.0
        assert data["fcf_yield"] != 0.0
        assert data["gross_margin"] != 0.0
        assert data["roe"] != 0.0
        assert data["current_ratio"] != 0.0

    def test_pe_uses_ttm_preferred(self):
        with patch.object(fundamentals, "finnhub_get", return_value=_aapl_payload(peTTM=30.0, peExclExtraTTM=999.0)):
            data = _fetch_single_ticker("AAPL")
        # 30.0 / 30 = 1.0
        assert data["pe_ratio"] == pytest.approx(1.0)

    def test_pe_falls_back_to_excl_extra(self):
        payload = _aapl_payload()
        payload["metric"]["peTTM"] = None
        payload["metric"]["peExclExtraTTM"] = 30.0
        with patch.object(fundamentals, "finnhub_get", return_value=payload):
            data = _fetch_single_ticker("AAPL")
        assert data["pe_ratio"] == pytest.approx(1.0)

    def test_debt_equity_handles_slashed_keyname(self):
        # Finnhub uses literal slash in the field name. Verify _pick handles it.
        payload = _aapl_payload()
        payload["metric"]["totalDebt/totalEquityAnnual"] = 4.0
        with patch.object(fundamentals, "finnhub_get", return_value=payload):
            data = _fetch_single_ticker("AAPL")
        # 4.0 / 2 = 2.0
        assert data["debt_to_equity"] == pytest.approx(2.0)

    def test_fcf_yield_computed_from_raw(self):
        payload = _aapl_payload(freeCashFlowTTM=10.0, marketCapitalization=200.0)
        with patch.object(fundamentals, "finnhub_get", return_value=payload):
            data = _fetch_single_ticker("AAPL")
        # 10/200 = 0.05
        assert data["fcf_yield"] == pytest.approx(0.05)

    def test_fcf_yield_neutral_when_market_cap_zero(self):
        payload = _aapl_payload(freeCashFlowTTM=10.0, marketCapitalization=0.0)
        with patch.object(fundamentals, "finnhub_get", return_value=payload):
            data = _fetch_single_ticker("AAPL")
        assert data["fcf_yield"] == 0.0

    def test_fcf_yield_neutral_when_fcf_missing(self):
        payload = _aapl_payload()
        payload["metric"]["freeCashFlowTTM"] = None
        payload["metric"]["freeCashFlowAnnual"] = None
        with patch.object(fundamentals, "finnhub_get", return_value=payload):
            data = _fetch_single_ticker("AAPL")
        assert data["fcf_yield"] == 0.0

    def test_clipping_extremes(self):
        # Insanely high P/E should clip to 3.0 (pe_raw / 30 capped at 3)
        payload = _aapl_payload(peTTM=10000.0)
        with patch.object(fundamentals, "finnhub_get", return_value=payload):
            data = _fetch_single_ticker("AAPL")
        assert data["pe_ratio"] == 3.0

    def test_empty_payload_returns_neutral(self):
        with patch.object(fundamentals, "finnhub_get", return_value={}):
            data = _fetch_single_ticker("UNKNOWN")
        assert data == NEUTRAL

    def test_payload_with_empty_metric_returns_neutral(self):
        with patch.object(fundamentals, "finnhub_get", return_value={"metric": {}}):
            data = _fetch_single_ticker("UNKNOWN")
        assert data == NEUTRAL

    def test_payload_with_null_metric_returns_neutral(self):
        with patch.object(fundamentals, "finnhub_get", return_value={"metric": None}):
            data = _fetch_single_ticker("UNKNOWN")
        assert data == NEUTRAL


# ── Phase 3a of attractiveness-pillars-260520 — Growth + Stewardship pillar
# substrate fields. Five new fundamental fields surfaced from existing Finnhub
# metric=all response; no new API integrations.


class TestPillarSubstrateFields:
    """Growth + Stewardship pillar quant substrate added in Phase 3a."""

    def test_neutral_includes_all_five_new_fields(self):
        """NEUTRAL must enumerate the 5 new fields (so an empty / malformed
        Finnhub payload still produces a complete fundamental record)."""
        for field in (
            "revenue_growth_3y",
            "eps_growth_3y",
            "payout_ratio",
            "dividend_yield",
            "capex_growth_5y",
        ):
            assert field in NEUTRAL, f"NEUTRAL missing {field}"
            assert NEUTRAL[field] == 0.0

    def test_full_payload_maps_growth_3y_fields(self):
        with patch.object(fundamentals, "finnhub_get", return_value=_aapl_payload()):
            data = _fetch_single_ticker("AAPL")
        # _aapl_payload sets revenueGrowth3Y=0.08, epsGrowth3Y=0.12 → clip
        # ranges (-0.5,1.5) and (-1.0,2.0) preserve the values.
        assert data["revenue_growth_3y"] == pytest.approx(0.08)
        assert data["eps_growth_3y"] == pytest.approx(0.12)

    def test_full_payload_maps_stewardship_fields(self):
        with patch.object(fundamentals, "finnhub_get", return_value=_aapl_payload()):
            data = _fetch_single_ticker("AAPL")
        assert data["payout_ratio"] == pytest.approx(0.18)
        assert data["dividend_yield"] == pytest.approx(0.005)
        assert data["capex_growth_5y"] == pytest.approx(0.10)

    def test_revenue_growth_3y_falls_back_to_5y(self):
        """3y CAGR is preferred; 5y is the fallback for newer listings."""
        payload = _aapl_payload()
        payload["metric"]["revenueGrowth3Y"] = None
        payload["metric"]["revenueGrowth5Y"] = 0.05
        with patch.object(fundamentals, "finnhub_get", return_value=payload):
            data = _fetch_single_ticker("AAPL")
        assert data["revenue_growth_3y"] == pytest.approx(0.05)

    def test_eps_growth_3y_falls_back_through_annual_5y(self):
        payload = _aapl_payload()
        payload["metric"]["epsGrowth3Y"] = None
        payload["metric"]["epsBasicExclExtraItemsAnnual5Y"] = 0.07
        with patch.object(fundamentals, "finnhub_get", return_value=payload):
            data = _fetch_single_ticker("AAPL")
        assert data["eps_growth_3y"] == pytest.approx(0.07)

    def test_payout_ratio_falls_back_to_annual(self):
        payload = _aapl_payload()
        payload["metric"]["payoutRatioTTM"] = None
        payload["metric"]["payoutRatioAnnual"] = 0.25
        with patch.object(fundamentals, "finnhub_get", return_value=payload):
            data = _fetch_single_ticker("AAPL")
        assert data["payout_ratio"] == pytest.approx(0.25)

    def test_dividend_yield_falls_back_to_ttm(self):
        payload = _aapl_payload()
        payload["metric"]["dividendYieldIndicatedAnnual"] = None
        payload["metric"]["currentDividendYieldTTM"] = 0.012
        with patch.object(fundamentals, "finnhub_get", return_value=payload):
            data = _fetch_single_ticker("AAPL")
        assert data["dividend_yield"] == pytest.approx(0.012)

    def test_clipping_extreme_growth_caps_at_upper_bound(self):
        """A 500% YoY growth (e.g. post-spinoff base-effect) clips at the
        upper bound. revenue_growth_3y cap is 1.5; eps_growth_3y cap is 2.0."""
        payload = _aapl_payload(revenueGrowth3Y=5.0, epsGrowth3Y=10.0)
        with patch.object(fundamentals, "finnhub_get", return_value=payload):
            data = _fetch_single_ticker("AAPL")
        assert data["revenue_growth_3y"] == 1.5
        assert data["eps_growth_3y"] == 2.0

    def test_clipping_extreme_dividend_yield_caps_at_20pct(self):
        """A 50% indicated yield (data error / micro-cap special dividend)
        clips at 0.20 — the prior-realistic real-world ceiling for sustainable
        dividend yields."""
        payload = _aapl_payload(dividendYieldIndicatedAnnual=0.50)
        with patch.object(fundamentals, "finnhub_get", return_value=payload):
            data = _fetch_single_ticker("AAPL")
        assert data["dividend_yield"] == 0.20

    def test_payout_ratio_clipped_above_2(self):
        """payout_ratio > 2.0 (paying out 2x earnings — possible briefly in
        a loss year as legacy dividend, but unsustainable) clips at 2.0."""
        payload = _aapl_payload(payoutRatioTTM=5.0)
        with patch.object(fundamentals, "finnhub_get", return_value=payload):
            data = _fetch_single_ticker("AAPL")
        assert data["payout_ratio"] == 2.0

    def test_empty_payload_still_returns_complete_neutral(self):
        """Same coverage as the existing test, but explicitly verifies the
        new fields are also zeroed (regression guard against future NEUTRAL
        drift dropping them)."""
        with patch.object(fundamentals, "finnhub_get", return_value={}):
            data = _fetch_single_ticker("UNKNOWN")
        assert data["revenue_growth_3y"] == 0.0
        assert data["eps_growth_3y"] == 0.0
        assert data["payout_ratio"] == 0.0
        assert data["dividend_yield"] == 0.0
        assert data["capex_growth_5y"] == 0.0

    def test_list_response_returns_neutral(self):
        # Finnhub typically returns dict; defensive: list response = malformed.
        with patch.object(fundamentals, "finnhub_get", return_value=[]):
            data = _fetch_single_ticker("UNKNOWN")
        assert data == NEUTRAL


# ── collect() — full collection flow ────────────────────────────────────────


class TestCollect:
    def test_dry_run_does_not_write_to_s3(self, monkeypatch):
        monkeypatch.setenv("FINNHUB_API_KEY", "test-key")
        with patch.object(fundamentals, "_fetch_single_ticker", return_value={"pe_ratio": 1.0, **{k: NEUTRAL[k] for k in NEUTRAL if k != "pe_ratio"}}):
            with patch("boto3.client") as mock_boto:
                result = fundamentals.collect(
                    bucket="test", tickers=["AAPL", "MSFT"], run_date="2026-04-25", dry_run=True,
                )
        assert result["status"] == "ok"
        assert result.get("dry_run") is True
        mock_boto.assert_not_called()

    def test_missing_api_key_returns_error(self, monkeypatch):
        monkeypatch.delenv("FINNHUB_API_KEY", raising=False)
        result = fundamentals.collect(
            bucket="test", tickers=["AAPL"], run_date="2026-04-25", dry_run=True,
        )
        assert result["status"] == "error"
        assert "FINNHUB_API_KEY" in result["error"]

    def test_low_ok_ratio_hard_fails(self, monkeypatch):
        """Most tickers returning NEUTRAL → status=error (silent-zeros guard)."""
        monkeypatch.setenv("FINNHUB_API_KEY", "test-key")
        # Only 1 of 10 returns real data → 10% < 90% threshold
        side_effect = [{"pe_ratio": 1.0, **{k: NEUTRAL[k] for k in NEUTRAL if k != "pe_ratio"}}] + [NEUTRAL.copy() for _ in range(9)]
        with patch.object(fundamentals, "_fetch_single_ticker", side_effect=side_effect):
            result = fundamentals.collect(
                bucket="test", tickers=[f"T{i}" for i in range(10)],
                run_date="2026-04-25", dry_run=True,
            )
        assert result["status"] == "error"
        assert "below" in result["error"].lower() or "threshold" in result["error"].lower()

    def test_high_ok_ratio_passes(self, monkeypatch):
        monkeypatch.setenv("FINNHUB_API_KEY", "test-key")
        # 10 of 10 return real data
        with patch.object(
            fundamentals, "_fetch_single_ticker",
            return_value={"pe_ratio": 1.0, **{k: NEUTRAL[k] for k in NEUTRAL if k != "pe_ratio"}}
        ):
            result = fundamentals.collect(
                bucket="test", tickers=[f"T{i}" for i in range(10)],
                run_date="2026-04-25", dry_run=True,
            )
        assert result["status"] == "ok"
        assert result["n_ok"] == 10

    def test_per_ticker_exception_falls_through_to_neutral(self, monkeypatch):
        """A single ticker raising shouldn't kill the whole collection — log + NEUTRAL."""
        monkeypatch.setenv("FINNHUB_API_KEY", "test-key")
        # 9 OK + 1 raises = 90% ok ratio (right at threshold; should still pass)
        good = {"pe_ratio": 1.0, **{k: NEUTRAL[k] for k in NEUTRAL if k != "pe_ratio"}}
        side_effect = [good] * 9 + [Exception("network blip")]
        with patch.object(fundamentals, "_fetch_single_ticker", side_effect=side_effect):
            result = fundamentals.collect(
                bucket="test", tickers=[f"T{i}" for i in range(10)],
                run_date="2026-04-25", dry_run=True,
            )
        # 90% ok ratio passes the 90% threshold (>= test in collect.py)
        assert result["status"] == "ok"
        assert result["n_ok"] == 9
        assert result["n_errors"] == 1


# ── Write-time value-range gate (ROADMAP L1243, extends #215) ────────────────


class TestFundamentalsValueRangeGate:
    """fundamentals.py writes a feature-source snapshot bypassing
    builders/daily_append.py's validate_today_row. This gate runs
    validate_feature_record over each per-ticker dict before the S3
    write. NaN/inf + negative-where-nonneg block (→ NEUTRAL); a gross
    outlier (defeated _clip) warns. Mirrors #215's split + env loader."""

    def _good(self):
        return {"pe_ratio": 1.0, **{k: NEUTRAL[k] for k in NEUTRAL if k != "pe_ratio"}}

    def test_nan_field_is_blocked_and_neutralized(self, monkeypatch):
        monkeypatch.setenv("FINNHUB_API_KEY", "test-key")
        bad = dict(self._good())
        bad["roe"] = float("nan")
        side_effect = [bad] + [self._good() for _ in range(9)]
        with patch.object(fundamentals, "_fetch_single_ticker", side_effect=side_effect):
            result = fundamentals.collect(
                bucket="test", tickers=[f"T{i}" for i in range(10)],
                run_date="2026-04-25", dry_run=True,
            )
        assert result["tickers_quality_blocked"] == 1
        assert result["quality_anomaly_counts"].get("nan_or_inf") == 1
        # Blocked ticker counted as an error (NEUTRAL'd, not written).
        assert result["n_errors"] == 1

    def test_negative_margin_is_blocked(self, monkeypatch):
        monkeypatch.setenv("FINNHUB_API_KEY", "test-key")
        bad = dict(self._good())
        bad["gross_margin"] = -0.2  # declared non-negative
        side_effect = [bad] + [self._good() for _ in range(9)]
        with patch.object(fundamentals, "_fetch_single_ticker", side_effect=side_effect):
            result = fundamentals.collect(
                bucket="test", tickers=[f"T{i}" for i in range(10)],
                run_date="2026-04-25", dry_run=True,
            )
        assert result["tickers_quality_blocked"] == 1
        assert result["quality_anomaly_counts"].get("negative_where_nonneg") == 1

    def test_gross_outlier_warns_not_blocks(self, monkeypatch):
        monkeypatch.setenv("FINNHUB_API_KEY", "test-key")
        bad = dict(self._good())
        bad["roe"] = 9.9  # well above the hi=1.0 band (a defeated _clip)
        side_effect = [bad] + [self._good() for _ in range(9)]
        with patch.object(fundamentals, "_fetch_single_ticker", side_effect=side_effect):
            result = fundamentals.collect(
                bucket="test", tickers=[f"T{i}" for i in range(10)],
                run_date="2026-04-25", dry_run=True,
            )
        assert result["tickers_quality_blocked"] == 0
        assert result["tickers_quality_warned"] == 1
        assert result["quality_anomaly_counts"].get("gross_outlier") == 1
        assert result["status"] == "ok"

    def test_clean_run_no_quality_anomalies(self, monkeypatch):
        monkeypatch.setenv("FINNHUB_API_KEY", "test-key")
        with patch.object(fundamentals, "_fetch_single_ticker", return_value=self._good()):
            result = fundamentals.collect(
                bucket="test", tickers=[f"T{i}" for i in range(5)],
                run_date="2026-04-25", dry_run=True,
            )
        assert result["tickers_quality_blocked"] == 0
        assert result["tickers_quality_warned"] == 0
        assert result["quality_anomaly_counts"] == {}

    def test_quality_fields_present_in_all_return_paths(self, monkeypatch):
        monkeypatch.setenv("FINNHUB_API_KEY", "test-key")
        with patch.object(fundamentals, "_fetch_single_ticker", return_value=self._good()):
            result = fundamentals.collect(
                bucket="test", tickers=["AAPL"], run_date="2026-04-25", dry_run=True,
            )
        for k in (
            "tickers_quality_blocked", "tickers_quality_warned",
            "quality_anomaly_counts", "quality_block_anomaly_types",
        ):
            assert k in result

    def test_malformed_block_env_raises(self, monkeypatch):
        monkeypatch.setenv("FINNHUB_API_KEY", "test-key")
        monkeypatch.setenv("FUNDAMENTALS_BLOCK_ANOMALY_TYPES", "not-json")
        with patch.object(fundamentals, "_fetch_single_ticker", return_value=self._good()):
            with pytest.raises(RuntimeError, match="not valid JSON"):
                fundamentals.collect(
                    bucket="test", tickers=["AAPL"],
                    run_date="2026-04-25", dry_run=True,
                )

    def test_unknown_block_type_in_env_raises(self, monkeypatch):
        monkeypatch.setenv("FINNHUB_API_KEY", "test-key")
        monkeypatch.setenv("FUNDAMENTALS_BLOCK_ANOMALY_TYPES", '["bogus_type"]')
        with patch.object(fundamentals, "_fetch_single_ticker", return_value=self._good()):
            with pytest.raises(RuntimeError, match="unknown anomaly types"):
                fundamentals.collect(
                    bucket="test", tickers=["AAPL"],
                    run_date="2026-04-25", dry_run=True,
                )

    def test_env_can_promote_gross_outlier_to_block(self, monkeypatch):
        monkeypatch.setenv("FINNHUB_API_KEY", "test-key")
        monkeypatch.setenv(
            "FUNDAMENTALS_BLOCK_ANOMALY_TYPES",
            '["nan_or_inf", "negative_where_nonneg", "gross_outlier"]',
        )
        bad = dict(self._good())
        bad["roe"] = 9.9
        side_effect = [bad] + [self._good() for _ in range(9)]
        with patch.object(fundamentals, "_fetch_single_ticker", side_effect=side_effect):
            result = fundamentals.collect(
                bucket="test", tickers=[f"T{i}" for i in range(10)],
                run_date="2026-04-25", dry_run=True,
            )
        assert result["tickers_quality_blocked"] == 1
        assert result["quality_anomaly_counts"].get("gross_outlier") == 1
