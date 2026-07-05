"""
Tests for DataPostflight (2026-04-17).

The postflight encodes the union of downstream consumer contracts:
  1. Predictor _verify_arctic_fresh (SPY freshness)
  2. Research MacroFetchError (macro.json shape)
  3. Research PriceFetchError (constituents.json shape, latest_weekly.json)
  4. Research preflight (universe sample staleness)

Each check should raise PostflightError with a specific named message.
These tests mock the S3 + ArcticDB layers to exercise each failure mode
deterministically.
"""

from __future__ import annotations

import io
import json
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from validators.postflight import DataPostflight, PostflightError


RUN_DATE = "2026-04-18"  # Saturday
PRIOR_TRADING_DAY = "2026-04-17"  # Friday
BUCKET = "test-bucket"
MARKET_PREFIX = "market_data/"


@pytest.fixture(autouse=True)
def _block_real_email(monkeypatch):
    """Hard guarantee no test in this module emits a real completion email.

    Background: on 2026-04-22 a test with a hardcoded "polygon 503" fixture
    and dry_run=False was run locally with EMAIL_SENDER/EMAIL_RECIPIENTS/
    GMAIL_APP_PASSWORD in the shell env. _finalize() unconditionally called
    send_step_email and a real "Alpha Engine Data Phase 1 | 2026-04-18 |
    FAILED" email landed in the operator's inbox. Patching inside each test
    is error-prone; this autouse fixture makes the safety net module-wide.
    """
    from unittest.mock import MagicMock
    monkeypatch.setattr(
        "weekly_collector.send_step_email",
        MagicMock(return_value=True),
        raising=False,
    )
    monkeypatch.setattr("emailer.send_step_email", MagicMock(return_value=True))


def _make_postflight(phase: int = 1) -> DataPostflight:
    return DataPostflight(
        bucket=BUCKET,
        run_date=RUN_DATE,
        market_prefix=MARKET_PREFIX,
        phase=phase,
    )


def _series_ending_at(last_date: str, n: int = 30) -> pd.DataFrame:
    """Build an ArcticDB-shaped DataFrame with a Close column ending on ``last_date``."""
    end = pd.Timestamp(last_date)
    dates = pd.date_range(end=end, periods=n, freq="B")
    return pd.DataFrame({"Close": [100.0] * len(dates)}, index=dates)


# ── Latest pointer check ─────────────────────────────────────────────────────

class TestLatestWeeklyPointer:
    def test_ok_when_date_matches(self):
        pf = _make_postflight()
        pf._s3 = MagicMock()
        pf._s3.get_object.return_value = {
            "Body": MagicMock(read=MagicMock(return_value=json.dumps({
                "date": RUN_DATE,
                "s3_prefix": f"{MARKET_PREFIX}weekly/{RUN_DATE}/",
            }).encode()))
        }
        pf._check_latest_weekly_pointer()  # no raise

    def test_fails_when_pointer_stale(self):
        pf = _make_postflight()
        pf._s3 = MagicMock()
        pf._s3.get_object.return_value = {
            "Body": MagicMock(read=MagicMock(return_value=json.dumps({
                "date": "2026-04-11",  # last week
                "s3_prefix": f"{MARKET_PREFIX}weekly/2026-04-11/",
            }).encode()))
        }
        with pytest.raises(PostflightError, match="Pointer did not roll forward"):
            pf._check_latest_weekly_pointer()

    def test_fails_on_s3_miss(self):
        pf = _make_postflight()
        pf._s3 = MagicMock()
        pf._s3.get_object.side_effect = RuntimeError("NoSuchKey")
        with pytest.raises(PostflightError, match="latest_weekly.json did not write"):
            pf._check_latest_weekly_pointer()


# ── macro.json shape check ────────────────────────────────────────────────────

class TestMacroJson:
    def test_ok_with_fed_funds_rate(self):
        pf = _make_postflight()
        pf._s3 = MagicMock()
        pf._s3.get_object.return_value = {
            "Body": MagicMock(read=MagicMock(return_value=json.dumps({
                "fed_funds_rate": 5.25,
                "vix": 16.5,
            }).encode()))
        }
        pf._check_macro_json_contract()

    def test_fails_when_fed_funds_missing(self):
        pf = _make_postflight()
        pf._s3 = MagicMock()
        pf._s3.get_object.return_value = {
            "Body": MagicMock(read=MagicMock(return_value=json.dumps({
                "vix": 16.5,  # fed_funds_rate absent
            }).encode()))
        }
        with pytest.raises(PostflightError, match="missing 'fed_funds_rate'"):
            pf._check_macro_json_contract()

    def test_fails_when_fed_funds_null(self):
        pf = _make_postflight()
        pf._s3 = MagicMock()
        pf._s3.get_object.return_value = {
            "Body": MagicMock(read=MagicMock(return_value=json.dumps({
                "fed_funds_rate": None,
            }).encode()))
        }
        with pytest.raises(PostflightError, match="missing 'fed_funds_rate'"):
            pf._check_macro_json_contract()


# ── constituents.json shape check ─────────────────────────────────────────────

class TestConstituentsJson:
    def test_ok_with_900_tickers(self):
        pf = _make_postflight()
        pf._s3 = MagicMock()
        tickers = [f"T{i:03d}" for i in range(900)]
        pf._s3.get_object.return_value = {
            "Body": MagicMock(read=MagicMock(return_value=json.dumps({
                "tickers": tickers,
                "sector_map": {t: "Tech" for t in tickers},
            }).encode()))
        }
        pf._check_constituents_json_contract()

    def test_fails_below_800_tickers(self):
        pf = _make_postflight()
        pf._s3 = MagicMock()
        pf._s3.get_object.return_value = {
            "Body": MagicMock(read=MagicMock(return_value=json.dumps({
                "tickers": [f"T{i}" for i in range(500)],
                "sector_map": {},
            }).encode()))
        }
        with pytest.raises(PostflightError, match="expected ≥ 800"):
            pf._check_constituents_json_contract()

    def test_fails_without_sector_map(self):
        pf = _make_postflight()
        pf._s3 = MagicMock()
        tickers = [f"T{i:03d}" for i in range(900)]
        pf._s3.get_object.return_value = {
            "Body": MagicMock(read=MagicMock(return_value=json.dumps({
                "tickers": tickers,
                # sector_map missing
            }).encode()))
        }
        with pytest.raises(PostflightError, match="missing 'sector_map'"):
            pf._check_constituents_json_contract()


# ── short_interest.json shape check ───────────────────────────────────────────

class TestShortInterestJson:
    def test_ok_with_well_populated_payload(self):
        pf = _make_postflight()
        pf._s3 = MagicMock()
        pf._s3.head_object.return_value = {}  # exists
        pf._s3.get_object.return_value = {
            "Body": MagicMock(read=MagicMock(return_value=json.dumps({
                "ticker_count": 900,
                "ok_count": 700,  # 78% populated
                "data": {f"T{i}": {"short_pct_float": 5.0} for i in range(900)},
            }).encode()))
        }
        pf._check_short_interest_json_contract()

    def test_fails_below_50pct_populated(self):
        pf = _make_postflight()
        pf._s3 = MagicMock()
        pf._s3.head_object.return_value = {}
        pf._s3.get_object.return_value = {
            "Body": MagicMock(read=MagicMock(return_value=json.dumps({
                "ticker_count": 900,
                "ok_count": 300,  # 33% populated
                "data": {},
            }).encode()))
        }
        with pytest.raises(PostflightError, match="yfinance outage suspected"):
            pf._check_short_interest_json_contract()

    def test_fails_when_data_dict_missing(self):
        pf = _make_postflight()
        pf._s3 = MagicMock()
        pf._s3.head_object.return_value = {}
        pf._s3.get_object.return_value = {
            "Body": MagicMock(read=MagicMock(return_value=json.dumps({
                "ticker_count": 900,
                "ok_count": 700,
                # data dict missing
            }).encode()))
        }
        with pytest.raises(PostflightError, match="missing 'data' dict"):
            pf._check_short_interest_json_contract()

    def test_absent_file_skips_check(self):
        """Soft-launch path: collector disabled → file missing → skip."""
        pf = _make_postflight()
        pf._s3 = MagicMock()
        pf._s3.head_object.side_effect = RuntimeError("NoSuchKey")
        # No raise — should log + skip
        pf._check_short_interest_json_contract()
        # get_object should never be called when head_object fails
        pf._s3.get_object.assert_not_called()


# ── ArcticDB macro.SPY freshness ──────────────────────────────────────────────

class TestMacroSpyFresh:
    def test_ok_when_spy_last_row_is_prior_day(self):
        pf = _make_postflight()
        macro_lib = MagicMock()
        macro_lib.read.return_value.data = _series_ending_at(PRIOR_TRADING_DAY)
        pf._universe_lib = MagicMock()
        pf._macro_lib = macro_lib
        pf._check_macro_spy_fresh()

    def test_fails_when_spy_too_stale(self):
        pf = _make_postflight()
        macro_lib = MagicMock()
        # 2026-04-10 = Fri; run_date 4/18 (Sat) expects 4/17 (Fri) → 5 trading days behind
        macro_lib.read.return_value.data = _series_ending_at("2026-04-10")
        pf._universe_lib = MagicMock()
        pf._macro_lib = macro_lib
        with pytest.raises(PostflightError, match="trading-day.*behind the expected last close"):
            pf._check_macro_spy_fresh()

    def test_fails_when_spy_empty(self):
        pf = _make_postflight()
        macro_lib = MagicMock()
        macro_lib.read.return_value.data = pd.DataFrame()
        pf._universe_lib = MagicMock()
        pf._macro_lib = macro_lib
        with pytest.raises(PostflightError, match="zero rows"):
            pf._check_macro_spy_fresh()

    def test_ok_on_sunday_redrive_with_friday_macro(self):
        """2026-05-24 incident regression: Sunday redrive of a Saturday SF
        where macro.SPY carries Friday's close. Friday → Sunday is +2 calendar
        days but 0 trading days; the migrated gate must pass via
        ``nousergon_lib.dates.is_fresh_in_trading_days``.
        """
        pf = _make_postflight()
        pf.run_date = "2026-05-24"  # Sunday
        macro_lib = MagicMock()
        macro_lib.read.return_value.data = _series_ending_at("2026-05-22")  # Friday
        pf._universe_lib = MagicMock()
        pf._macro_lib = macro_lib
        pf._check_macro_spy_fresh()  # must not raise

    def test_ok_on_memorial_day_monday_with_friday_macro(self):
        """Memorial Day Monday is an NYSE holiday — Friday is still the most
        recent close. Holiday-aware via the lib's NYSE calendar.
        """
        pf = _make_postflight()
        pf.run_date = "2026-05-25"  # Memorial Day Monday
        macro_lib = MagicMock()
        macro_lib.read.return_value.data = _series_ending_at("2026-05-22")
        pf._universe_lib = MagicMock()
        pf._macro_lib = macro_lib
        pf._check_macro_spy_fresh()  # must not raise


# ── ArcticDB universe sample ──────────────────────────────────────────────────

class TestUniverseSample:
    def test_ok_when_all_sampled_fresh(self):
        pf = _make_postflight()
        macro_lib = MagicMock()
        macro_lib.read.return_value.data = _series_ending_at(PRIOR_TRADING_DAY)

        universe_lib = MagicMock()
        # 900 stock symbols to sample from
        universe_lib.list_symbols.return_value = [f"T{i:03d}" for i in range(900)]
        # All reads return a frame ending on the prior trading day
        universe_lib.read.return_value.data = _series_ending_at(PRIOR_TRADING_DAY)

        pf._universe_lib = universe_lib
        pf._macro_lib = macro_lib
        pf._check_universe_sample_fresh()

    def test_fails_when_any_sampled_ticker_stale(self):
        pf = _make_postflight()
        macro_lib = MagicMock()
        macro_lib.read.return_value.data = _series_ending_at(PRIOR_TRADING_DAY)

        universe_lib = MagicMock()
        universe_lib.list_symbols.return_value = [f"T{i:03d}" for i in range(900)]

        # First 5 reads return a stale frame; the rest are fresh.
        fresh = _series_ending_at(PRIOR_TRADING_DAY)
        stale = _series_ending_at("2026-04-10")  # 7 days behind
        universe_lib.read.side_effect = [
            MagicMock(data=stale) for _ in range(5)
        ] + [
            MagicMock(data=fresh) for _ in range(95)
        ]

        pf._universe_lib = universe_lib
        pf._macro_lib = macro_lib
        with pytest.raises(PostflightError, match="stale vs SPY"):
            pf._check_universe_sample_fresh()

    def test_fails_when_universe_too_small(self):
        pf = _make_postflight()
        macro_lib = MagicMock()
        macro_lib.read.return_value.data = _series_ending_at(PRIOR_TRADING_DAY)

        universe_lib = MagicMock()
        # Only 10 non-macro symbols — below sample size
        universe_lib.list_symbols.return_value = [f"T{i:03d}" for i in range(10)]

        pf._universe_lib = universe_lib
        pf._macro_lib = macro_lib
        with pytest.raises(PostflightError, match="has only 10 non-macro symbols"):
            pf._check_universe_sample_fresh()


# ── Phase gating ──────────────────────────────────────────────────────────────

class TestPhaseGating:
    def test_phase3_or_higher_is_skipped(self):
        """Unknown phase values short-circuit. Phase 1 and Phase 2 each have
        their own check sets (see TestPhase2AlternativeManifest below)."""
        pf = DataPostflight(
            bucket=BUCKET,
            run_date=RUN_DATE,
            market_prefix=MARKET_PREFIX,
            phase=3,
        )
        # No mocks needed — run() should early-return for unknown phases.
        pf.run()


# ── Phase 2 alternative manifest contract ────────────────────────────────────


def _alt_manifest_payload(
    *,
    n_tickers: int = 25,
    overrides: dict[str, float] | None = None,
) -> dict:
    """Build a fully-populated alternative/manifest.json mirroring what
    ``collectors.alternative.collect`` writes."""
    floors = {
        "analyst_consensus": 0.80,
        "eps_revision":      0.50,
        "options_flow":      0.30,
        "insider_activity":  0.10,
        "institutional":     0.20,
        "news":              0.50,
    }
    # Default: all sources at 100% coverage
    ratios = {k: 1.0 for k in floors}
    if overrides:
        ratios.update(overrides)
    return {
        "run_date": RUN_DATE,
        "tickers_requested": n_tickers,
        "tickers_succeeded": n_tickers,
        "tickers_failed": 0,
        "source_ok_counts": {k: int(ratios[k] * n_tickers) for k in floors},
        "source_ok_ratios": ratios,
        "source_min_ok_ratios": floors,
        "errors": [],
    }


class TestPhase2AlternativeManifest:
    """The Phase 2 postflight re-verifies the per-source ok_ratio contract
    Phase 2's collector already enforced. Belt-and-suspenders against a
    partial write that bypassed the collector's status check."""

    def test_ok_when_all_sources_above_floor(self):
        pf = _make_postflight(phase=2)
        pf._s3 = MagicMock()
        pf._s3.head_object.return_value = {}
        payload = _alt_manifest_payload()
        pf._s3.get_object.return_value = {
            "Body": MagicMock(read=MagicMock(return_value=json.dumps(payload).encode())),
        }
        pf._check_alternative_manifest_contract()  # must not raise

    def test_fails_when_one_source_below_floor(self):
        pf = _make_postflight(phase=2)
        pf._s3 = MagicMock()
        pf._s3.head_object.return_value = {}
        payload = _alt_manifest_payload(overrides={"analyst_consensus": 0.4})
        pf._s3.get_object.return_value = {
            "Body": MagicMock(read=MagicMock(return_value=json.dumps(payload).encode())),
        }
        with pytest.raises(PostflightError, match="analyst_consensus"):
            pf._check_alternative_manifest_contract()

    def test_fails_with_all_breached_sources_named(self):
        """Multiple breaches must be listed together so the operator
        knows every provider that failed in one shot."""
        pf = _make_postflight(phase=2)
        pf._s3 = MagicMock()
        pf._s3.head_object.return_value = {}
        payload = _alt_manifest_payload(overrides={
            "analyst_consensus": 0.0, "eps_revision": 0.0,
        })
        pf._s3.get_object.return_value = {
            "Body": MagicMock(read=MagicMock(return_value=json.dumps(payload).encode())),
        }
        with pytest.raises(PostflightError) as excinfo:
            pf._check_alternative_manifest_contract()
        assert "analyst_consensus" in str(excinfo.value)
        assert "eps_revision" in str(excinfo.value)

    def test_fails_when_zero_tickers_requested(self):
        pf = _make_postflight(phase=2)
        pf._s3 = MagicMock()
        pf._s3.head_object.return_value = {}
        payload = _alt_manifest_payload()
        payload["tickers_requested"] = 0
        pf._s3.get_object.return_value = {
            "Body": MagicMock(read=MagicMock(return_value=json.dumps(payload).encode())),
        }
        with pytest.raises(PostflightError, match="empty payload"):
            pf._check_alternative_manifest_contract()

    def test_fails_when_schema_missing_floors_block(self):
        """If the collector writes a manifest without the per-source floors
        block, the gate's contract is broken — postflight must hard-fail
        rather than silently pass."""
        pf = _make_postflight(phase=2)
        pf._s3 = MagicMock()
        pf._s3.head_object.return_value = {}
        payload = _alt_manifest_payload()
        del payload["source_min_ok_ratios"]
        pf._s3.get_object.return_value = {
            "Body": MagicMock(read=MagicMock(return_value=json.dumps(payload).encode())),
        }
        with pytest.raises(PostflightError, match="schema violation"):
            pf._check_alternative_manifest_contract()

    def test_absent_manifest_skips_check(self):
        """Soft-launch: if Phase 2 hasn't run for this run_date yet, the
        manifest doesn't exist — log + skip rather than break a Phase-1-only
        invocation. Mirrors short_interest opt-out path."""
        pf = _make_postflight(phase=2)
        pf._s3 = MagicMock()
        pf._s3.head_object.side_effect = RuntimeError("NoSuchKey")
        pf._check_alternative_manifest_contract()  # must not raise
        pf._s3.get_object.assert_not_called()

    def test_phase2_run_invokes_alternative_check(self):
        """Calling run() with phase=2 must dispatch to the alternative
        manifest check (not phase-1's check set)."""
        pf = _make_postflight(phase=2)
        pf._s3 = MagicMock()
        pf._s3.head_object.return_value = {}
        payload = _alt_manifest_payload()
        pf._s3.get_object.return_value = {
            "Body": MagicMock(read=MagicMock(return_value=json.dumps(payload).encode())),
        }
        pf.run()  # must not raise
        # Ensure phase-1 ArcticDB checks did NOT run (lazy lib handles
        # would have been initialized otherwise).
        assert pf._universe_lib is None
        assert pf._macro_lib is None


# ── _finalize wiring ──────────────────────────────────────────────────────────

class TestFinalizeWiring:
    """Verify _finalize() catches PostflightError and flips status correctly.

    The contract: a successful collection (results['status']=='ok') that fails
    postflight must end up with status='postflight_failed' so main()'s
    SystemExit(1) propagates and Step Function HandleFailure fires. The health
    marker must reflect the new status so downstream consumers see
    'postflight_failed', not the stale 'ok'.
    """

    def test_postflight_failure_flips_status(self):
        from weekly_collector import _finalize

        results = {
            "phase": 1,
            "status": "ok",
            "started_at": "2026-04-18T00:00:00+00:00",
            "completed_at": "2026-04-18T00:30:00+00:00",
            "collectors": {"prices": {"status": "ok"}},
        }

        with patch("weekly_collector._write_manifest"), \
             patch("weekly_collector._write_validation_json"), \
             patch("weekly_collector._write_health_marker") as mock_health, \
             patch("weekly_collector.send_step_email", create=True) as _mock_email, \
             patch("validators.postflight.DataPostflight.run",
                   side_effect=PostflightError("forced failure for test")):
            _finalize(
                results=results,
                bucket=BUCKET,
                market_prefix=MARKET_PREFIX,
                run_date=RUN_DATE,
                dry_run=False,
                only=None,
            )

        assert results["status"] == "postflight_failed"
        assert "forced failure for test" in results["postflight_error"]
        # Health marker must be written with the failed status, not stale 'ok'
        mock_health.assert_called_once()
        marker_status = mock_health.call_args[0][3]
        assert marker_status == "postflight_failed"

    def test_postflight_skipped_when_collection_failed(self):
        """If results['status'] is already non-ok, postflight should not run.

        Postflight encodes consumer contracts on COLLECTED outputs — if
        collection itself failed (e.g. polygon outage), there's no clean
        output to validate and the existing failure already aborts the
        pipeline.
        """
        from weekly_collector import _finalize

        results = {
            "phase": 1,
            "status": "failed",
            "started_at": "2026-04-18T00:00:00+00:00",
            "completed_at": "2026-04-18T00:30:00+00:00",
            "collectors": {"prices": {"status": "error", "error": "polygon 503"}},
        }

        with patch("weekly_collector._write_manifest"), \
             patch("weekly_collector._write_validation_json"), \
             patch("weekly_collector._write_health_marker"), \
             patch("weekly_collector.send_step_email", create=True) as _mock_email, \
             patch("validators.postflight.DataPostflight.run") as mock_run:
            _finalize(
                results=results,
                bucket=BUCKET,
                market_prefix=MARKET_PREFIX,
                run_date=RUN_DATE,
                dry_run=False,
                only=None,
            )

        mock_run.assert_not_called()
        assert results["status"] == "failed"  # unchanged
