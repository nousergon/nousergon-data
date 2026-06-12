"""yfinance log-noise suppression + per-run coverage aggregation (config#1029).

The 2026-06-12 PCKM storm: one unpriceable holding (a 401(k) CIT yfinance can
never price) produced ≥5 distinctly-worded yfinance ERROR log records per EOD
run, each becoming its own Flow Doctor report/email. The fix demotes yfinance's
internal logger for the duration of each fetch and aggregates per-symbol
coverage into ONE record per artifact per run — the named recording surface.
"""

from __future__ import annotations

import logging

import pytest

from collectors import metron_market_data as mmd


class TestQuietYfinance:
    def test_demotes_yfinance_logger_inside_and_restores_after(self):
        yf_logger = logging.getLogger("yfinance")
        yf_logger.setLevel(logging.DEBUG)
        try:
            with mmd._quiet_yfinance():
                assert yf_logger.level == logging.CRITICAL
                # The PCKM failure mode: yfinance ERROR records must not pass
                # the logger's own level while a fetch is in flight.
                assert not yf_logger.isEnabledFor(logging.ERROR)
            assert yf_logger.level == logging.DEBUG
        finally:
            yf_logger.setLevel(logging.NOTSET)

    def test_restores_level_even_when_fetch_raises(self):
        yf_logger = logging.getLogger("yfinance")
        yf_logger.setLevel(logging.INFO)
        try:
            with pytest.raises(RuntimeError):
                with mmd._quiet_yfinance():
                    raise RuntimeError("batch failed")
            assert yf_logger.level == logging.INFO
        finally:
            yf_logger.setLevel(logging.NOTSET)

    def test_all_yfinance_fetchers_are_wrapped(self):
        # The decorator is the chokepoint: every default yfinance source must
        # run quieted, or one bad symbol storms Flow Doctor again.
        for fn in (
            mmd._yfinance_closes, mmd._yfinance_fx, mmd._yf_history,
            mmd._yfinance_sectors, mmd._yfinance_spy_weights,
            mmd._yfinance_earnings, mmd._yfinance_fundamentals,
            mmd._yfinance_intraday,
        ):
            assert hasattr(fn, "__wrapped__"), f"{fn.__name__} not under _yf_quiet"


class TestCoverageAggregation:
    def test_partial_miss_is_one_warning_naming_all_missing(self, caplog):
        with caplog.at_level(logging.DEBUG):
            mmd._log_yf_coverage("closes", ["AAPL", "PCKM", "ANET"], {"AAPL": 1, "ANET": 1})
        warns = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert len(warns) == 1
        assert "PCKM" in warns[0].message
        assert "1/3" in warns[0].message
        assert not [r for r in caplog.records if r.levelno >= logging.ERROR]

    def test_full_coverage_logs_nothing(self, caplog):
        with caplog.at_level(logging.DEBUG):
            mmd._log_yf_coverage("closes", ["AAPL"], {"AAPL": 1}, error_on_empty=True)
        assert not caplog.records

    def test_full_miss_on_load_bearing_artifact_is_single_error(self, caplog):
        with caplog.at_level(logging.DEBUG):
            mmd._log_yf_coverage("closes", ["AAPL", "ANET"], {}, error_on_empty=True)
        errors = [r for r in caplog.records if r.levelno == logging.ERROR]
        assert len(errors) == 1
        assert "AAPL" in errors[0].message and "ANET" in errors[0].message

    def test_full_miss_on_best_effort_artifact_stays_warn(self, caplog):
        with caplog.at_level(logging.DEBUG):
            mmd._log_yf_coverage("earnings", ["AAPL"], {})
        assert not [r for r in caplog.records if r.levelno >= logging.ERROR]
        assert [r for r in caplog.records if r.levelno == logging.WARNING]
