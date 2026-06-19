"""Shared yfinance log-noise chokepoint (collectors/yfinance_quiet.py).

In-repo single source of truth extracted from metron_market_data.py when the
config#1029 PCKM-storm bug class recurred through prices.py (2026-06-19 PCAR).
"""

from __future__ import annotations

import logging

import pytest

from collectors.yfinance_quiet import log_yf_coverage, quiet_yfinance, yf_quiet


class TestQuietYfinance:
    def test_demotes_yfinance_logger_inside_and_restores_after(self):
        yf_logger = logging.getLogger("yfinance")
        yf_logger.setLevel(logging.DEBUG)
        try:
            with quiet_yfinance():
                assert yf_logger.level == logging.CRITICAL
                # The storm failure mode: yfinance ERROR records must not pass
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
                with quiet_yfinance():
                    raise RuntimeError("batch failed")
            assert yf_logger.level == logging.INFO
        finally:
            yf_logger.setLevel(logging.NOTSET)

    def test_yf_quiet_decorator_runs_quieted_and_preserves_wrapped(self):
        seen = {}

        @yf_quiet
        def fetch():
            seen["level"] = logging.getLogger("yfinance").level
            return "ok"

        yf_logger = logging.getLogger("yfinance")
        yf_logger.setLevel(logging.DEBUG)
        try:
            assert fetch() == "ok"
            assert seen["level"] == logging.CRITICAL
            assert hasattr(fetch, "__wrapped__")  # functools.wraps chokepoint marker
            assert yf_logger.level == logging.DEBUG  # restored
        finally:
            yf_logger.setLevel(logging.NOTSET)


class TestLogYfCoverage:
    def test_full_coverage_logs_nothing(self, caplog):
        logger = logging.getLogger("test.yfq")
        with caplog.at_level(logging.DEBUG):
            log_yf_coverage(logger, "closes", ["AAPL"], {"AAPL"}, error_on_empty=True)
        assert not caplog.records

    def test_partial_miss_is_one_warning_naming_all_missing(self, caplog):
        logger = logging.getLogger("test.yfq")
        with caplog.at_level(logging.DEBUG):
            log_yf_coverage(logger, "closes", ["AAPL", "PCAR", "ANET"], {"AAPL", "ANET"})
        warns = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert len(warns) == 1
        assert "PCAR" in warns[0].message and "1/3" in warns[0].message
        assert not [r for r in caplog.records if r.levelno >= logging.ERROR]

    def test_covered_accepts_a_result_dict(self, caplog):
        # Callers pass the result dict directly — its KEYS are the covered set.
        logger = logging.getLogger("test.yfq")
        with caplog.at_level(logging.DEBUG):
            log_yf_coverage(logger, "closes", ["AAPL", "PCAR"], {"AAPL": (1.0, "d")})
        warns = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert len(warns) == 1 and "PCAR" in warns[0].message

    def test_full_miss_on_load_bearing_artifact_is_single_error(self, caplog):
        logger = logging.getLogger("test.yfq")
        with caplog.at_level(logging.DEBUG):
            log_yf_coverage(logger, "closes", ["AAPL", "ANET"], set(), error_on_empty=True)
        errors = [r for r in caplog.records if r.levelno == logging.ERROR]
        assert len(errors) == 1
        assert "AAPL" in errors[0].message and "ANET" in errors[0].message

    def test_full_miss_on_best_effort_artifact_stays_warn(self, caplog):
        logger = logging.getLogger("test.yfq")
        with caplog.at_level(logging.DEBUG):
            log_yf_coverage(logger, "earnings", ["AAPL"], set())
        assert not [r for r in caplog.records if r.levelno >= logging.ERROR]
        assert [r for r in caplog.records if r.levelno == logging.WARNING]

    def test_note_is_appended(self, caplog):
        logger = logging.getLogger("test.yfq")
        with caplog.at_level(logging.DEBUG):
            log_yf_coverage(logger, "closes", ["PCAR"], set(), note="universe-prune candidate")
        warns = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert len(warns) == 1 and "universe-prune candidate" in warns[0].message
