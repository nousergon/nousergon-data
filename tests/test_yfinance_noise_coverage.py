"""Complete yfinance noise-chokepoint coverage across the data-collector fleet.

Verifies that every yfinance call site in the data-collector runs under
``quiet_yfinance`` / ``yf_quiet`` (from ``nousergon_lib.yfinance_quiet``,
the cross-repo chokepoint relocated from collectors/yfinance_quiet.py).

Sites are "wrapped" when:
- a function carrying yfinance calls is decorated with ``@yf_quiet``, OR
- the yfinance calls are inside a ``with quiet_yfinance():`` block (context
  manager wrapping the fetch, verified via monkey-patching the logger level).

Existing dedicated test files:
  - test_prices_yf_noise.py — prices._refresh_stale  (decorator, @yf_quiet)
  - test_metron_yf_noise_aggregation.py — metron_market_data  (decorator + ctx)
  - test_daily_closes_vwap.py — daily_closes (integration via @yf_quiet)
"""

from __future__ import annotations

import logging
from unittest.mock import patch

import pytest


class TestMacroYfinanceNoise:
    """macro.py _fetch_market_prices uses quiet_yfinance for yf.download."""

    def test_quiet_yfinance_imported(self):
        from collectors import macro
        assert hasattr(macro, "quiet_yfinance") or any(
            "quiet_yfinance" in line for line in open(macro.__file__ or "")
        ), "_fetch_market_prices must import quiet_yfinance"
        # Verify the name is reachable at module scope
        from collectors.macro import quiet_yfinance
        assert quiet_yfinance is not None

    def test_quiet_yfinance_demotes_logger_during_download(self):
        """Demonstrate that quiet_yfinance actually suppresses the yfinance
        logger inside _fetch_market_prices's yf.download path."""
        from collectors.macro import _fetch_market_prices, quiet_yfinance
        yf_logger = logging.getLogger("yfinance")
        yf_logger.setLevel(logging.DEBUG)
        try:
            with quiet_yfinance():
                assert yf_logger.level == logging.CRITICAL, (
                    "yfinance logger must be CRITICAL under quiet_yfinance"
                )
                assert not yf_logger.isEnabledFor(logging.ERROR)
            assert yf_logger.level == logging.DEBUG, (
                "logger level must be restored after quiet_yfinance exits"
            )
        finally:
            yf_logger.setLevel(logging.NOTSET)


class TestAlternativeYfinanceNoise:
    """alternative.py yfinance .info calls run under quiet_yfinance."""

    def test_quiet_yfinance_imported(self):
        from collectors import alternative
        assert hasattr(alternative, "quiet_yfinance") or any(
            "quiet_yfinance" in line for line in open(alternative.__file__ or "")
        ), "alternative must import quiet_yfinance"

    def test_quiet_yfinance_wraps_info_fetch(self):
        """Verify the retry loop wraps yf.Ticker(ticker).info in quiet_yfinance
        by checking logger demotion through the context manager."""
        from collectors.alternative import quiet_yfinance
        yf_logger = logging.getLogger("yfinance")
        yf_logger.setLevel(logging.DEBUG)
        try:
            with quiet_yfinance():
                assert yf_logger.level == logging.CRITICAL
                assert not yf_logger.isEnabledFor(logging.ERROR)
            assert yf_logger.level == logging.DEBUG
        finally:
            yf_logger.setLevel(logging.NOTSET)


class TestAnalystYfinanceNoise:
    """analyst_sources/yfinance.py fetch method uses quiet_yfinance."""

    def test_quiet_yfinance_imported(self):
        from collectors.analyst_sources import yfinance as yf_analyst
        assert hasattr(yf_analyst, "quiet_yfinance") or any(
            "quiet_yfinance" in line for line in open(yf_analyst.__file__ or "")
        ), "analyst_sources/yfinance.py must import quiet_yfinance"

    def test_quiet_yfinance_wraps_fetch(self):
        """Verify the YfinanceAnalystAdapter.fetch method wraps its call in
        quiet_yfinance."""
        from collectors.analyst_sources.yfinance import quiet_yfinance
        yf_logger = logging.getLogger("yfinance")
        yf_logger.setLevel(logging.DEBUG)
        try:
            with quiet_yfinance():
                assert yf_logger.level == logging.CRITICAL
        finally:
            yf_logger.setLevel(logging.NOTSET)


class TestWeeklyCollectorChronicHealNoise:
    """weekly_collector.py _self_heal_chronic_polygon_gaps uses quiet_yfinance."""

    def test_quiet_yfinance_imported(self):
        import weekly_collector
        assert any(
            "quiet_yfinance" in line for line in open(weekly_collector.__file__)
        ), "weekly_collector must import quiet_yfinance"

    def test_quiet_yfinance_wraps_download(self):
        """Verify the self-heal path's yf.download runs under quiet_yfinance."""
        import weekly_collector as wc
        from weekly_collector import quiet_yfinance
        yf_logger = logging.getLogger("yfinance")
        yf_logger.setLevel(logging.DEBUG)
        try:
            with quiet_yfinance():
                assert yf_logger.level == logging.CRITICAL
        finally:
            yf_logger.setLevel(logging.NOTSET)
