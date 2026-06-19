"""prices.py price-cache refresh must run under the yfinance noise chokepoint.

The 2026-06-19 PCAR recurrence: collectors/prices.py::_refresh_stale called
yf.download WITHOUT the quiet_yfinance chokepoint that metron_market_data.py
adopted in config#1029. yfinance's multi-form "possibly delisted" ERROR spray
therefore escaped to Flow Doctor as three issues (#451/#452/#453) for ONE
active, non-delisted ticker (PCAR / PACCAR Inc.). This guards the wrap so the
storm can't silently regress.
"""

from __future__ import annotations

from collectors import prices


def test_refresh_stale_runs_under_quiet_yfinance():
    # The decorator is the chokepoint: the 10y refresh fetch must run quieted,
    # or one transient/unpriceable ticker storms Flow Doctor again.
    assert hasattr(prices._refresh_stale, "__wrapped__"), (
        "_refresh_stale must be decorated with @yf_quiet (yfinance noise chokepoint)"
    )
