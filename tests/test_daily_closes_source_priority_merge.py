"""Tests for the source-priority coalesce merge on
``collectors.daily_closes._coalesce_by_source_priority`` and its wiring into
``collect(source="polygon_only")``.

Background (incident 2026-06-01):

The Monday weekday pipeline halted at MorningEnrich. A FRED 429 rate-limit
storm meant ``TNX`` (DGS10, the 10Y yield) was never collected for the 5/29
target date. ``polygon_only`` mode then OVERWROTE the existing 5/29 parquet
wholesale — and because polygon never serves ``^TNX`` and yfinance was refused,
the rewrite BLANKED the ``TNX`` value the prior (Friday EOD) parquet already
held. ``daily_append`` correctly hard-failed on the missing critical macro key,
halting the pipeline.

Root cause: the ``polygon_only`` overwrite was not coverage-aware — it let a
transient live-fetch gap regress a populated cell to absent, violating the
2026-05-10 decision ("a cell is only updated if the data exists in the
authoritative source, else the prior datapoint is retained").

The fix is an institutional source-of-record waterfall (``_SOURCE_PRIORITY``):
a cell is replaced only by an equal-or-higher-priority source; lower or missing
never clobbers higher-quality existing data. These tests lock the contract.
"""

from __future__ import annotations

import math

import pandas as pd

from collectors.daily_closes import _coalesce_by_source_priority


def _row(ticker, close, source, vwap=None):
    return {
        "ticker": ticker,
        "date": "2026-05-29",
        "Open": close,
        "High": close,
        "Low": close,
        "Close": close,
        "Adj_Close": close,
        "Volume": 0,
        "VWAP": vwap,
        "source": source,
    }


def _by_ticker(records):
    return {r["ticker"]: r for r in records}


def test_retain_on_empty_keeps_prior_macro_cell():
    """The 2026-06-01 bug: TNX absent from this run must retain the prior value,
    not be dropped/blanked."""
    existing = [_row("TNX", 4.51, "fred")]
    new = [_row("AAPL", 200.0, "polygon", vwap=199.8)]  # live run, no TNX

    merged, stats = _coalesce_by_source_priority(new, existing, "2026-05-29")
    m = _by_ticker(merged)

    assert "TNX" in m, "TNX must be retained, not blanked"
    assert m["TNX"]["Close"] == 4.51
    assert m["TNX"]["source"] == "fred"
    assert stats["retained"] == 1
    assert m["AAPL"]["Close"] == 200.0


def test_restatement_same_source_overwrites():
    """Polygon re-emitting a corporate-action-adjusted close (tie on priority)
    must win — restatement absorption is the whole point of the morning pass."""
    existing = [_row("AAPL", 100.0, "polygon", vwap=99.5)]
    new = [_row("AAPL", 105.0, "polygon", vwap=104.5)]  # split-adjusted

    merged, stats = _coalesce_by_source_priority(new, existing, "2026-05-29")
    m = _by_ticker(merged)

    assert m["AAPL"]["Close"] == 105.0
    assert m["AAPL"]["VWAP"] == 104.5
    assert stats["overwritten"] == 1


def test_fresh_polygon_overwrites_prior_yfinance():
    """Morning polygon (adjusted + true VWAP) must replace the prior EOD
    yfinance row — higher priority wins."""
    existing = [_row("AAPL", 100.0, "yfinance", vwap=None)]
    new = [_row("AAPL", 100.2, "polygon", vwap=100.1)]

    merged, stats = _coalesce_by_source_priority(new, existing, "2026-05-29")
    m = _by_ticker(merged)

    assert m["AAPL"]["source"] == "polygon"
    assert m["AAPL"]["VWAP"] == 100.1
    assert stats["overwritten"] == 1


def test_lower_tier_cannot_downgrade_higher_tier():
    """A yfinance backstop value must NOT clobber an existing polygon cell —
    prevents the 2026-04-17 VWAP=None contamination class."""
    existing = [_row("AAPL", 100.0, "polygon", vwap=99.9)]
    new = [_row("AAPL", 100.0, "yfinance", vwap=None)]

    merged, stats = _coalesce_by_source_priority(new, existing, "2026-05-29")
    m = _by_ticker(merged)

    assert m["AAPL"]["source"] == "polygon", "must keep the higher-quality source"
    assert m["AAPL"]["VWAP"] == 99.9, "polygon VWAP must survive"
    assert stats["downgrade_blocked"] == 1


def test_null_close_fresh_does_not_win():
    """A fresh row with a null Close is treated as missing — the prior real
    value is kept."""
    existing = [_row("TNX", 4.5, "fred")]
    new = [_row("TNX", float("nan"), "fred")]

    merged, _ = _coalesce_by_source_priority(new, existing, "2026-05-29")
    m = _by_ticker(merged)

    assert m["TNX"]["Close"] == 4.5


def test_both_empty_ticker_dropped():
    """No usable value anywhere → ticker is not written as a null cell."""
    new = [_row("ZZZ", float("nan"), "polygon")]
    merged, _ = _coalesce_by_source_priority(new, [], "2026-05-29")
    assert all(r["ticker"] != "ZZZ" for r in merged)


def test_new_only_passthrough():
    """A brand-new ticker with no prior row passes straight through."""
    new = [_row("MSFT", 400.0, "polygon", vwap=399.0)]
    merged, stats = _coalesce_by_source_priority(new, [], "2026-05-29")
    m = _by_ticker(merged)

    assert m["MSFT"]["Close"] == 400.0
    assert stats["new_only"] == 1


def test_unknown_prior_source_retained_but_overwritten_by_fresh_primary():
    """A prior row with no ``source`` (legacy parquet) is backstop-tier: a fresh
    polygon value wins, but a missing fresh value still retains it."""
    existing_no_source = [{"ticker": "AAPL", "Close": 100.0, "VWAP": None, "date": "2026-05-29"}]

    # Fresh polygon overwrites the unknown-source prior.
    merged, _ = _coalesce_by_source_priority(
        [_row("AAPL", 101.0, "polygon", vwap=100.5)], existing_no_source, "2026-05-29"
    )
    assert _by_ticker(merged)["AAPL"]["source"] == "polygon"

    # No fresh AAPL → unknown-source prior is retained, not blanked.
    merged2, stats2 = _coalesce_by_source_priority([], existing_no_source, "2026-05-29")
    assert _by_ticker(merged2)["AAPL"]["Close"] == 100.0
    assert stats2["retained"] == 1


def test_mixed_scenario_stats():
    """End-to-end stats across all four outcomes in one merge."""
    existing = [
        _row("TNX", 4.5, "fred"),            # absent this run -> retained
        _row("AAPL", 100.0, "polygon", 99.9),  # downgraded attempt -> blocked
        _row("MSFT", 400.0, "yfinance"),     # fresh polygon -> overwritten
    ]
    new = [
        _row("AAPL", 100.0, "yfinance"),     # lower tier, blocked
        _row("MSFT", 401.0, "polygon", 400.5),  # higher tier, overwrites
        _row("NVDA", 900.0, "polygon", 899.0),  # brand new
    ]

    merged, stats = _coalesce_by_source_priority(new, existing, "2026-05-29")
    m = _by_ticker(merged)

    assert m["TNX"]["Close"] == 4.5            # retained
    assert m["AAPL"]["source"] == "polygon"    # downgrade blocked
    assert m["MSFT"]["source"] == "polygon"    # overwritten
    assert m["NVDA"]["Close"] == 900.0         # new
    assert stats == {
        "retained": 1,
        "downgrade_blocked": 1,
        "overwritten": 1,
        "new_only": 1,
    }
