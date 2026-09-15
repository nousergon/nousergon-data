"""Tests for the declared, reversible vendor-precedence pointer
(alpha-engine-config-I10783, champion-challenger-policy.md).

``collectors.daily_closes.VENDOR_PRECEDENCE`` is the ONE declared mapping
consumers read to decide which vendor's row wins a cell — never an implicit
write-order dependency. These tests demonstrate flipping it and observing
``_coalesce_by_source_priority`` (the merge consumer) follow, per I10783's
closes-when: "demonstrated by flipping it in a test and observing consumers
follow it."
"""

from __future__ import annotations

from collectors import daily_closes
from collectors.daily_closes import (
    VENDOR_CHAMPION,
    VENDOR_PRECEDENCE,
    _coalesce_by_source_priority,
)


def _row(ticker, close, source):
    return {
        "ticker": ticker, "date": "2026-09-14", "Open": close, "High": close,
        "Low": close, "Close": close, "Adj_Close": close, "Volume": 0,
        "VWAP": None, "source": source,
    }


def test_declared_champion_is_polygon():
    """The 2026-09-09 'stay on free polygon' ruling, as a named pointer."""
    assert VENDOR_CHAMPION == "polygon"
    assert VENDOR_PRECEDENCE["polygon"] >= VENDOR_PRECEDENCE["yfinance"]


def test_default_precedence_prefers_polygon_over_yfinance():
    existing = [_row("AAPL", 100.0, "yfinance")]
    fresh = [_row("AAPL", 101.0, "polygon")]
    merged, stats = _coalesce_by_source_priority(fresh, existing, "2026-09-14")
    by_ticker = {r["ticker"]: r for r in merged}
    assert by_ticker["AAPL"]["source"] == "polygon"
    assert stats["overwritten"] == 1


def test_flipping_the_pointer_changes_what_the_merge_consumer_does(monkeypatch):
    """Flip the ONE declared mapping; the coalesce consumer must follow it
    without any other code change — proving precedence is a config pointer,
    not an implicit overwrite order baked into the merge logic."""
    flipped = {"polygon": 1, "fred": 1, "yfinance": 3}
    monkeypatch.setattr(daily_closes, "VENDOR_PRECEDENCE", flipped)

    existing = [_row("AAPL", 100.0, "polygon")]
    fresh = [_row("AAPL", 101.0, "yfinance")]
    merged, stats = _coalesce_by_source_priority(fresh, existing, "2026-09-14")
    by_ticker = {r["ticker"]: r for r in merged}

    # Under the flipped pointer, yfinance now outranks polygon for this cell.
    assert by_ticker["AAPL"]["source"] == "yfinance"
    assert stats["overwritten"] == 1


def test_unflipped_pointer_blocks_the_same_downgrade():
    """Sanity check for the test above: WITHOUT flipping, the same fresh
    yfinance value is refused as a source-downgrade against a polygon prior."""
    existing = [_row("AAPL", 100.0, "polygon")]
    fresh = [_row("AAPL", 101.0, "yfinance")]
    merged, stats = _coalesce_by_source_priority(fresh, existing, "2026-09-14")
    by_ticker = {r["ticker"]: r for r in merged}
    assert by_ticker["AAPL"]["source"] == "polygon"
    assert stats["downgrade_blocked"] == 1
