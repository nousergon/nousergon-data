"""Share-class symbol bridge: Yahoo/our-universe dash ↔ polygon dot.

Regression lock for the 2026-05-19 weekday-pipeline FAILURE.

Root cause: our universe keys class shares with a dash + class letter
(BRK-B, BF-B, MOG-A); polygon serves the same securities under the dot
convention (BRK.B, BF.B, MOG.A). ``grouped.get("BRK-B")`` missed the
row that polygon's bulk grouped-daily call ALREADY contained under
"BRK.B", so the rate-limited (5/min) per-ticker fallback re-queried the
dash form and also missed — recovered 0/N on every one of the 14 window
dates, ~12 min of pure wasted retries that pushed weekday MorningEnrich
past its 30-min SSM timeout (SIGKILL/137 → whole pipeline FAILED).

These tests pin three invariants:

1. ``_polygon_symbol`` maps class shares to the dot form and leaves
   every other ticker shape (normal, index/^-stripped, ETF, no-hyphen)
   untouched — the anchored pattern cannot misfire.
2. ``_fetch_polygon_closes`` recovers a class share straight from the
   grouped-daily dot key, stores it under the DASH key, and does NOT
   reach the per-ticker fallback (the cost-quiet path that fixes the
   timeout).
3. ``_fetch_polygon_closes_per_ticker`` queries polygon with the dot
   form for a class share but persists the dash record key.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from collectors.daily_closes import (
    _fetch_polygon_closes,
    _fetch_polygon_closes_per_ticker,
    _polygon_symbol,
)


# ── _polygon_symbol unit ──────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "store_ticker,expected",
    [
        ("BRK-B", "BRK.B"),
        ("BF-B", "BF.B"),
        ("MOG-A", "MOG.A"),
        ("A-B", "A.B"),          # 1-char root still a valid class share
        ("ABCDE-A", "ABCDE.A"),  # 5-char root boundary
    ],
)
def test_class_share_maps_dash_to_dot(store_ticker, expected):
    assert _polygon_symbol(store_ticker) == expected


@pytest.mark.parametrize(
    "store_ticker",
    [
        "AAPL",      # normal common stock
        "PSTG",      # genuine chronic gap — no hyphen, must stay as-is
        "VIX",       # ^-stripped index symbol
        "VIX3M",
        "XLB",       # sector ETF
        "ABCDEF-A",  # 6-char root — outside the anchored {1,5} bound
        "ABC-AB",    # two-letter class segment — not the convention
        "ABC-1",     # digit segment — not a class share
        "BRK.B",     # already dot form — idempotent, unchanged
    ],
)
def test_non_class_share_unchanged(store_ticker):
    assert _polygon_symbol(store_ticker) == store_ticker


# ── _fetch_polygon_closes integration — the core regression + cost win ────────


def test_class_share_recovered_from_grouped_dot_key_no_fallback():
    """Polygon's grouped-daily returns BRK.B (dot). The collector must
    recover it via the dot-key lookup, persist it under the DASH key
    (BRK-B) for ArcticDB/universe consistency, and NOT spend a
    rate-limited per-ticker call — this is the ~12-min saving that pulls
    MorningEnrich back under its 30-min SSM ceiling."""
    records: list[dict] = []
    fake_client = MagicMock()
    fake_client.get_grouped_daily.return_value = {
        "AAPL": {"open": 99.0, "high": 101.0, "low": 98.0, "close": 100.0,
                 "volume": 5_000_000, "vwap": 99.5},
        "BRK.B": {"open": 487.0, "high": 490.0, "low": 486.0, "close": 488.38,
                  "volume": 3_000_000, "vwap": 488.0},
    }

    with patch("polygon_client.polygon_client", return_value=fake_client):
        polygon_count = _fetch_polygon_closes(
            ["AAPL", "BRK-B"], "2026-05-18", records, source="polygon_only",
        )

    assert polygon_count == 2
    # Stored under the DASH key, not the dot key polygon used.
    assert {r["ticker"] for r in records} == {"AAPL", "BRK-B"}
    brk = next(r for r in records if r["ticker"] == "BRK-B")
    assert brk["Close"] == 488.38
    assert brk["source"] == "polygon"
    # The cost-quiet invariant: no per-ticker fallback was needed.
    fake_client.get_single_day_bar.assert_not_called()


def test_class_share_genuine_grouped_drop_uses_dot_per_ticker():
    """If the bulk endpoint genuinely drops a class share on some date,
    the per-ticker fallback must query polygon with the DOT form and
    still persist the DASH record key."""
    records: list[dict] = []
    fake_client = MagicMock()
    fake_client.get_grouped_daily.return_value = {
        "AAPL": {"open": 99.0, "high": 101.0, "low": 98.0, "close": 100.0,
                 "volume": 5_000_000, "vwap": 99.5},
    }
    fake_client.get_single_day_bar.return_value = {
        "open": 487.0, "high": 490.0, "low": 486.0, "close": 488.38,
        "volume": 3_000_000, "vwap": 488.0,
    }

    with patch("polygon_client.polygon_client", return_value=fake_client):
        polygon_count = _fetch_polygon_closes(
            ["AAPL", "BRK-B"], "2026-05-18", records, source="polygon_only",
        )

    assert polygon_count == 2
    # polygon was asked for the DOT form…
    fake_client.get_single_day_bar.assert_called_once_with("BRK.B", "2026-05-18")
    # …but the record key stays DASH.
    assert {r["ticker"] for r in records} == {"AAPL", "BRK-B"}


def test_per_ticker_fallback_class_share_dot_query_dash_store():
    """Direct ``_fetch_polygon_closes_per_ticker`` unit: dot query,
    dash persisted key."""
    records: list[dict] = []
    fake_client = MagicMock()
    fake_client.get_single_day_bar.return_value = {
        "open": 305.0, "high": 308.0, "low": 304.0, "close": 306.2,
        "volume": 120_000, "vwap": 306.0,
    }

    with patch("polygon_client.polygon_client", return_value=fake_client):
        recovered = _fetch_polygon_closes_per_ticker(
            ["MOG-A"], "2026-05-18", records
        )

    assert recovered == 1
    fake_client.get_single_day_bar.assert_called_once_with("MOG.A", "2026-05-18")
    assert records[0]["ticker"] == "MOG-A"
    assert records[0]["Close"] == 306.2
