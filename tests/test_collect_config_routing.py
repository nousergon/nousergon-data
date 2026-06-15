"""Phase 1b — collect() dispatches fetches through the source registry.

Pins the two properties that make the rewire behavior-preserving:
  1. ``fetch_into`` mutates the PASSED records list in place and returns the
     count (so partial appends survive a mid-fetch error, exactly as the legacy
     ``_fetch_*`` functions do — the property collect()'s coverage gates rely on).
  2. ``collect()`` exposes the injectable per-role source params (defaults =
     today's behavior), so swapping polygon→databento is one param.

The end-to-end behavior-equivalence net is the existing ``test_daily_closes_*``
suite, which exercises collect() with mocked vendors and must pass unchanged.
"""

from __future__ import annotations

import inspect
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from collectors import daily_closes as dc  # noqa: E402
from sources import get_adapter  # noqa: E402


class _FakeGroupedClient:
    def get_grouped_daily(self, run_date):
        return {
            "AAPL": {"open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5,
                     "volume": 1000, "vwap": 1.4},
            "BRK.B": {"open": 10.0, "high": 11.0, "low": 9.0, "close": 10.5,
                      "volume": 50, "vwap": 10.2},
        }


def test_fetch_into_mutates_in_place_and_counts(monkeypatch):
    monkeypatch.setattr("polygon_client.polygon_client", lambda: _FakeGroupedClient())
    pre = {"ticker": "PRE", "date": "2026-06-12", "Open": 0.0, "High": 0.0,
           "Low": 0.0, "Close": 0.0, "Adj_Close": 0.0, "Volume": 0,
           "VWAP": None, "source": "prior"}
    records = [pre]
    n = get_adapter("polygon").fetch_into(records, ["AAPL", "BRK-B"], "2026-06-12")
    assert n == 2                       # count == appended
    assert records[0] is pre            # prior row preserved (mutate in place)
    assert {r["ticker"] for r in records[1:]} == {"AAPL", "BRK-B"}
    # The appended records are the canonical persisted shape (dash store-key kept).
    brk = next(r for r in records if r["ticker"] == "BRK-B")
    assert brk["Close"] == 10.5 and brk["VWAP"] == 10.2 and brk["source"] == "polygon"


def test_fetch_into_matches_legacy_fetch(monkeypatch):
    """fetch_into is a faithful pass-through of the legacy _fetch_polygon_closes."""
    monkeypatch.setattr("polygon_client.polygon_client", lambda: _FakeGroupedClient())
    via_adapter: list[dict] = []
    get_adapter("polygon").fetch_into(via_adapter, ["AAPL", "BRK-B"], "2026-06-12")
    via_legacy: list[dict] = []
    dc._fetch_polygon_closes(["AAPL", "BRK-B"], "2026-06-12", via_legacy, source="auto")
    assert via_adapter == via_legacy


def test_collect_exposes_injectable_source_params():
    params = inspect.signature(dc.collect).parameters
    assert params["equities_source"].default == "polygon"
    assert params["index_source"].default == "fred"
    assert params["fallback_source"].default == "yfinance"


def test_collect_window_forwards_source_params():
    params = inspect.signature(dc._collect_window).parameters
    assert params["equities_source"].default == "polygon"
    assert params["index_source"].default == "fred"
    assert params["fallback_source"].default == "yfinance"
