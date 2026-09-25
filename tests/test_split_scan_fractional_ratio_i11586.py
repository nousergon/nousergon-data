"""alpha-engine-config-I11586 — one fractional polygon split ratio must not
take down the price-cache split guard's whole-market scan.

Measured on the 2026-09-24 weekly rehearsal (rehearsal-2026-09-24-2) and on
every postmarket run since the split guard shipped (I11518, 2026-09-24):
``Split guard: the polygon split scan failed (ValueError) — refreshing all 930
fresh tickers``. Every ticker was force-refreshed, a behind-fetch refusal
(I11467) on HUBB then counted as a failed ticker, and DataPhase1 exited
``prices=partial``.

Cause: ``corporate_actions.splits_from_events`` still ``int()``-cast the ratio
fields that ``polygon_client._parse_split_row`` deliberately keeps fractional.
A ratio ``< 1`` became ``0`` and ``CorporateAction.from_split`` raised
``ValueError``; a ratio ``>= 1`` was silently truncated (``1:1.2`` -> ``1:1``).

These tests drive the REAL ``PolygonClient.get_recent_splits`` parse (only
the HTTP ``_get`` is stubbed) so the shape that failed on the box is the shape
under test.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

import polygon_client
from collectors import prices
from corporate_actions import CorporateAction, expected_factor, splits_from_events

THU = "2026-09-24"
WED_EOD = datetime(2026, 9, 23, 23, 30, tzinfo=timezone.utc)

# A polygon /v3/reference/splits page carrying the shapes that broke the scan
# beside ordinary integer splits.
_POLYGON_PAGE = {
    "results": [
        {"ticker": "NVDA", "execution_date": "2026-09-02", "split_from": 1, "split_to": 10},
        # fractional < 1: int() made this 0 -> from_split ValueError
        {"ticker": "FRAC", "execution_date": "2026-09-10", "split_from": 0.5, "split_to": 1},
        # fractional >= 1: int() silently truncated this to 1:1
        {"ticker": "CCBC", "execution_date": "2026-09-15", "split_from": 1, "split_to": 1.2},
        {"ticker": "NRWRF", "execution_date": "2026-09-16", "split_from": 20.625, "split_to": 21.625},
        {"ticker": "RVSE", "execution_date": "2026-09-18", "split_from": 8, "split_to": 1},
    ],
}


def _client(monkeypatch, page=_POLYGON_PAGE):
    client = polygon_client.PolygonClient(api_key="test-key-not-real")
    monkeypatch.setattr(client, "_get", lambda path, params=None: page)
    monkeypatch.setattr(polygon_client, "polygon_client", lambda *a, **k: client)
    return client


def test_fractional_ratio_below_one_no_longer_raises():
    (action,) = splits_from_events([
        {"ticker": "FRAC", "execution_date": "2026-09-10", "split_from": 0.5, "split_to": 1},
    ])
    assert (action.split_from, action.split_to) == (0.5, 1)
    assert expected_factor(action) == pytest.approx(0.5)


def test_fractional_ratio_above_one_is_not_truncated():
    (action,) = splits_from_events([
        {"ticker": "CCBC", "execution_date": "2026-09-15", "split_from": 1, "split_to": 1.2},
    ])
    assert action.split_to == 1.2
    assert action.action_id == CorporateAction.from_split("CCBC", "2026-09-15", 1, 1.2).action_id


def test_integer_ratio_action_ids_are_unchanged():
    """Integral floats normalize to int, so the registry's content-addressed
    ids for the dominant integer-ratio case do not move."""
    (a,) = splits_from_events([
        {"ticker": "NVDA", "execution_date": "2026-09-02", "split_from": 1.0, "split_to": 10.0},
    ])
    b = CorporateAction.from_split("NVDA", "2026-09-02", 1, 10)
    assert (a.split_from, a.split_to) == (1, 10)
    assert isinstance(a.split_to, int)
    assert a.action_id == b.action_id


@pytest.mark.parametrize("bad", [-2, "abc"])
def test_malformed_ratio_still_raises(bad):
    """The guard must still be able to tell "could not read" from "no splits":
    a ratio that is present but not a positive number is not skipped."""
    with pytest.raises(ValueError):
        splits_from_events([
            {"ticker": "X", "execution_date": "2026-09-10", "split_from": bad, "split_to": 1},
        ])


def test_production_split_scan_survives_the_polygon_page(monkeypatch):
    _client(monkeypatch)
    actions = prices._polygon_split_scan("2026-08-25", THU)
    got = {a.ticker: (a.split_from, a.split_to) for a in actions}
    assert got == {
        "NVDA": (1, 10),
        "FRAC": (0.5, 1),
        "CCBC": (1, 1.2),
        "NRWRF": (20.625, 21.625),
        "RVSE": (8, 1),
    }


def test_split_guard_does_not_force_every_fresh_ticker(monkeypatch):
    """The rehearsal failure end-to-end at the guard: with the real parse, the
    scan succeeds and only tickers a split actually concerns are forced.
    Before the fix every fresh ticker came back 'split scan unavailable'."""
    _client(monkeypatch)
    fresh = {
        "HUBB": ("reference/price_cache/HUBB.parquet", WED_EOD),
        "AAPL": ("reference/price_cache/AAPL.parquet", WED_EOD),
    }

    class _NoReads:
        def get_object(self, **_k):  # neither ticker has a split in the window
            raise AssertionError("no parquet read expected")

    forced = prices._split_guard(_NoReads(), "b", fresh, datetime(2026, 9, 24).date())
    assert forced == {}


def test_split_guard_still_forces_a_fractional_split_ticker(monkeypatch):
    """A fractional split written after the parquet is still a reason to
    re-fetch: the fix makes the action buildable, it does not drop it."""
    _client(monkeypatch)
    # Written 2026-09-09 EOD: holds at most 09-09, before FRAC's 09-10 ex_date.
    fresh = {"FRAC": ("reference/price_cache/FRAC.parquet",
                      datetime(2026, 9, 9, 23, 30, tzinfo=timezone.utc))}
    forced = prices._split_guard(object(), "b", fresh, datetime(2026, 9, 24).date())
    assert set(forced) == {"FRAC"}
    assert "0.5:1" in forced["FRAC"] and "before the ex_date" in forced["FRAC"]


def test_split_scan_failure_still_refreshes_everything():
    """Unchanged fallback: a scan that cannot be read still forces every fresh
    ticker rather than skipping blind (I11518)."""
    def broken(start, end):
        raise ValueError("unreadable")

    fresh = {"AAPL": ("reference/price_cache/AAPL.parquet", WED_EOD)}
    forced = prices._split_guard(
        object(), "b", fresh, datetime(2026, 9, 24).date(), split_scan=broken,
    )
    assert forced == {"AAPL": "split scan unavailable this run"}
