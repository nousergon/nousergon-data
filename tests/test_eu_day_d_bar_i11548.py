"""alpha-engine-config-I11548 — day D's bar for a non-US listing must be in D20/D21.

Measured on the 2026-09-21/22/23 same-day parity reports: the standalone D20
(``market_data/eod_closes/*``) and D21 (``market_data/close_history/consolidated.json``)
published D-1 as the latest bar for NOVN.SW, RMS.PA and SU.PA while v1 carried D.
Same code on both sides; the only difference was the fetch moment. v1 wrote at
20:09-20:13 UTC, the shadow at 22:45-22:50 UTC — after 00:00 of D+1 in Zurich/Paris.

yfinance reads a date-only ``end`` in the LISTING's timezone
(``yfinance.utils._parse_user_dt``), so ``end = D + 1`` is D 22:00 UTC for those
listings. Once that instant has passed the request covers only the past and the
vendor answers from its finalized daily store, which did not yet hold that
evening's EU bar. The fix asks for ``end = D + 2`` and clips the response to D.

These tests pin: (1) the requested end covers the observed shadow fetch moment in
every listing timezone the universe carries; (2) with a vendor stub that behaves
the way the parity evidence shows, D20 and D21 publish day D for a .SW and a .PA
listing — and the pre-fix window reproduces the incident; (3) a rerun late enough
to see D+1 still publishes D, never D+1.
"""

from __future__ import annotations

from datetime import date

import numpy as np
import pandas as pd
import pytest
from yfinance.utils import _parse_user_dt

import dates
from collectors import metron_market_data as mmd
from dates import history_window, vendor_request_window

D = "2026-09-23"
D_MINUS_1 = "2026-09-22"
D_PLUS_1 = "2026-09-24"

#: Where each stubbed listing trades — what yfinance resolves ``end`` against.
EXCHANGE_TZ = {
    "NOVN.SW": "Europe/Zurich",
    "RMS.PA": "Europe/Paris",
    "SU.PA": "Europe/Paris",
    "D05.SI": "Asia/Singapore",
    "AAPL": "America/New_York",
}

#: The standalone shadow's observed D20 write time on 2026-09-23.
SHADOW_FETCH = pd.Timestamp("2026-09-23T22:50:00Z")
#: v1's observed D20 write time on 2026-09-23.
V1_FETCH = pd.Timestamp("2026-09-23T20:10:00Z")


def _end_instant(end_iso: str, tz: str) -> pd.Timestamp:
    """The instant yfinance itself turns a date-only ``end`` into."""
    return pd.Timestamp(_parse_user_dt(end_iso, tz)).tz_convert("UTC")


# ── the window ───────────────────────────────────────────────────────────────


def test_vendor_request_window_reaches_one_day_past_history_window():
    assert vendor_request_window(D, "10d") == (date(2026, 9, 13), date(2026, 9, 25))
    start, end_excl = history_window(D, "10y")
    assert vendor_request_window(D, "10y") == (start, date(2026, 9, 25))
    assert end_excl == date(2026, 9, 24)


@pytest.mark.parametrize("tz", sorted(set(EXCHANGE_TZ.values())))
def test_requested_end_covers_the_same_evening_fetch_in_every_listing_tz(tz):
    _, end_excl = vendor_request_window(D, "10d")
    for fetch in (V1_FETCH, SHADOW_FETCH):
        assert _end_instant(end_excl.isoformat(), tz) > fetch, (tz, fetch)


@pytest.mark.parametrize("tz", ["Europe/Zurich", "Europe/Paris", "America/New_York"])
def test_requested_end_also_covers_a_next_day_rerun_for_eu_and_us(tz):
    """Not asserted for Asia/Singapore: its padded end is D+1 16:00 UTC, but an SGX
    bar is already in the finalized store by then — v1 read D05.SI's day-D bar at
    20:10 UTC on all three parity days with its (D + 1) end already 4 hours past."""
    _, end_excl = vendor_request_window(D, "10d")
    assert _end_instant(end_excl.isoformat(), tz) > pd.Timestamp("2026-09-24T20:10:00Z")


def test_the_pre_fix_end_had_already_passed_for_eu_listings_at_the_shadow_fetch():
    """Documents the defect: ``end = D + 1`` read in Zurich/Paris time is 22:00 UTC,
    before the shadow fetched and after v1 did — the exact split the parity saw."""
    _, old_end = history_window(D, "10d")
    for sym in ("NOVN.SW", "RMS.PA", "SU.PA"):
        instant = _end_instant(old_end.isoformat(), EXCHANGE_TZ[sym])
        assert V1_FETCH < instant < SHADOW_FETCH, sym
    assert _end_instant(old_end.isoformat(), "America/New_York") > SHADOW_FETCH


# ── a vendor stub that behaves the way the evidence shows ────────────────────


def _vendor_stub(now: pd.Timestamp, *, finalized_through: str, live_session: str, calls: list):
    """``yf.download`` stand-in. The finalized daily store holds sessions up to
    ``finalized_through``; ``live_session``'s bar is appended only while the
    requested range still covers ``now`` (the requested end, read in the
    listing's timezone, is after it) — v1 at 20:10 UTC got D, the shadow at
    22:50 UTC did not, and D-1 was always present. Index is tz-naive session
    dates, as ``yf.download`` returns for daily bars."""

    def _download(tickers, start, end, **kw):
        calls.append({"tickers": tickers, "start": start, "end": end, **kw})
        syms = [tickers] if isinstance(tickers, str) else list(tickers)
        frames = {}
        for sym in syms:
            sessions = [
                d for d in pd.bdate_range(start, finalized_through)
                if pd.Timestamp(start) <= d < pd.Timestamp(end)
            ]
            live = pd.Timestamp(live_session)
            if _end_instant(end, EXCHANGE_TZ[sym]) > now and pd.Timestamp(start) <= live < pd.Timestamp(end):
                sessions.append(live)
            idx = pd.DatetimeIndex(sessions)
            closes = np.array([100.0 + i for i in range(len(idx))])
            frames[sym] = pd.DataFrame(
                {"Open": closes, "High": closes, "Low": closes, "Close": closes,
                 "Adj Close": closes, "Volume": np.ones(len(idx))},
                index=idx,
            )
        if len(syms) == 1:
            return frames[syms[0]]
        return pd.concat(frames, axis=1)

    return _download


EU = ["NOVN.SW", "RMS.PA", "SU.PA"]


@pytest.fixture
def _no_sleep(monkeypatch):
    monkeypatch.setattr(mmd.time, "sleep", lambda *_: None)


def _install(monkeypatch, stub):
    import yfinance

    monkeypatch.setattr(yfinance, "download", stub)


def test_d20_publishes_day_d_for_sw_and_pa_on_a_same_evening_run(monkeypatch, _no_sleep):
    calls: list = []
    _install(monkeypatch, _vendor_stub(
        SHADOW_FETCH, finalized_through=D_MINUS_1, live_session=D, calls=calls,
    ))
    closes = mmd._yfinance_closes(EU + ["AAPL"], trading_day=D)
    assert {sym: closes[sym][1] for sym in EU + ["AAPL"]} == dict.fromkeys(EU + ["AAPL"], D)


def test_d21_series_ends_at_day_d_for_sw_and_pa_on_a_same_evening_run(monkeypatch, _no_sleep):
    calls: list = []
    _install(monkeypatch, _vendor_stub(
        SHADOW_FETCH, finalized_through=D_MINUS_1, live_session=D, calls=calls,
    ))
    series = mmd._yfinance_close_history_dividend_adjusted(EU, "10y", trading_day=D)
    assert {sym: series[sym][-1][0] for sym in EU} == dict.fromkeys(EU, D)
    assert all(series[sym][-2][0] == D_MINUS_1 for sym in EU)


def test_the_pre_fix_window_reproduces_the_missing_eu_bar(monkeypatch, _no_sleep):
    """Negative control: with the request ending at D + 1, the same stub yields
    exactly the parity report's shape — D-1 for the EU listings, D for AAPL."""
    calls: list = []
    _install(monkeypatch, _vendor_stub(
        SHADOW_FETCH, finalized_through=D_MINUS_1, live_session=D, calls=calls,
    ))
    monkeypatch.setattr(dates, "vendor_request_window", history_window)
    closes = mmd._yfinance_closes(EU + ["AAPL"], trading_day=D)
    assert {sym: closes[sym][1] for sym in EU} == dict.fromkeys(EU, D_MINUS_1)
    assert closes["AAPL"][1] == D


def test_a_rerun_that_can_see_d_plus_1_still_publishes_d(monkeypatch, _no_sleep):
    """A ``--date D`` rerun at D+1 20:10 UTC: the widened request now answers with
    D+1's live bar, and the fetch-boundary clip keeps it out (I10893 still holds)."""
    calls: list = []
    _install(monkeypatch, _vendor_stub(
        pd.Timestamp("2026-09-24T20:10:00Z"), finalized_through=D, live_session=D_PLUS_1, calls=calls,
    ))
    closes = mmd._yfinance_closes(EU + ["AAPL"], trading_day=D)
    series = mmd._yfinance_close_history_dividend_adjusted(EU, "10y", trading_day=D)
    assert {sym: closes[sym][1] for sym in EU + ["AAPL"]} == dict.fromkeys(EU + ["AAPL"], D)
    assert {sym: series[sym][-1][0] for sym in EU} == dict.fromkeys(EU, D)
    assert all(c["end"] == "2026-09-25" for c in calls)


def test_fx_request_also_reaches_past_d(monkeypatch, _no_sleep):
    """``{CCY}USD=X`` resolves to Europe/London, whose D+1 midnight is 23:00 UTC in
    summer — ten minutes after the shadow's observed write. Same padded window."""
    calls: list = []

    def _download(tickers, start, end, **kw):
        calls.append({"start": start, "end": end})
        idx = pd.bdate_range("2026-09-14", D_PLUS_1)
        return pd.DataFrame({"Close": np.linspace(1.0, 2.0, len(idx))}, index=idx)

    _install(monkeypatch, _download)
    fx = mmd._yfinance_fx(["CHF"], trading_day=D)
    hist = mmd._yfinance_fx_history(["CHF"], "10y", trading_day=D)
    assert [c["end"] for c in calls] == ["2026-09-25", "2026-09-25"]
    assert hist["CHF"][-1][0] == D
    assert fx["CHF"] == hist["CHF"][-1][1]
