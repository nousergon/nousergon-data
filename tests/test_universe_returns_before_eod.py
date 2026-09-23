"""The before-EOD 403 must not halt the weekly pipeline (config-I7714).

MEASURED 2026-08-18, weekly execution `watch-rerun-2026-08-18-3`: DataPhase1 ran
to completion in 2723s and then exited 1 because `universe_returns` raised on a
polygon 403 — `"Attempted to request today's data before end of day"` on the
grouped-daily endpoint for 2026-08-18. `weekly_collector.py` marks any non-ok
collector `partial` and deliberately exits 1, so the pipeline halted. It had not
succeeded since 2026-08-15, which is why every artifact downstream of the weekly
graph was four days stale and the freshness monitor had something real to say
underneath the cadence noise.

The collector walks FORWARD dates from each eval_date, so it asks about the
current session by construction. The request is early, not wrong.

These tests pin BOTH halves: the tolerated shape, and — more importantly — every
403 that must still raise. `polygon_client._get` raises on all 403s deliberately
(data#4496: swallowing them masked the 2026-04-17 to 2026-04-23 VWAP outage by
letting `daily_closes.collect` fall through to yfinance and write VWAP=None for
every stock). This narrowing must not become that swallow.
"""
from __future__ import annotations

from datetime import date

import pytest

from collectors.universe_returns import _grouped_daily_or_empty
from polygon_client import PolygonForbiddenError

_TODAY = date(2026, 8, 18)
_BEFORE_EOD = PolygonForbiddenError(
    "Polygon 403 on /v2/aggs/grouped/locale/us/market/stocks/2026-08-18: "
    "Attempted to request today's data before end of day"
)


class _Client:
    def __init__(self, outcome):
        self.outcome = outcome
        self.calls = []

    def get_grouped_daily(self, date_str):
        self.calls.append(date_str)
        if isinstance(self.outcome, Exception):
            raise self.outcome
        return self.outcome


def test_before_eod_on_the_current_session_yields_no_bars():
    """The tolerated shape: today's bar has not published yet."""
    client = _Client(_BEFORE_EOD)
    assert _grouped_daily_or_empty(client, "2026-08-18", today=_TODAY) == {}


def test_before_eod_on_a_future_date_yields_no_bars():
    """A forward date beyond today is the same condition, further out."""
    client = _Client(_BEFORE_EOD)
    assert _grouped_daily_or_empty(client, "2026-08-25", today=_TODAY) == {}


def test_before_eod_on_a_PAST_date_still_raises():
    """The session is over, so 'before end of day' cannot be the true
    explanation — something else is wrong and must not read as absent data."""
    client = _Client(_BEFORE_EOD)
    with pytest.raises(PolygonForbiddenError):
        _grouped_daily_or_empty(client, "2026-08-11", today=_TODAY)


@pytest.mark.parametrize("message", [
    "Polygon 403 on /v2/aggs/grouped/...: Not authorized",
    "Polygon 403 on /v2/aggs/grouped/...: Unknown API Key",
    "Polygon 403 on /v2/aggs/grouped/...: Your plan does not include this data",
])
def test_every_other_403_still_raises(message):
    """An invalid key, a revoked plan, or an entitlement the account does not
    hold are exactly the failures the raise exists for."""
    client = _Client(PolygonForbiddenError(message))
    with pytest.raises(PolygonForbiddenError):
        _grouped_daily_or_empty(client, "2026-08-18", today=_TODAY)


def test_a_malformed_date_string_raises_rather_than_being_tolerated():
    """If the date cannot be parsed, the 'has it closed yet' question cannot be
    answered, so the 403 stands."""
    client = _Client(_BEFORE_EOD)
    with pytest.raises(PolygonForbiddenError):
        _grouped_daily_or_empty(client, "not-a-date", today=_TODAY)


def test_a_successful_call_is_passed_straight_through():
    client = _Client({"SPY": {"close": 500.0}})
    assert _grouped_daily_or_empty(client, "2026-08-11", today=_TODAY) == {
        "SPY": {"close": 500.0}
    }
    assert client.calls == ["2026-08-11"]


def test_non_403_errors_are_not_caught():
    """Only PolygonForbiddenError is in scope; a transport or 5xx failure keeps
    its own handling."""
    class _Boom(Exception):
        pass

    client = _Client(_Boom("connection reset"))
    with pytest.raises(_Boom):
        _grouped_daily_or_empty(client, "2026-08-18", today=_TODAY)


# ── alpha-engine-config-I11445: the live "today" is the exchange's date ───────
#
# The 2026-09-23 rehearsal launched at 00:00 UTC = 20:00 ET on 2026-09-22. The
# UTC box's date.today() said 2026-09-23, so 2026-09-22 read as a PAST date,
# the before-EOD 403 Polygon returned for it re-raised, and DataPhase1 exited 1.


def test_market_today_is_the_eastern_date_in_the_utc_evening_gap():
    from datetime import datetime, timezone

    from collectors.universe_returns import _market_today

    at = datetime(2026, 9, 23, 0, 35, tzinfo=timezone.utc)  # 20:35 ET, 9/22
    assert _market_today(at) == date(2026, 9, 22)


def test_market_today_rolls_at_eastern_midnight():
    from datetime import datetime, timezone

    from collectors.universe_returns import _market_today

    assert _market_today(datetime(2026, 9, 23, 3, 59, tzinfo=timezone.utc)) == date(2026, 9, 22)
    assert _market_today(datetime(2026, 9, 23, 4, 1, tzinfo=timezone.utc)) == date(2026, 9, 23)
    # The production slot (09:00 UTC) is unchanged.
    assert _market_today(datetime(2026, 9, 19, 9, 0, tzinfo=timezone.utc)) == date(2026, 9, 19)


def test_the_rehearsal_request_is_tolerated_on_the_eastern_anchor():
    """The exact 2026-09-23 request: 9/22's grouped-daily at 20:35 ET."""
    from datetime import datetime, timezone

    from collectors.universe_returns import _market_today

    today = _market_today(datetime(2026, 9, 23, 0, 35, tzinfo=timezone.utc))
    client = _Client(PolygonForbiddenError(
        "Polygon 403 on /v2/aggs/grouped/locale/us/market/stocks/2026-09-22: "
        "Attempted to request today's data before end of day"
    ))
    assert _grouped_daily_or_empty(client, "2026-09-22", today=today) == {}
