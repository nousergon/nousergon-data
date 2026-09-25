"""alpha-engine-config-I11578: a rejected Yahoo session (HTTP 401) is refreshed
and retried, and a coverage collapse fails the unit instead of publishing.

The incident: on 2026-09-24 at 22:54 UTC every crumb-authenticated yfinance call
on the shadow box got HTTP 401. yfinance hides that error by default, so
``Ticker.info`` returned ``{}``. D22 wrote 0 of 75 sectors and D24 wrote 43 of 75
fundamentals, and both overwrote a healthy ``latest.json``.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock

import pytest
import yfinance

from collectors import metron_market_data as mmd
from collectors import short_interest, yahoo_session
from collectors.yahoo_session import YahooAuthError, call_yahoo, is_auth_failure, yahoo_info


class _HTTPError(Exception):
    """The shape yfinance raises: ``str()`` is ``HTTP Error <code>: ``, with a
    ``.response.status_code``."""

    def __init__(self, code: int):
        super().__init__(f"HTTP Error {code}: ")
        self.response = MagicMock(status_code=code)


class _Vendor:
    """A Yahoo whose session is rejected until it is refreshed ``bad_for`` times.

    While rejected it behaves like yfinance does: with ``hide_exceptions`` on (the
    default) ``.info`` is ``{}``, and with it off ``.info`` raises HTTP 401.
    Symbols in ``unlisted`` are a 404 whatever the session state.
    """

    def __init__(self, info: dict[str, dict], *, bad_for: int = 0, unlisted: tuple = ()):
        self.info_by_symbol = info
        self.bad_for = bad_for
        self.unlisted = set(unlisted)
        self.refreshes = 0
        self.requests: list[tuple[str, bool]] = []

    def refresh(self):
        self.refreshes += 1
        self.bad_for = max(0, self.bad_for - 1)

    def ticker(self, symbol):
        vendor = self

        class _T:
            @property
            def info(self):
                hidden = yfinance.config.debug.hide_exceptions
                vendor.requests.append((symbol, hidden))
                if vendor.bad_for:
                    if hidden:
                        return {}
                    raise _HTTPError(401)
                if symbol in vendor.unlisted:
                    if hidden:
                        return {}
                    raise _HTTPError(404)
                return dict(vendor.info_by_symbol.get(symbol, {}))

        return _T()


@pytest.fixture
def no_sleep(monkeypatch):
    slept: list[float] = []
    monkeypatch.setattr(yahoo_session, "_sleep", slept.append)
    return slept


def _install(monkeypatch, vendor: _Vendor):
    monkeypatch.setattr(yfinance, "Ticker", vendor.ticker)
    monkeypatch.setattr(yahoo_session, "refresh_yahoo_session", vendor.refresh)


# ── classification of errors ─────────────────────────────────────────────────


def test_auth_failure_is_a_401_and_nothing_else():
    assert is_auth_failure(_HTTPError(401))
    assert is_auth_failure(Exception("HTTP Error 401: "))  # the 09-24 funds_data message
    assert is_auth_failure(Exception('{"code":"Unauthorized","description":"Invalid Crumb"}'))
    assert not is_auth_failure(_HTTPError(404))
    assert not is_auth_failure(Exception("possibly delisted; no price data found"))


# ── the retry contract ───────────────────────────────────────────────────────


def test_a_non_empty_answer_is_one_request(monkeypatch, no_sleep):
    vendor = _Vendor({"AAPL": {"sector": "Technology"}})
    _install(monkeypatch, vendor)
    assert yahoo_info("AAPL") == {"sector": "Technology"}
    assert vendor.requests == [("AAPL", True)]
    assert vendor.refreshes == 0


def test_an_unlisted_symbol_stays_empty_without_a_refresh(monkeypatch, no_sleep):
    """The one diagnostic re-ask surfaces a 404, which is not the session's fault."""
    vendor = _Vendor({}, unlisted=("912810UJ5",))
    _install(monkeypatch, vendor)
    assert yahoo_info("912810UJ5") == {}
    assert vendor.requests == [("912810UJ5", True), ("912810UJ5", False)]
    assert vendor.refreshes == 0 and no_sleep == []
    assert yfinance.config.debug.hide_exceptions is True  # restored


def test_a_hidden_401_is_refreshed_and_retried(monkeypatch, no_sleep):
    vendor = _Vendor({"AAPL": {"sector": "Technology"}}, bad_for=1)
    _install(monkeypatch, vendor)
    assert yahoo_info("AAPL") == {"sector": "Technology"}
    assert vendor.refreshes == 1
    assert no_sleep == [yahoo_session.BACKOFF_S]
    assert yfinance.config.debug.hide_exceptions is True


def test_a_session_that_stays_rejected_raises_after_bounded_attempts(monkeypatch, no_sleep):
    vendor = _Vendor({"AAPL": {"sector": "Technology"}}, bad_for=99)
    _install(monkeypatch, vendor)
    with pytest.raises(YahooAuthError, match="3 consecutive attempts"):
        yahoo_info("AAPL")
    assert vendor.refreshes == yahoo_session.MAX_ATTEMPTS - 1
    assert no_sleep == [2.0, 4.0]


def test_a_raised_401_is_retried_and_other_errors_propagate(monkeypatch, no_sleep):
    """``funds_data`` raises its 401 even with errors hidden."""
    calls = {"n": 0}

    def weights():
        calls["n"] += 1
        if calls["n"] == 1:
            raise _HTTPError(401)
        return {"technology": 0.3}

    monkeypatch.setattr(yahoo_session, "refresh_yahoo_session", lambda: None)
    assert call_yahoo(weights, label="funds_data[SPY]") == {"technology": 0.3}

    def broken():
        raise KeyError("schema moved")

    with pytest.raises(KeyError):
        call_yahoo(broken, label="x")


def test_refresh_drops_crumb_cookie_persisted_cookie_and_response_cache(monkeypatch):
    """Pins the yfinance internals the refresh relies on, for the installed version."""
    from yfinance import cache as yf_cache
    from yfinance.data import YfData

    stored: list = []
    monkeypatch.setattr(yf_cache, "get_cookie_cache", lambda: MagicMock(store=lambda *a: stored.append(a)))
    data = YfData()
    old_session = data._session
    data._crumb, data._cookie = "stale-crumb", "stale-cookie"
    yahoo_session.refresh_yahoo_session()
    assert stored == [("curlCffi", None)]
    assert data._crumb is None and data._cookie is None
    assert data._session is not old_session
    assert YfData.cache_get.cache_info().currsize == 0


# ── the incident, end to end through the metron units ────────────────────────

UNIVERSE = [f"S{i:02d}" for i in range(75)]
SECTOR_INFO = {s: {"sector": "Technology", "country": "United States", "trailingPE": 20.0} for s in UNIVERSE[:61]}


def test_d22_sectors_recover_from_the_09_24_401(monkeypatch, no_sleep):
    vendor = _Vendor(SECTOR_INFO, bad_for=1)
    _install(monkeypatch, vendor)
    sectors, countries = mmd._yfinance_classification(UNIVERSE)
    assert len(sectors) == 61 and len(countries) == 61
    assert vendor.refreshes == 1


def test_d22_fails_the_unit_when_the_session_stays_rejected(monkeypatch, no_sleep):
    vendor = _Vendor(SECTOR_INFO, bad_for=99)
    _install(monkeypatch, vendor)
    s3 = MagicMock()
    monkeypatch.setattr(mmd, "load_metron_universe", lambda b, c: ([{"yf_symbol": s, "currency": "USD"} for s in UNIVERSE], []))
    with pytest.raises(YahooAuthError):
        mmd.collect_reference(bucket="b", run_date="2026-09-24", s3_client=s3)
    s3.put_object.assert_not_called()


def test_d24_refuses_a_near_empty_artifact(monkeypatch, no_sleep):
    """43/75, the 09-24 count, with no error visible to the fetcher."""
    info = {s: {"trailingPE": 20.0} for s in UNIVERSE[:43]}
    _install(monkeypatch, _Vendor(info))
    s3 = MagicMock()
    monkeypatch.setattr(mmd, "load_metron_universe", lambda b, c: ([{"yf_symbol": s, "currency": "USD"} for s in UNIVERSE], []))
    with pytest.raises(mmd.YfCoverageCollapse, match="43/75"):
        mmd.collect_fundamentals(bucket="b", run_date="2026-09-24", s3_client=s3)
    s3.put_object.assert_not_called()


@pytest.mark.parametrize("covered, raises", [(61, False), (53, False), (52, True), (19, True), (0, True)])
def test_the_collapse_floor(covered, raises):
    got = {s: 1 for s in UNIVERSE[:covered]}
    if raises:
        with pytest.raises(mmd.YfCoverageCollapse):
            mmd._log_yf_coverage("sectors", UNIVERSE, got, floor=mmd.METRON_YF_COVERAGE_FLOOR)
    else:
        mmd._log_yf_coverage("sectors", UNIVERSE, got, floor=mmd.METRON_YF_COVERAGE_FLOOR)


def test_the_floor_ignores_a_request_too_small_to_measure():
    mmd._log_yf_coverage("earnings", ["AAPL", "MSFT"], {}, floor=mmd.METRON_YF_COVERAGE_FLOOR)


def test_short_interest_aborts_instead_of_retrying_every_ticker(monkeypatch, no_sleep):
    vendor = _Vendor({t: {"shortRatio": 1.0} for t in ("A", "B", "C")}, bad_for=99)
    _install(monkeypatch, vendor)
    monkeypatch.setattr(short_interest.boto3, "client", lambda *a, **k: MagicMock())
    with pytest.raises(YahooAuthError, match=r"info\[A\]"):
        short_interest.collect(bucket="b", tickers=["A", "B", "C"], run_date="2026-09-24", inter_request_delay=0.0)
    assert {sym for sym, _ in vendor.requests} == {"A"}


def test_short_interest_is_unchanged_on_a_healthy_session(monkeypatch, no_sleep):
    _install(monkeypatch, _Vendor({t: {"shortRatio": 1.0} for t in ("A", "B")}))
    s3 = MagicMock()
    monkeypatch.setattr(short_interest.boto3, "client", lambda *a, **k: s3)
    result = short_interest.collect(bucket="b", tickers=["A", "B"], run_date="2026-09-24", inter_request_delay=0.0)
    assert result["status"] == "ok"
    assert json.loads(s3.put_object.call_args.kwargs["Body"])["data"]["A"]["short_ratio"] == 1.0
