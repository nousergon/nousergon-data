"""A replay of trading day D reproduces D's content, whatever day it runs on.

`alpha-engine-config-I11216`. `collectors/metron_market_data.py` selected
CONTENT with the wall clock in two places:

    _yfinance_earnings   "next earnings date >= utcnow()"
    _fred_series_history "observation_start = utcnow() - lookback_years"

so replaying 2026-09-18 on 2026-09-20 dropped the 15 symbols that reported on
09-18/09-19 and returned every FRED series one observation short -- under an
artifact still stamped `as_of: 2026-09-18`.

WHY THE EXISTING TESTS MISSED IT: they assert the `as_of` FIELD, which was
always correct, and they inject sources that are deterministic by construction,
so no fake clock could change their answer. A test of this defect has to move
the wall clock while holding `run_date` fixed, and assert on the CONTENT. That
is what the tests here do: each drives the real windowing function under two
different wall clocks and requires one answer.
"""

from __future__ import annotations

import datetime as dt
import sys
import types
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import collectors.metron_market_data as mmd  # noqa: E402

RUN_DATE = dt.date(2026, 9, 18)

#: Two earnings dates straddling the replay lag: one BEFORE the day the replay
#: actually ran (2026-09-20) and one after. Anchored to the run date, the
#: earlier one is the answer; anchored to the wall clock it vanishes, which is
#: the 15-symbol drop.
EARNINGS_DATES = ["2026-09-19", "2026-11-04"]


class _FakeTicker:
    def __init__(self, symbol: str) -> None:
        self.symbol = symbol

    def get_earnings_dates(self, limit: int = 8):
        return pd.DataFrame(
            {"EPS Estimate": [1.0] * len(EARNINGS_DATES)},
            index=pd.to_datetime(EARNINGS_DATES),
        )


@pytest.fixture
def fake_yfinance(monkeypatch):
    module = types.ModuleType("yfinance")
    module.Ticker = _FakeTicker
    monkeypatch.setitem(sys.modules, "yfinance", module)
    return module


def _at(wall_clock: str):
    """Pin `pandas.Timestamp.utcnow` so a wall-clock read is visible if it happens."""

    class _PinnedTimestamp(pd.Timestamp):
        @classmethod
        def utcnow(cls):  # pragma: no cover - only reached by the defect
            return pd.Timestamp(wall_clock, tz="UTC")

    return _PinnedTimestamp


def test_earnings_window_is_the_run_date_not_the_wall_clock(fake_yfinance, monkeypatch):
    """The defect: replayed on 09-20, a 09-19 reporter drops out of a 09-18 run."""
    on_the_day = mmd._yfinance_earnings(["ANF"], RUN_DATE)

    monkeypatch.setattr(pd, "Timestamp", _at("2026-09-20"), raising=True)
    two_days_later = mmd._yfinance_earnings(["ANF"], RUN_DATE)

    assert on_the_day == {"ANF": "2026-09-19"}
    assert two_days_later == on_the_day, (
        "the same run_date produced different content on a different wall clock"
    )


def test_earnings_excludes_a_date_before_the_run_date(fake_yfinance):
    """The window is 'on or after the run date' — it does not simply take the first."""
    later_run = mmd._yfinance_earnings(["ANF"], dt.date(2026, 9, 30))
    assert later_run == {"ANF": "2026-11-04"}


def test_fred_observation_start_is_derived_from_the_run_date(monkeypatch):
    """`observation_start` is run_date - lookback_years, on any wall clock."""
    seen: list[str] = []

    def _capture(url, timeout=None):  # noqa: ARG001
        from urllib.parse import parse_qs, urlparse

        seen.append(parse_qs(urlparse(url).query)["observation_start"][0])

        class _Response:
            def __enter__(self_inner):
                return self_inner

            def __exit__(self_inner, *exc):
                return False

            def read(self_inner):
                return b'{"observations": []}'

        return _Response()

    import urllib.request

    monkeypatch.setattr(urllib.request, "urlopen", _capture)
    mmd._fred_series_history(["DGS10"], RUN_DATE, "key")

    assert seen == ["2024-09-18"], "two years before the RUN DATE, not before today"


def test_fred_leap_day_run_date_still_resolves():
    """The Feb-29 branch is anchored too — it used to read the wall clock."""
    assert mmd._run_date_anchor("2026-02-28") == dt.date(2026, 2, 28)


def test_an_unparseable_run_date_raises_rather_than_falling_back():
    """A wall-clock fallback here would silently restore the defect."""
    with pytest.raises(ValueError):
        mmd._run_date_anchor("not-a-date")


def test_the_source_protocols_carry_the_anchor():
    """An unanchored source should be impossible to write by accident."""
    import inspect

    for func in (mmd._yfinance_earnings, mmd._fred_series_history):
        assert "as_of" in inspect.signature(func).parameters, (
            f"{func.__name__} must take the run date explicitly"
        )
