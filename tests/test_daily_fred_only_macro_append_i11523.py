"""The daily append keeps macro/TWO, macro/BAA10Y and macro/HYOAS current
through FRED alone (alpha-engine-config-I11523).

``weekly_collector._MACRO_DAILY_TICKERS`` carried only the four caret indices,
so between Saturday backfills nothing added a TWO / BAA10Y / HYOAS bar to the
ArcticDB ``macro`` library. The naive fix — add ``TWO`` to that list — is
unsafe: ``staging/daily_closes`` is keyed by bare ticker, ``TWO`` is also the
Two Harbors equity, and a FRED miss there falls through to yfinance, which
answers ``TWO`` with the equity.

These tests pin the shape that replaced it:

* a FRED miss for TWO is a NAMED miss and never reaches yfinance or polygon;
* the daily append writes TWO and BAA10Y (and HYOAS) bars, on FRED's own
  observation dates, only past the stored series' last row;
* the equity ``TWO`` is untouched — its universe bar carries the equity close,
  and the FRED-only symbols never enter the daily_closes ticker namespace.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pandas as pd
import pytest

import tests.test_daily_append_missing_from_closes as harness


class _FakeMacroLib:
    """In-memory stand-in for the ArcticDB ``macro`` library: read / update /
    write / list_symbols with ArcticDB's date-range ``update`` semantics."""

    def __init__(self, frames: dict[str, pd.DataFrame]):
        self.frames = {k: v.copy() for k, v in frames.items()}
        self.updates: list[tuple[str, pd.DataFrame]] = []

    def read(self, symbol):
        if symbol not in self.frames:
            raise KeyError(f"No symbol {symbol}")
        return MagicMock(spec=[], data=self.frames[symbol].copy())

    def update(self, symbol, df, **_):
        self.updates.append((symbol, df.copy()))
        old = self.frames.get(symbol)
        if old is None:
            self.frames[symbol] = df.copy()
            return
        lo, hi = df.index.min(), df.index.max()
        kept = old[(old.index < lo) | (old.index > hi)]
        self.frames[symbol] = pd.concat([kept, df]).sort_index()

    def write(self, symbol, df, **_):
        self.frames[symbol] = df.copy()

    def list_symbols(self):
        return sorted(self.frames)


def _series(start: str, end: str, value: float) -> pd.DataFrame:
    idx = pd.bdate_range(start, end)
    df = pd.DataFrame({"Close": value}, index=idx)
    df.index.name = "date"
    return df


def _fred_obs(dates: list[str], values: list[float]) -> pd.Series:
    return pd.Series(values, index=pd.DatetimeIndex(dates), dtype=float)


# ── FRED miss never falls through ────────────────────────────────────────────


def test_fred_miss_for_two_is_named_and_never_calls_yfinance(monkeypatch):
    """The REAL fetch seam, with FRED down: TWO is a named miss, and neither
    yfinance nor polygon is ever asked for anything."""
    from builders import daily_append as _da
    import collectors.daily_closes as dc
    import collectors.fred_history as fh

    def _boom(*a, **k):
        raise AssertionError(
            "a FRED-only macro symbol reached a market-data source — TWO would "
            "resolve to the Two Harbors equity"
        )

    monkeypatch.setattr(dc, "_fetch_yfinance_closes", _boom)
    monkeypatch.setattr(dc, "_fetch_polygon_closes_per_ticker", _boom)
    import yfinance

    monkeypatch.setattr(yfinance, "download", _boom)
    monkeypatch.setattr(yfinance, "Ticker", _boom)

    import requests

    requested: list[str] = []

    def _fred_down(url, params=None, **k):
        requested.append(params["series_id"])
        raise requests.exceptions.ConnectionError("FRED unreachable")

    monkeypatch.setenv("FRED_API_KEY", "test-key")
    monkeypatch.setattr(fh.requests, "get", _fred_down)
    monkeypatch.setattr(fh.time, "sleep", lambda *_: None)

    before = _series("2026-08-03", "2026-09-18", 3.55)
    lib = _FakeMacroLib({"TWO": before, "BAA10Y": before, "HYOAS": before})

    out = _da._append_fred_only_macro_series(lib, "2026-09-23")

    assert set(out["missing"]) == {"TWO", "BAA10Y", "HYOAS"}
    assert "FRED DGS2" in out["missing"]["TWO"]
    assert out["appended"] == {}
    assert lib.updates == []
    pd.testing.assert_frame_equal(lib.frames["TWO"], before)
    # Every request went to FRED by series id — never by the bare symbol.
    assert set(requested) == {"DGS2", "BAA10Y", "BAMLH0A0HYM2"}
    assert "TWO" not in requested


def test_fred_miss_for_one_symbol_does_not_stop_the_others():
    from builders import daily_append as _da

    lib = _FakeMacroLib({
        s: _series("2026-08-03", "2026-09-18", 1.0) for s in ("TWO", "BAA10Y", "HYOAS")
    })

    def _fetch(series_id, date_str):
        if series_id == "DGS2":
            raise RuntimeError("FRED returned no observations for DGS2")
        return _fred_obs(["2026-09-21", "2026-09-22"], [1.8, 1.9])

    out = _da._append_fred_only_macro_series(lib, "2026-09-23", fetch=_fetch)

    assert list(out["missing"]) == ["TWO"]
    assert "DGS2" in out["missing"]["TWO"]
    assert set(out["appended"]) == {"BAA10Y", "HYOAS"}


def test_unseeded_symbol_is_a_named_miss_not_a_stub_series():
    """A two-week window must not become a stub the consumer reads as present;
    the weekly backfill seeds the full history."""
    from builders import daily_append as _da

    lib = _FakeMacroLib({"HYOAS": _series("2026-08-03", "2026-09-18", 3.0)})
    fetched: list[str] = []

    def _fetch(series_id, date_str):
        fetched.append(series_id)
        return _fred_obs(["2026-09-21"], [3.1])

    out = _da._append_fred_only_macro_series(lib, "2026-09-23", fetch=_fetch)

    assert "unseeded" in out["missing"]["TWO"]
    assert "unseeded" in out["missing"]["BAA10Y"]
    assert "TWO" not in lib.frames and "BAA10Y" not in lib.frames
    assert fetched == ["BAMLH0A0HYM2"]


# ── the append itself ────────────────────────────────────────────────────────


def test_appends_two_and_baa10y_on_fred_observation_dates():
    from builders import daily_append as _da

    lib = _FakeMacroLib({
        "TWO": _series("2026-08-03", "2026-09-17", 3.50),
        "BAA10Y": _series("2026-08-03", "2026-09-17", 1.70),
        "HYOAS": _series("2026-08-03", "2026-09-17", 2.90),
    })
    fred = {
        # Overlaps the stored tail (09-16/09-17 must not be rewritten) and
        # carries an observation past the run date (must not be written).
        "DGS2": _fred_obs(
            ["2026-09-16", "2026-09-17", "2026-09-18", "2026-09-21", "2026-09-22", "2026-09-24"],
            [9.0, 9.0, 3.52, 3.55, 3.58, 9.0],
        ),
        "BAA10Y": _fred_obs(["2026-09-17", "2026-09-18", "2026-09-21"], [9.0, 1.72, 1.74]),
        "BAMLH0A0HYM2": _fred_obs(["2026-09-18"], [2.95]),
    }

    out = _da._append_fred_only_macro_series(
        lib, "2026-09-23", fetch=lambda sid, d: fred[sid]
    )

    assert out["missing"] == {}
    assert out["appended"]["TWO"] == ["2026-09-18", "2026-09-21", "2026-09-22"]
    assert out["appended"]["BAA10Y"] == ["2026-09-18", "2026-09-21"]
    assert out["appended"]["HYOAS"] == ["2026-09-18"]

    two = lib.frames["TWO"]["Close"]
    assert two.index.max() == pd.Timestamp("2026-09-22")
    assert two.loc["2026-09-17"] == 3.50  # stored history untouched
    assert two.loc["2026-09-22"] == 3.58
    assert pd.Timestamp("2026-09-24") not in two.index
    assert two.index.is_monotonic_increasing
    assert lib.frames["BAA10Y"]["Close"].loc["2026-09-21"] == 1.74


def test_nothing_newer_is_current_not_missing():
    from builders import daily_append as _da

    lib = _FakeMacroLib({
        s: _series("2026-08-03", "2026-09-22", 1.0) for s in ("TWO", "BAA10Y", "HYOAS")
    })
    out = _da._append_fred_only_macro_series(
        lib, "2026-09-23",
        fetch=lambda sid, d: _fred_obs(["2026-09-21", "2026-09-22"], [2.0, 2.0]),
    )
    assert out["missing"] == {}
    assert out["appended"] == {}
    assert sorted(out["current"]) == ["BAA10Y", "HYOAS", "TWO"]
    assert lib.updates == []


# ── end to end through daily_append, with the equity TWO in the universe ─────


def test_daily_append_writes_fred_macro_and_leaves_equity_two_alone(monkeypatch):
    from builders import daily_append as _da

    date_str = "2026-04-28"
    universe_lib, harness_macro = harness._patch_targets(
        monkeypatch,
        universe_symbols=["AAPL", "TWO"],
        closes_tickers=["AAPL", "TWO"],
    )
    # The equity TWO's close in today's daily_closes — distinct from any
    # DGS2 value so a crossed wire in either direction is visible (and inside
    # the harness bar's High/Low so the quality gate admits it).
    closes = _da._load_daily_closes(None, None, date_str)
    closes["TWO"] = dict(closes["TWO"], Close=100.37)
    monkeypatch.setattr(_da, "_load_daily_closes", lambda *a, **k: closes)

    stored = harness_macro.read.return_value.data  # history through 2026-04-28
    frames = {s: stored for s in (
        "SPY", "VIX", "VIX3M", "TNX", "IRX", "GLD", "USO",
        "XLB", "XLC", "XLE", "XLF", "XLI", "XLK", "XLP", "XLRE", "XLU", "XLV", "XLY",
    )}
    for sym in ("TWO", "BAA10Y", "HYOAS"):
        frames[sym] = _series("2026-03-02", "2026-04-23", 2.0)
    macro_lib = _FakeMacroLib(frames)
    monkeypatch.setattr(_da, "get_macro_lib", lambda *a, **k: macro_lib)

    fred = {
        "DGS2": _fred_obs(["2026-04-24", "2026-04-27"], [3.91, 3.93]),
        "BAA10Y": _fred_obs(["2026-04-24", "2026-04-27"], [1.61, 1.62]),
        "BAMLH0A0HYM2": _fred_obs(["2026-04-24"], [3.05]),
    }
    monkeypatch.setattr(_da, "_fetch_fred_only_series", lambda sid, d: fred[sid])

    result = _da.daily_append(date_str=date_str)

    assert result["status"] == "ok"
    assert result["fred_macro"]["missing"] == {}
    assert result["fred_macro"]["appended"]["TWO"] == ["2026-04-24", "2026-04-27"]
    assert result["fred_macro"]["appended"]["BAA10Y"] == ["2026-04-24", "2026-04-27"]

    two_macro = macro_lib.frames["TWO"]["Close"]
    assert two_macro.loc["2026-04-27"] == 3.93
    assert 100.37 not in set(two_macro.values), "equity TWO close leaked into macro/TWO"
    assert macro_lib.frames["BAA10Y"]["Close"].loc["2026-04-27"] == 1.62

    # The equity TWO's universe bar is the equity close, never a DGS2 value.
    two_universe = []
    for call in universe_lib.method_calls:
        for arg in [*call.args, *call.kwargs.values()]:
            for p in arg if isinstance(arg, list) else [arg]:
                if getattr(p, "symbol", None) == "TWO" and hasattr(p, "data"):
                    two_universe.append(p.data)
    assert two_universe, "the equity TWO was not written to the universe"
    for df in two_universe:
        assert df["Close"].iloc[-1] == 100.37


def test_dry_run_records_skip_and_fetches_nothing(monkeypatch):
    """A dry run touches no library and asks FRED for nothing."""
    from builders import daily_append as _da

    harness._patch_targets(
        monkeypatch, universe_symbols=["AAPL"], closes_tickers=["AAPL"],
    )

    def _no_fetch(*a, **k):
        raise AssertionError("dry run fetched FRED")

    monkeypatch.setattr(_da, "_fetch_fred_only_series", _no_fetch)
    result = _da.daily_append(date_str="2026-04-28", dry_run=True)
    assert result["fred_macro"] == {"skipped": "dry_run"}


# ── the namespace guard ──────────────────────────────────────────────────────


def test_fred_only_series_never_enter_the_daily_closes_ticker_list():
    """``staging/daily_closes`` shares its bare-ticker namespace with equities
    and falls back to yfinance on a FRED miss. The FRED-only symbols must stay
    out of it, whichever spelling."""
    from collectors.fred_history import FRED_HISTORY_MAP, FRED_ONLY_MACRO_SERIES
    from weekly_collector import _MACRO_DAILY_TICKERS

    requested = {t.lstrip("^") for t in _MACRO_DAILY_TICKERS}
    leaked = sorted(requested & set(FRED_ONLY_MACRO_SERIES))
    assert not leaked, f"FRED-only macro symbols in _MACRO_DAILY_TICKERS: {leaked}"

    assert FRED_ONLY_MACRO_SERIES == {
        "TWO": "DGS2", "HYOAS": "BAMLH0A0HYM2", "BAA10Y": "BAA10Y",
    }
    for sym, sid in FRED_ONLY_MACRO_SERIES.items():
        assert FRED_HISTORY_MAP[sym] == sid


@pytest.mark.parametrize("sym", ["TWO", "BAA10Y", "HYOAS"])
def test_every_fred_only_series_is_also_backfilled_weekly(sym):
    """The daily append only extends; the weekly backfill must seed it."""
    from builders.backfill import _RAW_MACRO_SERIES

    assert sym in _RAW_MACRO_SERIES
