"""How ``_apply_daily_delta`` reports a missing ``staging/daily_closes`` date.

rehearsal-2026-09-25-1 DataPhase1: the delta window opens at the OLDEST
price-cache ticker's last date, and HOLX (delisted; cache ends 2026-04-07)
pinned it at 2026-04-08. ``staging/`` objects expire after 7 days, so ~105
trading days had no object and each logged "daily_closes/<d>.parquet missing
(market holiday?)" — wrong for every one of them (they were trading days
beyond staging retention), and any real gap would have been one line in that
wall. The loaded data was right; the reporting was not.
"""

from __future__ import annotations

import logging

import pandas as pd

from tests.test_apply_daily_delta_min_last_date import (
    _daily_closes_frame,
    _ohlcv,
    _stub_s3_with_daily_closes,
)


def _day(close: float) -> pd.DataFrame:
    return _daily_closes_frame(
        {
            "AAPL": {"Open": close, "High": close, "Low": close, "Close": close, "Volume": 1},
            "SPY": {"Open": close, "High": close, "Low": close, "Close": close, "Volume": 1},
        }
    )


def _run(caplog, price_data, staged: dict[str, pd.DataFrame], date_str: str):
    from features import compute as _c

    s3 = _stub_s3_with_daily_closes(staged)
    with caplog.at_level(logging.DEBUG, logger=_c.log.name):
        out, _ = _c._apply_daily_delta(s3, "bucket", date_str, price_data)
    warnings = [r.getMessage() for r in caplog.records if r.levelno >= logging.WARNING]
    infos = [r.getMessage() for r in caplog.records if r.levelno == logging.INFO]
    return out, warnings, infos


def test_a_delisted_ticker_pinning_the_window_is_one_warning_not_one_per_day(caplog):
    # Active tickers are current to 2026-09-11; HOLX stopped on 2026-04-07.
    price_data = {
        "AAPL": _ohlcv("2026-09-11"),
        "SPY": _ohlcv("2026-09-11"),
        "HOLX": _ohlcv("2026-04-07"),
    }
    # staging/ still holds only the last week, Labor Day 09-07 has no object.
    staged = {d: _day(200.0 + i) for i, d in enumerate(
        ["2026-09-04", "2026-09-08", "2026-09-09", "2026-09-10", "2026-09-11",
         "2026-09-14", "2026-09-15"]
    )}

    out, warnings, infos = _run(caplog, price_data, staged, "2026-09-15")

    # The data outcome is unchanged: active tickers advance to the last staged day.
    assert out["AAPL"].index.max() == pd.Timestamp("2026-09-15")
    assert out["SPY"].index.max() == pd.Timestamp("2026-09-15")

    assert not any("market holiday?" in w for w in warnings), warnings
    per_day = [w for w in warnings if w.startswith("daily_closes/")]
    assert per_day == [], per_day
    [summary] = [w for w in warnings if "beyond the delta's reach" in w]
    assert "2026-04-08 -> 2026-09-03" in summary, summary
    assert "(2026-09-04)" in summary, summary
    # The line that names who stretched the window.
    assert any("pinned by 1 ticker(s)" in i and "HOLX" in i for i in infos), infos


def test_a_trading_day_missing_inside_the_readable_window_is_a_gap(caplog):
    price_data = {"AAPL": _ohlcv("2026-09-11"), "SPY": _ohlcv("2026-09-11")}
    # 09-15 (a Tuesday, an NYSE trading day) is missing between present days.
    staged = {"2026-09-14": _day(1.0), "2026-09-16": _day(2.0)}

    _, warnings, _ = _run(caplog, price_data, staged, "2026-09-16")

    assert warnings == [
        "daily_closes/2026-09-15.parquet missing on an NYSE trading day inside "
        "the readable staging window — a gap in the daily_closes delta"
    ], warnings


def test_a_holiday_inside_the_window_is_not_reported_as_a_gap(caplog):
    price_data = {"AAPL": _ohlcv("2026-09-04"), "SPY": _ohlcv("2026-09-04")}
    # Monday 2026-09-07 is Labor Day.
    staged = {"2026-09-08": _day(1.0)}

    _, warnings, _ = _run(caplog, price_data, staged, "2026-09-08")

    assert warnings == [], warnings
