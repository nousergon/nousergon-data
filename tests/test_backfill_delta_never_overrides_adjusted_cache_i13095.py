"""alpha-engine-config-I13095: the weekly backfill must not let a raw
``staging/daily_closes`` row replace a dividend-adjusted price-cache row.

Measured 2026-11-02 (ArcticDB ``universe``, NLY): as of D31's 2026-10-16 read,
rows from 2026-09-18 on were raw daily captures (09-18 close 21.05) while every
older row was ``auto_adjust=True``-adjusted (09-17 close 20.52 against a raw
~21.3). The weekly 10-24 log shows why: ``delta window pinned by 1 ticker(s)
whose cache ends 2026-04-07: ['HOLX']`` — one delisted cache pins the delta
window, so every readable daily_closes object (2026-09-28 -> 10-23) overrode
the adjusted cache for EVERY ticker. The resulting raw tail is about 20 trading
days, the ``momentum_20d`` lookback, so on the days the anchor fell inside it
the D31 first write mixed a raw 20-day anchor with an adjusted history (145-160
names off, dividend-sized: NLY +3.8%) while ``return_60d`` stayed clean.
"""

from __future__ import annotations

import io
from unittest.mock import MagicMock

import pandas as pd


def _stub_s3(daily_closes_by_date: dict[str, pd.DataFrame]):
    s3 = MagicMock()

    class _NoSuchKey(Exception):
        pass

    s3.exceptions.NoSuchKey = _NoSuchKey

    def _get_object(Bucket, Key):
        for date, df in daily_closes_by_date.items():
            if Key == f"staging/daily_closes/{date}.parquet":
                buf = io.BytesIO()
                df.to_parquet(buf, engine="pyarrow")
                buf.seek(0)
                return {"Body": MagicMock(read=lambda buf=buf: buf.read())}
        raise _NoSuchKey(f"key={Key} not stubbed")

    s3.get_object.side_effect = _get_object
    return s3


def _frame(end: str, closes: list[float]) -> pd.DataFrame:
    idx = pd.bdate_range(end=end, periods=len(closes))
    return pd.DataFrame(
        {"Open": closes, "High": closes, "Low": closes, "Close": closes, "Volume": 1_000_000.0},
        index=idx,
    )


def _closes_file(rows: dict[str, float]) -> pd.DataFrame:
    df = pd.DataFrame(
        {t: {"Open": c, "High": c, "Low": c, "Close": c, "Volume": 1.0} for t, c in rows.items()}
    ).T
    df.index.name = "ticker"
    return df


def _scenario():
    # NLY's adjusted cache runs to Fri 2026-10-23; the raw captures for 10-21 and
    # 10-22 sit ~3.7% above it (an ex-dividend between them and the cache refresh).
    # STALE is a delisted name whose cache ends 10-20, pinning the delta window.
    price_data = {
        "NLY": _frame("2026-10-23", [19.50, 19.60, 19.70, 19.80, 19.90]),
        "STALE": _frame("2026-10-20", [10.0, 10.0, 10.0]),
    }
    daily = {
        "2026-10-21": _closes_file({"NLY": 20.43}),
        "2026-10-22": _closes_file({"NLY": 20.53}),
        "2026-10-23": _closes_file({"NLY": 19.90}),
        "2026-10-26": _closes_file({"NLY": 20.10}),
    }
    return price_data, _stub_s3(daily)


def test_weekly_path_keeps_adjusted_cache_rows_and_extends_past_cache_end():
    from features import compute as _c

    price_data, s3 = _scenario()
    out, _ = _c._apply_daily_delta(
        s3, "b", "2026-10-26", price_data, registry=None, override_base=False,
    )
    nly = out["NLY"]["Close"]
    # The adjusted cache rows survive the pinned window untouched...
    assert nly.loc["2026-10-21"] == 19.70
    assert nly.loc["2026-10-22"] == 19.80
    # ...and the delta still bridges the gap past the ticker's own cache end.
    assert nly.index[-1] == pd.Timestamp("2026-10-26")
    assert nly.loc["2026-10-26"] == 20.10
    assert not out["NLY"].index.has_duplicates
    assert set(out["NLY"]["source"].loc[:"2026-10-23"]) == {"yfinance"}


def test_weekly_backfill_calls_delta_with_override_base_false():
    """The weekly writer is the one that restates ArcticDB history, so it is the
    call site that must opt out of overriding (the daily D31 default is unchanged)."""
    from pathlib import Path

    src = (Path(__file__).resolve().parents[1] / "builders" / "backfill.py").read_text()
    call = src[src.index("price_data, split_tickers = _apply_daily_delta("):]
    call = call[: call.index(")") + 1]
    assert "override_base=False" in call


def test_daily_default_still_lets_delta_rows_win():
    from features import compute as _c

    price_data, s3 = _scenario()
    out, _ = _c._apply_daily_delta(s3, "b", "2026-10-26", price_data, registry=None)
    assert out["NLY"]["Close"].loc["2026-10-21"] == 20.43
