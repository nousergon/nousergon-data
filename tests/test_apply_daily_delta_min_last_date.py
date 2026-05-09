"""Regression test for ``features/compute.py::_apply_daily_delta``.

Locks the 2026-05-09 weekly-SF DataPhase1 incident: when ``prices.collect``
flagged a single ticker as stale and refreshed it via yfinance to the
prior trading day, every OTHER cache parquet still ended at an older
date. The legacy ``max(valid_dates)`` lookup picked the freshly-refreshed
ticker's date as ``slim_last_date``, and on a Saturday run that turned
``bdate_range(slim_last_date+1, today)`` into an empty range — the
delta loader returned ``{}``, every other ticker stayed stuck at the
older cache date, and the backfill regression preflight rejected the
write because planned (5/6) < existing-in-ArcticDB (5/8) across all
macro / sector ETF / sampled-universe symbols.

The fix uses ``min(valid_dates)`` so even one fresh ticker can't
suppress the delta load for the others. Duplicate dates that result
from overlap with the fresh ticker are deduped by ``keep="last"`` in
``_apply_daily_delta``'s combine step (already covered by the existing
backfill happy-path tests).
"""

from __future__ import annotations

import io
from unittest.mock import MagicMock

import numpy as np
import pandas as pd


def _ohlcv(end_date: str, n: int = 5) -> pd.DataFrame:
    idx = pd.bdate_range(end=end_date, periods=n)
    return pd.DataFrame(
        {"Open": 100.0, "High": 101.0, "Low": 99.0, "Close": 100.0, "Volume": 1_000_000},
        index=idx,
    )


def _stub_s3_with_daily_closes(daily_closes_by_date: dict[str, pd.DataFrame]):
    """Build an S3 stub whose ``get_object`` serves parquet bytes for the
    given ``staging/daily_closes/{date}.parquet`` keys; raises NoSuchKey
    for anything else (matches the live boto3 contract)."""
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


def _daily_closes_frame(rows: dict[str, dict]) -> pd.DataFrame:
    """Build a daily_closes parquet body shape: index=ticker, columns
    include Open/High/Low/Close/Volume."""
    df = pd.DataFrame.from_dict(rows, orient="index")
    df.index.name = "ticker"
    return df


def test_apply_daily_delta_uses_min_last_date_when_one_ticker_freshly_refreshed():
    """The 2026-05-09 incident: VEEV got freshly refreshed by yfinance to
    Friday's close, every other parquet still ended at Wednesday's close.
    The delta load must still pick up Thu + Fri so SPY/VIX/XL*/universe
    advance to Friday — without this, backfill's regression preflight
    rejects the write.
    """
    from features import compute as _c

    # 920 tickers stuck at 5/6 (Wed), 1 ticker freshly refreshed to 5/8 (Fri)
    price_data = {
        "SPY": _ohlcv("2026-05-06", n=10),
        "XLF": _ohlcv("2026-05-06", n=10),
        "AAPL": _ohlcv("2026-05-06", n=10),
        "VEEV": _ohlcv("2026-05-08", n=10),  # the freshly-refreshed ticker
    }

    # daily_closes for 5/7 + 5/8 carry every ticker (matches MorningEnrich
    # polygon-T+1 fill shape)
    daily_closes = {
        "2026-05-07": _daily_closes_frame({
            "SPY":  {"Open": 730.0, "High": 732.0, "Low": 729.0, "Close": 731.0, "Volume": 1},
            "XLF":  {"Open": 50.0,  "High": 51.0,  "Low": 49.0,  "Close": 50.5,  "Volume": 1},
            "AAPL": {"Open": 200.0, "High": 202.0, "Low": 199.0, "Close": 201.0, "Volume": 1},
            "VEEV": {"Open": 250.0, "High": 252.0, "Low": 249.0, "Close": 251.0, "Volume": 1},
        }),
        "2026-05-08": _daily_closes_frame({
            "SPY":  {"Open": 734.0, "High": 738.0, "Low": 733.0, "Close": 737.0, "Volume": 1},
            "XLF":  {"Open": 51.0,  "High": 52.0,  "Low": 50.0,  "Close": 51.5,  "Volume": 1},
            "AAPL": {"Open": 202.0, "High": 204.0, "Low": 201.0, "Close": 203.0, "Volume": 1},
            "VEEV": {"Open": 252.0, "High": 254.0, "Low": 251.0, "Close": 253.0, "Volume": 1},
        }),
    }
    s3 = _stub_s3_with_daily_closes(daily_closes)

    # Saturday run: today=5/9, no business day range with max=5/8 + 1 = 5/9
    out, _splits = _c._apply_daily_delta(s3, "test-bucket", "2026-05-09", price_data)

    # Every ticker must end at 5/8 — the frozen ones must NOT remain stuck at 5/6
    assert out["SPY"].index[-1] == pd.Timestamp("2026-05-08"), (
        f"SPY stuck at {out['SPY'].index[-1]} — delta loader skipped 5/7 + 5/8 "
        f"because slim_last_date was poisoned by VEEV's fresh 5/8 mtime"
    )
    assert out["XLF"].index[-1] == pd.Timestamp("2026-05-08")
    assert out["AAPL"].index[-1] == pd.Timestamp("2026-05-08")
    assert out["VEEV"].index[-1] == pd.Timestamp("2026-05-08")

    # VEEV's overlapping 5/8 row must dedupe to one (keep="last"). Sanity:
    # no duplicate index entries.
    assert not out["VEEV"].index.has_duplicates
    assert not out["SPY"].index.has_duplicates


def test_apply_daily_delta_returns_early_when_all_tickers_already_at_today():
    """Sanity: if EVERY ticker is already at today, ``min`` equals today
    and the bdate_range is empty — early-return is fine, no clobber."""
    from features import compute as _c

    price_data = {
        "SPY": _ohlcv("2026-05-08", n=5),
        "XLF": _ohlcv("2026-05-08", n=5),
    }

    s3 = MagicMock()  # never called: bdate_range is empty so loader short-circuits

    out, _splits = _c._apply_daily_delta(s3, "test-bucket", "2026-05-09", price_data)

    assert out["SPY"].index[-1] == pd.Timestamp("2026-05-08")
    assert out["XLF"].index[-1] == pd.Timestamp("2026-05-08")
    s3.get_object.assert_not_called()
