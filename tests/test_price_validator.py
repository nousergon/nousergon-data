"""Tests for validators/price_validator.py."""

import pandas as pd
import pytest

from validators.price_validator import (
    ANOMALY_BAD_OHLC,
    ANOMALY_EXTREME_DAILY_MOVE,
    ANOMALY_NEGATIVE_OR_ZERO_CLOSE,
    ANOMALY_VOLUME_SPIKE,
    ANOMALY_ZERO_VOLUME,
    DEFAULT_BLOCK_ANOMALY_TYPES,
    validate_parquet,
    validate_today_row,
)


def _make_ohlcv(n=30, base_close=100.0):
    """Build a clean OHLCV DataFrame with n trading days."""
    dates = pd.bdate_range("2025-01-02", periods=n)
    close = [base_close + i * 0.5 for i in range(n)]
    return pd.DataFrame(
        {
            "Open": [c - 0.1 for c in close],
            "High": [c + 1.0 for c in close],
            "Low": [c - 1.0 for c in close],
            "Close": close,
            "Volume": [1_000_000 + i * 1000 for i in range(n)],
        },
        index=dates,
    )


class TestValidateParquet:
    def test_clean_data(self):
        df = _make_ohlcv()
        result = validate_parquet(df, "AAPL")
        assert result["status"] == "clean"
        assert result["anomalies"] == []

    def test_empty_dataframe(self):
        df = pd.DataFrame()
        result = validate_parquet(df, "EMPTY")
        assert result["status"] == "empty"

    def test_high_less_than_low(self):
        df = _make_ohlcv()
        df.iloc[5, df.columns.get_loc("High")] = df.iloc[5]["Low"] - 1
        result = validate_parquet(df, "BAD_HL")
        assert result["status"] == "anomaly"
        assert any("High<Low" in a for a in result["anomalies"])

    def test_zero_close(self):
        df = _make_ohlcv()
        df.iloc[10, df.columns.get_loc("Close")] = 0
        result = validate_parquet(df, "ZERO")
        assert result["status"] == "anomaly"
        assert any("Close<=0" in a for a in result["anomalies"])

    def test_extreme_daily_return(self):
        df = _make_ohlcv()
        # 60% jump
        df.iloc[15, df.columns.get_loc("Close")] = df.iloc[14]["Close"] * 1.61
        result = validate_parquet(df, "SPIKE")
        assert result["status"] == "anomaly"
        assert any("50%" in a for a in result["anomalies"])

    def test_zero_volume(self):
        df = _make_ohlcv()
        df.iloc[5, df.columns.get_loc("Volume")] = 0
        result = validate_parquet(df, "NOVOL")
        assert result["status"] == "anomaly"
        assert any("zero volume" in a for a in result["anomalies"])

    def test_volume_spike(self):
        df = _make_ohlcv(n=40)
        # 15x median volume
        df.iloc[35, df.columns.get_loc("Volume")] = df.iloc[34]["Volume"] * 15
        result = validate_parquet(df, "VOLSPIKE")
        assert result["status"] == "anomaly"
        assert any("volume" in a and "median" in a for a in result["anomalies"])

    def test_trading_day_gap(self):
        df = _make_ohlcv(n=40)
        # Remove 8 calendar days worth of rows (creates a >5 day gap)
        gap_start = df.index[15]
        gap_end = gap_start + pd.Timedelta(days=8)
        df = df[(df.index < gap_start) | (df.index >= gap_end)]
        result = validate_parquet(df, "GAP")
        assert result["status"] == "anomaly"
        assert any("gap" in a.lower() for a in result["anomalies"])

    def test_multiple_anomalies(self):
        df = _make_ohlcv()
        df.iloc[5, df.columns.get_loc("Close")] = 0
        df.iloc[10, df.columns.get_loc("Volume")] = 0
        result = validate_parquet(df, "MULTI")
        assert result["status"] == "anomaly"
        assert len(result["anomalies"]) >= 2


def _make_today_row(
    close=100.0, high=101.0, low=99.0, volume=1_000_000, vwap=100.0,
):
    ts = pd.Timestamp("2026-05-12")
    return pd.DataFrame(
        {
            "Open": [close - 0.1],
            "High": [high],
            "Low": [low],
            "Close": [close],
            "Volume": [volume],
            "VWAP": [vwap],
        },
        index=pd.DatetimeIndex([ts], name="date"),
    )


class TestValidateTodayRow:
    def test_clean_row_no_anomalies(self):
        hist = _make_ohlcv(n=30, base_close=100.0)
        today = _make_today_row(close=100.0)
        result = validate_today_row(today, hist, "AAPL")
        assert result == {"ticker": "AAPL", "anomalies": []}

    def test_empty_today_row(self):
        result = validate_today_row(pd.DataFrame(), pd.DataFrame(), "EMPTY")
        assert result == {"ticker": "EMPTY", "anomalies": []}

    def test_bad_ohlc_blocks(self):
        hist = _make_ohlcv()
        today = _make_today_row(high=98.0, low=99.0, close=98.5)
        result = validate_today_row(today, hist, "BADHL")
        types = {a["type"] for a in result["anomalies"]}
        assert ANOMALY_BAD_OHLC in types
        bad_ohlc = next(a for a in result["anomalies"] if a["type"] == ANOMALY_BAD_OHLC)
        assert bad_ohlc["severity"] == "block"

    def test_zero_close_blocks(self):
        hist = _make_ohlcv()
        today = _make_today_row(close=0.0, high=1.0, low=0.0)
        result = validate_today_row(today, hist, "ZERO")
        types = {a["type"] for a in result["anomalies"]}
        assert ANOMALY_NEGATIVE_OR_ZERO_CLOSE in types
        zero = next(a for a in result["anomalies"] if a["type"] == ANOMALY_NEGATIVE_OR_ZERO_CLOSE)
        assert zero["severity"] == "block"

    def test_negative_close_blocks(self):
        hist = _make_ohlcv()
        today = _make_today_row(close=-5.0, high=1.0, low=-10.0)
        result = validate_today_row(today, hist, "NEG")
        types = {a["type"] for a in result["anomalies"]}
        assert ANOMALY_NEGATIVE_OR_ZERO_CLOSE in types

    def test_extreme_daily_move_warns(self):
        # hist last close is 100 + 29*0.5 = 114.5 (base_close=100, n=30)
        hist = _make_ohlcv(n=30, base_close=100.0)
        prior_close = hist["Close"].iloc[-1]
        today = _make_today_row(close=prior_close * 1.61)  # +61% move
        result = validate_today_row(today, hist, "MOVE")
        types = {a["type"] for a in result["anomalies"]}
        assert ANOMALY_EXTREME_DAILY_MOVE in types
        move = next(a for a in result["anomalies"] if a["type"] == ANOMALY_EXTREME_DAILY_MOVE)
        assert move["severity"] == "warn"

    def test_no_extreme_move_flag_when_hist_empty(self):
        # First write for a symbol — no prior close to compare against.
        today = _make_today_row(close=999.0)
        result = validate_today_row(today, pd.DataFrame(), "NEW")
        types = {a["type"] for a in result["anomalies"]}
        assert ANOMALY_EXTREME_DAILY_MOVE not in types

    def test_zero_volume_warns(self):
        hist = _make_ohlcv()
        today = _make_today_row(volume=0)
        result = validate_today_row(today, hist, "NOVOL")
        types = {a["type"] for a in result["anomalies"]}
        assert ANOMALY_ZERO_VOLUME in types
        zv = next(a for a in result["anomalies"] if a["type"] == ANOMALY_ZERO_VOLUME)
        assert zv["severity"] == "warn"

    def test_volume_spike_warns(self):
        hist = _make_ohlcv(n=30)
        median_vol = hist["Volume"].tail(20).median()
        today = _make_today_row(volume=int(median_vol * 15))
        result = validate_today_row(today, hist, "VOL")
        types = {a["type"] for a in result["anomalies"]}
        assert ANOMALY_VOLUME_SPIKE in types

    def test_volume_spike_skipped_when_hist_too_short(self):
        # hist <20 rows → no baseline available, skip volume spike check.
        hist = _make_ohlcv(n=10)
        today = _make_today_row(volume=999_999_999)
        result = validate_today_row(today, hist, "SHORT")
        types = {a["type"] for a in result["anomalies"]}
        assert ANOMALY_VOLUME_SPIKE not in types

    def test_multiple_anomalies_stack(self):
        hist = _make_ohlcv()
        today = _make_today_row(close=0.0, high=0.5, low=0.0, volume=0)
        result = validate_today_row(today, hist, "MULTI")
        types = {a["type"] for a in result["anomalies"]}
        assert ANOMALY_NEGATIVE_OR_ZERO_CLOSE in types
        assert ANOMALY_ZERO_VOLUME in types

    def test_nan_close_skipped(self):
        # Upstream-NaN close shouldn't trip the gate — it's handled earlier.
        hist = _make_ohlcv()
        today = _make_today_row(close=float("nan"))
        result = validate_today_row(today, hist, "NAN")
        # No close-based anomalies should fire; volume/OHLC checks may still
        # run independently — assert specifically that the close-anomaly
        # types are absent.
        types = {a["type"] for a in result["anomalies"]}
        assert ANOMALY_NEGATIVE_OR_ZERO_CLOSE not in types
        assert ANOMALY_EXTREME_DAILY_MOVE not in types

    def test_default_block_set_constants(self):
        # Lock the default-block contract — operators reading the env-var
        # docs should be able to trust that flipping the env to "[]" gives
        # them pure observability mode, while leaving it unset blocks
        # bad_ohlc + negative_or_zero_close.
        assert DEFAULT_BLOCK_ANOMALY_TYPES == frozenset({
            ANOMALY_BAD_OHLC,
            ANOMALY_NEGATIVE_OR_ZERO_CLOSE,
        })
