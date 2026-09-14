"""Tests for `compute_technical_rating` (metron-ops#293, close-only
TradingView-Technical-Ratings method): the MA/oscillator vote rules, the
label thresholds, per-vote short-history skipping, and the all-None coverage
gap. Refs nousergon/metron-ops#293.
"""
from __future__ import annotations

import os
import sys

import pandas as pd
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from features.feature_engineer import _rating_label, compute_technical_rating


def _flat(n: int, price: float = 100.0) -> pd.Series:
    """A perfectly flat series — every MA vote lands exactly ON the average
    (neutral, 0), and RSI/MACD/momentum read as unmoved."""
    return pd.Series([price] * n, dtype="float64")


def _uptrend(n: int, start: float = 50.0, step: float = 1.0) -> pd.Series:
    """A strictly ascending series — long enough (n >= 235) to populate every
    MA window, every oscillator vote, and to push RSI/MACD/momentum bullish."""
    return pd.Series([start + i * step for i in range(n)], dtype="float64")


def _downtrend(n: int, start: float = 300.0, step: float = 1.0) -> pd.Series:
    return pd.Series([start - i * step for i in range(n)], dtype="float64")


def _wavy_uptrend(n: int, start: float = 50.0) -> pd.Series:
    """A net-ascending series with occasional down-ticks (every 5th bar). A PERFECTLY
    monotonic series has zero losses, so Wilder's RSI (``_compute_rsi``) divides by a
    zero average-loss and reads NaN forever -- this fixture gives RSI real up/down bars
    to compute against so its vote is exercised, while staying net bullish (the last
    close sits well above every trailing average)."""
    vals = [start]
    for i in range(1, n):
        step = -0.4 if i % 5 == 0 else 1.2
        vals.append(vals[-1] + step)
    return pd.Series(vals, dtype="float64")


class TestReturnsNoneOnInsufficientHistory:
    def test_empty_series_returns_none(self):
        assert compute_technical_rating(pd.Series([], dtype="float64")) is None

    def test_single_bar_returns_none(self):
        # Below every MA window (min 10) and every oscillator minimum (min 11)
        # -- zero votes computable in either group.
        assert compute_technical_rating(pd.Series([100.0])) is None

    def test_nine_bars_below_shortest_ma_window_returns_none(self):
        # SMA/EMA-10 needs >=10 obs; momentum(10) needs >=11; RSI needs >=16;
        # MACD needs >=35. Nine flat bars produce zero votes in both groups.
        assert compute_technical_rating(_flat(9)) is None

    def test_non_positive_and_nan_values_are_dropped_before_gating(self):
        s = pd.Series([100.0, float("nan"), -5.0, 0.0] + [101.0] * 10)
        # 10 valid positive closes survive filtering -> exactly the SMA/EMA-10
        # window's worth, so a rating IS computable (not None).
        out = compute_technical_rating(s)
        assert out is not None
        assert out["n_votes"] >= 2  # at least the 10-window SMA+EMA pair


class TestVoteSkippingByHistoryDepth:
    def test_ten_bars_yields_only_the_ten_window_ma_votes(self):
        s = _uptrend(10)
        out = compute_technical_rating(s)
        assert out is not None
        # Only the 10-window MA pair is computable; no oscillator vote (RSI
        # needs >=16, MACD >=35, momentum >=11) -- osc_score is None, dropped
        # from the overall average rather than counted as 0.
        assert out["n_votes"] == 2
        assert out["osc_score"] is None
        assert out["ma_score"] == out["score"]

    def test_eleven_bars_adds_the_momentum_vote_only(self):
        s = _uptrend(11)
        out = compute_technical_rating(s)
        assert out is not None
        assert out["n_votes"] == 3  # 2 MA(10) votes + momentum(10)
        assert out["osc_score"] is not None

    def test_sixteen_bars_adds_rsi_vote(self):
        # Wavy (not pure-monotonic): a strictly one-directional series has zero RSI
        # losses, so Wilder's RSI divides by zero and reads NaN forever -- see
        # `_wavy_uptrend`'s docstring.
        below = _wavy_uptrend(15)
        at = _wavy_uptrend(16)
        assert compute_technical_rating(below)["n_votes"] == 3  # MA(10) x2 + momentum
        assert compute_technical_rating(at)["n_votes"] == 4  # + RSI

    def test_macd_vote_requires_35_bars(self):
        below = _uptrend(34)
        at = _uptrend(35)
        n_below = compute_technical_rating(below)["n_votes"]
        n_at = compute_technical_rating(at)["n_votes"]
        assert n_at == n_below + 1

    def test_full_history_yields_all_twelve_ma_votes_plus_three_oscillator_votes(self):
        s = _wavy_uptrend(260)
        out = compute_technical_rating(s)
        assert out["n_votes"] == 15  # 12 MA + RSI + MACD + momentum
        assert out["ma_score"] is not None and out["osc_score"] is not None


class TestMaVoteDirection:
    def test_strong_uptrend_last_above_every_ma_all_buy(self):
        out = compute_technical_rating(_uptrend(260))
        assert out["ma_score"] == 1.0
        assert out["n_sell"] == 0

    def test_strong_downtrend_last_below_every_ma_all_sell(self):
        out = compute_technical_rating(_downtrend(260))
        assert out["ma_score"] == -1.0
        assert out["n_buy"] == 0

    def test_flat_series_every_ma_vote_neutral(self):
        out = compute_technical_rating(_flat(260))
        assert out["ma_score"] == 0.0


class TestLabelThresholds:
    @pytest.mark.parametrize(
        "score,label",
        [
            (-1.0, "Strong Sell"),
            (-0.5, "Strong Sell"),
            (-0.499, "Sell"),
            (-0.101, "Sell"),
            (-0.1, "Neutral"),  # NOT "< -0.1" (exact match falls through to Neutral)
            (-0.099, "Neutral"),
            (0.0, "Neutral"),
            (0.1, "Neutral"),
            (0.101, "Buy"),
            (0.499, "Buy"),
            (0.5, "Strong Buy"),
            (1.0, "Strong Buy"),
        ],
    )
    def test_boundaries(self, score, label):
        assert _rating_label(score) == label

    def test_strong_uptrend_labels_strong_buy(self):
        assert compute_technical_rating(_uptrend(260))["label"] == "Strong Buy"

    def test_strong_downtrend_labels_strong_sell(self):
        assert compute_technical_rating(_downtrend(260))["label"] == "Strong Sell"

    def test_flat_series_labels_neutral(self):
        assert compute_technical_rating(_flat(260))["label"] == "Neutral"


class TestOverallScoreDropsZeroVoteGroup:
    def test_group_with_zero_votes_excluded_from_average_not_counted_as_zero(self):
        # 10 bars: only the MA group has votes (2); if osc were counted as 0
        # the overall score would be halved. It must instead equal ma_score
        # exactly, since osc_score is None and dropped from the average.
        out = compute_technical_rating(_uptrend(10))
        assert out["score"] == out["ma_score"]


class TestVoteCounts:
    def test_n_buy_n_sell_n_neutral_sum_to_n_votes(self):
        out = compute_technical_rating(_uptrend(260))
        assert out["n_buy"] + out["n_neutral"] + out["n_sell"] == out["n_votes"]

    def test_output_schema_fields(self):
        out = compute_technical_rating(_uptrend(260))
        assert set(out) == {
            "score", "label", "ma_score", "osc_score",
            "n_buy", "n_neutral", "n_sell", "n_votes",
        }
