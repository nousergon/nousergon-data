"""Tests for `compute_technical_rating` (metron-ops#293, close-only
TradingView-Technical-Ratings method): the MA/oscillator vote rules, the
label thresholds, per-vote short-history skipping, and the all-None coverage
gap. metron-ops#297 adds the rule-alignment coverage (momentum voting on the
momentum VALUE's own trend, the corrected label boundaries, Hull MA(9), and
the `rating_version` stamp). Refs nousergon/metron-ops#293,
nousergon/metron-ops#297.
"""
from __future__ import annotations

import os
import sys

import pandas as pd
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from features.feature_engineer import _compute_hull_ma, _rating_label, compute_technical_rating


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

    def test_eleven_bars_adds_hull_ma_vote_only(self):
        # HMA(9)'s min-obs is 11 (period + sqrt_len - 1 = 9 + 3 - 1); momentum
        # now needs TWO trailing momentum readings (metron-ops#297), so it
        # needs 12, not 11 -- osc_score stays None at exactly 11 bars.
        s = _uptrend(11)
        out = compute_technical_rating(s)
        assert out is not None
        assert out["n_votes"] == 3  # 2 MA(10) votes + Hull MA(9)
        assert out["osc_score"] is None
        assert out["ma_score"] == out["score"]

    def test_twelve_bars_adds_momentum_vote(self):
        s = _uptrend(12)
        out = compute_technical_rating(s)
        assert out is not None
        assert out["n_votes"] == 4  # 2 MA(10) + Hull MA(9) + momentum(10)
        assert out["osc_score"] is not None

    def test_sixteen_bars_adds_rsi_vote(self):
        # Wavy (not pure-monotonic): a strictly one-directional series has zero RSI
        # losses, so Wilder's RSI divides by zero and reads NaN forever -- see
        # `_wavy_uptrend`'s docstring.
        below = _wavy_uptrend(15)
        at = _wavy_uptrend(16)
        assert compute_technical_rating(below)["n_votes"] == 4  # MA(10) x2 + HMA(9) + momentum
        assert compute_technical_rating(at)["n_votes"] == 5  # + RSI

    def test_macd_vote_requires_35_bars(self):
        below = _uptrend(34)
        at = _uptrend(35)
        n_below = compute_technical_rating(below)["n_votes"]
        n_at = compute_technical_rating(at)["n_votes"]
        assert n_at == n_below + 1

    def test_full_history_yields_all_thirteen_ma_votes_plus_three_oscillator_votes(self):
        s = _wavy_uptrend(260)
        out = compute_technical_rating(s)
        assert out["n_votes"] == 16  # 12 SMA/EMA + Hull MA(9) + RSI + MACD + momentum
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
            (-0.501, "Strong Sell"),
            (-0.5, "Sell"),  # exact -0.5 -> Sell, NOT Strong Sell (metron-ops#297)
            (-0.499, "Sell"),
            (-0.101, "Sell"),
            (-0.1, "Neutral"),  # NOT "< -0.1" (exact match falls through to Neutral)
            (-0.099, "Neutral"),
            (0.0, "Neutral"),
            (0.1, "Neutral"),
            (0.101, "Buy"),
            (0.499, "Buy"),
            (0.5, "Buy"),  # exact 0.5 -> Buy, NOT Strong Buy (metron-ops#297)
            (0.501, "Strong Buy"),
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
            "n_buy", "n_neutral", "n_sell", "n_votes", "rating_version",
        }

    def test_rating_version_stamped(self):
        from features.feature_engineer import RATING_VERSION

        out = compute_technical_rating(_uptrend(260))
        assert out["rating_version"] == RATING_VERSION == 2


class TestMomentumVotesOnMomentumTrend:
    """metron-ops#297: the published rule is "the momentum VALUE is rising",
    not "price is up vs. 10 bars ago". A series with net-positive price
    change but a DECELERATING momentum value must vote Sell under the new
    rule -- the old buggy code (``close - close[-11] > 0``) would have voted
    Buy on this exact fixture."""

    def test_decelerating_gains_vote_sell_despite_net_positive_price(self):
        # last=109 > close[-11]=101 (net "up" over 10 bars -- the OLD rule's
        # buy signal), but mom_t=8 < mom_t1=9 (the momentum VALUE itself is
        # falling) -- the new rule must vote sell (-1).
        s = pd.Series([100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 109, 109], dtype="float64")
        out = compute_technical_rating(s)
        assert out is not None
        # At 12 bars only momentum has an oscillator vote (RSI needs 16, MACD
        # needs 35) -- osc_score is exactly that one vote.
        assert out["osc_score"] == -1.0

    def test_accelerating_gains_vote_buy(self):
        s = pd.Series([100, 100, 100, 100, 100, 100, 100, 100, 100, 101, 103, 106], dtype="float64")
        out = compute_technical_rating(s)
        assert out is not None
        assert out["osc_score"] == 1.0

    def test_constant_momentum_value_votes_neutral(self):
        # A perfectly linear (constant-slope) series has a CONSTANT momentum
        # value (mom_t == mom_t-1 always) -- neutral, not buy, even though
        # price is unambiguously rising.
        s = _uptrend(12)
        out = compute_technical_rating(s)
        assert out is not None
        assert out["osc_score"] == 0.0


class TestHullMa:
    def test_matches_independent_reference_implementation(self):
        vals = [50.0]
        step = 0.3
        for i in range(1, 30):
            step += 0.05 if i % 3 else -0.1
            vals.append(vals[-1] + step)

        def _ref_wma(arr, length):
            window = arr[-length:]
            weights = list(range(1, length + 1))
            return sum(w * v for w, v in zip(weights, window)) / sum(weights)

        def _ref_hma(arr, period=9):
            half, sqrt_len = 5, 3  # round-half-up(9/2)=5, round-half-up(sqrt(9))=3
            diffs = []
            for k in range(sqrt_len):
                sub = arr[: len(arr) - k]
                diffs.append(2 * _ref_wma(sub, half) - _ref_wma(sub, period))
            diffs = diffs[::-1]
            weights = list(range(1, sqrt_len + 1))
            return sum(w * v for w, v in zip(weights, diffs)) / sum(weights)

        expected = _ref_hma(vals, 9)
        actual = _compute_hull_ma(pd.Series(vals, dtype="float64"), period=9)
        assert actual == pytest.approx(expected, rel=1e-9)

    def test_below_min_obs_returns_none(self):
        # min_obs = period + sqrt_len - 1 = 9 + 3 - 1 = 11.
        assert _compute_hull_ma(pd.Series([100.0] * 10, dtype="float64"), period=9) is None

    def test_at_min_obs_computes(self):
        assert _compute_hull_ma(_uptrend(11), period=9) is not None

    def test_hma_vote_included_in_ma_group_for_strong_trends(self):
        # Full-history up/down trends already assert ma_score == +-1.0 --
        # possible only if the HMA vote (13th MA-group member) agrees with
        # every SMA/EMA vote. Direct cross-check here for clarity.
        up = compute_technical_rating(_uptrend(260))
        down = compute_technical_rating(_downtrend(260))
        assert up["n_sell"] == 0 and up["ma_score"] == 1.0
        assert down["n_buy"] == 0 and down["ma_score"] == -1.0
