"""Tests for rag/pipelines/_corpus_scope.py — the shared RAG corpus
ticker-scope resolver (config#2943 binding ruling).

Ruling: the RAG corpus scope is holdings ∪ active candidates ∪ top-60
signals board — NOT the full signals.json universe (~900 tickers) that
``--from-signals`` used to pull in. These tests pin the resolver's shape:
each slice fails soft independently, the union is correct, and the
top-N board ranking is by ``score`` descending with graceful handling of
missing/non-numeric scores.
"""

from __future__ import annotations

import json
from datetime import date
from io import BytesIO
from unittest.mock import MagicMock

from rag.pipelines import _corpus_scope as scope_mod


def _body(obj):
    return {"Body": BytesIO(json.dumps(obj).encode())}


def _mock_s3(holdings=None, candidates=None, signals_universe=None, buy_candidates=None,
             signals_date="2026-07-19", candidates_date="2026-07-19",
             missing_holdings=False, missing_candidates=False, missing_signals=False):
    """Build an S3 mock serving holdings/candidates/signals for a fixed 'today'."""
    s3 = MagicMock()

    def _get_object(Bucket, Key):
        if Key == scope_mod.HOLDINGS_UNIVERSE_KEY:
            if missing_holdings or holdings is None:
                raise RuntimeError("NoSuchKey")
            return _body({"tickers": holdings})
        if Key == f"candidates/{candidates_date}/candidates.json":
            if missing_candidates or candidates is None:
                raise RuntimeError("NoSuchKey")
            return _body({"candidates": candidates})
        if Key == f"signals/{signals_date}/signals.json":
            if missing_signals:
                raise RuntimeError("NoSuchKey")
            return _body({
                "universe": signals_universe or [],
                "buy_candidates": buy_candidates or [],
            })
        raise RuntimeError(f"unexpected key {Key}")

    s3.get_object.side_effect = _get_object
    return s3


class TestLoadHoldings:
    def test_reads_tickers_slice(self):
        s3 = _mock_s3(holdings=["aapl", "MSFT"])
        assert scope_mod.load_holdings("b", s3) == {"AAPL", "MSFT"}

    def test_fail_soft_missing(self):
        s3 = _mock_s3(missing_holdings=True)
        assert scope_mod.load_holdings("b", s3) == set()


class TestLoadActiveCandidates:
    def test_unions_scanner_and_buy_candidates(self):
        s3 = _mock_s3(
            candidates=[{"ticker": "NVDA"}, {"ticker": "amd"}],
            buy_candidates=[{"ticker": "MSFT"}],
        )
        result = scope_mod.load_active_candidates("b", s3, as_of=date(2026, 7, 19))
        assert result == {"NVDA", "AMD", "MSFT"}

    def test_fail_soft_missing_scanner_keeps_buy_candidates(self):
        s3 = _mock_s3(missing_candidates=True, buy_candidates=[{"ticker": "MSFT"}])
        result = scope_mod.load_active_candidates("b", s3, as_of=date(2026, 7, 19))
        assert result == {"MSFT"}

    def test_fail_soft_missing_signals_keeps_scanner(self):
        s3 = _mock_s3(candidates=[{"ticker": "NVDA"}], missing_signals=True)
        result = scope_mod.load_active_candidates("b", s3, as_of=date(2026, 7, 19))
        assert result == {"NVDA"}

    def test_both_missing_returns_empty(self):
        s3 = _mock_s3(missing_candidates=True, missing_signals=True)
        result = scope_mod.load_active_candidates("b", s3, as_of=date(2026, 7, 19))
        assert result == set()

    def test_falls_back_to_prior_day_when_today_missing(self):
        # Today's candidates.json doesn't exist yet; yesterday's does.
        s3 = _mock_s3(
            candidates=[{"ticker": "NVDA"}],
            candidates_date="2026-07-18",
            missing_signals=True,
        )
        result = scope_mod.load_active_candidates("b", s3, as_of=date(2026, 7, 19))
        assert result == {"NVDA"}

    def test_scanner_candidates_list_shape(self):
        # candidates.json may be a bare list rather than {"candidates": [...]}.
        s3 = MagicMock()

        def _get_object(Bucket, Key):
            if Key == "candidates/2026-07-19/candidates.json":
                return _body([{"ticker": "NVDA"}])
            raise RuntimeError("NoSuchKey")

        s3.get_object.side_effect = _get_object
        result = scope_mod.load_active_candidates("b", s3, as_of=date(2026, 7, 19))
        assert result == {"NVDA"}


class TestLoadBoardTopN:
    def test_ranks_by_score_descending(self):
        universe = [
            {"ticker": "LOW", "score": 10.0},
            {"ticker": "HIGH", "score": 90.0},
            {"ticker": "MID", "score": 50.0},
        ]
        s3 = _mock_s3(signals_universe=universe)
        result = scope_mod.load_board_top_n("b", s3, as_of=date(2026, 7, 19), top_n=2)
        assert result == {"HIGH", "MID"}

    def test_missing_score_sorts_last_not_crash(self):
        universe = [
            {"ticker": "NOSCORE"},
            {"ticker": "HASSCORE", "score": 5.0},
        ]
        s3 = _mock_s3(signals_universe=universe)
        result = scope_mod.load_board_top_n("b", s3, as_of=date(2026, 7, 19), top_n=1)
        assert result == {"HASSCORE"}

    def test_non_numeric_score_does_not_crash(self):
        universe = [
            {"ticker": "BADSCORE", "score": "not-a-number"},
            {"ticker": "GOODSCORE", "score": 5.0},
        ]
        s3 = _mock_s3(signals_universe=universe)
        result = scope_mod.load_board_top_n("b", s3, as_of=date(2026, 7, 19), top_n=1)
        assert result == {"GOODSCORE"}

    def test_fail_soft_missing_signals(self):
        s3 = _mock_s3(missing_signals=True)
        assert scope_mod.load_board_top_n("b", s3, as_of=date(2026, 7, 19)) == set()

    def test_top_n_smaller_than_universe(self):
        universe = [{"ticker": f"T{i}", "score": float(i)} for i in range(100)]
        s3 = _mock_s3(signals_universe=universe)
        result = scope_mod.load_board_top_n("b", s3, as_of=date(2026, 7, 19), top_n=60)
        assert len(result) == 60
        # Highest-scored tickers T99..T40 must be included.
        assert "T99" in result
        assert "T39" not in result


class TestResolveCorpusScope:
    def test_unions_all_three_slices(self):
        s3 = _mock_s3(
            holdings=["AAPL"],
            candidates=[{"ticker": "NVDA"}],
            signals_universe=[{"ticker": "MSFT", "score": 99.0}],
            buy_candidates=[],
        )
        result = scope_mod.resolve_corpus_scope("b", s3, as_of=date(2026, 7, 19))
        assert result == {"AAPL", "NVDA", "MSFT"}

    def test_holdings_retained_even_outside_top_60(self):
        # Ruling carve-out: held names must retain coverage even outside
        # the top-60 board ranking.
        universe = [{"ticker": f"T{i}", "score": float(i)} for i in range(100)]
        s3 = _mock_s3(holdings=["HELD_BUT_LOW_SCORE"], signals_universe=universe)
        result = scope_mod.resolve_corpus_scope("b", s3, as_of=date(2026, 7, 19), board_top_n=60)
        assert "HELD_BUT_LOW_SCORE" in result

    def test_all_sources_unavailable_returns_empty(self):
        s3 = _mock_s3(missing_holdings=True, missing_candidates=True, missing_signals=True)
        result = scope_mod.resolve_corpus_scope("b", s3, as_of=date(2026, 7, 19))
        assert result == set()

    def test_does_not_include_full_universe_tickers_outside_scope(self):
        # The core regression this ruling exists to prevent: a huge
        # signals.json universe must NOT all end up in scope.
        universe = [{"ticker": f"T{i}", "score": float(i)} for i in range(900)]
        s3 = _mock_s3(holdings=[], candidates=[], signals_universe=universe, buy_candidates=[])
        result = scope_mod.resolve_corpus_scope("b", s3, as_of=date(2026, 7, 19))
        assert len(result) == 60
        assert "T0" not in result  # lowest-scored, not in top-60, not held/candidate

    def test_fetches_signals_json_exactly_once(self):
        # load_active_candidates (buy_candidates) and load_board_top_n
        # (universe ranking) both read signals/{date}/signals.json —
        # resolve_corpus_scope must fetch it ONCE and share the parsed
        # dict, not GET it twice per call.
        s3 = _mock_s3(
            holdings=["AAPL"],
            candidates=[{"ticker": "NVDA"}],
            signals_universe=[{"ticker": "MSFT", "score": 99.0}],
            buy_candidates=[{"ticker": "TSLA"}],
        )
        scope_mod.resolve_corpus_scope("b", s3, as_of=date(2026, 7, 19))
        signals_json_calls = [
            c for c in s3.get_object.call_args_list
            if c.kwargs.get("Key") == "signals/2026-07-19/signals.json"
        ]
        assert len(signals_json_calls) == 1, (
            f"expected exactly 1 signals.json GET, got {len(signals_json_calls)}"
        )


class TestResolveTickersFromArgs:
    def test_explicit_tickers_wins_over_scope(self):
        args = MagicMock(tickers="AAPL,msft", scope=scope_mod.SCOPE_FLAG_VALUE, bucket="b")
        assert scope_mod.resolve_tickers_from_args(args) == ["AAPL", "MSFT"]

    def test_scope_flag_resolves_via_resolver(self):
        s3 = _mock_s3(holdings=["AAPL"], candidates=[], signals_universe=[], buy_candidates=[])
        args = MagicMock(tickers=None, scope=scope_mod.SCOPE_FLAG_VALUE, bucket="b")
        result = scope_mod.resolve_tickers_from_args(args, s3_client=s3)
        assert result == ["AAPL"]

    def test_neither_flag_returns_empty(self):
        args = MagicMock(tickers=None, scope=None, bucket="b")
        assert scope_mod.resolve_tickers_from_args(args) == []
