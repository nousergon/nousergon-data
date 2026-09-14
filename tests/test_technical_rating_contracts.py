"""Producer contract tests for the metron-ops#293 technical-rating artifacts.

Mirrors the existing SLOT-contract pattern (contracts/__init__.py::validate_signals /
validate_predictions): every write this producer makes must validate cleanly against
its own versioned JSON Schema, checked at PR time rather than trusted by inspection.

Covers:
  - technicals.schema.json against `collect_technicals`'s actual write (with and
    without a per-symbol `rating`, v3 additive).
  - technical_ratings.schema.json (new artifact) against `collect_intraday`'s actual
    write, including the empty-ratings shape (a legitimately empty run must still
    validate -- an empty dict is not a schema violation).
  - A hand-built minimal fixture for each schema, independent of the producer code,
    so a producer bug that silently matches its own (wrong) output can't also pass
    the contract by construction.

Refs nousergon/metron-ops#293.
"""
from __future__ import annotations

import json
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from collectors import metron_market_data as mmd
from contracts import validate_technical_ratings, validate_technicals
from tests.test_metron_market_data import _puts, _universe_s3, _UNIVERSE


def _closes(n: int, start_date: str = "2025-01-02", start: float = 50.0, step: float = 1.0):
    base = date.fromisoformat(start_date)
    return [[str(base + timedelta(days=i)), start + i * step] for i in range(n)]


class TestTechnicalsContract:
    def test_producer_write_with_rating_validates(self, monkeypatch):
        monkeypatch.setattr(
            mmd, "load_price_derived_universe",
            lambda bucket, s3_client: ([{"yf_symbol": "AAPL", "currency": "USD"}], []),
        )
        s3 = _universe_s3(_UNIVERSE, close_history={"AAPL": {"closes": _closes(260)}})
        mmd.collect_technicals(bucket="b", run_date="2026-06-26", s3_client=s3)
        art = _puts(s3)["market_data/technicals/latest.json"]
        assert "rating" in art["technicals"]["AAPL"]
        assert validate_technicals(art) == []

    def test_producer_write_without_any_symbol_validates(self, monkeypatch):
        """No close_history published for any symbol -- an empty `technicals` map is a
        legitimate (if degenerate) run, not a contract violation."""
        monkeypatch.setattr(
            mmd, "load_price_derived_universe",
            lambda bucket, s3_client: ([{"yf_symbol": "AAPL", "currency": "USD"}], []),
        )
        s3 = _universe_s3(_UNIVERSE, close_history={})
        mmd.collect_technicals(bucket="b", run_date="2026-06-26", s3_client=s3)
        art = _puts(s3)["market_data/technicals/latest.json"]
        assert art["technicals"] == {}
        assert validate_technicals(art) == []

    def test_minimal_hand_built_fixture_validates(self):
        """Independent of the producer: a schema-conformant fixture must pass on its
        own construction, not merely because it happens to match collect_technicals's
        output shape."""
        art = {
            "schema_version": 3, "as_of": "2026-06-26", "source": "computed",
            "technicals": {
                "AAPL": {
                    "rsi_14": 55.2, "macd_hist": 0.4, "ma_50": 200.1, "ma_200": None,
                    "pct_to_ma_50": 0.01, "pct_to_ma_200": None, "high_52w": 210.0,
                    "low_52w": 180.0, "pct_in_52w_range": 0.66, "pct_from_52wk_high": -0.03,
                    "mom_20d": 0.02, "mom_60d": 0.05,
                    "rating": {
                        "score": 0.6, "label": "Strong Buy", "ma_score": 1.0, "osc_score": 0.2,
                        "n_buy": 8, "n_neutral": 4, "n_sell": 0, "n_votes": 12,
                    },
                }
            },
        }
        assert validate_technicals(art) == []

    def test_wrong_label_fails(self):
        art = {
            "schema_version": 3, "as_of": "2026-06-26", "source": "computed",
            "technicals": {"AAPL": {"rating": {
                "score": 0.6, "label": "Very Bullish", "ma_score": 1.0, "osc_score": 0.2,
                "n_buy": 8, "n_neutral": 4, "n_sell": 0, "n_votes": 12,
            }}},
        }
        assert validate_technicals(art) != []


class TestTechnicalRatingsContract:
    _RTH = datetime(2026, 6, 12, 15, 0, tzinfo=timezone.utc)

    def _stub(self, last: float):
        quote = {"AAPL": {"last": last, "open": last - 1, "prev_close": last - 0.5,
                           "session_date": "2026-06-12", "prev_session_date": "2026-06-11"}}
        index = {s: {"last": 1.0, "open": 1.0, "prev_close": 1.0,
                     "session_date": "2026-06-12", "prev_session_date": "2026-06-11"}
                 for s in mmd.INDEX_PROXY_SYMBOLS}
        fund = {s: {"last": 1.0, "open": 1.0, "prev_close": 1.0,
                    "session_date": "2026-06-12", "prev_session_date": "2026-06-11"}
                for s in mmd.FUND_PROXY_ETFS}
        merged = {**quote, **index, **fund}
        return lambda syms: {s: dict(merged[s]) for s in syms if s in merged}

    def test_producer_write_with_ratings_validates(self):
        closes = _closes(260)
        s3 = _universe_s3(_UNIVERSE, close_history={"AAPL": {"closes": closes}})
        mmd.collect_intraday(
            bucket="b", s3_client=s3, intraday_source=self._stub(closes[-1][1] + 5), now=self._RTH,
        )
        art = _puts(s3)["market_data/intraday/technical_ratings.json"]
        assert art["ratings"]
        assert validate_technical_ratings(art) == []

    def test_producer_write_with_zero_ratings_validates(self):
        """No close_history for the held symbol -- an empty `ratings` map is a
        legitimate run (nothing computable yet), not a contract violation."""
        s3 = _universe_s3(_UNIVERSE, close_history={})
        mmd.collect_intraday(
            bucket="b", s3_client=s3, intraday_source=self._stub(100.0), now=self._RTH,
        )
        art = _puts(s3)["market_data/intraday/technical_ratings.json"]
        assert art["ratings"] == {}
        assert validate_technical_ratings(art) == []

    def test_minimal_hand_built_fixture_validates(self):
        art = {
            "schema_version": 1, "as_of_utc": "2026-06-12T15:00:00Z",
            "quote_as_of_utc": "2026-06-12T15:00:00Z", "source": "computed_intraday",
            "ratings": {
                "AAPL": {
                    "score": 0.6, "label": "Strong Buy", "ma_score": 1.0, "osc_score": 0.2,
                    "n_buy": 8, "n_neutral": 4, "n_sell": 0, "n_votes": 12,
                    "price": 202.5, "bar_date": "2026-06-12", "basis": "intraday",
                }
            },
        }
        assert validate_technical_ratings(art) == []

    def test_missing_basis_field_fails(self):
        art = {
            "schema_version": 1, "as_of_utc": "2026-06-12T15:00:00Z",
            "quote_as_of_utc": "2026-06-12T15:00:00Z", "source": "computed_intraday",
            "ratings": {
                "AAPL": {
                    "score": 0.6, "label": "Strong Buy", "ma_score": 1.0, "osc_score": 0.2,
                    "n_buy": 8, "n_neutral": 4, "n_sell": 0, "n_votes": 12,
                    "price": 202.5, "bar_date": "2026-06-12",
                }
            },
        }
        assert validate_technical_ratings(art) != []
