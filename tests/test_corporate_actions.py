"""Unit tests for the ``corporate_actions`` model layer (config#1431).

Covers ``CorporateAction`` deterministic ``action_id``, ``expected_factor`` for
forward/reverse splits (delegating to ``split_factor``), and ``detect_splits``
mapping polygon events via a fake client (mirroring the ``get_recent_splits``
mocking in ``tests/test_daily_closes_skip_if_canonical.py``).
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

import corporate_actions as ca


class TestActionIdDeterminism:
    def test_same_inputs_yield_same_id(self):
        a = ca.CorporateAction.from_split("HON", "2026-06-27", 2, 1)
        b = ca.CorporateAction.from_split("HON", "2026-06-27", 2, 1)
        assert a.action_id == b.action_id
        assert len(a.action_id) == 16

    def test_different_ratio_yields_different_id(self):
        a = ca.CorporateAction.from_split("HON", "2026-06-27", 2, 1)
        b = ca.CorporateAction.from_split("HON", "2026-06-27", 1, 2)
        assert a.action_id != b.action_id

    def test_different_ticker_or_date_yields_different_id(self):
        base = ca.CorporateAction.from_split("HON", "2026-06-27", 2, 1)
        assert (
            ca.CorporateAction.from_split("MMM", "2026-06-27", 2, 1).action_id
            != base.action_id
        )
        assert (
            ca.CorporateAction.from_split("HON", "2026-06-28", 2, 1).action_id
            != base.action_id
        )

    def test_explicit_id_round_trips_through_to_from_dict(self):
        a = ca.CorporateAction.from_split("NVDA", "2026-06-10", 1, 10, raw={"x": 1})
        b = ca.CorporateAction.from_dict(a.to_dict())
        assert b.action_id == a.action_id
        assert b.raw == {"x": 1}


class TestExpectedFactor:
    def test_forward_split_1_for_n_divides(self):
        # forward 10-for-1 split: pre-split prices divided by 10 → factor 0.1
        a = ca.CorporateAction.from_split("NVDA", "2026-06-10", 1, 10)
        assert ca.expected_factor(a) == pytest.approx(0.1)

    def test_reverse_split_n_for_1_multiplies(self):
        # reverse 1-for-2 split (split_from=2, split_to=1): prices double → 2.0
        a = ca.CorporateAction.from_split("HON", "2026-06-27", 2, 1)
        assert ca.expected_factor(a) == pytest.approx(2.0)

    def test_reverse_1_for_10_multiplies_by_10(self):
        a = ca.CorporateAction.from_split("XYZ", "2026-06-15", 10, 1)
        assert ca.expected_factor(a) == pytest.approx(10.0)

    def test_dividend_not_implemented_this_pr(self):
        a = ca.CorporateAction(type="dividend", ticker="AAPL", ex_date="2026-06-01", cash_amount=0.25)
        with pytest.raises(NotImplementedError):
            ca.expected_factor(a)

    def test_human_descriptions(self):
        assert (
            ca.CorporateAction.from_split("HON", "2026-06-27", 2, 1).human()
            == "1-for-2 reverse split"
        )
        assert (
            ca.CorporateAction.from_split("NVDA", "2026-06-10", 1, 10).human()
            == "10-for-1 forward split"
        )


class TestDetectSplits:
    def _fake_client(self, events):
        client = MagicMock()
        client.get_recent_splits.return_value = events
        return client

    def test_maps_polygon_events_to_actions(self):
        client = self._fake_client([
            {"ticker": "HON", "execution_date": "2026-06-27", "split_from": 2, "split_to": 1},
            {"ticker": "NVDA", "execution_date": "2026-06-10", "split_from": 1, "split_to": 10},
        ])
        actions = ca.detect_splits("2026-06-01", "2026-06-30", client=client)
        assert client.get_recent_splits.call_count == 1
        assert {a.ticker for a in actions} == {"HON", "NVDA"}
        hon = next(a for a in actions if a.ticker == "HON")
        assert hon.type == "split"
        assert hon.ex_date == "2026-06-27"
        assert hon.split_from == 2 and hon.split_to == 1
        assert hon.raw["execution_date"] == "2026-06-27"

    def test_malformed_events_skipped(self):
        client = self._fake_client([
            {"ticker": "HON", "execution_date": "2026-06-27", "split_from": 2, "split_to": 1},
            {"ticker": "BAD", "execution_date": "", "split_from": 2, "split_to": 1},
            {"ticker": None, "execution_date": "2026-06-20", "split_from": 1, "split_to": 4},
        ])
        actions = ca.detect_splits("2026-06-01", "2026-06-30", client=client)
        assert [a.ticker for a in actions] == ["HON"]

    def test_fetch_failure_degrades_to_empty(self):
        client = MagicMock()
        client.get_recent_splits.side_effect = RuntimeError("polygon down")
        assert ca.detect_splits("2026-06-01", "2026-06-30", client=client) == []

    def test_dividends_and_renames_not_implemented(self):
        with pytest.raises(NotImplementedError):
            ca.detect_dividends("2026-06-01", "2026-06-30")
        with pytest.raises(NotImplementedError):
            ca.detect_renames("2026-06-01", "2026-06-30")
