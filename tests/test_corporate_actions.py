"""Unit tests for the ``corporate_actions`` model layer (config#1431).

Covers ``CorporateAction`` deterministic ``action_id``, ``expected_factor`` for
forward/reverse splits (delegating to ``split_factor``), and ``detect_splits``
mapping polygon events via a fake client (mirroring the ``get_recent_splits``
mocking in ``tests/test_daily_closes_skip_if_canonical.py``).
"""

from __future__ import annotations

import io

from unittest.mock import MagicMock

import numpy as np
import pandas as pd
import pytest
from botocore.exceptions import ClientError

import corporate_actions as ca
from corporate_actions import CorporateActionRegistry
from corporate_actions import restate_series_for_splits


class _FakeS3:
    """Minimal in-memory S3 double (per-bucket key→bytes store) — mirrors the
    one in ``tests/test_corporate_actions_registry.py`` so ``apply`` can be
    exercised against a faithful write-if-absent / read-back registry."""

    def __init__(self):
        self.store: dict[str, bytes] = {}
        self.put_calls = 0

    def _client_error(self, code: str, op: str) -> ClientError:
        return ClientError({"Error": {"Code": code, "Message": "missing"}}, op)

    def head_object(self, *, Bucket, Key):
        if Key not in self.store:
            raise self._client_error("404", "HeadObject")
        return {"ContentLength": len(self.store[Key])}

    def get_object(self, *, Bucket, Key):
        if Key not in self.store:
            raise self._client_error("NoSuchKey", "GetObject")
        return {"Body": io.BytesIO(self.store[Key])}

    def put_object(self, *, Bucket, Key, Body, ContentType=None):
        self.put_calls += 1
        self.store[Key] = Body if isinstance(Body, bytes) else bytes(Body)
        return {"ETag": '"fake"'}

    def list_objects_v2(self, *, Bucket, Prefix, ContinuationToken=None):
        keys = sorted(k for k in self.store if k.startswith(Prefix))
        return {"Contents": [{"Key": k} for k in keys], "IsTruncated": False}


def _registry():
    return CorporateActionRegistry(_FakeS3(), "alpha-engine-research")


def _unfolded_reverse_split_frame():
    """A DD-style series un-restated across a 1-for-3 REVERSE split (ex 6/24):
    pre-split rows on the OLD (~$48) scale, post-split on the NEW (~$144)
    scale — a ~3x boundary jump that restatement must remove."""
    pre_dates = pd.bdate_range("2026-06-01", "2026-06-23")
    post_dates = pd.bdate_range("2026-06-24", "2026-07-01")
    pre = pd.Series(np.linspace(47.5, 48.5, len(pre_dates)), index=pre_dates)
    post = pd.Series(np.linspace(142.5, 145.5, len(post_dates)), index=post_dates)
    close = pd.concat([pre, post])
    return pd.DataFrame(
        {
            "Open": close, "High": close * 1.01, "Low": close * 0.99,
            "Close": close, "Volume": np.full(len(close), 1_000_000.0),
        }
    )


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

    def test_renames_now_implemented(self):
        # Renames are implemented (PR6): detect_renames takes CANDIDATE tickers
        # and returns a RenameDetection (no longer a NotImplementedError stub).
        client = MagicMock()
        client.get_ticker_events.return_value = []
        result = ca.detect_renames(["FOO"], client=client)
        assert isinstance(result, ca.RenameDetection)
        assert result.renames == []
        assert result.failed_candidates == set()


class TestApply:
    _STORE = ca.STORE_ARCTICDB_UNIVERSE

    def test_restates_split_with_parity_to_restate_series(self):
        """``apply`` (registry=None) restates a split series identically to a
        direct ``restate_series_for_splits`` — it must not re-derive the factor
        convention, only route through it."""
        df = _unfolded_reverse_split_frame()
        action = ca.CorporateAction.from_split("DD", "2026-06-24", 3, 1)

        out, results = ca.apply(df, [action], store=self._STORE, registry=None)

        events = [{"execution_date": "2026-06-24", "split_from": 3, "split_to": 1}]
        expected = restate_series_for_splits(df, events)
        pd.testing.assert_frame_equal(out, expected)

        # The boundary jump is gone (fully continuous), and the applied_result
        # captures the contract fields.
        assert out["Close"].pct_change().abs().max() < 0.45
        assert len(results) == 1
        r = results[0]
        assert r["action_id"] == action.action_id
        assert r["store"] == self._STORE
        assert r["status"] == "applied"
        assert r["factor"] == pytest.approx(3.0)
        assert r["n_rows_adjusted"] == int(df.index.size - 6)  # 6 post-split rows

    def test_registry_idempotency_second_apply_is_noop(self):
        """Applying the SAME action twice WITH a registry: the first restates +
        marks applied; the second is a registry noop (no double-adjust)."""
        reg = _registry()
        df = _unfolded_reverse_split_frame()
        action = ca.CorporateAction.from_split("DD", "2026-06-24", 3, 1)

        out1, res1 = ca.apply(df, [action], store=self._STORE, registry=reg, run_id="r1")
        assert res1[0]["status"] == "applied"
        assert res1[0]["n_rows_adjusted"] > 0
        assert reg.is_applied(self._STORE, action.action_id) is True

        # Second call on the ALREADY-restated frame: marker short-circuits →
        # noop, frame returned unchanged (NOT double-adjusted).
        out2, res2 = ca.apply(out1, [action], store=self._STORE, registry=reg, run_id="r2")
        assert res2[0]["status"] == "noop"
        assert res2[0]["n_rows_adjusted"] == 0
        pd.testing.assert_frame_equal(out2, out1)

    def test_registry_idempotency_guards_reapply_to_unrestated_source(self):
        """The marker is DECOUPLED from source purity: once marked applied,
        re-applying to the still-UN-restated source is a noop (the double-apply
        guard for the already-restated-store read path) — it does NOT silently
        double-adjust the raw frame."""
        reg = _registry()
        df = _unfolded_reverse_split_frame()
        action = ca.CorporateAction.from_split("DD", "2026-06-24", 3, 1)

        ca.apply(df, [action], store=self._STORE, registry=reg, run_id="r1")
        # Feed the ORIGINAL un-restated frame again — marker present → noop.
        out2, res2 = ca.apply(df, [action], store=self._STORE, registry=reg, run_id="r2")
        assert res2[0]["status"] == "noop"
        pd.testing.assert_frame_equal(out2, df)  # untouched, not double-adjusted

    def test_without_registry_structural_idempotency_from_raw(self):
        """registry=None → idempotency is structural: re-running on the same raw
        source yields the identical result (deterministic full-factor restate)."""
        df = _unfolded_reverse_split_frame()
        action = ca.CorporateAction.from_split("DD", "2026-06-24", 3, 1)
        out_a, _ = ca.apply(df, [action], store=self._STORE, registry=None)
        out_b, _ = ca.apply(df, [action], store=self._STORE, registry=None)
        pd.testing.assert_frame_equal(out_a, out_b)

    def test_dividend_action_raises_not_implemented(self):
        df = _unfolded_reverse_split_frame()
        div = ca.CorporateAction(
            type="dividend", ticker="DD", ex_date="2026-06-24", cash_amount=0.5,
        )
        with pytest.raises(NotImplementedError):
            ca.apply(df, [div], store=self._STORE, registry=None)

    def test_empty_inputs_are_safe_noops(self):
        df = _unfolded_reverse_split_frame()
        out, res = ca.apply(df, [], store=self._STORE, registry=None)
        assert out is df and res == []
        out2, res2 = ca.apply(pd.DataFrame(), [ca.CorporateAction.from_split("DD", "2026-06-24", 3, 1)], store=self._STORE)
        assert res2 == []
