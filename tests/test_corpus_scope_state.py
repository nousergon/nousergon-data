"""Tests for rag/pipelines/_corpus_scope_state.py — the persisted
corpus-scope pointer that drives ticker-churn detection (config#2943
deliverable 2b: a ticker newly entering scope gets its 2yr filings
backfill folded into that day's delta pass).
"""

from __future__ import annotations

import json
from datetime import date, timedelta
from io import BytesIO
from unittest.mock import MagicMock

from rag.pipelines import _corpus_scope_state as state_mod


class TestLoadPriorScope:
    def test_reads_tickers(self):
        s3 = MagicMock()
        s3.get_object.return_value = {
            "Body": BytesIO(json.dumps({"as_of": "2026-07-18", "tickers": ["AAPL", "msft"]}).encode()),
        }
        result = state_mod.load_prior_scope("b", s3)
        assert result == {"AAPL", "MSFT"}

    def test_fail_soft_missing_returns_empty(self):
        s3 = MagicMock()
        s3.get_object.side_effect = RuntimeError("NoSuchKey")
        assert state_mod.load_prior_scope("b", s3) == set()


class TestWriteScopeState:
    def test_writes_expected_payload(self):
        s3 = MagicMock()
        key = state_mod.write_scope_state({"AAPL", "MSFT"}, as_of=date(2026, 7, 19), bucket="b", s3_client=s3)
        assert key == state_mod.SCOPE_STATE_KEY
        call = s3.put_object.call_args
        assert call.kwargs["Bucket"] == "b"
        assert call.kwargs["Key"] == state_mod.SCOPE_STATE_KEY
        payload = json.loads(call.kwargs["Body"])
        assert payload["as_of"] == "2026-07-19"
        assert payload["tickers"] == ["AAPL", "MSFT"]
        assert payload["count"] == 2


class TestDiffScope:
    def test_new_and_dropped(self):
        current = {"AAPL", "MSFT", "NVDA"}
        prior = {"AAPL", "TSLA"}
        new_to_scope, dropped = state_mod.diff_scope(current, prior)
        assert new_to_scope == {"MSFT", "NVDA"}
        assert dropped == {"TSLA"}

    def test_no_prior_means_everything_is_new(self):
        current = {"AAPL", "MSFT"}
        new_to_scope, dropped = state_mod.diff_scope(current, set())
        assert new_to_scope == current
        assert dropped == set()

    def test_identical_scope_no_churn(self):
        current = {"AAPL", "MSFT"}
        new_to_scope, dropped = state_mod.diff_scope(current, current)
        assert new_to_scope == set()
        assert dropped == set()


class TestNeedsWideTopup:
    """config#2943: the Saturday top-up must widen its lookback windows
    back to full-coverage if the week's daily corpus-delta passes didn't
    actually run — cold start (first deploy) or a sustained daily-delta
    outage. Every failure mode must default to True (widen), never False
    (stay thin) — fail-safe, not fail-thin."""

    def test_cold_start_no_pointer_widens(self):
        s3 = MagicMock()
        s3.get_object.side_effect = RuntimeError("NoSuchKey")
        assert state_mod.needs_wide_topup("b", s3) is True

    def test_malformed_pointer_widens(self):
        s3 = MagicMock()
        s3.get_object.return_value = {"Body": BytesIO(b"not json")}
        assert state_mod.needs_wide_topup("b", s3) is True

    def test_fresh_pointer_stays_thin(self):
        s3 = MagicMock()
        s3.get_object.return_value = {
            "Body": BytesIO(json.dumps({"as_of": date.today().isoformat()}).encode()),
        }
        assert state_mod.needs_wide_topup("b", s3) is False

    def test_pointer_at_threshold_stays_thin(self):
        # Exactly STALE_COVERAGE_THRESHOLD_DAYS old — boundary is
        # inclusive of "still fresh" (age_days > threshold triggers wide,
        # not >=).
        as_of = date.today() - timedelta(days=state_mod.STALE_COVERAGE_THRESHOLD_DAYS)
        s3 = MagicMock()
        s3.get_object.return_value = {
            "Body": BytesIO(json.dumps({"as_of": as_of.isoformat()}).encode()),
        }
        assert state_mod.needs_wide_topup("b", s3) is False

    def test_stale_pointer_widens(self):
        as_of = date.today() - timedelta(days=state_mod.STALE_COVERAGE_THRESHOLD_DAYS + 1)
        s3 = MagicMock()
        s3.get_object.return_value = {
            "Body": BytesIO(json.dumps({"as_of": as_of.isoformat()}).encode()),
        }
        assert state_mod.needs_wide_topup("b", s3) is True

    def test_missing_as_of_key_widens(self):
        s3 = MagicMock()
        s3.get_object.return_value = {"Body": BytesIO(json.dumps({"tickers": ["AAPL"]}).encode())}
        assert state_mod.needs_wide_topup("b", s3) is True
