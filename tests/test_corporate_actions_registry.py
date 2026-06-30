"""Tests for ``corporate_actions.registry.CorporateActionRegistry`` (config#1431).

S3 is mocked with a small in-memory fake that implements the four operations
the registry uses (``head_object`` / ``get_object`` / ``put_object`` /
``list_objects_v2``) and raises a faithful ``ClientError`` (404 / NoSuchKey) for
missing keys — the same boto3-error surface the production code branches on.
This follows the suite's prevailing MagicMock-based S3 mocking
(``tests/test_daily_closes_skip_if_canonical.py``) but models the bucket
faithfully so write-if-absent + read-back round-trips through "S3".
"""

from __future__ import annotations

import io

from botocore.exceptions import ClientError

import corporate_actions as ca
from corporate_actions import CorporateActionRegistry


class _FakeS3:
    """Minimal in-memory S3 double (per-bucket key→bytes store)."""

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


class TestRecordDetected:
    def test_write_if_absent_is_idempotent(self):
        reg = _registry()
        action = ca.CorporateAction.from_split("HON", "2026-06-27", 2, 1)
        assert reg.record_detected(action, run_id="2026-06-26") is True
        s3 = reg.s3
        puts_after_first = s3.put_calls
        # Second call for the same action_id must NOT overwrite and returns False.
        assert reg.record_detected(action, run_id="2026-06-26-rerun") is False
        assert s3.put_calls == puts_after_first  # no second PUT

    def test_recorded_record_is_readable_and_round_trips(self):
        reg = _registry()
        action = ca.CorporateAction.from_split("NVDA", "2026-06-10", 1, 10)
        reg.record_detected(action, run_id="run-1")
        got = reg.get_action(action.action_id)
        assert got is not None
        assert got.ticker == "NVDA"
        assert got.split_to == 10
        assert got.action_id == action.action_id

    def test_list_actions_filters_by_ticker_and_type(self):
        reg = _registry()
        reg.record_detected(ca.CorporateAction.from_split("HON", "2026-06-27", 2, 1), run_id="r")
        reg.record_detected(ca.CorporateAction.from_split("NVDA", "2026-06-10", 1, 10), run_id="r")
        assert {a.ticker for a in reg.list_actions(types=["split"])} == {"HON", "NVDA"}
        assert [a.ticker for a in reg.list_actions(ticker="HON")] == ["HON"]
        assert reg.list_actions(types=["dividend"]) == []


class TestMarkApplied:
    def test_mark_applied_write_once(self):
        reg = _registry()
        action = ca.CorporateAction.from_split("HON", "2026-06-27", 2, 1)
        assert reg.is_applied("arcticdb_universe", action.action_id) is False
        assert reg.mark_applied(action, "arcticdb_universe", run_id="r") is True
        assert reg.is_applied("arcticdb_universe", action.action_id) is True
        assert reg.mark_applied(action, "arcticdb_universe", run_id="r2") is False


class TestExplainsDiscrepancy:
    def test_hon_reverse_split_explains_2x_jump(self):
        # HON-shaped 1-for-2 reverse split: 229.49 -> 458.98 (factor 2.0).
        reg = _registry()
        action = ca.CorporateAction.from_split("HON", "2026-06-27", 2, 1)
        reg.record_detected(action, run_id="r")
        got = reg.explains_discrepancy("HON", "2026-06-25", 229.49, 458.98)
        assert got is not None
        assert got.action_id == action.action_id
        assert got.human() == "1-for-2 reverse split"

    def test_unexplained_2x_with_no_registered_action_returns_none(self):
        reg = _registry()  # empty registry
        assert reg.explains_discrepancy("FOO", "2026-06-25", 100.0, 200.0) is None

    def test_action_for_other_ticker_does_not_explain(self):
        reg = _registry()
        reg.record_detected(ca.CorporateAction.from_split("HON", "2026-06-27", 2, 1), run_id="r")
        assert reg.explains_discrepancy("MMM", "2026-06-25", 100.0, 200.0) is None

    def test_wrong_factor_does_not_match(self):
        # Registered 2:1 reverse split, but the observed jump is 3x — not a match.
        reg = _registry()
        reg.record_detected(ca.CorporateAction.from_split("HON", "2026-06-27", 2, 1), run_id="r")
        assert reg.explains_discrepancy("HON", "2026-06-25", 100.0, 300.0) is None

    def test_ex_date_before_discrepancy_date_does_not_match(self):
        # A split whose ex_date is well BEFORE the discrepancy date cannot
        # explain it (the split restates dates strictly before its ex date).
        reg = _registry()
        reg.record_detected(ca.CorporateAction.from_split("HON", "2026-05-01", 2, 1), run_id="r")
        assert reg.explains_discrepancy("HON", "2026-06-25", 229.49, 458.98) is None

    def test_forward_split_factor_explains_division(self):
        # NVDA 10-for-1 forward: 1000 -> 100 (factor 0.1).
        reg = _registry()
        action = ca.CorporateAction.from_split("NVDA", "2026-06-12", 1, 10)
        reg.record_detected(action, run_id="r")
        got = reg.explains_discrepancy("NVDA", "2026-06-10", 1000.0, 100.0)
        assert got is not None
        assert got.action_id == action.action_id
