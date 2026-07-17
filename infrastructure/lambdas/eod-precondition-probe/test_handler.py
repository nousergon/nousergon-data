"""Unit tests for the alpha-engine-eod-precondition-probe Lambda
(alpha-engine-config-I2702 deliverable #1).

Verify-by-artifact: precondition_met must be driven by the S3 sentinel's
run_date + verified_keys content, never by a launch-phase flag, and any
non-"absent" S3 failure must raise (fail-loud) rather than resolve to False.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest
from botocore.exceptions import ClientError

import index


def _s3_with_object(body: dict | None, *, error_code: str | None = None):
    cli = MagicMock()
    if error_code is not None:
        cli.get_object.side_effect = ClientError(
            {"Error": {"Code": error_code, "Message": "nope"}}, "GetObject"
        )
    else:
        payload = json.dumps(body).encode("utf-8")
        cli.get_object.return_value = {"Body": MagicMock(read=MagicMock(return_value=payload))}
    return cli


class TestReadSentinel:
    def test_missing_object_returns_none(self):
        cli = _s3_with_object(None, error_code="NoSuchKey")
        assert index._read_sentinel(cli, "bucket", "key") is None

    def test_404_returns_none(self):
        cli = _s3_with_object(None, error_code="404")
        assert index._read_sentinel(cli, "bucket", "key") is None

    def test_other_client_error_raises(self):
        cli = _s3_with_object(None, error_code="AccessDenied")
        with pytest.raises(ClientError):
            index._read_sentinel(cli, "bucket", "key")

    def test_present_object_parses_json(self):
        cli = _s3_with_object({"run_date": "2026-07-15", "verified_keys": ["SPY"]})
        assert index._read_sentinel(cli, "bucket", "key") == {
            "run_date": "2026-07-15", "verified_keys": ["SPY"],
        }


class TestEvaluate:
    def test_absent_sentinel_is_not_met(self):
        met, reason = index._evaluate(None, "2026-07-15")
        assert met is False
        assert "no macro-freshness sentinel" in reason

    def test_wrong_run_date_is_not_met(self):
        sentinel = {"run_date": "2026-07-14", "verified_keys": ["SPY"]}
        met, reason = index._evaluate(sentinel, "2026-07-15")
        assert met is False
        assert "does not match requested" in reason

    def test_missing_required_key_is_not_met(self):
        sentinel = {"run_date": "2026-07-15", "verified_keys": ["VIX", "TNX"]}
        met, reason = index._evaluate(sentinel, "2026-07-15")
        assert met is False
        assert "SPY" in reason

    def test_matching_run_date_with_spy_is_met(self):
        sentinel = {"run_date": "2026-07-15", "verified_keys": ["SPY", "VIX"]}
        met, reason = index._evaluate(sentinel, "2026-07-15")
        assert met is True
        assert "verified present" in reason


class TestHealDeadline:
    def test_deadline_is_9am_utc_next_day(self):
        assert index._heal_deadline_iso("2026-07-15") == "2026-07-16T09:00:00Z"

    def test_deadline_respects_month_boundary(self):
        assert index._heal_deadline_iso("2026-07-31") == "2026-08-01T09:00:00Z"


class TestHandler:
    def test_raises_without_run_date(self):
        with pytest.raises(ValueError):
            index.handler({}, None)

    def test_precondition_met_true(self, monkeypatch):
        cli = _s3_with_object({"run_date": "2026-07-15", "verified_keys": ["SPY"]})
        monkeypatch.setattr(index.boto3, "client", lambda *a, **k: cli)
        result = index.handler({"run_date": "2026-07-15"}, None)
        assert result["precondition_met"] is True
        assert result["run_date"] == "2026-07-15"
        assert result["deadline_iso"] == "2026-07-16T09:00:00Z"
        assert result["sentinel"]["verified_keys"] == ["SPY"]

    def test_precondition_met_false_on_absent_sentinel(self, monkeypatch):
        cli = _s3_with_object(None, error_code="NoSuchKey")
        monkeypatch.setattr(index.boto3, "client", lambda *a, **k: cli)
        result = index.handler({"run_date": "2026-07-15"}, None)
        assert result["precondition_met"] is False
        assert result["sentinel"] is None

    def test_s3_failure_propagates(self, monkeypatch):
        cli = _s3_with_object(None, error_code="AccessDenied")
        monkeypatch.setattr(index.boto3, "client", lambda *a, **k: cli)
        with pytest.raises(ClientError):
            index.handler({"run_date": "2026-07-15"}, None)

    def test_past_deadline_true_when_now_after_deadline(self, monkeypatch):
        cli = _s3_with_object(None, error_code="NoSuchKey")
        monkeypatch.setattr(index.boto3, "client", lambda *a, **k: cli)

        class _FrozenDatetime(datetime):
            @classmethod
            def now(cls, tz=None):
                return datetime(2026, 7, 16, 10, 0, 0, tzinfo=timezone.utc)

        monkeypatch.setattr(index, "datetime", _FrozenDatetime)
        result = index.handler({"run_date": "2026-07-15"}, None)
        assert result["past_deadline"] is True

    def test_not_past_deadline_when_now_before_deadline(self, monkeypatch):
        cli = _s3_with_object(None, error_code="NoSuchKey")
        monkeypatch.setattr(index.boto3, "client", lambda *a, **k: cli)

        class _FrozenDatetime(datetime):
            @classmethod
            def now(cls, tz=None):
                return datetime(2026, 7, 15, 23, 0, 0, tzinfo=timezone.utc)

        monkeypatch.setattr(index, "datetime", _FrozenDatetime)
        result = index.handler({"run_date": "2026-07-15"}, None)
        assert result["past_deadline"] is False
