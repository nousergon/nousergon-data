"""Unit tests for the alpha-engine-eod-precondition-probe Lambda
(alpha-engine-config-I2702 deliverable #1; config#3237 universe-close
addition).

Verify-by-artifact: precondition_met must be driven by the S3 sentinels'
run_date + verified content, never by a launch-phase flag, and any
non-"absent" S3 failure must raise (fail-loud) rather than resolve to False.
precondition_met is the AND of the macro-SPY sentinel (config-I2702) and the
universe-close sentinel (config#3237) — reconcile needs both libraries'
run_date rows.
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


def _s3_with_objects_by_key(bodies_by_key: dict[str, dict], *, error_code: str | None = None):
    """Mock returning a different body per S3 Key — mirrors the two distinct
    sentinels (macro + universe) the handler now reads. A key absent from
    ``bodies_by_key`` raises NoSuchKey (or ``error_code`` if given)."""
    cli = MagicMock()

    def _get_object(Bucket, Key):  # noqa: N803 — matches boto3's call signature
        if Key not in bodies_by_key:
            raise ClientError(
                {"Error": {"Code": error_code or "NoSuchKey", "Message": "nope"}}, "GetObject"
            )
        payload = json.dumps(bodies_by_key[Key]).encode("utf-8")
        return {"Body": MagicMock(read=MagicMock(return_value=payload))}

    cli.get_object.side_effect = _get_object
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


class TestEvaluateUniverse:
    """config#3237: the `universe`-library counterpart to TestEvaluate."""

    def test_absent_sentinel_is_not_met(self):
        met, reason = index._evaluate_universe(None, "2026-07-21")
        assert met is False
        assert "no universe-close-freshness sentinel" in reason

    def test_wrong_run_date_is_not_met(self):
        sentinel = {"run_date": "2026-07-20", "verified_ticker_count": 500}
        met, reason = index._evaluate_universe(sentinel, "2026-07-21")
        assert met is False
        assert "does not match requested" in reason

    def test_zero_verified_count_is_not_met(self):
        # The 2026-07-21 incident shape when only the count degrades (rather
        # than the sentinel being entirely absent, e.g. a producer regression).
        sentinel = {"run_date": "2026-07-21", "verified_ticker_count": 0}
        met, reason = index._evaluate_universe(sentinel, "2026-07-21")
        assert met is False
        assert "below floor" in reason

    def test_missing_verified_ticker_count_key_is_not_met(self):
        sentinel = {"run_date": "2026-07-21"}
        met, reason = index._evaluate_universe(sentinel, "2026-07-21")
        assert met is False
        assert "below floor" in reason

    def test_matching_run_date_with_nonzero_count_is_met(self):
        sentinel = {"run_date": "2026-07-21", "verified_ticker_count": 512}
        met, reason = index._evaluate_universe(sentinel, "2026-07-21")
        assert met is True
        assert "512" in reason


class TestHealDeadline:
    def test_deadline_is_9am_utc_next_day(self):
        assert index._heal_deadline_iso("2026-07-15") == "2026-07-16T09:00:00Z"

    def test_deadline_respects_month_boundary(self):
        assert index._heal_deadline_iso("2026-07-31") == "2026-08-01T09:00:00Z"


class TestHandler:
    def test_raises_without_run_date(self):
        with pytest.raises(ValueError):
            index.handler({}, None)

    def test_precondition_met_true_when_both_sentinels_verify(self, monkeypatch):
        cli = _s3_with_objects_by_key({
            index.MACRO_FRESHNESS_SENTINEL_KEY: {"run_date": "2026-07-15", "verified_keys": ["SPY"]},
            index.UNIVERSE_FRESHNESS_SENTINEL_KEY: {"run_date": "2026-07-15", "verified_ticker_count": 500},
        })
        monkeypatch.setattr(index.boto3, "client", lambda *a, **k: cli)
        result = index.handler({"run_date": "2026-07-15"}, None)
        assert result["precondition_met"] is True
        assert result["run_date"] == "2026-07-15"
        assert result["deadline_iso"] == "2026-07-16T09:00:00Z"
        assert result["sentinel"]["verified_keys"] == ["SPY"]
        assert result["universe_sentinel"]["verified_ticker_count"] == 500

    def test_precondition_met_false_on_absent_macro_sentinel(self, monkeypatch):
        cli = _s3_with_objects_by_key({
            index.UNIVERSE_FRESHNESS_SENTINEL_KEY: {"run_date": "2026-07-15", "verified_ticker_count": 500},
        })
        monkeypatch.setattr(index.boto3, "client", lambda *a, **k: cli)
        result = index.handler({"run_date": "2026-07-15"}, None)
        assert result["precondition_met"] is False
        assert result["sentinel"] is None
        assert "macro:" in result["reason"]
        assert "universe:" not in result["reason"]

    def test_precondition_met_false_on_absent_universe_sentinel(self, monkeypatch):
        # config#3237's exact 2026-07-21 shape: macro sentinel present +
        # matching, universe sentinel entirely missing (100% universe-append
        # failure never reached the write point) — must NOT report met=true.
        cli = _s3_with_objects_by_key({
            index.MACRO_FRESHNESS_SENTINEL_KEY: {"run_date": "2026-07-21", "verified_keys": ["SPY"]},
        })
        monkeypatch.setattr(index.boto3, "client", lambda *a, **k: cli)
        result = index.handler({"run_date": "2026-07-21"}, None)
        assert result["precondition_met"] is False
        assert result["universe_sentinel"] is None
        assert "universe:" in result["reason"]

    def test_precondition_met_false_on_absent_both_sentinels(self, monkeypatch):
        cli = _s3_with_objects_by_key({})
        monkeypatch.setattr(index.boto3, "client", lambda *a, **k: cli)
        result = index.handler({"run_date": "2026-07-15"}, None)
        assert result["precondition_met"] is False
        assert result["sentinel"] is None
        assert result["universe_sentinel"] is None
        assert "macro:" in result["reason"]
        assert "universe:" in result["reason"]

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
