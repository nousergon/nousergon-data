"""Unit tests for the sf-watch-market-hours-toggler handler.

Covers:
- is_market_hours() boundary correctness (weekday/weekend/holiday, the
  9:30/16:00 ET open/close edges) — must match
  crucible-executor/executor/market_hours.py::is_market_hours() exactly,
  since that's the live constant sf-watch-executor-role-policy-market-
  hours.json's Ask block was ruled against.
- handler() idempotency: no PutRolePolicy call when the live policy
  already matches the desired variant.
- handler() picks the correct variant file and writes it when it doesn't.
"""

from __future__ import annotations

import json
import sys
from datetime import datetime
from pathlib import Path
from unittest import mock
from zoneinfo import ZoneInfo

sys.path.insert(0, str(Path(__file__).parent))
import index  # noqa: E402

_ET = ZoneInfo("America/New_York")


def _et(y, m, d, hh, mm):
    return datetime(y, m, d, hh, mm, tzinfo=_ET)


# ----- is_market_hours() ----------------------------------------------------

def test_weekday_during_session():
    assert index.is_market_hours(_et(2026, 7, 21, 10, 0)) is True  # Tuesday


def test_weekend_closed():
    assert index.is_market_hours(_et(2026, 7, 18, 10, 0)) is False  # Saturday


def test_holiday_closed():
    assert index.is_market_hours(_et(2026, 7, 3, 10, 0)) is False  # July 4 observed


def test_before_open_closed():
    assert index.is_market_hours(_et(2026, 7, 21, 9, 29)) is False


def test_at_open_boundary_is_open():
    assert index.is_market_hours(_et(2026, 7, 21, 9, 30)) is True


def test_at_close_boundary_is_closed():
    assert index.is_market_hours(_et(2026, 7, 21, 16, 0)) is False


def test_just_before_close_is_open():
    assert index.is_market_hours(_et(2026, 7, 21, 15, 59)) is True


# ----- handler() -------------------------------------------------------------

def _iam_mock(current_document):
    iam = mock.Mock()

    class NoSuchEntityException(Exception):
        pass

    iam.exceptions.NoSuchEntityException = NoSuchEntityException
    if current_document is None:
        iam.get_role_policy.side_effect = NoSuchEntityException()
    else:
        iam.get_role_policy.return_value = {"PolicyDocument": current_document}
    return iam


def test_handler_noop_when_already_market_hours_variant(monkeypatch):
    monkeypatch.setattr(index, "_now", lambda: _et(2026, 7, 21, 10, 0))
    desired = index._load_json(index.MARKET_HOURS_POLICY_FILE)
    iam = _iam_mock(desired)
    with mock.patch("boto3.client", return_value=iam):
        result = index.handler({}, None)

    assert result == {"changed": False, "variant": "market-hours", "market_open": True}
    iam.put_role_policy.assert_not_called()


def test_handler_applies_market_hours_variant_when_stale(monkeypatch):
    monkeypatch.setattr(index, "_now", lambda: _et(2026, 7, 21, 10, 0))
    permissive = index._load_json(index.PERMISSIVE_POLICY_FILE)
    iam = _iam_mock(permissive)
    with mock.patch("boto3.client", return_value=iam):
        result = index.handler({}, None)

    assert result == {"changed": True, "variant": "market-hours", "market_open": True}
    iam.put_role_policy.assert_called_once()
    kwargs = iam.put_role_policy.call_args.kwargs
    assert kwargs["RoleName"] == index.ROLE_NAME
    assert kwargs["PolicyName"] == index.POLICY_NAME
    assert json.loads(kwargs["PolicyDocument"]) == index._load_json(index.MARKET_HOURS_POLICY_FILE)


def test_handler_applies_permissive_variant_off_hours(monkeypatch):
    monkeypatch.setattr(index, "_now", lambda: _et(2026, 7, 21, 20, 0))
    iam = _iam_mock(None)  # first-ever run, role has no inline policy yet
    with mock.patch("boto3.client", return_value=iam):
        result = index.handler({}, None)

    assert result == {"changed": True, "variant": "permissive", "market_open": False}
    kwargs = iam.put_role_policy.call_args.kwargs
    assert json.loads(kwargs["PolicyDocument"]) == index._load_json(index.PERMISSIVE_POLICY_FILE)


def test_market_hours_variant_only_drops_trading_pipelines():
    """The restricted variant must differ from the permissive one ONLY by
    dropping the two trading-pipeline resources from RerunFleetSFFromFailedStep
    — every other statement (diagnosis, S3, SSM, DynamoDB) must be identical,
    so the toggle never accidentally narrows/widens anything else."""
    permissive = index._load_json(index.PERMISSIVE_POLICY_FILE)
    restricted = index._load_json(index.MARKET_HOURS_POLICY_FILE)

    def statement(doc, sid):
        return next(s for s in doc["Statement"] if s["Sid"] == sid)

    for sid in [
        "DiagnoseFleetSF", "DiagnoseLogs", "DiagnoseMetrics",
        "ReadArtifactsForDiagnosis", "EnrichWatchLog",
        "WriteSfWatchUsageTelemetry", "ReadWriteSfWatchCompletionMarkers",
        "ReadFleetPatAndClaudeTokenAndTelegramCreds", "RerunHelperMutexSteal",
    ]:
        assert statement(permissive, sid) == statement(restricted, sid)

    permissive_resources = set(statement(permissive, "RerunFleetSFFromFailedStep")["Resource"])
    restricted_resources = set(statement(restricted, "RerunFleetSFFromFailedStep")["Resource"])
    dropped = permissive_resources - restricted_resources
    assert dropped == {
        "arn:aws:states:us-east-1:711398986525:stateMachine:ne-preopen-trading-pipeline",
        "arn:aws:states:us-east-1:711398986525:stateMachine:ne-postclose-trading-pipeline",
    }
    assert restricted_resources < permissive_resources
