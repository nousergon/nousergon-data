"""``market_data/latest_weekly.json`` advances only on a Phase 1 run whose
constituents collector succeeded and whose ``constituents.json`` is on S3.

The pointer names the ``weekly/{date}/`` prefix whose ``constituents.json``
its readers resolve, and only Phase 1 writes that file (D01,
alpha-engine-config-I10855). On 2026-10-16 two weekday rehearsals' DataPhase2
advanced the pointer to ``2026-10-16``, a prefix with no ``constituents.json``,
and D48 failed that night on SPX ``weight_method='unavailable'``
(alpha-engine-config-I12377). A Phase 1 run whose constituents collector
failed did the same thing one phase earlier (alpha-engine-config-I12410).
"""
from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError

import weekly_collector as wc

MANIFEST = "market_data/weekly/2026-10-16/manifest.json"
POINTER = "market_data/latest_weekly.json"
TARGET = "market_data/weekly/2026-10-16/constituents.json"


def _not_found(*_a, **_kw):
    raise ClientError({"Error": {"Code": "404"}}, "HeadObject")


def _run(results: dict, *, target_exists: bool = True, head_error: str | None = None):
    s3 = MagicMock()
    if head_error:
        s3.head_object.side_effect = ClientError({"Error": {"Code": head_error}}, "HeadObject")
    elif not target_exists:
        s3.head_object.side_effect = _not_found
    with patch.object(wc.boto3, "client", return_value=s3):
        wc._write_manifest("bkt", "market_data/", "2026-10-16", results)
    return s3


def _keys(s3) -> list[str]:
    return [c.kwargs["Key"] for c in s3.put_object.call_args_list]


def _manifest(s3) -> dict:
    (call,) = [c for c in s3.put_object.call_args_list if c.kwargs["Key"] == MANIFEST]
    return json.loads(call.kwargs["Body"])


def _phase1(constituents_status: str | None = "ok", status: str = "ok") -> dict:
    collectors = {"prices": {"status": "ok"}}
    if constituents_status is not None:
        collectors["constituents"] = {"status": constituents_status}
    return {"phase": 1, "status": status, "collectors": collectors}


def test_phase1_with_constituents_ok_and_present_advances_pointer():
    s3 = _run(_phase1())
    assert _keys(s3) == [MANIFEST, POINTER]
    s3.head_object.assert_called_once_with(Bucket="bkt", Key=TARGET)
    pointer = json.loads(s3.put_object.call_args_list[1].kwargs["Body"])
    assert pointer == {"date": "2026-10-16", "s3_prefix": "market_data/weekly/2026-10-16/"}
    assert _manifest(s3)["latest_weekly_pointer"]["advanced"] is True
    assert _manifest(s3)["status"] == "ok"


@pytest.mark.parametrize("phase", [2, None])
def test_other_phases_write_manifest_but_never_the_pointer(phase):
    s3 = _run({"phase": phase, "status": "ok", "collectors": {}})
    assert _keys(s3) == [MANIFEST]
    s3.head_object.assert_not_called()


@pytest.mark.parametrize("constituents_status", ["error", "skipped", None])
def test_phase1_with_failed_constituents_collector_leaves_pointer(constituents_status, caplog):
    results = _phase1(constituents_status, status="partial")
    s3 = _run(results)
    assert _keys(s3) == [MANIFEST]
    # The collector's own verdict decides first; no HEAD is spent on it.
    s3.head_object.assert_not_called()
    record = _manifest(s3)["latest_weekly_pointer"]
    assert record["advanced"] is False
    assert record["key"] == POINTER and record["target"] == TARGET
    assert repr(constituents_status) in record["reason"]
    assert results["status"] == "partial"  # already non-zero; not rewritten
    errors = [r.getMessage() for r in caplog.records if r.levelname == "ERROR"]
    assert any(POINTER in m and TARGET in m for m in errors)


@pytest.mark.parametrize("status", ["ok", "degraded"])
def test_phase1_constituents_ok_but_artifact_missing_leaves_pointer_and_fails_run(status):
    results = _phase1("ok", status=status)
    s3 = _run(results, target_exists=False)
    assert _keys(s3) == [MANIFEST]
    assert results["status"] == "failed"
    manifest = _manifest(s3)
    assert manifest["status"] == "failed"
    assert manifest["latest_weekly_pointer"]["advanced"] is False
    assert TARGET in manifest["latest_weekly_pointer"]["reason"]


def test_phase1_unanswerable_head_raises_and_never_writes_pointer():
    s3 = MagicMock()
    s3.head_object.side_effect = ClientError({"Error": {"Code": "AccessDenied"}}, "HeadObject")
    with patch.object(wc.boto3, "client", return_value=s3), pytest.raises(ClientError):
        wc._write_manifest("bkt", "market_data/", "2026-10-16", _phase1())
    assert POINTER not in _keys(s3)
