"""A retry of the weekly phase 1 must neither skip a degraded unit nor bury a
published one (alpha-engine-config-I11812, the 2026-10-03 weekly).

What happened, in the order these tests replay it:

1. Attempt 0 of `weekly-phase-one` published every unit `ok` except D02, which
   DEGRADED. The phase marker for D02 still said `status: ok` (with an empty
   `artifact_keys` list), because the registry writes `ok` for any block that
   exits without raising.
2. The Step Function retried the workload. The registry auto-skips a phase whose
   marker says `ok` and whose declared artifacts all exist — an empty list
   always does — so attempt 1 skipped D02 instead of recomputing it.
3. Every unit attempt 0 had published auto-skipped too, and each filed a
   same-date `not_applicable` manifest as its NEWEST. D03 and D08, which always
   recompute, found nothing new and filed `failed` EmptyProduction.
4. The completion predicate (producer VerifyRunManifests and the v1 consumer's
   readiness wait) grades only the newest manifest, so the cycle failed on
   units that had published.

These tests use the REAL `nousergon_lib.phase_registry.PhaseRegistry` and the
REAL run-manifest wrapper over one in-memory S3, so the marker, the manifest and
the predicate all read what the others wrote.
"""

from __future__ import annotations

import json
import time

import pytest
from botocore.exceptions import ClientError
from nousergon_lib.phase_registry import PhaseRegistry

import run_units
import weekly_collector
from data_gate import run_manifest_predicate as predicate

BUCKET = "alpha-engine-research"
DAY = "2026-10-02"
D02_KEY = "market_data/historical_constituents.json"
D02_MARKER = f"data/{DAY}/.phases/historical_constituents.json"
STARTED = "2026-10-03T09:00:40Z"


class MemS3:
    """The five S3 calls the registry, the manifest sink and the predicate make."""

    def __init__(self):
        self.objects: dict[str, bytes] = {}

    def put_object(self, Bucket, Key, Body, ContentType=None, **kw):  # noqa: N803
        self.objects[Key] = Body if isinstance(Body, bytes) else Body.encode()
        return {"ETag": '"e"'}

    def get_object(self, Bucket, Key):  # noqa: N803
        if Key not in self.objects:
            raise ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject")
        payload = self.objects[Key]

        class _Body:
            def read(self_inner):
                return payload

        return {"Body": _Body()}

    def head_object(self, Bucket, Key):  # noqa: N803
        if Key not in self.objects:
            raise ClientError({"Error": {"Code": "404"}}, "HeadObject")
        return {"ContentLength": len(self.objects[Key]), "ETag": '"e"'}

    def list_objects_v2(self, Bucket, Prefix, StartAfter="", ContinuationToken=None, **kw):  # noqa: N803
        keys = sorted(k for k in self.objects if k.startswith(Prefix) and k > StartAfter)
        return {"Contents": [{"Key": k} for k in keys], "IsTruncated": False}

    def json(self, key: str) -> dict:
        return json.loads(self.objects[key])

    def manifests(self, unit: str) -> list[dict]:
        prefix = f"data_collection/runs/{unit}/{DAY}/"
        return [self.json(k) for k in sorted(self.objects) if k.startswith(prefix)]


@pytest.fixture(autouse=True)
def _measured_environment(monkeypatch):
    monkeypatch.setenv("NE_DATA_CODE_SHA", "a" * 40)
    monkeypatch.setenv("NE_DATA_LOG_LOCATION", "cloudwatch:/alpha-engine/data-spot:s-1")
    monkeypatch.setenv("NE_DATA_TRIGGER", "scheduled")
    monkeypatch.setenv("RUN_TOKEN", "")
    predicate.reset_unit_cache()


def _attempt(s3: MemS3) -> PhaseRegistry:
    """A fresh registry, exactly as a new `weekly_collector --phase 1` process builds one."""
    time.sleep(0.005)  # run ids are ULIDs; keep attempts in distinct milliseconds
    reg = PhaseRegistry(date=DAY, bucket=BUCKET, marker_prefix="data", s3_client=s3)
    reg.data_mode = "phase1"
    return reg


def _d02(s3: MemS3, status: str, calls: list):
    def run():
        calls.append(status)
        s3.put_object(Bucket=BUCKET, Key=D02_KEY, Body=b'{"changes": []}')
        result = {"status": status, "n_changes": 12}
        if status == "degraded":
            result["error"] = "1 unexplained reference disagreement(s): ['observed added VYLR not in reference']"
        return result

    return run


def _collect_d02(s3, reg, status, calls):
    return weekly_collector._phase_collect(
        reg, "historical_constituents", _d02(s3, status, calls), artifact_key=D02_KEY,
    )


def _verify(s3, units, started_at=STARTED):
    return predicate.completion_check(
        {"collection": "weekly", "units": list(units), "started_at": started_at}, s3_client=s3,
    )["completion"]


# ── A. the marker says what happened ─────────────────────────────────────────


def test_a_degraded_phase_writes_an_error_marker_not_ok():
    s3 = MemS3()
    result = _collect_d02(s3, _attempt(s3), "degraded", [])

    # The PROCESS posture is unchanged: the collector's own result comes back.
    assert result["status"] == "degraded"
    marker = s3.json(D02_MARKER)
    assert marker["status"] == "error"
    assert "degraded" in marker["error"]
    assert marker["artifact_keys"] == []
    # ...and the manifest still files `failed`, naming the defect.
    (manifest,) = s3.manifests("D02")
    assert manifest["status"] == "failed"
    assert "VYLR" in manifest["reason"]


def test_a_retry_recomputes_a_degraded_phase_instead_of_skipping_it():
    """The 10-03 botch, step 2: attempt 1 must call the collector again."""
    s3 = MemS3()
    calls: list = []
    _collect_d02(s3, _attempt(s3), "degraded", calls)

    retry = _attempt(s3)
    assert retry.should_run("historical_constituents", supports_auto_skip=True) == (
        True, "default_run",
    )
    result = _collect_d02(s3, retry, "ok", calls)

    assert calls == ["degraded", "ok"]
    assert result["status"] == "ok"
    assert s3.json(D02_MARKER)["status"] == "ok"
    assert s3.json(D02_MARKER)["artifact_keys"] == [D02_KEY]
    assert [m["status"] for m in s3.manifests("D02")] == ["failed", "ok"]


@pytest.mark.parametrize("status", ["partial", "skipped"])
def test_every_non_ok_collector_status_withholds_the_ok_marker(status):
    s3 = MemS3()
    weekly_collector._phase_collect(
        _attempt(s3), "historical_constituents",
        lambda: {"status": status, "reason": "x"}, artifact_key=D02_KEY,
    )
    assert s3.json(D02_MARKER)["status"] == "error"


def test_an_ok_phase_still_writes_an_ok_marker_and_still_auto_skips():
    """The fix must not cost the idempotent rerun its cache hit."""
    s3 = MemS3()
    calls: list = []
    _collect_d02(s3, _attempt(s3), "ok", calls)
    assert s3.json(D02_MARKER)["status"] == "ok"

    result = _collect_d02(s3, _attempt(s3), "ok", calls)
    assert calls == ["ok"]
    assert result["auto_skipped"] is True


# ── B. a retry never buries a published run ──────────────────────────────────


def test_a_retry_auto_skip_does_not_mask_the_published_run():
    """The 10-03 botch, steps 3-4, for an auto-skipped unit (D01, D05-D07,
    D10-D12 that day): attempt 1's `not_applicable` is the newest manifest, and
    the predicate grades the `ok` run it points back to."""
    s3 = MemS3()
    calls: list = []
    _collect_d02(s3, _attempt(s3), "ok", calls)
    _collect_d02(s3, _attempt(s3), "ok", calls)

    statuses = [m["status"] for m in s3.manifests("D02")]
    assert statuses == ["ok", "not_applicable"]
    assert s3.manifests("D02")[-1]["reason"] == run_units.NOT_RUN_NO_NEW_DATA_DECLARED

    completion = _verify(s3, ["D02"])
    assert completion["ok"] is True, completion["summary"]
    (row,) = completion["units"]
    assert row["status"] == "ok"
    assert row["carried_forward_from"] is not None
    assert row["manifest"] != row["carried_forward_from"]


def test_a_new_execution_whose_units_all_auto_skip_still_verifies():
    """The recovery shape: a NEW execution, started after attempt 0 finished.
    The no-op is fresh; the run it points back to is older and still graded."""
    s3 = MemS3()
    _collect_d02(s3, _attempt(s3), "ok", [])
    time.sleep(1.1)
    from datetime import datetime, timezone

    later = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    _collect_d02(s3, _attempt(s3), "ok", [])
    assert _verify(s3, ["D02"], started_at=later)["ok"] is True


def test_a_no_op_over_a_failed_run_still_fails():
    """Carry-forward grades the earlier run; it never upgrades it."""
    s3 = MemS3()
    prefix = f"data_collection/runs/D02/{DAY}/"
    base = {"schema_version": "data_run_manifest.v1", "unit_id": "D02", "trading_day": DAY,
            "started": "2026-10-03T09:10:00Z", "outputs": [], "guards": []}
    s3.put_object(Bucket=BUCKET, Key=prefix + "01A.json", Body=json.dumps(
        {**base, "status": "failed", "reason": "_DegradedRun: VYLR",
         "finished": "2026-10-03T10:40:00Z"}).encode())
    s3.put_object(Bucket=BUCKET, Key=prefix + "01B.json", Body=json.dumps(
        {**base, "status": "not_applicable", "reason": "no_new_data_declared",
         "finished": "2026-10-03T11:20:00Z"}).encode())

    completion = _verify(s3, ["D02"])
    assert completion["failure_mode"] == "run_not_ok"
    assert "VYLR" in completion["findings"][0]["detail"]
    assert "01B.json" in completion["findings"][0]["detail"]


def test_a_no_op_with_no_earlier_run_is_still_a_finding():
    s3 = MemS3()
    s3.put_object(Bucket=BUCKET, Key=f"data_collection/runs/D02/{DAY}/01B.json", Body=json.dumps({
        "unit_id": "D02", "trading_day": DAY, "status": "not_applicable",
        "reason": "no_new_data_declared", "started": "2026-10-03T11:00:00Z",
        "finished": "2026-10-03T11:20:00Z", "outputs": [],
    }).encode())
    completion = _verify(s3, ["D02"])
    assert completion["failure_mode"] == "run_not_ok"
    assert "no earlier run" in completion["findings"][0]["detail"]


def test_the_v1_readiness_wait_reads_through_the_no_op_too():
    s3 = MemS3()
    _collect_d02(s3, _attempt(s3), "ok", [])
    _collect_d02(s3, _attempt(s3), "ok", [])
    readiness = predicate.readiness_check(
        {"collection": "weekly", "units": ["D02"], "not_before": STARTED, "lookback_seconds": 0},
        s3_client=s3,
    )["readiness"]
    assert readiness["ready"] is True, readiness["summary"]


def test_an_empty_same_date_recompute_defers_to_the_published_run():
    """The 10-03 botch for D03/D08: units that always recompute found nothing
    new on the retry and filed `failed` EmptyProduction over a real `ok`."""
    s3 = MemS3()
    first = weekly_collector._phase_collect(
        _attempt(s3), "historical_constituents", _d02(s3, "ok", []),
        artifact_key=D02_KEY, supports_auto_skip=False,
    )
    assert first["status"] == "ok"
    # The recompute: ran, status ok, published nothing (no artifact_key write).
    weekly_collector._phase_collect(
        _attempt(s3), "historical_constituents", lambda: {"status": "ok", "n_changes": 0},
        supports_auto_skip=False,
    )
    statuses = [(m["status"], m["reason"]) for m in s3.manifests("D02")]
    assert statuses == [("ok", ""), ("not_applicable", "no_new_data_declared")]
    assert _verify(s3, ["D02"])["ok"] is True


def test_an_empty_run_with_no_earlier_ok_is_still_empty_production():
    s3 = MemS3()
    weekly_collector._phase_collect(
        _attempt(s3), "historical_constituents", lambda: {"status": "ok", "n_changes": 0},
        supports_auto_skip=False,
    )
    (manifest,) = s3.manifests("D02")
    assert manifest["status"] == "failed"
    assert "EmptyProduction" in manifest["reason"]


def test_the_predicates_no_op_reason_is_the_writers():
    """The predicate ships in Lambda zips without `run_units`, so it carries the
    literal; this pins the two together."""
    assert predicate.SAME_DATE_NOOP_REASON == run_units.NOT_RUN_NO_NEW_DATA_DECLARED
