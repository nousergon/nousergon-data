"""A shadow run reads live INPUTS but its OWN run state (`alpha-engine-config-I10891`).

The first complete shadow run (trading day 2026-09-14) read v1's live
``data/2026-09-14/.phases/*`` markers through the interceptor's read
pass-through, auto-skipped D19/D20/D22-D31, and reported ``ok`` with nothing
published. These tests drive the REAL interceptor, the REAL ``PhaseRegistry``
(via ``weekly_collector._build_registry``) and the REAL ``_phase_collect``
against an in-memory S3 installed at the botocore boundary, so what is graded
is the wiring a shadow run actually goes through.
"""

from __future__ import annotations

import datetime as dt
import io
import json
from types import SimpleNamespace

import boto3
import botocore.client
import pytest
from botocore.exceptions import ClientError

import weekly_collector
from collectors import metron_market_data
from shadow import interceptor
from shadow.root import ShadowGuardViolation, ShadowRoot, activate, deactivate
from shadow.run_state import RunStatePhaseRegistry

DAY = "2026-09-14"
ROOT = ShadowRoot(dt.date.fromisoformat(DAY))
BUCKET = "alpha-engine-research"
PHASE = "metron_market_data"  # D20 in `--daily`
MARKER = "data/" + DAY + "/.phases/" + PHASE + ".json"
EOD_OUT = metron_market_data.CLOSES_PREFIX + DAY + ".json"


class MemoryS3:
    """The bucket, as botocore's ``_make_api_call`` would see it."""

    def __init__(self, objects=None):
        self.objects: dict[str, bytes] = dict(objects or {})
        self.puts: list[str] = []
        self.reads: list[tuple[str, str]] = []

    def __call__(self, client, operation: str, params: dict):  # the _make_api_call signature
        name = params.get("Key")
        if operation == "PutObject":
            body = params.get("Body", b"")
            if hasattr(body, "read"):
                body = body.read()
            self.objects[name] = body if isinstance(body, bytes) else str(body).encode()
            self.puts.append(name)
            return {"ETag": "e"}
        if operation in ("GetObject", "HeadObject"):
            self.reads.append((operation, name))
            if name not in self.objects:
                code = "NoSuchKey" if operation == "GetObject" else "404"
                raise ClientError({"Error": {"Code": code, "Message": "missing"}}, operation)
            size = len(self.objects[name])
            if operation == "HeadObject":
                return {"ContentLength": size}
            return {"Body": io.BytesIO(self.objects[name]), "ContentLength": size}
        raise AssertionError(f"MemoryS3 does not model {operation}")


def _ok_marker(artifact: str) -> bytes:
    return json.dumps(
        {"schema_version": 1, "phase": PHASE, "date": DAY, "status": "ok", "artifact_keys": [artifact]}
    ).encode()


@pytest.fixture(autouse=True)
def _env(monkeypatch):
    # No credentials are needed: request signing happens inside the
    # `_make_api_call` that MemoryS3 replaces.
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("NE_DATA_CODE_SHA", "a" * 40)
    monkeypatch.setenv("NE_DATA_LOG_LOCATION", "cloudwatch:/alpha-engine/data-spot:s-1")
    monkeypatch.setenv("NE_DATA_TRIGGER", "scheduled")


@pytest.fixture
def shadow_s3():
    """Activate the root, then put ``MemoryS3`` UNDER the interceptor."""
    real = botocore.client.BaseClient._make_api_call
    s3 = MemoryS3()
    activate(ROOT)
    interceptor._ORIGINAL = s3
    try:
        yield s3
    finally:
        # Restore the real call BEFORE uninstall, which writes _ORIGINAL back
        # onto BaseClient — otherwise the fake would outlive this test.
        interceptor._ORIGINAL = real
        deactivate()
    assert botocore.client.BaseClient._make_api_call is real


def _registry() -> RunStatePhaseRegistry:
    args = SimpleNamespace(dry_run=False, force=False, only=None, daily=True)
    reg = weekly_collector._build_registry({"bucket": BUCKET}, args, DAY)
    assert isinstance(reg, RunStatePhaseRegistry)
    return reg


def _d20_run_fn(calls: list):
    def run_fn():
        calls.append(1)
        boto3.client("s3").put_object(Bucket=BUCKET, Key=EOD_OUT, Body=b'{"closes": {"SPY": 1.0}}')
        return {"status": "ok", "closes": 1}

    return run_fn


def _manifests(s3: MemoryS3) -> list[dict]:
    runs = ROOT.prefix + "data_collection/runs/"
    return [json.loads(s3.objects[k]) for k in s3.puts if k.startswith(runs)]


# ── the defect, withheld ────────────────────────────────────────────────────


def test_under_a_shadow_root_a_phase_whose_live_marker_exists_still_runs_and_writes_shadow_outputs(shadow_s3):
    shadow_s3.objects[MARKER] = _ok_marker(EOD_OUT)  # v1's LIVE completion
    shadow_s3.objects[EOD_OUT] = b'{"live": true}'   # v1's LIVE artifact
    calls: list = []

    result = weekly_collector._phase_collect(
        _registry(), PHASE, _d20_run_fn(calls), artifact_key=EOD_OUT, bucket=BUCKET
    )

    assert calls == [1], "the live marker must not short-circuit the shadow run"
    assert result["status"] == "ok" and not result.get("auto_skipped")
    assert ROOT.key(EOD_OUT) in shadow_s3.objects
    assert json.loads(shadow_s3.objects[ROOT.key(MARKER)])["status"] == "ok"
    # The marker read went to the shadow, never to the live marker.
    assert ("GetObject", MARKER) not in shadow_s3.reads
    assert ("GetObject", ROOT.key(MARKER)) in shadow_s3.reads
    # Nothing live was touched, and the unit's manifest names its output.
    assert shadow_s3.objects[MARKER] == _ok_marker(EOD_OUT)
    assert shadow_s3.objects[EOD_OUT] == b'{"live": true}'
    assert all(k.startswith(ROOT.prefix) for k in shadow_s3.puts)
    (manifest,) = _manifests(shadow_s3)
    assert manifest["unit_id"] == "D20" and manifest["status"] == "ok"
    assert EOD_OUT in [o["key"] for o in manifest["outputs"]]


def test_with_the_shadow_side_marker_present_the_run_auto_skips_and_never_reports_ok(shadow_s3):
    shadow_s3.objects[ROOT.key(MARKER)] = _ok_marker(EOD_OUT)
    shadow_s3.objects[ROOT.key(EOD_OUT)] = b"{}"
    calls: list = []

    result = weekly_collector._phase_collect(
        _registry(), PHASE, _d20_run_fn(calls), artifact_key=EOD_OUT, bucket=BUCKET
    )

    assert calls == [], "a shadow-side completion is the run's own state: it skips"
    assert result["status"] == "error"
    assert "auto-skipped under shadow" in result["error"]
    (manifest,) = _manifests(shadow_s3)
    assert manifest["status"] == "failed" and manifest["outputs"] == []
    assert "auto-skipped under shadow" in manifest["reason"]


def test_without_a_shadow_root_a_live_marker_still_auto_skips(monkeypatch):
    """Production is unchanged: no root, no interceptor, the live marker skips."""
    assert not interceptor.installed()
    s3 = MemoryS3({MARKER: _ok_marker(EOD_OUT), EOD_OUT: b"{}"})
    # A function, so botocore binds it as a method (a callable instance would not be).
    monkeypatch.setattr(
        botocore.client.BaseClient, "_make_api_call", lambda self, op, params: s3(self, op, params)
    )
    calls: list = []

    result = weekly_collector._phase_collect(
        _registry(), PHASE, _d20_run_fn(calls), artifact_key=EOD_OUT, bucket=BUCKET
    )

    assert calls == []
    assert result == {"status": "ok", "auto_skipped": True, "skip_reason": "auto_skip_marker_ok"}
    assert ("GetObject", MARKER) in s3.reads
    touched = s3.puts + [k for _, k in s3.reads]
    assert not any(k.startswith("staging/shadow/") for k in touched)


# ── the classification, at its one place ────────────────────────────────────


def test_an_unclassified_read_of_a_key_the_run_also_writes_raises_before_the_write(shadow_s3):
    target = "market_data/technicals/latest.json"
    shadow_s3.objects[target] = b"{}"
    client = boto3.client("s3")
    client.get_object(Bucket=BUCKET, Key=target)  # read LIVE, as an input

    with pytest.raises(ShadowGuardViolation, match="unclassified read of a key the run also writes"):
        client.put_object(Bucket=BUCKET, Key=target, Body=b"{}")
    assert shadow_s3.puts == [], "the refusal happens before anything is sent"


def test_a_key_the_run_already_wrote_reads_back_from_the_shadow(shadow_s3):
    client = boto3.client("s3")
    client.put_object(Bucket=BUCKET, Key=EOD_OUT, Body=b"{}")
    client.head_object(Bucket=BUCKET, Key=EOD_OUT)
    assert shadow_s3.reads == [("HeadObject", ROOT.key(EOD_OUT))]


def test_the_daily_closes_merge_base_is_run_state():
    """collectors/daily_closes.py reads the prior parquet as its coalesce base."""
    base = "staging/daily_closes/" + DAY + ".parquet"
    params = interceptor.rewrite_params(
        "HeadObject", {"Bucket": BUCKET, "Key": base}, service="s3", root=ROOT,
        ledger=interceptor.RunLedger(),
    )
    assert params["Key"] == ROOT.key(base)


@pytest.mark.parametrize(
    ("path", "expected"),
    [
        (MARKER, "own_state:phase_marker"),
        ("staging/daily_closes/" + DAY + ".parquet", "own_state:daily_closes_merge_base"),
        (EOD_OUT, "input"),
        ("constituents/latest.json", "input"),
    ],
)
def test_every_own_state_pattern_classifies_and_inputs_stay_live(path, expected):
    assert interceptor.classify_read(path, bucket=BUCKET, ledger=interceptor.RunLedger()) == expected


def test_inside_the_own_state_scope_every_keyed_read_is_run_state_and_a_listing_raises():
    ledger = interceptor.RunLedger()
    with interceptor.own_state_reads():
        assert interceptor.classify_read(EOD_OUT, bucket=BUCKET, ledger=ledger) == "own_state_scope"
        params = interceptor.rewrite_params(
            "HeadObject", {"Bucket": BUCKET, "Key": EOD_OUT}, service="s3", root=ROOT, ledger=ledger
        )
        assert params["Key"] == ROOT.key(EOD_OUT)
        with pytest.raises(ShadowGuardViolation, match="not classified"):
            interceptor.rewrite_params(
                "ListObjectsV2", {"Bucket": BUCKET, "Prefix": "data/"}, service="s3", root=ROOT,
                ledger=ledger,
            )
    assert interceptor.classify_read(EOD_OUT, bucket=BUCKET, ledger=ledger) == "input"
