"""Tests for the unregistered ``ops/checks/`` envelope sweep
(alpha-engine-config-I11840), moved into the freshness monitor by Brian's ruling
#25 (2026-10-14).

The closes-when the issue sets is pinned end to end by
``test_handler_pages_on_a_stale_unregistered_envelope``. It seeds a stale
envelope that has no registry row, and the daily handler pages on it.
"""

from __future__ import annotations

import io
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest import mock

import pytest

sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# No sys.modules stub for `flow_doctor_telegram` here: test_handler.py owns
# that guard and captures the REAL module first, so a stub installed by this
# file (if collected first) would hand it the stub. Every handler run below
# patches `index.notify_via_flow_doctor` and `index.publish` itself.

import unregistered_envelopes as ue  # noqa: E402
from nousergon_lib.artifact_freshness import ArtifactSpec  # noqa: E402

NOW = datetime(2026, 10, 15, 12, 0, tzinfo=timezone.utc)
BUCKET = "alpha-engine-research"


def _env(check_id: str, *, age_h: float, cadence: float | None = 60,
         status: str = "ok") -> ue.Envelope:
    seen = NOW - timedelta(hours=age_h)
    return ue.Envelope(
        check_id=check_id, key=f"ops/checks/{check_id}/latest.json",
        last_modified=seen, ran_at=seen, cadence_minutes=cadence, status=status,
    )


# ── grading ─────────────────────────────────────────────────────────────────


def test_threshold_is_twice_cadence_above_the_floor():
    assert ue.threshold_minutes(60, None) == 720            # the 12h floor wins
    assert ue.threshold_minutes(1440, None) == 2880         # 2 x cadence wins
    assert ue.threshold_minutes(60, "laptop-launchd") == 4320


def test_grade_states_and_registered_keys_are_skipped():
    envs = [
        _env("fresh_one", age_h=2),
        _env("stale_one", age_h=13),
        _env("event_driven", age_h=500, cadence=None),
        _env("registered", age_h=500),
    ]
    grades = ue.grade(
        envs, registered={"ops/checks/registered/latest.json"},
        substrates={}, now=NOW,
    )
    by_id = {g.check_id: g.state for g in grades}
    assert by_id == {
        "fresh_one": ue.STATE_FRESH,
        "stale_one": ue.STATE_STALE,
        "event_driven": ue.STATE_UNCLOCKED,
    }


def test_laptop_substrate_gets_the_longer_floor():
    grades = ue.grade([_env("laptop_check", age_h=13)], registered=set(),
                      substrates={"laptop_check": "laptop-launchd"}, now=NOW)
    assert grades[0].state == ue.STATE_FRESH
    assert grades[0].threshold_minutes == 4320


def test_envelope_from_never_raises_on_a_bad_body():
    env = ue.envelope_from("ops/checks/x/latest.json", NOW, b"not json")
    assert env.check_id == "x"
    assert env.cadence_minutes is None
    env = ue.envelope_from("ops/checks/x/latest.json", NOW,
                           json.dumps({"cadence_minutes": True}).encode())
    assert env.cadence_minutes is None  # a bool is not a cadence


def test_substrate_map_reads_aliases():
    rows = [{"component_id": "a", "alias_ids": ["a_old"], "substrate": "laptop-launchd"},
            {"component_id": "b"}, "not a row"]
    assert ue.substrate_map(rows) == {"a": "laptop-launchd", "a_old": "laptop-launchd"}


def test_registered_keys_reads_only_ops_checks_rows():
    specs = [
        ue.sweep_spec(BUCKET),
        ArtifactSpec(
            artifact_id="row", s3_bucket=BUCKET,
            s3_key_template="ops/checks/row/latest.json", cadence="continuous",
            sla_minutes_after_cron=60, severity="warning", owner_repo="t",
            created_at=NOW.date(), interval_minutes=60, run_calendar="all_days",
        ),
    ]
    assert ue.registered_keys(specs) == {"ops/checks/row/latest.json"}


# ── verdict ─────────────────────────────────────────────────────────────────


def test_verdict_fresh_when_nothing_is_stale():
    grades = ue.grade([_env("a", age_h=1)], registered=set(), substrates={}, now=NOW)
    result = ue.verdict(grades, NOW)
    assert result.state == "fresh"
    assert result.sla_violated_by_minutes == 0


def test_verdict_stale_is_a_confirmed_miss_naming_every_stale_envelope():
    grades = ue.grade([_env("a", age_h=13), _env("b", age_h=30), _env("c", age_h=1)],
                      registered=set(), substrates={}, now=NOW)
    result = ue.verdict(grades, NOW)
    assert result.state == "stale"
    assert result.sla_violated_by_minutes == 30 * 60 - 720
    assert "a (" in result.reason and "b (" in result.reason
    assert "c (" not in result.reason
    # The newest last-write among the stale set: the episode key is built
    # from it, so a NEW envelope going stale opens a new episode.
    assert result.last_modified == NOW - timedelta(hours=13)


def test_a_newly_stale_envelope_changes_the_episode():
    import index

    spec = ue.sweep_spec(BUCKET)
    before = ue.verdict(ue.grade([_env("a", age_h=30)], registered=set(),
                                 substrates={}, now=NOW), NOW)
    after = ue.verdict(ue.grade([_env("a", age_h=30), _env("b", age_h=13)],
                                registered=set(), substrates={}, now=NOW), NOW)
    ep_before = index._episode_signature(spec, before, None, NOW)
    ep_after = index._episode_signature(spec, after, None, NOW)
    assert ep_before["key"] != ep_after["key"]


def test_sweep_spec_is_a_valid_critical_row():
    spec = ue.sweep_spec(BUCKET)
    assert spec.artifact_id == ue.SWEEP_ARTIFACT_ID
    assert spec.severity == "critical"  # I11840: a page within one day
    assert spec.s3_key_template == ue.REPORT_KEY


# ── I/O ─────────────────────────────────────────────────────────────────────


class _FakeS3:
    """Lists and GETs from one dict of key -> (LastModified, body)."""

    def __init__(self, objects: dict[str, tuple[datetime, bytes]], *,
                 list_raises: Exception | None = None):
        self.objects = objects
        self.list_raises = list_raises
        self.put_calls: list[tuple[str, bytes]] = []

    def get_paginator(self, _name):
        fake = self

        class _P:
            def paginate(self, *, Bucket=None, Prefix="", **_):  # noqa: N803 — boto3 kwargs
                if fake.list_raises and Prefix == ue.CHECKS_PREFIX:
                    raise fake.list_raises
                return iter([{"Contents": [
                    {"Key": k, "LastModified": lm}
                    for k, (lm, _) in sorted(fake.objects.items())
                    if k.startswith(Prefix)
                ]}])

        return _P()

    def get_object(self, *, Bucket, Key):  # noqa: N803
        if Key not in self.objects:
            raise KeyError(Key)
        return {"Body": io.BytesIO(self.objects[Key][1])}

    def put_object(self, *, Bucket, Key, Body, **_):  # noqa: N803
        self.put_calls.append((Key, Body))
        return {}


def _envelope_obj(age_h: float, cadence: float | None = 60) -> tuple[datetime, bytes]:
    seen = NOW - timedelta(hours=age_h)
    return seen, json.dumps({
        "check_id": "x", "status": "ok", "ran_at": seen.isoformat(),
        "cadence_minutes": cadence,
    }).encode()


def test_list_envelopes_takes_only_latest_json_at_depth_two():
    s3 = _FakeS3({
        "ops/checks/a/latest.json": _envelope_obj(1),
        "ops/checks/a/history/2026-10-14.json": _envelope_obj(1),
        "ops/checks/unregistered_envelope_staleness/all_clear.json": _envelope_obj(1),
    })
    assert [e.check_id for e in ue.list_envelopes(s3, BUCKET)] == ["a"]


def test_run_sweep_failed_listing_is_probe_failed_not_fresh():
    s3 = _FakeS3({}, list_raises=RuntimeError("AccessDenied"))
    spec, result, report = ue.run_sweep(s3, BUCKET, [], NOW)
    assert result.state == "probe_failed"
    assert report["state"] == "probe_failed"


def test_run_sweep_reads_the_published_observability_floor():
    s3 = _FakeS3({
        "ops/checks/laptop/latest.json": _envelope_obj(13),
        "ops/registry/observability.d/laptop.yaml": (
            NOW, b"component_id: laptop\nsubstrate: laptop-launchd\n"),
    })
    _, result, report = ue.run_sweep(s3, BUCKET, [], NOW)
    assert result.state == "fresh"
    assert report["grades"][0]["substrate"] == "laptop-launchd"


# ── through the handler (the I11840 closes-when) ─────────────────────────────


_REGISTRY = b"""\
schema_version: 1
defaults:
  s3_bucket: alpha-engine-research
artifacts:
  - artifact_id: registered_check
    s3_key_template: "ops/checks/registered_check/latest.json"
    cadence: continuous
    interval_minutes: 60
    sla_minutes_after_cron: 60
    run_calendar: all_days
    calendar_aware: false
    severity: warning
    owner_repo: alpha-engine-test
    created_at: 2025-01-01
"""


class _HandlerS3(_FakeS3):
    """Adds the registry object and HEAD, which the registry probe uses."""

    def get_object(self, *, Bucket, Key):  # noqa: N803
        if Key.endswith("ARTIFACT_REGISTRY.yaml"):
            return {"Body": io.BytesIO(_REGISTRY)}
        return super().get_object(Bucket=Bucket, Key=Key)

    def head_object(self, *, Bucket, Key):  # noqa: N803
        if Key in self.objects:
            return {"LastModified": self.objects[Key][0]}
        err = Exception("Not Found")
        err.response = {"Error": {"Code": "404"},  # type: ignore[attr-defined]
                        "ResponseMetadata": {"HTTPStatusCode": 404}}
        raise err

    def __getattr__(self, name):  # any other client call the handler makes
        return mock.Mock(return_value={})


def _run_handler(monkeypatch, objects):
    monkeypatch.setenv("FRESHNESS_MONITOR_ENABLED", "true")
    monkeypatch.setenv("UNREGISTERED_ENVELOPE_SWEEP_ENABLED", "true")
    import importlib
    import index
    importlib.reload(index)

    real_dt = index.datetime

    class _FixedDT(real_dt):
        @classmethod
        def now(cls, tz=None):
            return NOW if tz is None else NOW.astimezone(tz)

    monkeypatch.setattr(index, "datetime", _FixedDT)
    s3 = _HandlerS3(objects)
    monkeypatch.setattr(index, "boto3", mock.Mock(client=lambda *a, **k: s3))
    # No live SSM / GitHub: the owning-item join degrades, the page still fires.
    monkeypatch.setattr(index, "_resolve_owning_item",
                        lambda *a, **k: index._unresolved("test"))
    publish_mock = mock.Mock(return_value=mock.Mock(dedup_skipped=False))
    monkeypatch.setattr(index, "publish", publish_mock)
    monkeypatch.setattr(index, "notify_via_flow_doctor", mock.Mock(return_value=True))
    index.handler({}, None)
    rows = json.loads(next(b for k, b in s3.put_calls
                           if k == "_freshness_monitor/check_results.json"))
    return s3, publish_mock, rows


def _row(check_results, artifact_id):
    entries = check_results.get("results") or check_results.get("artifacts") or []
    return next(r for r in entries if r.get("artifact_id") == artifact_id)


def test_handler_pages_on_a_stale_unregistered_envelope(monkeypatch):
    s3, publish_mock, rows = _run_handler(monkeypatch, {
        "ops/checks/registered_check/latest.json": _envelope_obj(1),
        "ops/checks/gone_quiet/latest.json": _envelope_obj(64 * 24),
        "ops/checks/healthy/latest.json": _envelope_obj(1),
    })
    assert publish_mock.called
    page = json.dumps([[c.args, c.kwargs] for c in publish_mock.call_args_list],
                      default=str)
    assert ue.SWEEP_ARTIFACT_ID in page
    assert "gone_quiet" in page

    row = _row(rows, ue.SWEEP_ARTIFACT_ID)
    assert row["state"] == "stale"

    report = json.loads(next(b for k, b in s3.put_calls if k == ue.REPORT_KEY))
    graded = {g["check_id"]: g["state"] for g in report["grades"]}
    # The registered key is the registry row's job, not the sweep's.
    assert graded == {"gone_quiet": "stale", "healthy": "fresh"}


def test_handler_does_not_page_when_every_unregistered_envelope_is_fresh(monkeypatch):
    s3, publish_mock, rows = _run_handler(monkeypatch, {
        "ops/checks/registered_check/latest.json": _envelope_obj(1),
        "ops/checks/healthy/latest.json": _envelope_obj(1),
    })
    assert _row(rows, ue.SWEEP_ARTIFACT_ID)["state"] == "fresh"
    assert not publish_mock.called


def test_handler_sweep_can_be_disabled_by_env(monkeypatch):
    import index

    monkeypatch.setenv("UNREGISTERED_ENVELOPE_SWEEP_ENABLED", "false")
    specs, pre = index._with_unregistered_envelope_sweep(mock.Mock(), [], NOW)
    assert specs == [] and pre == {}


def test_a_real_registry_row_wins_over_the_synthetic_one(monkeypatch):
    import index

    monkeypatch.setenv("UNREGISTERED_ENVELOPE_SWEEP_ENABLED", "true")
    real = ue.sweep_spec(BUCKET)
    specs, pre = index._with_unregistered_envelope_sweep(mock.Mock(), [real], NOW)
    assert specs == [real] and pre == {}


def test_a_sweep_that_raises_becomes_probe_failed(monkeypatch):
    import index

    monkeypatch.setenv("UNREGISTERED_ENVELOPE_SWEEP_ENABLED", "true")
    monkeypatch.setattr(index.unregistered_envelopes, "run_sweep",
                        mock.Mock(side_effect=ValueError("boom")))
    specs, pre = index._with_unregistered_envelope_sweep(mock.Mock(), [], NOW)
    assert [s.artifact_id for s in specs] == [ue.SWEEP_ARTIFACT_ID]
    assert pre[ue.SWEEP_ARTIFACT_ID].state == "probe_failed"


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-q"]))
