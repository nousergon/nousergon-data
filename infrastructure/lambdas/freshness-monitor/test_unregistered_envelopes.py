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


# ── declared_off: tombstoned rows never page (alpha-engine-config-I12689) ──────


_RETIRED_ROW = b"""\
component_id: tombstoned
alias_ids: [tombstoned_old]
substrate: github-actions
lifecycle: retired
lifecycle_reason: >-
  Retired 2026-09-14. The workflow was deleted by
  crucible-predictor-PR626.
lifecycle_owner: brian
lifecycle_reexam: 2027-09-14
"""

_DISABLED_ROW = b"""\
component_id: paused
substrate: ec2-spot
lifecycle: disabled
lifecycle_reason: Disabled pending the v2 cutover.
lifecycle_owner: brian
lifecycle_reexam: 2026-12-01
"""


def _decl_rows():
    import yaml
    return [yaml.safe_load(_RETIRED_ROW), yaml.safe_load(_DISABLED_ROW)]


def test_a_tombstoned_envelope_grades_declared_off_and_a_live_stale_one_still_pages():
    """The I12689 regression: a retired/disabled row's frozen envelope is
    ``declared_off`` and leaves the stale set, and a live stale envelope beside
    it still makes the verdict stale."""
    grades = ue.grade(
        [_env("tombstoned_old", age_h=24 * 42), _env("paused", age_h=24 * 9),
         _env("live_but_stale", age_h=13)],
        registered=set(), substrates={}, now=NOW,
        declared_off=ue.declared_off_map(_decl_rows()),
    )
    by_id = {g.check_id: g for g in grades}
    assert by_id["tombstoned_old"].state == ue.STATE_DECLARED_OFF  # via alias_ids
    assert by_id["tombstoned_old"].lifecycle == "retired"
    assert "crucible-predictor-PR626" in by_id["tombstoned_old"].lifecycle_reason
    assert by_id["paused"].state == ue.STATE_DECLARED_OFF
    assert by_id["paused"].lifecycle == "disabled"
    assert by_id["live_but_stale"].state == ue.STATE_STALE

    result = ue.verdict(grades, NOW)
    assert result.state == "stale"
    assert "live_but_stale (" in result.reason
    assert "tombstoned" not in result.reason.split(". ")[0]
    assert "paused (" not in result.reason
    assert "2 declared_off" in result.reason


def test_only_declared_off_envelopes_leave_the_verdict_fresh():
    grades = ue.grade([_env("tombstoned", age_h=24 * 42)], registered=set(),
                      substrates={}, now=NOW,
                      declared_off=ue.declared_off_map(_decl_rows()))
    assert ue.verdict(grades, NOW).state == "fresh"


def test_an_incomplete_lifecycle_declaration_is_not_declared_off():
    """§2.2/§8.3: off is DECLARED, never inferred. A bare ``lifecycle: retired``
    with no reason or re-exam is not a declaration, so it grades on its clock."""
    rows = [
        {"component_id": "no_reason", "lifecycle": "retired",
         "lifecycle_reexam": "2027-01-01"},
        {"component_id": "no_reexam", "lifecycle": "disabled",
         "lifecycle_reason": "off"},
        {"component_id": "deprecated_one", "lifecycle": "deprecated",
         "lifecycle_reason": "successor x", "lifecycle_reexam": "2027-01-01"},
    ]
    assert ue.declared_off_map(rows) == {}
    grades = ue.grade([_env("no_reason", age_h=13)], registered=set(),
                      substrates={}, now=NOW, declared_off=ue.declared_off_map(rows))
    assert grades[0].state == ue.STATE_STALE


def test_an_in_service_row_naming_the_same_id_wins():
    import yaml
    live = {"component_id": "renamed_live", "alias_ids": ["tombstoned_old"],
            "lifecycle": "in-service", "substrate": "lambda"}
    off = ue.declared_off_map([yaml.safe_load(_RETIRED_ROW), live])
    assert "tombstoned_old" not in off
    assert "tombstoned" in off


def test_run_sweep_grades_a_published_tombstone_declared_off():
    s3 = _FakeS3({
        "ops/checks/tombstoned/latest.json": _envelope_obj(24 * 42),
        "ops/checks/live_but_stale/latest.json": _envelope_obj(13),
        "ops/registry/observability.d/tombstoned.yaml": (NOW, _RETIRED_ROW),
    })
    _, result, report = ue.run_sweep(s3, BUCKET, [], NOW)
    graded = {g["check_id"]: g for g in report["grades"]}
    assert graded["tombstoned"]["state"] == ue.STATE_DECLARED_OFF
    assert graded["tombstoned"]["lifecycle"] == "retired"
    assert graded["tombstoned"]["lifecycle_reason"].startswith("Retired 2026-09-14.")
    assert graded["live_but_stale"]["state"] == ue.STATE_STALE
    assert result.state == "stale"


def test_an_unreadable_registry_fails_toward_paging_not_declared_off():
    class _RegistryDown(_FakeS3):
        def get_paginator(self, name):
            inner = super().get_paginator(name)

            class _P:
                def paginate(self, *, Bucket=None, Prefix="", **kw):  # noqa: N803
                    if Prefix == ue.OBSERVABILITY_PREFIX:
                        raise RuntimeError("AccessDenied")
                    return inner.paginate(Bucket=Bucket, Prefix=Prefix, **kw)

            return _P()

    s3 = _RegistryDown({
        "ops/checks/tombstoned/latest.json": _envelope_obj(24 * 42),
        "ops/registry/observability.d/tombstoned.yaml": (NOW, _RETIRED_ROW),
    })
    _, result, report = ue.run_sweep(s3, BUCKET, [], NOW)
    assert report["grades"][0]["state"] == ue.STATE_STALE
    assert result.state == "stale"
    assert any("declared_off" in n for n in report["notes"])


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


def test_handler_does_not_page_on_a_tombstoned_envelope(monkeypatch):
    """The I12689 closes-when, end to end: the report grades the retired row's
    envelope ``declared_off`` and the synthetic row does not page."""
    s3, publish_mock, rows = _run_handler(monkeypatch, {
        "ops/checks/registered_check/latest.json": _envelope_obj(1),
        "ops/checks/tombstoned/latest.json": _envelope_obj(24 * 42),
        "ops/checks/healthy/latest.json": _envelope_obj(1),
        "ops/registry/observability.d/tombstoned.yaml": (NOW, _RETIRED_ROW),
    })
    assert _row(rows, ue.SWEEP_ARTIFACT_ID)["state"] == "fresh"
    assert not publish_mock.called
    report = json.loads(next(b for k, b in s3.put_calls if k == ue.REPORT_KEY))
    graded = {g["check_id"]: g["state"] for g in report["grades"]}
    assert graded == {"tombstoned": "declared_off", "healthy": "fresh"}


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


# ── dispatch-triggered: the drain is graded against its trigger (I12762) ──────


_DRAIN_ROW = b"""\
component_id: overseer-alert-drain
substrate: ec2-spot
lifecycle: in-service
cadence: event_driven
"""

_DRAIN_IDS = ("overseer-alert-drain", "t1-freshness-critical-auto-verify",
              "t1-groom-lifecycle-digest-fold")


def _drain_envs(age_h: float) -> list[ue.Envelope]:
    # The live envelopes still declare 1440 (alpha-engine-config-I12386).
    return [_env(c, age_h=age_h, cadence=1440) for c in _DRAIN_IDS]


def _states(grades):
    return {g.check_id: g.state for g in grades}


def test_an_idle_drain_with_every_dispatch_answered_is_not_stale():
    """The I12762 closes-when: 48h+ after the last drain, with no dispatch since,
    the three rows do not grade stale although their 1440 clock says so."""
    grades = ue.grade(
        _drain_envs(age_h=72), registered=set(), substrates={}, now=NOW,
        dispatch=ue.DispatchRecord(True, NOW - timedelta(hours=72, minutes=15)),
        event_driven={"overseer-alert-drain"},
    )
    assert set(_states(grades).values()) == {"fresh"}
    assert all("trigger-graded" in g.detail for g in grades)
    assert ue.verdict(grades, NOW).state == "fresh"


def test_no_dispatch_on_record_is_not_stale_either():
    grades = ue.grade(
        _drain_envs(age_h=400), registered=set(), substrates={}, now=NOW,
        dispatch=ue.DispatchRecord(True, None), event_driven={"overseer-alert-drain"},
    )
    assert set(_states(grades).values()) == {"fresh"}


def test_an_unanswered_dispatch_past_the_grace_pages():
    """A real drain stoppage still pages: the monitor dispatched and no envelope
    followed within the grace."""
    grades = ue.grade(
        _drain_envs(age_h=30), registered=set(), substrates={}, now=NOW,
        dispatch=ue.DispatchRecord(True, NOW - timedelta(hours=24)),
        event_driven={"overseer-alert-drain"},
    )
    assert set(_states(grades).values()) == {"stale"}
    assert all("no envelope followed" in g.detail for g in grades)
    assert ue.verdict(grades, NOW).state == "stale"


def test_a_dispatch_inside_the_grace_is_in_flight_not_stale():
    grades = ue.grade(
        _drain_envs(age_h=30), registered=set(), substrates={}, now=NOW,
        dispatch=ue.DispatchRecord(
            True, NOW - timedelta(minutes=ue.DISPATCH_ANSWER_GRACE_MINUTES - 1)),
        event_driven={"overseer-alert-drain"},
    )
    assert set(_states(grades).values()) == {"fresh"}


def test_the_clock_returns_when_the_lane_declares_a_cadence_again():
    """Re-arming the schedules re-declares the cadence in the registry row; with
    no ``event_driven`` declaration the 1440 clock grades as before."""
    grades = ue.grade(
        _drain_envs(age_h=72), registered=set(), substrates={}, now=NOW,
        dispatch=ue.DispatchRecord(True, NOW - timedelta(hours=73)),
        event_driven=set(),
    )
    assert set(_states(grades).values()) == {"stale"}


def test_an_unreadable_dispatch_record_falls_back_to_the_clock():
    grades = ue.grade(
        _drain_envs(age_h=72), registered=set(), substrates={}, now=NOW,
        dispatch=ue.DispatchRecord(False), event_driven={"overseer-alert-drain"},
    )
    assert set(_states(grades).values()) == {"stale"}


def test_trigger_grading_touches_only_the_dispatch_triggered_ids():
    grades = ue.grade(
        [_env("other_daily", age_h=72, cadence=1440)], registered=set(),
        substrates={}, now=NOW, dispatch=ue.DispatchRecord(True, None),
        event_driven={"other_daily"},
    )
    assert _states(grades) == {"other_daily": "stale"}


def test_event_driven_ids_reads_the_cadence_declaration():
    import yaml
    rows = [yaml.safe_load(_DRAIN_ROW), {"component_id": "x", "cadence": 60}]
    assert ue.event_driven_ids(rows) == {"overseer-alert-drain"}


class _ErrS3(_FakeS3):
    def get_object(self, *, Bucket, Key):  # noqa: N803
        if Key == ue.DRAIN_DISPATCH_MARKER_KEY:
            err = Exception("AccessDenied")
            err.response = {"Error": {"Code": "AccessDenied"}}  # type: ignore[attr-defined]
            raise err
        return super().get_object(Bucket=Bucket, Key=Key)


def test_read_dispatch_classifies_missing_present_and_unreadable():
    at = NOW - timedelta(hours=5)
    present = _FakeS3({ue.DRAIN_DISPATCH_MARKER_KEY: (
        NOW, json.dumps({"dispatched_at": at.isoformat()}).encode())})
    assert ue.read_dispatch(present, BUCKET) == ue.DispatchRecord(True, at)
    assert ue.read_dispatch(_FakeS3({}), BUCKET) == ue.DispatchRecord(True, None)
    assert ue.read_dispatch(_ErrS3({}), BUCKET).readable is False


def test_run_sweep_grades_the_idle_drain_fresh_from_the_published_registry():
    objects = {f"ops/checks/{c}/latest.json": _envelope_obj(72, cadence=1440)
               for c in _DRAIN_IDS}
    objects["ops/registry/observability.d/overseer-alert-drain.yaml"] = (NOW, _DRAIN_ROW)
    objects[ue.DRAIN_DISPATCH_MARKER_KEY] = (NOW, json.dumps(
        {"dispatched_at": (NOW - timedelta(hours=72, minutes=15)).isoformat()}).encode())
    _, result, report = ue.run_sweep(_FakeS3(objects), BUCKET, [], NOW)
    assert result.state == "fresh"
    assert report["last_drain_dispatch"] is not None
    assert {g["state"] for g in report["grades"]} == {"fresh"}


def test_index_reads_the_same_marker_key_the_sweep_grades():
    import index
    assert index.DRAIN_DISPATCH_MARKER_KEY == ue.DRAIN_DISPATCH_MARKER_KEY


def test_handler_does_not_page_on_an_idle_drain(monkeypatch):
    """I12762 end to end: an idle day 72h after the last drain does not page."""
    objects = {f"ops/checks/{c}/latest.json": _envelope_obj(72, cadence=1440)
               for c in _DRAIN_IDS}
    objects["ops/checks/registered_check/latest.json"] = _envelope_obj(1)
    objects["ops/registry/observability.d/overseer-alert-drain.yaml"] = (NOW, _DRAIN_ROW)
    objects[ue.DRAIN_DISPATCH_MARKER_KEY] = (NOW - timedelta(hours=72, minutes=15),
        json.dumps({"dispatched_at": (NOW - timedelta(hours=72, minutes=15)).isoformat()}).encode())
    _, publish_mock, rows = _run_handler(monkeypatch, objects)
    assert _row(rows, ue.SWEEP_ARTIFACT_ID)["state"] == "fresh"
    assert not publish_mock.called
