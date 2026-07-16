"""Unit tests for the groom-liveness-probe handler.

Cover the load-bearing logic with no AWS/GitHub I/O: trigger enumeration
(maturity + day-of-week filtering), per-trigger miss attribution, S3 dedup
suppression, and the fail-loud contract on the PRIMARY input.

Hermetic: `nousergon_lib` is a git-only dependency the deploy test gate does NOT
install (deploy.sh runs pytest on bare python + boto3), so its two submodules
that `index` imports at module scope are stubbed in sys.modules BEFORE
`import index` — matching the sibling scheduled-groom-dispatcher test. `index`
only touches nousergon_lib at import time via the `_OPS_TOPICS` tuple
(`flow_doctor_fleet.FleetTelegramTopic`) and, transitively through
`flow_doctor_telegram`, `telegram.send_message`; the notify path itself is
monkeypatched per-test (`index.notify_via_flow_doctor`). Migrated onto
`nousergon_lib.*` from the old `nousergon_lib.telegram` stub by the
flow-doctor cutover (config#1742, #622) — keep this stub tracking `index.py`'s
real module-level imports.
"""

from __future__ import annotations

import sys
import types
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# ── Stub nousergon_lib submodules before importing index ──────────────────────
_ng = types.ModuleType("nousergon_lib")
_ng_telegram = types.ModuleType("nousergon_lib.telegram")
_ng_telegram.send_message = lambda *a, **k: None
_ng_fleet = types.ModuleType("nousergon_lib.flow_doctor_fleet")


class _FleetTelegramTopic:
    CRITICAL = "CRITICAL"
    OPS_HEALTH = "OPS_HEALTH"


_ng_fleet.FleetTelegramTopic = _FleetTelegramTopic
_ng.telegram = _ng_telegram
_ng.flow_doctor_fleet = _ng_fleet
sys.modules.setdefault("nousergon_lib", _ng)
sys.modules.setdefault("nousergon_lib.telegram", _ng_telegram)
sys.modules.setdefault("nousergon_lib.flow_doctor_fleet", _ng_fleet)

# config#2208 fleet-pattern hardening: stub the whole sibling module wholesale
# (mirrors scheduled-groom-dispatcher/pipeline-watchdog) so `index.notify_via_
# flow_doctor` is a safe no-op by construction on every `import`/`reload`, not
# only when each handler-calling test remembers its own `_wire()` monkeypatch.
# hermetic_import_guard treats an already-stubbed sibling as satisfied (its
# own imports become irrelevant), so this is additive, not a guard conflict.
_fdt = types.ModuleType("flow_doctor_telegram")
_fdt.notify_via_flow_doctor = lambda *a, **k: True  # type: ignore[attr-defined]
sys.modules["flow_doctor_telegram"] = _fdt

# Derive the stub requirement from index.py's live (transitive) import graph and
# fail loud here if it has drifted, rather than as a cryptic ModuleNotFoundError
# at deploy time. See _shared/hermetic_import_guard.py (config#1746).
from _shared.hermetic_import_guard import (  # noqa: E402
    assert_hermetic_imports_satisfied,
)

assert_hermetic_imports_satisfied(__file__)

import index  # noqa: E402

UTC = timezone.utc


def _dt(y, mo, d, h, mi=0):
    return datetime(y, mo, d, h, mi, tzinfo=UTC)


# ---- _expected_triggers ----------------------------------------------------


def test_only_mature_triggers_returned(monkeypatch):
    """A trigger younger than CEILING+MARGIN is NOT yet checkable."""
    monkeypatch.setattr(index, "CEILING_MIN", 360)
    monkeypatch.setattr(index, "MARGIN_MIN", 45)
    monkeypatch.setattr(index, "LOOKBACK_HOURS", 30)
    # Tue 14:00 UTC. The 07:00 Tue trigger matured (7h ago); 01:00 Tue matured
    # (13h ago). 19:00 Mon (prev day) also mature.
    now = _dt(2026, 6, 30, 14, 0)  # 2026-06-30 is a Tuesday
    trigs = index._expected_triggers(now)
    ats = {t["at"] for t in trigs}
    assert _dt(2026, 6, 30, 7, 0) in ats   # 07:00 Tue Sonnet, matured
    assert _dt(2026, 6, 30, 1, 0) in ats   # 01:00 Tue high-only, matured
    assert _dt(2026, 6, 29, 19, 0) in ats  # 19:00 Mon Haiku, matured
    # Nothing in the immature zone (now - 6.75h .. now).
    assert all(t["at"] <= now - timedelta(minutes=405) for t in trigs)


def test_saturday_0700_included(monkeypatch):
    """Uniform 3x/day, all 7 days (2026-07-02, no exceptions) — Saturday MUST
    produce a 07:00 trigger like every other day (the old Sun-Fri Sat-skip was
    removed; see scheduled-groom-dispatcher/README.md)."""
    monkeypatch.setattr(index, "CEILING_MIN", 360)
    monkeypatch.setattr(index, "MARGIN_MIN", 45)
    monkeypatch.setattr(index, "LOOKBACK_HOURS", 12)
    # Sat 2026-06-27 18:00 UTC → 07:00 Sat matured (11h ago, > CEILING+MARGIN).
    now = _dt(2026, 6, 27, 18, 0)  # Saturday
    trigs = index._expected_triggers(now)
    assert _dt(2026, 6, 27, 7, 0) in {t["at"] for t in trigs}


def test_0100_high_only_schedule_included(monkeypatch):
    """The 01:00 UTC dedicated complexity:high schedule must be tracked."""
    monkeypatch.setattr(index, "CEILING_MIN", 360)
    monkeypatch.setattr(index, "MARGIN_MIN", 45)
    monkeypatch.setattr(index, "LOOKBACK_HOURS", 30)
    # Wed 2026-07-01 10:00 UTC -> 01:00 Wed matured (9h ago).
    now = _dt(2026, 7, 1, 10, 0)
    trigs = index._expected_triggers(now)
    assert _dt(2026, 7, 1, 1, 0) in {t["at"] for t in trigs}


def test_three_daily_triggers_returned(monkeypatch):
    """All three tier-split schedules appear in a single day's lookback."""
    monkeypatch.setattr(index, "CEILING_MIN", 360)
    monkeypatch.setattr(index, "MARGIN_MIN", 45)
    monkeypatch.setattr(index, "LOOKBACK_HOURS", 24)
    now = _dt(2026, 7, 2, 14, 0)
    trigs = index._expected_triggers(now)
    ats = {t["at"] for t in trigs}
    assert _dt(2026, 7, 2, 1, 0) in ats
    assert _dt(2026, 7, 2, 7, 0) in ats
    assert _dt(2026, 7, 1, 19, 0) in ats


# ---- _missed ---------------------------------------------------------------


def test_trigger_with_artifact_in_window_is_not_missed(monkeypatch):
    monkeypatch.setattr(index, "CEILING_MIN", 360)
    monkeypatch.setattr(index, "MARGIN_MIN", 45)
    trig = {"at": _dt(2026, 6, 29, 23, 0), "label": "23:00 daily"}
    # Artifact's run_start is 6 min into the run → inside [23:00, 05:45].
    stamps = [_dt(2026, 6, 29, 23, 6)]
    assert index._missed([trig], stamps) == []


def test_trigger_with_no_artifact_is_missed(monkeypatch):
    monkeypatch.setattr(index, "CEILING_MIN", 360)
    monkeypatch.setattr(index, "MARGIN_MIN", 45)
    trig = {"at": _dt(2026, 6, 29, 23, 0), "label": "23:00 daily"}
    # Only artifact is from a DIFFERENT run window (the next morning's groom).
    stamps = [_dt(2026, 6, 30, 7, 10)]
    assert [m["at"] for m in index._missed([trig], stamps)] == [trig["at"]]


def test_single_silent_death_not_masked_by_later_success(monkeypatch):
    """The key property: a missed 23:00 run is still flagged even though the next
    07:00 run wrote an artifact (per-trigger windows, not latest-age)."""
    monkeypatch.setattr(index, "CEILING_MIN", 360)
    monkeypatch.setattr(index, "MARGIN_MIN", 45)
    t_dead = {"at": _dt(2026, 6, 29, 23, 0), "label": "23:00 daily"}
    t_ok = {"at": _dt(2026, 6, 30, 7, 0), "label": "07:00 daily"}
    stamps = [_dt(2026, 6, 30, 7, 8)]  # only the 07:00 run reported
    misses = index._missed([t_dead, t_ok], stamps)
    assert [m["at"] for m in misses] == [t_dead["at"]]


# ---- _lookback_dates / _fetch_run_artifact_timestamps ----------------------


def test_lookback_dates_spans_horizon_to_now(monkeypatch):
    monkeypatch.setattr(index, "LOOKBACK_HOURS", 30)
    now = _dt(2026, 7, 2, 14, 0)  # horizon = 2026-07-01 08:00
    assert index._lookback_dates(now) == ["2026-07-01", "2026-07-02"]


class _FakeArtifactS3:
    """Fakes list_objects_v2 (single page) + get_object for the artifact fetch."""

    def __init__(self, objects_by_prefix):
        self._objects = objects_by_prefix  # {prefix: [(key, body_dict), ...]}

    def list_objects_v2(self, Bucket, Prefix, ContinuationToken=None):  # noqa: N803
        items = self._objects.get(Prefix, [])
        return {"Contents": [{"Key": k} for k, _ in items], "IsTruncated": False}

    def get_object(self, Bucket, Key):  # noqa: N803
        import io

        for items in self._objects.values():
            for k, body in items:
                if k == Key:
                    return {"Body": io.BytesIO(index.json.dumps(body).encode())}
        raise AssertionError(f"unexpected key {Key}")


def test_fetch_run_artifact_timestamps_reads_run_start(monkeypatch):
    monkeypatch.setattr(index, "LOOKBACK_HOURS", 6)
    monkeypatch.setattr(index, "RUN_ARTIFACT_PREFIX", "groom/")
    now = _dt(2026, 7, 2, 10, 0)
    s3 = _FakeArtifactS3({
        "groom/2026-07-02/": [
            ("groom/2026-07-02/run1.json", {"run_start": "2026-07-02T07:00:05+00:00"}),
        ],
    })
    stamps = index._fetch_run_artifact_timestamps(s3, now)
    assert stamps == [datetime(2026, 7, 2, 7, 0, 5, tzinfo=UTC)]


def test_fetch_run_artifact_timestamps_skips_non_json_and_missing_run_start(monkeypatch):
    monkeypatch.setattr(index, "LOOKBACK_HOURS", 6)
    monkeypatch.setattr(index, "RUN_ARTIFACT_PREFIX", "groom/")
    now = _dt(2026, 7, 2, 10, 0)
    s3 = _FakeArtifactS3({
        "groom/2026-07-02/": [
            ("groom/2026-07-02/marker.txt", {}),
            ("groom/2026-07-02/empty.json", {}),
        ],
    })
    assert index._fetch_run_artifact_timestamps(s3, now) == []


# ---- config#2667: decision-log-driven expected triggers (sweep-mode) -------


def test_decision_launched_true_for_top_level_launched_bool():
    assert index._decision_launched({"launched": True}) is True


def test_decision_launched_true_for_top_level_launch_bool():
    assert index._decision_launched({"launch": True}) is True


def test_decision_launched_true_when_any_decision_entry_launches():
    record = {
        "decisions": [
            {"launch": False, "issue_filter": "low-only"},
            {"launch": True, "issue_filter": "high-only"},
        ]
    }
    assert index._decision_launched(record) is True


def test_decision_launched_false_for_skip_only_record():
    """A demand-gate/concurrent-lane skip (or a failed-enumeration fail-closed
    skip) — decisions is empty or every entry is launch=false. Must be
    ignored, not treated as an expected trigger."""
    assert index._decision_launched({"decisions": []}) is False
    assert index._decision_launched({
        "decisions": [{"launch": False, "reason": "concurrent_tier_skip"}]
    }) is False
    assert index._decision_launched({}) is False


def test_expected_triggers_from_decisions_sweep_launch_true_included(monkeypatch):
    """The actual config#2667 gap: a sweep dispatch (no fixed cron) with a
    launch=true decision must surface as an expected trigger via the
    decision log."""
    monkeypatch.setattr(index, "LOOKBACK_HOURS", 12)
    monkeypatch.setattr(index, "CEILING_MIN", 360)
    monkeypatch.setattr(index, "MARGIN_MIN", 45)
    monkeypatch.setattr(index, "DECISION_RECORD_PREFIX", "groom/decisions/")
    now = _dt(2026, 7, 15, 12, 0)
    s3 = _FakeArtifactS3({
        "groom/decisions/2026-07-15/": [
            ("groom/decisions/2026-07-15/sweep-195600.json", {
                "schema_version": 2, "trigger": "launch_decided", "run_mode": "sweep",
                "decisions": [{"launch": True, "issue_filter": "mid-only"}],
                "decided_at": "2026-07-15T04:56:00+00:00",
            }),
        ],
    })
    trigs = index._expected_triggers_from_decisions(s3, now)
    assert [t["at"] for t in trigs] == [_dt(2026, 7, 15, 4, 56)]


def test_expected_triggers_from_decisions_skip_only_excluded(monkeypatch):
    """A skip-only decision record (launch=false for every entry) must NOT
    become an expected trigger — it was never supposed to produce a run
    artifact."""
    monkeypatch.setattr(index, "LOOKBACK_HOURS", 12)
    monkeypatch.setattr(index, "CEILING_MIN", 360)
    monkeypatch.setattr(index, "MARGIN_MIN", 45)
    monkeypatch.setattr(index, "DECISION_RECORD_PREFIX", "groom/decisions/")
    now = _dt(2026, 7, 15, 12, 0)
    s3 = _FakeArtifactS3({
        "groom/decisions/2026-07-15/": [
            ("groom/decisions/2026-07-15/sweep-193000.json", {
                "schema_version": 2, "trigger": "launch_decided", "run_mode": "sweep",
                "decisions": [{"launch": False, "reason": "concurrent_tier_skip"}],
                "decided_at": "2026-07-15T04:30:00+00:00",
            }),
            ("groom/decisions/2026-07-15/trigger-0700.json", {
                "schema_version": 2, "trigger": "demand-all", "skip_reason": "demand_gate_skip",
                "decisions": [], "decided_at": "2026-07-15T07:00:00+00:00",
            }),
        ],
    })
    assert index._expected_triggers_from_decisions(s3, now) == []


def test_expected_triggers_from_decisions_immature_excluded(monkeypatch):
    """A decision record decided too recently to have had CEILING+MARGIN
    minutes to finish must not yet be checkable."""
    monkeypatch.setattr(index, "LOOKBACK_HOURS", 12)
    monkeypatch.setattr(index, "CEILING_MIN", 360)
    monkeypatch.setattr(index, "MARGIN_MIN", 45)
    monkeypatch.setattr(index, "DECISION_RECORD_PREFIX", "groom/decisions/")
    now = _dt(2026, 7, 15, 12, 0)
    s3 = _FakeArtifactS3({
        "groom/decisions/2026-07-15/": [
            ("groom/decisions/2026-07-15/sweep-113000.json", {
                "schema_version": 2, "trigger": "launch_decided", "run_mode": "sweep",
                "decisions": [{"launch": True}],
                "decided_at": "2026-07-15T11:30:00+00:00",  # 30 min ago — not mature
            }),
        ],
    })
    assert index._expected_triggers_from_decisions(s3, now) == []


def test_expected_triggers_from_decisions_skips_malformed_record(monkeypatch):
    """A single unreadable/malformed record must not hide the rest — mirrors
    _fetch_run_artifact_timestamps's malformed-artifact tolerance."""
    monkeypatch.setattr(index, "LOOKBACK_HOURS", 12)
    monkeypatch.setattr(index, "CEILING_MIN", 360)
    monkeypatch.setattr(index, "MARGIN_MIN", 45)
    monkeypatch.setattr(index, "DECISION_RECORD_PREFIX", "groom/decisions/")
    now = _dt(2026, 7, 15, 12, 0)

    class _BoomOnOneKey(_FakeArtifactS3):
        def get_object(self, Bucket, Key):  # noqa: N803
            if "boom" in Key:
                raise RuntimeError("corrupt object")
            return super().get_object(Bucket=Bucket, Key=Key)

    s3 = _BoomOnOneKey({
        "groom/decisions/2026-07-15/": [
            ("groom/decisions/2026-07-15/boom.json", {}),
            ("groom/decisions/2026-07-15/sweep-195600.json", {
                "schema_version": 2, "trigger": "launch_decided", "run_mode": "sweep",
                "decisions": [{"launch": True}],
                "decided_at": "2026-07-15T04:56:00+00:00",
            }),
        ],
    })
    trigs = index._expected_triggers_from_decisions(s3, now)
    assert [t["at"] for t in trigs] == [_dt(2026, 7, 15, 4, 56)]


def test_all_expected_triggers_dedupes_same_instant(monkeypatch):
    """A full-mode trigger legitimately appears in BOTH the fixed-cron
    schedule AND the decision log (demand-all writes a record too) — must not
    be double-counted."""
    monkeypatch.setattr(index, "LOOKBACK_HOURS", 12)
    monkeypatch.setattr(index, "CEILING_MIN", 360)
    monkeypatch.setattr(index, "MARGIN_MIN", 45)
    trig = _dt(2026, 7, 15, 7, 0)
    monkeypatch.setattr(index, "_expected_triggers", lambda now: [{"at": trig, "label": "07:00 daily"}])
    monkeypatch.setattr(
        index, "_expected_triggers_from_decisions",
        lambda s3, now: [{"at": trig, "label": "decision-log:demand-all"}],
    )
    out = index._all_expected_triggers(object(), _dt(2026, 7, 15, 12, 0))
    assert [t["at"] for t in out] == [trig]


# ---- handler (dedup + fail-loud) -------------------------------------------


class _FakeS3:
    def __init__(self, alerted=None):
        self._alerted = alerted or []
        self.put_calls = []

    def get_object(self, Bucket, Key):  # noqa: N803 — boto3 kwarg names
        import io

        body = index.json.dumps({"alerted": self._alerted}).encode()
        return {"Body": io.BytesIO(body)}

    def put_object(self, Bucket, Key, Body, ContentType):  # noqa: N803
        self.put_calls.append(index.json.loads(Body))


def _wire(monkeypatch, *, triggers, stamps, s3, sent=True, now=_dt(2026, 6, 30, 0, 0)):
    # Freeze _now() — handler()'s _load_alerted() prunes the alerted-set to
    # [now - LOOKBACK_HOURS, now], so leaving _now() on the REAL wall clock while
    # every test hardcodes a 2026-06-29-ish trigger date makes the suite flaky-by
    # -design: it silently breaks the moment real time drifts far enough past the
    # hardcoded dates to prune them out of the lookback window (caught 2026-07-02
    # — test_handler_suppresses_already_alerted_miss started failing with no code
    # change, purely from elapsed wall-clock time). Default `now` sits ~1h after
    # the trigger dates the other tests share.
    monkeypatch.setattr(index, "_now", lambda: now)
    monkeypatch.setattr(index, "_all_expected_triggers", lambda s3, now: triggers)
    monkeypatch.setattr(index, "_fetch_run_artifact_timestamps", lambda s3, now: stamps)
    monkeypatch.setattr(index, "_s3_client", lambda: s3)
    sends = []
    monkeypatch.setattr(
        index,
        "notify_via_flow_doctor",
        lambda text, **kwargs: sends.append(text) or sent,
    )
    return sends


def test_handler_alerts_new_miss_and_records_state(monkeypatch):
    trig = {"at": _dt(2026, 6, 29, 23, 0), "label": "23:00 daily"}
    s3 = _FakeS3(alerted=[])
    sends = _wire(monkeypatch, triggers=[trig], stamps=[], s3=s3)
    out = index.handler({}, None)
    assert out["new_missed"] == 1 and out["alerted"] is True
    assert len(sends) == 1
    # State persisted so the next run suppresses it.
    assert s3.put_calls and trig["at"].isoformat() in s3.put_calls[-1]["alerted"]


def test_handler_suppresses_already_alerted_miss(monkeypatch):
    trig = {"at": _dt(2026, 6, 29, 23, 0), "label": "23:00 daily"}
    s3 = _FakeS3(alerted=[trig["at"].isoformat()])
    sends = _wire(monkeypatch, triggers=[trig], stamps=[], s3=s3)
    out = index.handler({}, None)
    assert out["missed"] == 1 and out["new_missed"] == 0 and out["alerted"] is False
    assert sends == []  # no duplicate ping


def test_handler_all_reported_no_alert(monkeypatch):
    trig = {"at": _dt(2026, 6, 29, 23, 0), "label": "23:00 daily"}
    s3 = _FakeS3(alerted=[])
    sends = _wire(monkeypatch, triggers=[trig], stamps=[_dt(2026, 6, 29, 23, 5)], s3=s3)
    out = index.handler({}, None)
    assert out["missed"] == 0 and out["alerted"] is False and sends == []


def test_handler_fail_loud_on_s3_error(monkeypatch):
    """The PRIMARY input (S3 run-artifact fetch) RAISES — a silently-skipped
    check is the exact failure this guards against."""
    trig = {"at": _dt(2026, 6, 29, 23, 0), "label": "23:00 daily"}
    monkeypatch.setattr(index, "_all_expected_triggers", lambda s3, now: [trig])
    monkeypatch.setattr(index, "_s3_client", lambda: _FakeS3())

    def _boom(s3, now):
        raise RuntimeError("s3 500")

    monkeypatch.setattr(index, "_fetch_run_artifact_timestamps", _boom)
    with pytest.raises(RuntimeError):
        index.handler({}, None)


def test_handler_no_mature_triggers_short_circuits(monkeypatch):
    monkeypatch.setattr(index, "_all_expected_triggers", lambda s3, now: [])
    monkeypatch.setattr(index, "_s3_client", lambda: _FakeS3())
    out = index.handler({}, None)
    assert out["checked"] == 0 and out["alerted"] is False


# ---- config#2667: end-to-end sweep-mode detection via the real S3 reads ----
#
# These three exercise the FULL real path (_all_expected_triggers →
# _expected_triggers_from_decisions → _fetch_run_artifact_timestamps →
# _missed → handler), with only notify_via_flow_doctor + the dedup-state
# object stubbed — the acceptance criteria the issue calls out explicitly:
# a sweep dispatch with launch=true + a matching artifact is NOT flagged; a
# sweep dispatch with launch=true and NO matching artifact after maturity IS
# flagged; a skip-only decision record is correctly ignored.


class _FakeFullS3:
    """One fake S3 serving decision records, run artifacts, AND the liveness
    dedup-state blob — everything handler() touches for these end-to-end
    tests. ``decisions``/``artifacts`` map date -> [(key, body_dict), ...];
    ``alerted`` seeds the dedup-state GET.
    """

    def __init__(self, *, decisions=None, artifacts=None, alerted=None):
        self._decisions = decisions or {}
        self._artifacts = artifacts or {}
        self._alerted = alerted or []
        self.put_calls = []

    def list_objects_v2(self, Bucket, Prefix, ContinuationToken=None):  # noqa: N803
        for date, items in self._decisions.items():
            if Prefix == f"groom/decisions/{date}/":
                return {"Contents": [{"Key": k} for k, _ in items], "IsTruncated": False}
        for date, items in self._artifacts.items():
            if Prefix == f"groom/{date}/":
                return {"Contents": [{"Key": k} for k, _ in items], "IsTruncated": False}
        return {"Contents": [], "IsTruncated": False}

    def get_object(self, Bucket, Key):  # noqa: N803
        import io

        if Key == index.STATE_KEY:
            return {"Body": io.BytesIO(index.json.dumps({"alerted": self._alerted}).encode())}
        for items in list(self._decisions.values()) + list(self._artifacts.values()):
            for k, body in items:
                if k == Key:
                    return {"Body": io.BytesIO(index.json.dumps(body).encode())}
        raise AssertionError(f"unexpected key {Key}")

    def put_object(self, Bucket, Key, Body, ContentType=None):  # noqa: N803
        self.put_calls.append((Key, index.json.loads(Body)))


def _sweep_decision(decided_at: str, *, launch: bool) -> dict:
    return {
        "schema_version": 2, "trigger": "launch_decided", "run_mode": "sweep",
        "decisions": [{"launch": launch, "issue_filter": "mid-only"}],
        "decided_at": decided_at,
    }


def test_e2e_sweep_with_matching_artifact_not_flagged(monkeypatch):
    monkeypatch.setattr(index, "_now", lambda: _dt(2026, 7, 15, 12, 0))
    monkeypatch.setattr(index, "LOOKBACK_HOURS", 30)
    monkeypatch.setattr(index, "CEILING_MIN", 360)
    monkeypatch.setattr(index, "MARGIN_MIN", 45)
    # Isolate to the decision-log source only — the fixed-cron schedule would
    # otherwise ALSO contribute real 01:00/07:00/19:00 triggers for this `now`,
    # polluting the assertions below (the cron source is covered by its own
    # dedicated test section above).
    monkeypatch.setattr(index, "_expected_triggers", lambda now: [])
    sends = []
    monkeypatch.setattr(index, "notify_via_flow_doctor", lambda text, **kw: sends.append(text) or True)

    s3 = _FakeFullS3(
        decisions={
            "2026-07-15": [
                ("groom/decisions/2026-07-15/sweep-195600.json",
                 _sweep_decision("2026-07-15T04:56:00+00:00", launch=True)),
            ],
        },
        artifacts={
            "2026-07-15": [
                ("groom/2026-07-15/sweep-run1.json", {"run_start": "2026-07-15T04:56:30+00:00"}),
            ],
        },
    )
    monkeypatch.setattr(index, "_s3_client", lambda: s3)

    out = index.handler({}, None)
    assert out["missed"] == 0
    assert out["alerted"] is False
    assert sends == []


def test_e2e_sweep_with_no_artifact_after_maturity_is_flagged(monkeypatch):
    monkeypatch.setattr(index, "_now", lambda: _dt(2026, 7, 15, 12, 0))
    monkeypatch.setattr(index, "LOOKBACK_HOURS", 30)
    monkeypatch.setattr(index, "CEILING_MIN", 360)
    monkeypatch.setattr(index, "MARGIN_MIN", 45)
    # Isolate to the decision-log source only — the fixed-cron schedule would
    # otherwise ALSO contribute real 01:00/07:00/19:00 triggers for this `now`,
    # polluting the assertions below (the cron source is covered by its own
    # dedicated test section above).
    monkeypatch.setattr(index, "_expected_triggers", lambda now: [])
    sends = []
    monkeypatch.setattr(index, "notify_via_flow_doctor", lambda text, **kw: sends.append(text) or True)

    s3 = _FakeFullS3(
        decisions={
            "2026-07-15": [
                ("groom/decisions/2026-07-15/sweep-195600.json",
                 _sweep_decision("2026-07-15T04:56:00+00:00", launch=True)),
            ],
        },
        artifacts={},  # the box died silently — NO run artifact anywhere
    )
    monkeypatch.setattr(index, "_s3_client", lambda: s3)

    out = index.handler({}, None)
    assert out["new_missed"] == 1
    assert out["alerted"] is True
    assert len(sends) == 1
    assert "SILENT FAILURE" in sends[0]


def test_e2e_skip_only_sweep_decision_not_flagged(monkeypatch):
    """A sweep box that skipped (concurrent-lane guard — a prior cycle's
    sweep still live) writes launch=false. Must be ignored — it was never
    expected to produce a run artifact, so its absence is correct, not a
    silent failure."""
    monkeypatch.setattr(index, "_now", lambda: _dt(2026, 7, 15, 12, 0))
    monkeypatch.setattr(index, "LOOKBACK_HOURS", 30)
    monkeypatch.setattr(index, "CEILING_MIN", 360)
    monkeypatch.setattr(index, "MARGIN_MIN", 45)
    # Isolate to the decision-log source only — the fixed-cron schedule would
    # otherwise ALSO contribute real 01:00/07:00/19:00 triggers for this `now`,
    # polluting the assertions below (the cron source is covered by its own
    # dedicated test section above).
    monkeypatch.setattr(index, "_expected_triggers", lambda now: [])
    sends = []
    monkeypatch.setattr(index, "notify_via_flow_doctor", lambda text, **kw: sends.append(text) or True)

    s3 = _FakeFullS3(
        decisions={
            "2026-07-15": [
                ("groom/decisions/2026-07-15/sweep-195600.json",
                 _sweep_decision("2026-07-15T04:56:00+00:00", launch=False)),
            ],
        },
        artifacts={},
    )
    monkeypatch.setattr(index, "_s3_client", lambda: s3)

    out = index.handler({}, None)
    assert out["checked"] == 0
    assert out["alerted"] is False
    assert sends == []
