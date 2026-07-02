"""Unit tests for the groom-liveness-probe handler.

Cover the load-bearing logic with no AWS/GitHub I/O: trigger enumeration
(maturity + day-of-week filtering), per-trigger miss attribution, S3 dedup
suppression, and the fail-loud contract on the PRIMARY input.
"""

from __future__ import annotations

import sys
import types
from datetime import datetime, timedelta, timezone

import pytest

# Stub alpha_engine_lib.telegram before importing index (the real lib isn't on the
# test path; the probe only uses send_message).
_ael = types.ModuleType("alpha_engine_lib")
_tg = types.ModuleType("alpha_engine_lib.telegram")
_tg.send_message = lambda *a, **k: True  # type: ignore[attr-defined]
_ael.telegram = _tg  # type: ignore[attr-defined]
sys.modules.setdefault("alpha_engine_lib", _ael)
sys.modules.setdefault("alpha_engine_lib.telegram", _tg)

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
    # Tue 14:00 UTC. The 07:00 Tue trigger matured (7h ago); a hypothetical recent
    # one would not. 23:00 Mon (prev day) also mature.
    now = _dt(2026, 6, 30, 14, 0)  # 2026-06-30 is a Tuesday
    trigs = index._expected_triggers(now)
    ats = {t["at"] for t in trigs}
    assert _dt(2026, 6, 30, 7, 0) in ats   # 07:00 Tue, matured
    assert _dt(2026, 6, 29, 23, 0) in ats  # 23:00 Mon, matured
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


def test_1500_opus_schedule_included(monkeypatch):
    """config#1571: the 15:00 UTC Opus/complexity:high schedule must be tracked
    too, not just the two Sonnet schedules — its silent death was previously
    invisible to this probe (the schedule existed since 2026-07-01, config#1495
    follow-up, but was never added here)."""
    monkeypatch.setattr(index, "CEILING_MIN", 360)
    monkeypatch.setattr(index, "MARGIN_MIN", 45)
    monkeypatch.setattr(index, "LOOKBACK_HOURS", 30)
    # Wed 2026-07-01 22:00 UTC -> 15:00 Wed matured (7h ago).
    now = _dt(2026, 7, 1, 22, 0)
    trigs = index._expected_triggers(now)
    assert _dt(2026, 7, 1, 15, 0) in {t["at"] for t in trigs}


def test_three_daily_triggers_dont_overlap_in_one_day(monkeypatch):
    """The three windows [T, T+CEILING+MARGIN] must stay disjoint so per-trigger
    attribution remains 1:1 (see _missed's docstring) now that a 3rd schedule
    shares the day with the original two."""
    monkeypatch.setattr(index, "CEILING_MIN", 360)
    monkeypatch.setattr(index, "MARGIN_MIN", 45)
    monkeypatch.setattr(index, "LOOKBACK_HOURS", 24)
    now = _dt(2026, 7, 2, 6, 0)
    trigs = index._expected_triggers(now)
    ats = sorted(t["at"] for t in trigs)
    assert len(ats) == 3
    window = timedelta(minutes=360 + 45)
    for a, b in zip(ats, ats[1:]):
        assert a + window <= b, f"{a} window overlaps {b}"


# ---- _missed ---------------------------------------------------------------


def test_trigger_with_digest_in_window_is_not_missed(monkeypatch):
    monkeypatch.setattr(index, "CEILING_MIN", 360)
    monkeypatch.setattr(index, "MARGIN_MIN", 45)
    trig = {"at": _dt(2026, 6, 29, 23, 0), "label": "23:00 daily"}
    # Digest filed 6 min into the run → inside [23:00, 05:45].
    stamps = [_dt(2026, 6, 29, 23, 6)]
    assert index._missed([trig], stamps) == []


def test_trigger_with_no_digest_is_missed(monkeypatch):
    monkeypatch.setattr(index, "CEILING_MIN", 360)
    monkeypatch.setattr(index, "MARGIN_MIN", 45)
    trig = {"at": _dt(2026, 6, 29, 23, 0), "label": "23:00 daily"}
    # Only digest is from a DIFFERENT run window (the next morning's groom).
    stamps = [_dt(2026, 6, 30, 7, 10)]
    assert [m["at"] for m in index._missed([trig], stamps)] == [trig["at"]]


def test_single_silent_death_not_masked_by_later_success(monkeypatch):
    """The key property: a missed 23:00 run is still flagged even though the next
    07:00 run filed a digest (per-trigger windows, not latest-age)."""
    monkeypatch.setattr(index, "CEILING_MIN", 360)
    monkeypatch.setattr(index, "MARGIN_MIN", 45)
    t_dead = {"at": _dt(2026, 6, 29, 23, 0), "label": "23:00 daily"}
    t_ok = {"at": _dt(2026, 6, 30, 7, 0), "label": "07:00 daily"}
    stamps = [_dt(2026, 6, 30, 7, 8)]  # only the 07:00 run reported
    misses = index._missed([t_dead, t_ok], stamps)
    assert [m["at"] for m in misses] == [t_dead["at"]]


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
    monkeypatch.setattr(index, "_expected_triggers", lambda now: triggers)
    monkeypatch.setattr(index, "_get_github_pat", lambda: "pat")
    monkeypatch.setattr(index, "_fetch_digest_timestamps", lambda pat: stamps)
    monkeypatch.setattr(index, "_s3_client", lambda: s3)
    sends = []
    monkeypatch.setattr(index, "send_message", lambda *a, **k: sends.append(a) or sent)
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


def test_handler_fail_loud_on_github_error(monkeypatch):
    """The PRIMARY input (digest fetch) RAISES — a silently-skipped check is the
    exact failure this guards against."""
    trig = {"at": _dt(2026, 6, 29, 23, 0), "label": "23:00 daily"}
    monkeypatch.setattr(index, "_expected_triggers", lambda now: [trig])
    monkeypatch.setattr(index, "_get_github_pat", lambda: "pat")

    def _boom(pat):
        raise RuntimeError("github 500")

    monkeypatch.setattr(index, "_fetch_digest_timestamps", _boom)
    with pytest.raises(RuntimeError):
        index.handler({}, None)


def test_handler_no_mature_triggers_short_circuits(monkeypatch):
    monkeypatch.setattr(index, "_expected_triggers", lambda now: [])
    out = index.handler({}, None)
    assert out["checked"] == 0 and out["alerted"] is False
