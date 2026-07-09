"""Unit tests for the usage-pace-alert handler.

Cover the load-bearing logic with no AWS/Telegram I/O: WARN/OVER threshold
math, per-tier rising-edge dedup, fail-loud on the PRIMARY inputs (pacing
config + weekly usage read), and week-boundary re-arm.

Hermetic: mirrors groom-liveness-probe/test_handler.py's stub pattern —
nousergon_lib submodules index.py imports at module scope are stubbed in
sys.modules BEFORE `import index`; the notify path itself is monkeypatched
per-test.
"""

from __future__ import annotations

import io
import json
import sys
import types
from datetime import datetime, timedelta
from pathlib import Path

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

from _shared.hermetic_import_guard import (  # noqa: E402
    assert_hermetic_imports_satisfied,
)

assert_hermetic_imports_satisfied(__file__)

import index  # noqa: E402

ANCHOR = datetime(2026, 7, 12, 21, 0)   # PT-naive, Sunday 9pm PT
PERIOD = timedelta(days=7)
CEILING = 850_000_000.0
USAGE_PREFIX = index.USAGE_PREFIX


class _FakeS3:
    """Minimal boto3-shaped S3 stub: pacing config + usage docs + dedup state."""

    def __init__(self, *, usage_docs=None, state=None, pacing_missing=False):
        self._usage_docs = usage_docs or {}  # key -> dict
        self._state = state  # dict or None
        self._pacing_missing = pacing_missing
        self.put_calls = []

    def get_object(self, Bucket, Key):  # noqa: N803
        if Key == index.PACING_CONFIG_KEY:
            if self._pacing_missing:
                raise RuntimeError("NoSuchKey")
            doc = {
                "schema_version": 1,
                "weekly_wet_ceiling": CEILING,
                "weekly_reset_anchor_pt": ANCHOR.isoformat(),
                "calibrated_date": "2026-07-08",
                "calibration_basis": "test",
            }
            return {"Body": io.BytesIO(json.dumps(doc).encode())}
        if Key == index.STATE_KEY:
            if self._state is None:
                raise RuntimeError("NoSuchKey")
            return {"Body": io.BytesIO(json.dumps(self._state).encode())}
        if Key in self._usage_docs:
            return {"Body": io.BytesIO(json.dumps(self._usage_docs[Key]).encode())}
        raise RuntimeError(f"unexpected get_object key: {Key}")

    def put_object(self, Bucket, Key, Body, ContentType):  # noqa: N803
        self.put_calls.append(json.loads(Body))

    def get_paginator(self, op):
        assert op == "list_objects_v2"
        keys = list(self._usage_docs.keys())

        class _Pager:
            def paginate(self_, **kwargs):
                return [{"Contents": [{"Key": k} for k in keys]}]

        return _Pager()


def _wire(monkeypatch, *, now, s3, sent=True):
    monkeypatch.setattr(index, "_now_pt", lambda: now)
    monkeypatch.setattr(index, "_s3_client", lambda: s3)
    sends = []

    def _fake_notify(text, **kwargs):
        sends.append((kwargs.get("dedup_key"), text))
        return sent

    monkeypatch.setattr(index, "notify_via_flow_doctor", _fake_notify)
    return sends


def _usage_doc(date_str: str, hour: int, wet: float) -> dict:
    return {"by_hour": {str(hour): {"claude-sonnet-5": {"wet": wet}}}}


# ---- threshold math ----------------------------------------------------------


def test_no_breach_when_on_pace(monkeypatch):
    # Tuesday 9pm PT = 48h elapsed = 28.6%. 25% used is under both thresholds.
    now = ANCHOR + timedelta(hours=48)
    wet = 0.25 * CEILING
    s3 = _FakeS3(usage_docs={f"{USAGE_PREFIX}interactive/2026-07-14.json": _usage_doc("2026-07-14", 21, wet)})
    sends = _wire(monkeypatch, now=now, s3=s3)
    out = index.handler({}, None)
    assert out["warn_breached"] is False
    assert out["over_breached"] is False
    assert sends == []


def test_warn_fires_within_margin_not_over(monkeypatch):
    # Tuesday 9pm PT: elapsed 28.6%. WARN threshold 26.6%. used=27% -> WARN only.
    now = ANCHOR + timedelta(hours=48)
    wet = 0.27 * CEILING
    s3 = _FakeS3(usage_docs={f"{USAGE_PREFIX}interactive/2026-07-14.json": _usage_doc("2026-07-14", 21, wet)})
    sends = _wire(monkeypatch, now=now, s3=s3)
    out = index.handler({}, None)
    assert out["warn_breached"] is True
    assert out["over_breached"] is False
    assert out["new_warn"] is True and out["new_over"] is False
    assert len(sends) == 1 and sends[0][0].startswith(f"{index._FLOW_NAME}:warn:")


def test_over_implies_warn_both_fire(monkeypatch):
    # used=30% > elapsed 28.6% -> OVER (and OVER is inside the WARN margin too).
    now = ANCHOR + timedelta(hours=48)
    wet = 0.30 * CEILING
    s3 = _FakeS3(usage_docs={f"{USAGE_PREFIX}interactive/2026-07-14.json": _usage_doc("2026-07-14", 21, wet)})
    sends = _wire(monkeypatch, now=now, s3=s3)
    out = index.handler({}, None)
    assert out["warn_breached"] is True and out["over_breached"] is True
    assert out["new_warn"] is True and out["new_over"] is True
    keys = {k for k, _ in sends}
    assert f"{index._FLOW_NAME}:warn:{out['window_start']}" in keys
    assert f"{index._FLOW_NAME}:over:{out['window_start']}" in keys


def test_warn_guard_skips_first_moments_after_reset(monkeypatch):
    # 30 min after reset: elapsed_frac (~0.3%) << PACE_ALERT_MARGIN (2%) -> WARN
    # suppressed even though a near-zero threshold would otherwise trip on any
    # nonzero usage. used_frac (0.1%) stays under elapsed_frac too, so OVER is
    # correctly false on its own merits (not because of the WARN guard, which
    # doesn't apply to OVER).
    now = ANCHOR + timedelta(minutes=30)
    wet = 0.001 * CEILING
    s3 = _FakeS3(usage_docs={f"{USAGE_PREFIX}interactive/2026-07-12.json": _usage_doc("2026-07-12", 21, wet)})
    sends = _wire(monkeypatch, now=now, s3=s3)
    out = index.handler({}, None)
    assert out["warn_breached"] is False
    assert sends == []


# ---- rising-edge dedup ---------------------------------------------------------


def test_warn_suppressed_once_already_recorded_this_window(monkeypatch):
    now = ANCHOR + timedelta(hours=48)
    win_start_iso = ANCHOR.isoformat()
    wet = 0.27 * CEILING
    s3 = _FakeS3(
        usage_docs={f"{USAGE_PREFIX}interactive/2026-07-14.json": _usage_doc("2026-07-14", 21, wet)},
        state={"window_start": win_start_iso, "warn_breached": True, "over_breached": False},
    )
    sends = _wire(monkeypatch, now=now, s3=s3)
    out = index.handler({}, None)
    assert out["warn_breached"] is True and out["new_warn"] is False
    assert sends == []  # no duplicate ping


def test_warn_rearms_after_dropping_then_recrossing(monkeypatch):
    # Prior state this SAME window: warn was breached, then usage dropped back
    # under (state now says warn_breached: False after an intervening run) —
    # a fresh crossing must alert again.
    now = ANCHOR + timedelta(hours=48)
    win_start_iso = ANCHOR.isoformat()
    wet = 0.27 * CEILING
    s3 = _FakeS3(
        usage_docs={f"{USAGE_PREFIX}interactive/2026-07-14.json": _usage_doc("2026-07-14", 21, wet)},
        state={"window_start": win_start_iso, "warn_breached": False, "over_breached": False},
    )
    sends = _wire(monkeypatch, now=now, s3=s3)
    out = index.handler({}, None)
    assert out["new_warn"] is True
    assert len(sends) == 1


def test_new_week_state_ignored_both_tiers_rearm(monkeypatch):
    # State on file is from the PRIOR window (window_start doesn't match the
    # current one) -> treated as no prior state, both tiers re-arm.
    now = ANCHOR + PERIOD + timedelta(hours=48)
    wet = 0.30 * (CEILING)
    s3 = _FakeS3(
        usage_docs={f"{USAGE_PREFIX}interactive/2026-07-21.json": _usage_doc("2026-07-21", 21, wet)},
        state={"window_start": ANCHOR.isoformat(), "warn_breached": True, "over_breached": True},
    )
    sends = _wire(monkeypatch, now=now, s3=s3)
    out = index.handler({}, None)
    assert out["new_warn"] is True and out["new_over"] is True
    assert len(sends) == 2


def test_state_persisted_after_run(monkeypatch):
    now = ANCHOR + timedelta(hours=48)
    wet = 0.30 * CEILING
    s3 = _FakeS3(usage_docs={f"{USAGE_PREFIX}interactive/2026-07-14.json": _usage_doc("2026-07-14", 21, wet)})
    _wire(monkeypatch, now=now, s3=s3)
    index.handler({}, None)
    assert s3.put_calls
    saved = s3.put_calls[-1]
    assert saved["warn_breached"] is True and saved["over_breached"] is True
    assert saved["window_start"] == ANCHOR.isoformat()


# ---- fail-loud on PRIMARY inputs ----------------------------------------------


def test_fail_loud_when_pacing_config_missing(monkeypatch):
    now = ANCHOR + timedelta(hours=48)
    s3 = _FakeS3(pacing_missing=True)
    _wire(monkeypatch, now=now, s3=s3)
    try:
        index.handler({}, None)
        assert False, "expected an exception"
    except RuntimeError:
        pass


def test_fail_loud_when_usage_read_fails(monkeypatch):
    now = ANCHOR + timedelta(hours=48)
    s3 = _FakeS3()

    def boom(s3_, win):
        raise RuntimeError("s3 list failed")

    monkeypatch.setattr(index, "_read_weekly_wet", boom)
    _wire(monkeypatch, now=now, s3=s3)
    try:
        index.handler({}, None)
        assert False, "expected an exception"
    except RuntimeError:
        pass
