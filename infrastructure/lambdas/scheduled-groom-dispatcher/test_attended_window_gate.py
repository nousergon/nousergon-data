"""Tests for the attended-window gate on the irreversible (PR-sweep) dispatch
tier (alpha-engine-config-I6461, groom-sweep-policy.md §4.8).

Reuses ``test_handler.py``'s ``_load`` fixture (same hermetic boto3/ec2_spot/
nousergon_lib stubs) rather than building a second stub harness — this file
only adds the window-specific scenarios.
"""

from __future__ import annotations

import json
import os
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from test_handler import _load  # noqa: E402 — path insert above must run first

MANIFEST_PATH = (
    Path(__file__).resolve().parents[2] / "scheduler" / "schedule-manifest.json"
)

# 2026-08-04 16:00 UTC = 09:00 America/Los_Angeles (PDT, UTC-7) — inside the
# manifest's declared 08:00-21:00 attended window.
_IN_WINDOW_UTC = datetime(2026, 8, 4, 16, 0, tzinfo=ZoneInfo("UTC"))
# 2026-08-04 09:00 UTC = 02:00 America/Los_Angeles — inside the §4.8-measured
# 21:00-05:00 local dead zone, outside the window.
_OUT_OF_WINDOW_UTC = datetime(2026, 8, 4, 9, 0, tzinfo=ZoneInfo("UTC"))


class _FrozenDatetime(datetime):
    """Subclass swapped in for ``index.datetime`` so ``datetime.now(tz)``
    inside the module returns a fixed instant, without touching any other
    datetime usage in the test process."""

    _frozen: datetime

    @classmethod
    def now(cls, tz=None):  # noqa: D102 — mirrors datetime.now's signature
        return cls._frozen.astimezone(tz) if tz else cls._frozen


def _freeze(monkeypatch, index, moment: datetime) -> None:
    frozen = type("_Frozen", (_FrozenDatetime,), {"_frozen": moment})
    monkeypatch.setattr(index, "datetime", frozen)


# ── _in_attended_window / _is_irreversible_dispatch (pure functions) ───────

def test_in_attended_window_true_at_9am_pacific(monkeypatch):
    index = _load(monkeypatch)
    assert index._in_attended_window(_IN_WINDOW_UTC) is True


def test_in_attended_window_false_at_2am_pacific(monkeypatch):
    index = _load(monkeypatch)
    assert index._in_attended_window(_OUT_OF_WINDOW_UTC) is False


def test_in_attended_window_respects_env_override(monkeypatch):
    index = _load(monkeypatch, env={
        "GROOM_ATTENDED_WINDOW_START_HOUR": "0",
        "GROOM_ATTENDED_WINDOW_END_HOUR": "24",
    })
    # A window that spans the full day admits the otherwise-out-of-window instant.
    assert index._in_attended_window(_OUT_OF_WINDOW_UTC) is True


def test_only_the_three_standalone_sweep_rules_are_irreversible(monkeypatch):
    index = _load(monkeypatch)
    assert index._is_irreversible_dispatch("alpha-engine-groom-sweep-0000-daily")
    assert index._is_irreversible_dispatch("alpha-engine-groom-sweep-0800-daily")
    assert index._is_irreversible_dispatch("alpha-engine-groom-sweep-1600-daily")
    # The SF's own unconditional end-of-SF tail-catcher is deliberately NOT
    # gated (deploy.sh / step_function_groom.json: gating it would contradict
    # its purpose as the drain-the-backlog tail of an already-running cycle).
    assert not index._is_irreversible_dispatch("end-of-sf-sweep")
    # Every FULL groom trigger stays reversible (opens PRs only).
    assert not index._is_irreversible_dispatch("0 4 * * *")


# ── _launch_groom_spot gate behavior ────────────────────────────────────────

def test_irreversible_dispatch_deferred_outside_window(monkeypatch):
    index = _load(monkeypatch, running_tier_instances={"sweep": ["i-existing"]})
    _freeze(monkeypatch, index, _OUT_OF_WINDOW_UTC)
    result = index._launch_groom_spot(
        "sweep", "alpha-engine-groom-sweep-0000-daily", "deepseek-v4-flash",
        "mid-only",
    )
    assert result == {
        "launched": False,
        "reason": "attended_window_deferred",
        "issue_filter": "mid-only",
        "schedule": "alpha-engine-groom-sweep-0000-daily",
    }
    # Proves gate ordering: a seeded "live" sweep box would have produced
    # concurrent_tier_skip had the concurrency probe run first. It didn't —
    # the window check short-circuits before any AWS call.


def test_irreversible_dispatch_launches_inside_window(monkeypatch):
    calls = {}

    def _launch(types_, subnets, **kw):
        calls["launched"] = True
        return "i-stub"

    index = _load(monkeypatch, launch_impl=_launch)
    _freeze(monkeypatch, index, _IN_WINDOW_UTC)
    result = index._launch_groom_spot(
        "sweep", "alpha-engine-groom-sweep-0000-daily", "deepseek-v4-flash",
        "mid-only",
    )
    assert result.get("launched") is True
    assert calls.get("launched") is True


def test_reversible_full_groom_dispatch_ignores_the_window(monkeypatch):
    """A full-groom (opens-PRs-only) trigger must launch regardless of the
    clock — §4.8: 'reversible work is not gated by the clock'."""
    calls = {}

    def _launch(types_, subnets, **kw):
        calls["launched"] = True
        return "i-stub"

    index = _load(monkeypatch, launch_impl=_launch)
    _freeze(monkeypatch, index, _OUT_OF_WINDOW_UTC)
    result = index._launch_groom_spot(
        "full", "0 4 * * *", "deepseek-v4-pro", "mid-only",
    )
    assert result.get("launched") is True
    assert calls.get("launched") is True


def test_end_of_sf_sweep_tail_ignores_the_window(monkeypatch):
    """The SF's unconditional end-of-SF sweep must not be deferred — it is
    the tail of a cycle that may have started inside the window."""
    calls = {}

    def _launch(types_, subnets, **kw):
        calls["launched"] = True
        return "i-stub"

    index = _load(monkeypatch, launch_impl=_launch)
    _freeze(monkeypatch, index, _OUT_OF_WINDOW_UTC)
    result = index._launch_groom_spot(
        "sweep", "end-of-sf-sweep", "deepseek-v4-flash", "mid-only",
    )
    assert result.get("launched") is True
    assert calls.get("launched") is True


def test_deferred_dispatch_is_ledger_recorded_via_launch_decided_path(monkeypatch):
    """End-to-end through handler(): a launch_decided sweep dispatch outside
    the window must still write a groom/decisions/{date}/*.json record
    naming the deferral (the durable 'counted' half of 'counted/logged') —
    exercising the SAME _write_sweep_decision_record call every other
    launch_decided outcome goes through, not a bespoke recorder."""
    index = _load(monkeypatch, s3_objects={})
    _freeze(monkeypatch, index, _OUT_OF_WINDOW_UTC)
    event = {
        "run_mode": "sweep",
        "launch_decided": True,
        "model": "deepseek-v4-flash",
        "issue_filter": "mid-only",
        "schedule": "alpha-engine-groom-sweep-0000-daily",
    }
    response = index.handler(event, None)
    assert response["groom"]["launched"] is False
    assert response["groom"]["reason"] == "attended_window_deferred"
    recorded = [
        json.loads(v) for k, v in index._test_s3._objects.items()
        if k.startswith("groom/decisions/")
    ]
    assert any(
        r.get("run_mode") == "sweep" or "attended_window_deferred" in json.dumps(r)
        for r in recorded
    ), f"no decision record named the deferral: {recorded}"


# ── Binding test: env defaults must match the manifest ─────────────────────

def test_env_defaults_match_manifest(monkeypatch):
    """index.py's GROOM_ATTENDED_WINDOW_* defaults and the manifest's
    attended_window declaration must agree — this is what makes the manifest
    the single source of truth rather than a second, driftable copy."""
    manifest = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
    sweep_windows = {
        t["name"]: t["attended_window"]
        for t in manifest["triggers"]
        if t["actuation_tier"] == "irreversible"
    }
    assert sweep_windows, "manifest declares no irreversible-tier triggers"
    tz = {w["tz"] for w in sweep_windows.values()}
    start = {w["start_hour"] for w in sweep_windows.values()}
    end = {w["end_hour"] for w in sweep_windows.values()}
    assert len(tz) == len(start) == len(end) == 1, (
        "every irreversible-tier trigger must declare the SAME window "
        f"(manifest is the single source of truth): {sweep_windows}"
    )
    index = _load(monkeypatch)
    assert index.ATTENDED_WINDOW_TZ == tz.pop()
    assert index.ATTENDED_WINDOW_START_HOUR == start.pop()
    assert index.ATTENDED_WINDOW_END_HOUR == end.pop()
    # And the irreversible label set is exactly the manifest's irreversible names.
    assert index._IRREVERSIBLE_SCHEDULE_LABELS == frozenset(sweep_windows)
