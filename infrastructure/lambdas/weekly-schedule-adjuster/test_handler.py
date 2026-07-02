"""Unit tests for weekly-schedule-adjuster index.handler.

The NYSE calendar is VENDORED into index.py (pure Python), so these tests run
directly against it — no lib stub. A fake EventBridge client captures the
enable/disable/one-shot actions without AWS. ``test_vendored_holidays_match_lib``
drift-guards the vendored set against the canonical ``nousergon_lib`` (skipped
where the lib isn't importable).
"""

from __future__ import annotations

import sys
from datetime import date, timedelta
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent))
import index  # noqa: E402


class _FakeExc(Exception):
    pass


class _FakeEvents:
    """Captures EventBridge calls; models alpha-engine-saturday + one-shots."""

    class exceptions:  # noqa: N801 — mirror boto3 client.exceptions
        ResourceNotFoundException = _FakeExc

    def __init__(self, saturday_state="ENABLED", existing_oneshots=None):
        self.saturday_state = saturday_state
        self.rules = dict(existing_oneshots or {})  # name -> {"targets": [...]}
        self.calls: list[tuple] = []

    def describe_rule(self, Name):
        self.calls.append(("describe", Name))
        if Name == "alpha-engine-saturday":
            return {"State": self.saturday_state}
        if Name in self.rules:
            return {"State": "ENABLED"}
        raise self.exceptions.ResourceNotFoundException(Name)

    def enable_rule(self, Name):
        self.calls.append(("enable", Name)); self.saturday_state = "ENABLED"

    def disable_rule(self, Name):
        self.calls.append(("disable", Name)); self.saturday_state = "DISABLED"

    def put_rule(self, Name, ScheduleExpression, State, Description):
        self.calls.append(("put_rule", Name, ScheduleExpression))
        self.rules.setdefault(Name, {"targets": []})

    def put_targets(self, Rule, Targets):
        self.calls.append(("put_targets", Rule, Targets[0]["Arn"], Targets[0]["RoleArn"]))
        self.rules.setdefault(Rule, {})["targets"] = Targets

    def list_targets_by_rule(self, Rule):
        return {"Targets": self.rules.get(Rule, {}).get("targets", [])}

    def remove_targets(self, Rule, Ids):
        self.calls.append(("remove_targets", Rule))

    def delete_rule(self, Name):
        self.calls.append(("delete_rule", Name)); self.rules.pop(Name, None)

    def get_paginator(self, _op):
        rules = [{"Name": n} for n in self.rules if n.startswith(index.ONESHOT_PREFIX)]
        fake = self

        class _P:
            def paginate(self, NamePrefix):
                return [{"Rules": [r for r in rules if r["Name"].startswith(NamePrefix)]}]

        return _P()


@pytest.fixture
def patch_client(monkeypatch):
    holder = {}

    def _factory(state="ENABLED", oneshots=None):
        holder["c"] = _FakeEvents(state, oneshots)
        monkeypatch.setattr(index.boto3, "client", lambda *a, **k: holder["c"])
        return holder["c"]

    return _factory


def _ev(d: date) -> dict:
    return {"time": f"{d.isoformat()}T06:00:00Z"}


# --- calendar ---------------------------------------------------------------

def test_weekly_run_day_normal_week_is_saturday():
    # Wed 2026-07-08 -> last trading Fri 7/10 -> run Sat 7/11
    assert index.weekly_run_day(date(2026, 7, 8)) == date(2026, 7, 11)


def test_weekly_run_day_july4_week_is_friday():
    # Wed 2026-07-01 -> Fri 7/3 holiday, last trading Thu 7/2 -> run Fri 7/3
    assert index.weekly_run_day(date(2026, 7, 1)) == date(2026, 7, 3)


def test_weekly_run_day_good_friday_and_christmas():
    assert index.weekly_run_day(date(2026, 3, 31)) == date(2026, 4, 3)   # Good Friday
    assert index.weekly_run_day(date(2026, 12, 23)) == date(2026, 12, 25)  # Christmas Fri


# --- normal week ------------------------------------------------------------

def test_normal_week_enables_saturday_no_oneshot(patch_client):
    c = patch_client(state="ENABLED")
    out = index.handler(_ev(date(2026, 7, 8)), None)
    assert out["acted"] == "normal"
    assert not any(k == "put_rule" for k in (call[0] for call in c.calls))
    assert not any(k == "disable" for k in (call[0] for call in c.calls))


def test_normal_week_heals_prior_disable(patch_client):
    c = patch_client(state="DISABLED")  # left disabled by a prior holiday week
    out = index.handler(_ev(date(2026, 7, 8)), None)
    assert out["saturday"] == "enabled"
    assert ("enable", "alpha-engine-saturday") in c.calls


# --- holiday week -----------------------------------------------------------

def test_holiday_week_creates_oneshot_then_disables_saturday(patch_client):
    c = patch_client(state="ENABLED")
    out = index.handler(_ev(date(2026, 7, 1)), None)
    assert out["acted"] == "holiday_shift"
    assert out["oneshot"] == "alpha-engine-weekly-oneshot-20260703"
    kinds = [call[0] for call in c.calls]
    # one-shot rule + target created BEFORE the Saturday disable (fail-safe order)
    assert kinds.index("put_rule") < kinds.index("disable")
    assert kinds.index("put_targets") < kinds.index("disable")
    # one-shot targets the weekly SF via the sfn target role
    pt = next(call for call in c.calls if call[0] == "put_targets")
    assert pt[2].endswith("ne-weekly-freshness-pipeline")
    assert pt[3].endswith("alpha-engine-eventbridge-sfn-role")


def test_holiday_week_idempotent(patch_client):
    # already adjusted: Saturday disabled + one-shot present -> no state churn
    c = patch_client(state="DISABLED", oneshots={"alpha-engine-weekly-oneshot-20260703": {"targets": []}})
    out = index.handler(_ev(date(2026, 7, 1)), None)
    assert out["acted"] == "holiday_shift"
    assert out["saturday"] == "already_disabled"  # no redundant disable call


def test_normal_week_reaps_stale_oneshot(patch_client):
    c = patch_client(state="ENABLED", oneshots={"alpha-engine-weekly-oneshot-20260703": {"targets": [{"Id": "x"}]}})
    out = index.handler(_ev(date(2026, 7, 8)), None)  # after 7/3
    assert "alpha-engine-weekly-oneshot-20260703" in out["reaped"]
    assert ("delete_rule", "alpha-engine-weekly-oneshot-20260703") in c.calls


def test_weekly_input_shape_matches_cron():
    inp = index._weekly_input()
    assert '"pipeline_role": "weekly"' in inp
    assert '"ec2_instance_id": ["i-09b539c844515d549"]' in inp


# --- drift guard: vendored calendar must match the canonical lib -------------

def test_vendored_holidays_match_lib():
    tc = pytest.importorskip("nousergon_lib.trading_calendar")
    lib_hol = {d for d in tc.NYSE_HOLIDAYS if 2026 <= d.year <= 2030}
    assert index._NYSE_HOLIDAYS == lib_hol, "vendored NYSE holidays diverged from nousergon_lib"
    # and the session predicate agrees day-for-day across the vendored range
    d = date(2026, 1, 1)
    while d <= date(2030, 12, 31):
        assert index.is_trading_day(d) == tc.is_trading_day(d), d.isoformat()
        d += timedelta(days=1)
