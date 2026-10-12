"""The trading box is stopped only by an execution that started it, and never
by one started between the preopen pipeline's start and the close.

Incident, 2026-10-09. A manual replay of ``ne-postclose-reconcile-pipeline``
(``eod-heal-replay-2026-10-08-manual-1``) started at 13:27:21Z, 09:27 ET. The
preopen pipeline had started the trading box at 12:15Z and finished at 13:22Z.
``MarketHoursGate`` let the replay through, because the gate Lambda refuses
only [09:30, 16:00) ET. The replay's success path,
``EODReconcile -> DrainTraderReconcile -> StopTradingInstance``, then stopped
the box. It stayed down the whole session: the v2 trader's 10:45 ET session
never ran and the v1 intraday daemon never ticked.

Two independent fixes, each pinned here:

1. **Pre-session window** (both box-stopping machines: the post-close and the
   reconcile pipelines). After a market-closed verdict, a start in
   [08:00, 09:30) ET is refused. The preopen pipeline starts at 08:15 ET
   (``alpha-engine-weekday``, ``cron(15 5)`` America/Los_Angeles). The window
   is evaluated in UTC as [12:00, 14:30), the union of the ET window under EDT
   and EST, so it needs no Lambda and no DST table. ``TestPreSessionWindow``
   checks it minute by minute against ``zoneinfo`` on a summer and a winter
   date, so a change that opens a gap, or that starts blocking a legitimate
   evening start, fails here.

2. **Box ownership** (reconcile machine). ``ec2:StartInstances`` returns the
   state it found the box in. If the box was ``running`` or ``pending``,
   another actor owns it and this execution never stops it, on the success
   route (``CheckBoxOwnedBeforeStop``) or the failure route
   (``CheckBoxOwnedBeforeForceStop``). A heal replay inherits ownership from
   the execution that dispatched it (``box_owned_by_dispatcher``). That keeps
   nousergon-data#2127's hand-off working: the replay's own start always
   finds the box running, so without the inherited flag nothing would stop
   the box. The post-close machine has no ownership check: its stop at the
   close is the ruled stop of the box the preopen pipeline started (Brian,
   2026-10-01), so the window in (1) is its guard.

The Choice rules are executed against real payload shapes by a minimal ASL
evaluator. A rule using an operator the evaluator does not implement raises,
so an unknown operator can never pass as a silent false.
"""

from __future__ import annotations

import json
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

REPO = Path(__file__).resolve().parents[1]
INFRA = REPO / "infrastructure"
POSTCLOSE = "step_function_eod.json"
RECONCILE = "step_function_eod_reconcile.json"
BOX_STOPPERS = [POSTCLOSE, RECONCILE]
ET = ZoneInfo("America/New_York")

PRE_SESSION_STATES = {"StampStartClockUtc", "PreSessionWindowChoice", "NotifyPreSessionBlocked"}
OWNERSHIP_STATES = {
    "ResolveBoxOwnership", "StampBoxOwned", "StampBoxNotOwned",
    "CheckBoxOwnedBeforeStop", "NotifyStopSkippedNotBoxOwner",
    "CheckBoxOwnedBeforeForceStop",
}


def _sf(name: str) -> dict:
    return json.loads((INFRA / name).read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def defs() -> dict:
    return {name: _sf(name) for name in BOX_STOPPERS}


@pytest.fixture(scope="module")
def rec() -> dict:
    return _sf(RECONCILE)["States"]


# ── a minimal ASL Choice evaluator ───────────────────────────────────────────

_UNSET = object()


class StatesRuntime(Exception):
    """A Choice compared a path that does not exist: ASL raises, it does not
    fall through to Default."""


def _resolve(path: str, doc):
    assert path.startswith("$.")
    cur = doc
    for raw in path[2:].split("."):
        key, idx = raw, None
        if raw.endswith("]"):
            key, idx = raw[:-1].split("[")
            idx = int(idx)
        if not isinstance(cur, dict) or key not in cur:
            return _UNSET
        cur = cur[key]
        if idx is not None:
            if not isinstance(cur, list) or idx >= len(cur):
                return _UNSET
            cur = cur[idx]
    return cur


def _matches(rule: dict, doc) -> bool:
    if "And" in rule:
        return all(_matches(r, doc) for r in rule["And"])
    if "Or" in rule:
        return any(_matches(r, doc) for r in rule["Or"])
    if "Not" in rule:
        return not _matches(rule["Not"], doc)
    value = _resolve(rule["Variable"], doc)
    ops = [k for k in rule if k not in ("Variable", "Next", "Comment")]
    assert len(ops) == 1, rule
    op = ops[0]
    if op == "IsPresent":
        return (value is not _UNSET) == rule[op]
    if value is _UNSET:
        raise StatesRuntime(rule["Variable"])
    if op == "IsBoolean":
        return isinstance(value, bool) == rule[op]
    if op == "BooleanEquals":
        return isinstance(value, bool) and value == rule[op]
    if op == "StringEquals":
        return isinstance(value, str) and value == rule[op]
    if op == "StringGreaterThanEquals":
        return isinstance(value, str) and value >= rule[op]
    if op == "StringLessThan":
        return isinstance(value, str) and value < rule[op]
    raise NotImplementedError(f"{op}: implement it rather than evaluating a silent false")


def evaluate(choice: dict, doc: dict) -> str:
    assert choice["Type"] == "Choice"
    for rule in choice["Choices"]:
        if _matches(rule, doc):
            return rule["Next"]
    return choice["Default"]


def _targets(st: dict) -> list[str]:
    out = [st[k] for k in ("Next", "Default") if k in st]
    out += [c["Next"] for c in st.get("Choices", [])]
    out += [c["Next"] for c in st.get("Catch", [])]
    return out


def _reachable(states: dict, start: str, blocked: frozenset = frozenset()) -> set[str]:
    seen, todo = set(), [start]
    while todo:
        n = todo.pop()
        if n in seen or n in blocked:
            continue
        seen.add(n)
        todo.extend(_targets(states[n]))
    return seen


# ── (1) the pre-session window ───────────────────────────────────────────────

_HHMM_EXPR = (
    "States.Format('{}{}', "
    "States.ArrayGetItem(States.StringSplit(States.ArrayGetItem(States.StringSplit($$.Execution.StartTime, 'T'), 1), ':'), 0), "
    "States.ArrayGetItem(States.StringSplit($$.Execution.StartTime, ':'), 1))"
)


def _string_split(s: str, delims: str) -> list[str]:
    """States.StringSplit: every character of the delimiter splits; empty
    pieces are dropped."""
    out, cur = [], ""
    for ch in s:
        if ch in delims:
            if cur:
                out.append(cur)
            cur = ""
        else:
            cur += ch
    if cur:
        out.append(cur)
    return out


def _stamp_hhmm(start_time: str) -> str:
    """_HHMM_EXPR, evaluated the way Step Functions evaluates it."""
    after_t = _string_split(start_time, "T")[1]
    return _string_split(after_t, ":")[0] + _string_split(start_time, ":")[1]


def _sfn_start_time(dt_utc: datetime) -> str:
    """$$.Execution.StartTime's shape: ISO-8601 UTC, millisecond precision."""
    return dt_utc.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt_utc.microsecond // 1000:03d}Z"


def _window_verdict(states: dict, dt_utc: datetime) -> str:
    st = states["StampStartClockUtc"]
    doc = {"start_clock_utc": {"hhmm": _stamp_hhmm(_sfn_start_time(dt_utc))}}
    assert st["Next"] == "PreSessionWindowChoice"
    return evaluate(states["PreSessionWindowChoice"], doc)


class TestPreSessionWindow:
    @pytest.mark.parametrize("name", BOX_STOPPERS)
    def test_the_clock_stamp_is_exactly_the_evaluated_expression(self, defs, name):
        st = defs[name]["States"]["StampStartClockUtc"]
        assert st["Type"] == "Pass"
        assert st["ResultPath"] == "$.start_clock_utc"
        assert st["Parameters"] == {"start_time.$": "$$.Execution.StartTime", "hhmm.$": _HHMM_EXPR}

    @pytest.mark.parametrize("start,hhmm", [
        ("2026-10-09T13:27:21.123Z", "1327"),
        ("2026-12-10T09:05:00.000Z", "0905"),
        ("2026-12-10T00:00:00Z", "0000"),
    ])
    def test_the_stamp_is_zero_padded_hhmm(self, start, hhmm):
        assert _stamp_hhmm(start) == hhmm

    @pytest.mark.parametrize("name", BOX_STOPPERS)
    def test_the_2026_10_09_replay_is_refused(self, defs, name):
        states = defs[name]["States"]
        incident = datetime(2026, 10, 9, 13, 27, 21, 123000, tzinfo=timezone.utc)
        assert _window_verdict(states, incident) == "NotifyPreSessionBlocked"

    @pytest.mark.parametrize("name", BOX_STOPPERS)
    @pytest.mark.parametrize("day", [date(2026, 10, 9), date(2026, 12, 10)], ids=["EDT", "EST"])
    def test_every_minute_from_0800_to_0930_et_is_refused(self, defs, name, day):
        states = defs[name]["States"]
        t = datetime.combine(day, time(8, 0), tzinfo=ET)
        while t.timetz().replace(tzinfo=None) < time(9, 30):
            got = _window_verdict(states, t.astimezone(timezone.utc))
            assert got == "NotifyPreSessionBlocked", t.isoformat()
            t += timedelta(minutes=1)

    @pytest.mark.parametrize("name", BOX_STOPPERS)
    @pytest.mark.parametrize("day", [date(2026, 10, 9), date(2026, 12, 10)], ids=["EDT", "EST"])
    def test_no_evening_or_overnight_start_is_refused(self, defs, name, day):
        """Every start these machines legitimately get is after the close: the
        daemon-shutdown start at 16:00 ET (13:00 ET on an early close), the
        collection's terminal event 19:00-21:00 ET, the reconcile backstop at
        02:15 UTC. Minute by minute from 13:00 ET to 07:00 ET the next day,
        none of them is refused by this window. [13:00, 16:00) belongs to the
        gate Lambda, which refuses it in-session and knows the early closes."""
        states = defs[name]["States"]
        t = datetime.combine(day, time(13, 0), tzinfo=ET)
        end = datetime.combine(day + timedelta(days=1), time(7, 0), tzinfo=ET)
        while t < end:
            got = _window_verdict(states, t.astimezone(timezone.utc))
            assert got == "CheckMutexRole", t.isoformat()
            t += timedelta(minutes=1)

    @pytest.mark.parametrize("name", BOX_STOPPERS)
    def test_a_closed_market_verdict_passes_through_the_window(self, defs, name):
        states = defs[name]["States"]
        proceed = [c for c in states["MarketHoursGateChoice"]["Choices"]
                   if c.get("StringEquals") == "PROCEED"]
        assert [c["Next"] for c in proceed] == ["StampStartClockUtc"]

    @pytest.mark.parametrize("name", BOX_STOPPERS)
    def test_an_unverified_gate_still_gets_the_window(self, defs, name):
        """The window is pure clock arithmetic, so a gate Lambda outage does
        not open it."""
        st = defs[name]["States"]["NotifyMarketHoursUnverified"]
        assert st["Next"] == "StampStartClockUtc"
        assert [c["Next"] for c in st["Catch"]] == ["StampStartClockUtc"]

    @pytest.mark.parametrize("name", BOX_STOPPERS)
    def test_a_refusal_is_announced_then_fails(self, defs, name):
        states = defs[name]["States"]
        notify = states["NotifyPreSessionBlocked"]
        assert notify["Resource"] == "arn:aws:states:::sns:publish"
        assert notify["Next"] == "MarketHoursBlocked"
        assert [c["Next"] for c in notify["Catch"]] == ["MarketHoursBlocked"]
        assert states["MarketHoursBlocked"]["Type"] == "Fail"

    @pytest.mark.parametrize("name", BOX_STOPPERS)
    def test_nothing_that_spends_runs_before_the_window(self, defs, name):
        """The window is decided before the mutex and before the box start."""
        states = defs[name]["States"]
        before = _reachable(states, defs[name]["StartAt"],
                            blocked=frozenset({"PreSessionWindowChoice", "RecordMarketHoursOverride"}))
        assert not {"CheckMutexRole", "AcquireMutex", "StartTradingInstance"} & before


# ── (2) box ownership (reconcile machine) ────────────────────────────────────


def _start_result(previous: str | None) -> dict:
    """ec2:StartInstances' response shape through the SDK integration."""
    inst = {"InstanceId": "i-018eb3307a21329bf", "CurrentState": {"Code": 0, "Name": "pending"}}
    if previous is not None:
        inst["PreviousState"] = {"Code": 80, "Name": previous}
    return {"ec2_start_result": {"StartingInstances": [inst]}}


def _owner_stamp(rec: dict, doc: dict) -> bool:
    stamp = rec[evaluate(rec["ResolveBoxOwnership"], doc)]
    assert stamp["Type"] == "Pass"
    assert stamp["ResultPath"] == "$.box_ownership"
    assert stamp["Next"] == "WaitForInstanceReady"
    return stamp["Result"]["owned"]


class TestBoxOwnership:
    def test_ownership_is_read_off_the_start_itself(self, rec):
        start = rec["StartTradingInstance"]
        assert start["Resource"] == "arn:aws:states:::aws-sdk:ec2:startInstances"
        assert start["ResultPath"] == "$.ec2_start_result"
        assert start["Next"] == "ResolveBoxOwnership"
        assert {rec["StampBoxOwned"]["Result"]["owned"], rec["StampBoxNotOwned"]["Result"]["owned"]} == {True, False}

    @pytest.mark.parametrize("previous,owned", [
        ("stopped", True),
        ("stopping", True),
        ("running", False),   # 2026-10-09: the preopen pipeline's box
        ("pending", False),
        (None, False),        # unreadable: never stop on a guess
    ])
    def test_the_previous_state_decides(self, rec, previous, owned):
        assert _owner_stamp(rec, _start_result(previous)) is owned

    def test_the_2026_10_09_replay_did_not_own_the_box(self, rec):
        doc = {"pipeline_role": "operator-replay", "skip_post_market_data": True,
               **_start_result("running")}
        assert _owner_stamp(rec, doc) is False

    @pytest.mark.parametrize("inherited", [True, False])
    def test_a_heal_replay_inherits_its_dispatchers_ownership(self, rec, inherited):
        doc = {"pipeline_role": "operator-replay", "box_owned_by_dispatcher": inherited,
               **_start_result("running")}
        assert _owner_stamp(rec, doc) is inherited

    @pytest.mark.parametrize("role", [None, "eod", "operator"])
    def test_inheritance_needs_the_replay_role(self, rec, role):
        doc = {"box_owned_by_dispatcher": True, **_start_result("running")}
        if role is not None:
            doc["pipeline_role"] = role
        assert _owner_stamp(rec, doc) is False

    def test_a_non_boolean_inherited_flag_is_ignored(self, rec):
        doc = {"pipeline_role": "operator-replay", "box_owned_by_dispatcher": "true",
               **_start_result("running")}
        assert _owner_stamp(rec, doc) is False

    def test_the_dispatcher_hands_its_ownership_to_the_replay(self, rec):
        replay_input = rec["HealDispatchReplay"]["Parameters"]["Input"]
        assert replay_input["pipeline_role"] == "operator-replay"
        assert replay_input["box_owned_by_dispatcher.$"] == "$.box_ownership.owned"
        # The dispatch is reachable only after the ownership stamp.
        before = _reachable(rec, _sf(RECONCILE)["StartAt"],
                            blocked=frozenset({"StampBoxOwned", "StampBoxNotOwned"}))
        assert "HealDispatchReplay" not in before

    @pytest.mark.parametrize("guard,owned_next,skip_next", [
        ("CheckBoxOwnedBeforeStop", "DrainTraderReconcile", "NotifyStopSkippedNotBoxOwner"),
        ("CheckBoxOwnedBeforeForceStop", "ForceStopInstance", "FailExecution"),
    ])
    def test_the_guards_stop_only_an_owned_box(self, rec, guard, owned_next, skip_next):
        choice = rec[guard]
        assert evaluate(choice, {"box_ownership": {"owned": True}}) == owned_next
        assert evaluate(choice, {"box_ownership": {"owned": False}}) == skip_next
        # A failure before the start never stamped ownership: no stop.
        assert evaluate(choice, {}) == skip_next

    def test_every_route_into_a_stop_passes_an_ownership_guard(self, rec):
        assert {n for n, st in rec.items() if "DrainTraderReconcile" in _targets(st)} == {
            "CheckBoxOwnedBeforeStop"}
        assert {n for n, st in rec.items() if "ForceStopInstance" in _targets(st)} == {
            "CheckBoxOwnedBeforeForceStop"}
        start = _sf(RECONCILE)["StartAt"]
        unguarded = _reachable(rec, start, blocked=frozenset(
            {"CheckBoxOwnedBeforeStop", "CheckBoxOwnedBeforeForceStop"}))
        assert not {"DrainTraderReconcile", "StopTradingInstance", "ForceStopInstance"} & unguarded

    def test_the_work_paths_all_enter_the_guard(self, rec):
        feeders = {n for n, st in rec.items() if "CheckBoxOwnedBeforeStop" in _targets(st)}
        assert feeders == {"CheckEODStatus", "CheckSkipEODReconcile",
                           "HealReplayDispatchFailed", "HealNonConvergent"}
        assert {n for n, st in rec.items() if "CheckBoxOwnedBeforeForceStop" in _targets(st)} == {
            "HandleFailure"}

    def test_a_skipped_stop_never_stops_and_still_terminates(self, rec):
        skip = rec["NotifyStopSkippedNotBoxOwner"]
        assert skip["Resource"] == "arn:aws:states:::sns:publish"
        assert skip["Next"] == "ReadExerciseCadence"
        assert [c["Next"] for c in skip["Catch"]] == ["ReadExerciseCadence"]
        after = _reachable(rec, "NotifyStopSkippedNotBoxOwner")
        assert not {"DrainTraderReconcile", "StopTradingInstance", "ForceStopInstance"} & after
        assert {"NormalSucceeded", "DegradedRun"} <= after

    def test_no_new_ec2_api_is_called(self, rec):
        """Ownership costs no IAM change: the SF role's only EC2 grants are
        StartInstances and StopInstances on the trading instance."""
        ec2 = {st["Resource"] for st in rec.values() if ":ec2:" in st.get("Resource", "")}
        assert ec2 == {"arn:aws:states:::aws-sdk:ec2:startInstances",
                       "arn:aws:states:::aws-sdk:ec2:stopInstances"}


def test_the_post_close_machine_carries_the_window_but_not_ownership(defs):
    """Its stop at the close is the ruled stop of the box the preopen pipeline
    started, so an ownership check there would never stop it. The window is
    its guard against an in-day start."""
    states = set(defs[POSTCLOSE]["States"])
    assert PRE_SESSION_STATES <= states
    assert not OWNERSHIP_STATES & states
    assert PRE_SESSION_STATES | OWNERSHIP_STATES <= set(defs[RECONCILE]["States"])
