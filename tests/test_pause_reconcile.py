#!/usr/bin/env python3
"""The pause manifest, the observability registry and live AWS must agree.

`automation_pause.py` reconciles the manifest against live AWS **for the names
the manifest happens to list** — its coverage is hand-listed, which is
`observability-policy.md` §2.2's defect one level up. And nothing reconciled the
registry's `lifecycle: disabled` declarations, which are hand-copied assertions
about a file in another repository, against the manifest they quote.
`pause_reconcile.py` closes both (alpha-engine-config-I7118).

Every finding class below is exercised by INDUCING the condition, per §9's
commissioning standard — a detector nobody has watched fail is not a detector.
The three registers are injected, so the whole comparison runs with no AWS and
no network; the live wiring is asserted separately (`test_ci_runs_the_reconcile`).
"""

from __future__ import annotations

import datetime
import importlib.util
import json
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
INFRA = REPO_ROOT / "infrastructure"
MODULE_PATH = INFRA / "pause_reconcile.py"
MANIFEST_PATH = INFRA / "automation_pause.json"
WORKFLOW = REPO_ROOT / ".github" / "workflows" / "pause-reconcile.yml"


def _load():
    spec = importlib.util.spec_from_file_location("pause_reconcile", MODULE_PATH)
    mod = importlib.util.module_from_spec(spec)
    sys.modules["pause_reconcile"] = mod
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="module")
def mod():
    return _load()


@pytest.fixture(scope="module")
def manifest() -> dict:
    return json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))


def _row(cid: str, lifecycle: str = "in-service", **kw) -> dict:
    row = {"component_id": cid, "lifecycle": lifecycle, "substrate": "eventbridge"}
    row.update(kw)
    return row


def _manifest(paused: dict | None = None, kept: dict | None = None) -> dict:
    return {
        "ruling": {"by": "Brian", "date": "2026-08-07", "statement": "x"},
        "not_paused": kept or {},
        "pending": {},
        "paused": {"events_rules": paused or {}, "scheduler_schedules": {}},
    }


def _reconcile(mod, *, manifest, rows, triggers, targets=None, sf=None, ran=None,
                alarm_actions_of=None, alarm_breaching_of=None):
    return mod.reconcile(
        manifest=manifest, rows=rows, triggers=triggers,
        targets_of=(lambda t: (targets or {}).get(t["name"], [])),
        sf_invoked=sf or set(),
        invocations_of=(lambda cid: (ran or {}).get(cid, 0)),
        alarm_actions_of=alarm_actions_of,
        # alpha-engine-config-I8712: every alarm-direction test below predates
        # the notBreaching distinction and asserts the `breaching` behaviour,
        # so default it True here rather than touch each call site — a test
        # exercising the notBreaching skip passes its own `alarm_breaching_of`.
        alarm_breaching_of=alarm_breaching_of if alarm_breaching_of is not None
        else (lambda name: True),
    )


# ── the finding classes, each induced ────────────────────────────────────────

def test_undeclared_dark_fires_when_no_register_accounts_for_a_dark_trigger(mod):
    """A trigger off with nobody's decision behind it is §8.3's forbidden collapse."""
    findings = _reconcile(
        mod, manifest=_manifest(), rows={"other": _row("other")},
        triggers=[{"surface": "events", "name": "ghost", "state": "DISABLED"}],
    )
    assert [(f["kind"], f["trigger"]) for f in findings] == [("undeclared-dark", "ghost")]


def test_a_dark_trigger_is_declared_by_EITHER_register(mod):
    """The union is the declaration surface — the manifest OR the registry.

    Measured 2026-08-12: eight live rules (`ae-executor-start`, `ae-predictor-*`,
    `alpha-research-daily`, …) are DISABLED and named nowhere in the manifest,
    and all eight already carry `lifecycle: deprecated` in the registry. A
    reconciler that read only the manifest would have demanded eight new manifest
    entries, back-dating a 2026-08-07 ruling over decisions that predate it.
    """
    dark = [{"surface": "events", "name": "legacy", "state": "DISABLED"}]
    by_registry = _reconcile(
        mod, manifest=_manifest(), rows={"legacy": _row("legacy", "deprecated")},
        triggers=dark)
    by_manifest = _reconcile(
        mod, manifest=_manifest(paused={"legacy": "ruled off"}), rows={}, triggers=dark)
    assert by_registry == [] and by_manifest == []


def test_running_while_declared_off_fires_on_a_live_trigger_behind_a_disabled_row(mod):
    """I7118 Finding 2, induced: the component runs again, the row still says off."""
    findings = _reconcile(
        mod, manifest=_manifest(), rows={"probe": _row("probe", "disabled")},
        triggers=[{"surface": "events", "name": "probe", "state": "ENABLED"}],
    )
    assert [(f["kind"], f["trigger"]) for f in findings] == \
        [("running-while-declared-off", "probe")]


def test_orphaned_lifecycle_declaration_fires_when_the_quoted_entry_is_gone(mod):
    """The row quotes automation_pause.json; the manifest no longer names it."""
    findings = _reconcile(
        mod, manifest=_manifest(),
        rows={"tap": _row("tap", "disabled",
                          lifecycle_reason="paused per automation_pause.json")},
        triggers=[{"surface": "events", "name": "tap", "state": "DISABLED"}],
    )
    kinds = {f["kind"] for f in findings}
    assert "orphaned-lifecycle-declaration" in kinds, findings


def test_orphan_is_silent_while_the_entry_still_exists(mod):
    findings = _reconcile(
        mod, manifest=_manifest(paused={"tap": "ruled off 2026-08-07"}),
        rows={"tap": _row("tap", "disabled",
                          lifecycle_reason="paused per automation_pause.json")},
        triggers=[{"surface": "events", "name": "tap", "state": "DISABLED"}],
    )
    assert findings == []


def test_undeclared_enabled_fires_on_scheduled_work_no_register_names(mod):
    """Found live: `alpha-research-openrouter-shadow-weekly`, ENABLED under the
    pause, in neither block and carrying no registry row."""
    findings = _reconcile(
        mod, manifest=_manifest(), rows={},
        triggers=[{"surface": "events", "name": "stowaway", "state": "ENABLED"}],
    )
    assert [(f["kind"], f["trigger"]) for f in findings] == \
        [("undeclared-enabled", "stowaway")]


def test_dark_and_undeclared_component_fires_for_a_target_behind_dark_triggers(mod):
    """alpha-engine-config-I7117's rule, derived rather than hand-listed."""
    findings = _reconcile(
        mod, manifest=_manifest(paused={"tick": "ruled off"}),
        rows={"tick": _row("tick", "deprecated"),
              "worker": _row("worker", "in-service", substrate="lambda")},
        triggers=[{"surface": "events", "name": "tick", "state": "DISABLED"}],
        targets={"tick": ["arn:aws:lambda:us-east-1:1:function:worker"]},
        ran={"worker": 0},
    )
    assert [(f["kind"], f["trigger"]) for f in findings] == \
        [("dark-and-undeclared-component", "worker")]


def test_enabled_trigger_silent_target_fires_when_the_live_target_never_runs(mod):
    """alpha-engine-config-I9469: an ENABLED schedule whose live target has zero
    invocations must be a finding — the schedule firing is not evidence the
    downstream work happened."""
    findings = _reconcile(
        mod, manifest=_manifest(),
        rows={"daily": _row("daily"), "worker": _row("worker", "in-service", substrate="lambda")},
        triggers=[{"surface": "events", "name": "daily", "state": "ENABLED"}],
        targets={"daily": ["arn:aws:lambda:us-east-1:1:function:worker"]},
        ran={"worker": 0},
    )
    assert [(f["kind"], f["trigger"]) for f in findings] == \
        [("enabled-trigger-silent-target", "daily")]


def test_enabled_trigger_silent_target_does_not_reproduce_the_i9469_false_positive(mod):
    """The original 9469 audit flagged `alpha-engine-research-thinktank` (0
    invocations) off name-similarity to `alpha-research-thinktank-daily`. This
    direction reads the LIVE target map instead: the rule's real target is the
    dispatcher, which is invoked, so no finding — the retired Lambda is never
    even consulted because it is not a live target of anything."""
    findings = _reconcile(
        mod, manifest=_manifest(),
        rows={"alpha-research-thinktank-daily": _row("alpha-research-thinktank-daily"),
              "alpha-engine-thinktank-spot-dispatcher":
              _row("alpha-engine-thinktank-spot-dispatcher", "in-service", substrate="lambda")},
        triggers=[{"surface": "events", "name": "alpha-research-thinktank-daily", "state": "ENABLED"}],
        targets={"alpha-research-thinktank-daily":
                 ["arn:aws:lambda:us-east-1:1:function:alpha-engine-thinktank-spot-dispatcher"]},
        ran={"alpha-engine-thinktank-spot-dispatcher": 10},
    )
    assert findings == []


def test_enabled_trigger_silent_target_skips_a_target_declared_off(mod):
    """The target's own row already says it is not expected to run — that is
    direction A/C's territory, not this one's."""
    findings = _reconcile(
        mod, manifest=_manifest(),
        rows={"daily": _row("daily"), "worker": _row("worker", "retired", substrate="lambda")},
        triggers=[{"surface": "events", "name": "daily", "state": "ENABLED"}],
        targets={"daily": ["arn:aws:lambda:us-east-1:1:function:worker"]},
        ran={"worker": 0},
    )
    assert findings == []


def test_enabled_trigger_silent_target_skips_an_sf_invoked_target(mod):
    """A target reached by a live Step Functions definition has other live
    invocation paths this direction cannot see from the trigger map alone."""
    findings = _reconcile(
        mod, manifest=_manifest(),
        rows={"daily": _row("daily"), "worker": _row("worker", "in-service", substrate="lambda")},
        triggers=[{"surface": "events", "name": "daily", "state": "ENABLED"}],
        targets={"daily": ["arn:aws:lambda:us-east-1:1:function:worker"]},
        ran={"worker": 0}, sf={"worker"},
    )
    assert findings == []


def test_enabled_trigger_silent_target_skips_a_paused_trigger(mod):
    """A trigger the manifest declares paused but that is live ENABLED is
    direction A's `running-while-declared-off` — not duplicated here."""
    findings = _reconcile(
        mod, manifest=_manifest(paused={"daily": "ruled off"}),
        rows={"daily": _row("daily", "deprecated"),
              "worker": _row("worker", "in-service", substrate="lambda")},
        triggers=[{"surface": "events", "name": "daily", "state": "ENABLED"}],
        targets={"daily": ["arn:aws:lambda:us-east-1:1:function:worker"]},
        ran={"worker": 0},
    )
    assert [f["kind"] for f in findings] == ["running-while-declared-off"]


# ── the two guards that keep the detector from writing false decisions ───────

def test_a_step_functions_stage_is_not_reported_dark(mod):
    """Measured 2026-08-12: `alpha-engine-predictor-inference` and
    `alpha-engine-research-runner` have only DEPRECATED rules of their own and
    are invoked by the KEPT weekly and preopen pipelines on every run. Without
    the Step Functions register this reports both as dark, and acting on that
    declares two live Crucible stages deliberately off."""
    args = dict(
        manifest=_manifest(paused={"tick": "ruled off"}),
        rows={"tick": _row("tick", "deprecated"),
              "stage": _row("stage", "in-service", substrate="lambda")},
        triggers=[{"surface": "events", "name": "tick", "state": "DISABLED"}],
        targets={"tick": ["arn:aws:lambda:us-east-1:1:function:stage"]},
        ran={"stage": 0},
    )
    assert _reconcile(mod, sf=set(), **args), "the guard cannot be shown to bind"
    assert _reconcile(mod, sf={"stage"}, **args) == []


def test_invocations_behind_a_declared_off_row_are_not_a_finding(mod):
    """The ruling says "manual invocation only", so a paused component that ran
    by hand is the ruling WORKING. Measured: `alpha-engine-predictor-health-check`
    shows 10 post-ruling invocations at irregular single-call hours with its own
    rule correctly DISABLED — firing on that pages on compliance."""
    findings = _reconcile(
        mod, manifest=_manifest(paused={"tick": "ruled off"}),
        rows={"tick": _row("tick", "deprecated"),
              "worker": _row("worker", "disabled", substrate="lambda")},
        triggers=[{"surface": "events", "name": "tick", "state": "DISABLED"}],
        targets={"tick": ["arn:aws:lambda:us-east-1:1:function:worker"]},
        ran={"worker": 250},
    )
    assert findings == []


def test_the_invocation_window_opens_after_the_ruling_not_14_days_back(mod, manifest):
    """A flat trailing window counts PRE-ruling invocations as evidence against a
    post-ruling declaration. Measured 2026-08-12: that produced eleven
    `running-while-declared-off` findings of which ten were pre-pause traffic —
    `alpha-engine-crypto-balances` scored 853 and has not run once since the
    pause. The bound is read from the manifest, never written as a literal."""
    start = mod.window_start(manifest, days=365)
    ruled = datetime.datetime.fromisoformat(
        manifest["ruling"]["date"]).replace(tzinfo=datetime.timezone.utc)
    assert start > ruled, "a 365-day window must still not reach before the ruling"


# ── the never-mutate boundary, and the wiring ────────────────────────────────

def test_the_module_cannot_write_anything(mod):
    """I7118: a job that edits lifecycle declarations autonomously would be
    inferring a decision from a file diff, which §8.3 forbids."""
    source = MODULE_PATH.read_text(encoding="utf-8")
    for forbidden in ("enable-rule", "disable-rule", "update-schedule",
                      "put-rule", "delete-rule", "write_text", "s3 cp"):
        assert forbidden not in source, (
            f"pause_reconcile.py contains {forbidden!r} — it is a detector, and a "
            "detector that can act can act on a wrong reading")


def test_an_empty_registry_raises_rather_than_reporting_everything_undeclared(mod, tmp_path):
    """Grading against an empty registry would report the whole account as
    undeclared — a check that fails LOUD beats one that invents 195 findings."""
    with pytest.raises(RuntimeError, match="empty"):
        mod.load_registry(tmp_path)


def test_ci_runs_the_reconcile(mod):
    text = WORKFLOW.read_text(encoding="utf-8")
    assert "pause_reconcile.py" in text, (
        "the reconciler is not wired into the daily out-of-band sweep; a detector "
        "that runs nowhere is the gap it was built to close"
    )
    step = text[text.index("pause_reconcile.py") - 600:text.index("pause_reconcile.py") + 200]
    assert "if:" not in step.split("- name:")[-1], (
        "the reconcile step carries an `if:` — the same reachability regression "
        "test_ci_runs_the_pause_check exists to prevent"
    )


def test_the_check_publishes_its_own_console_row(mod):
    """§2.2 — a detector that reports nowhere is unobserved."""
    text = WORKFLOW.read_text(encoding="utf-8")
    assert "--publish" in text, "the reconcile step does not write its console row"
    assert mod.CADENCE_MINUTES == 1440, (
        "the row's cadence must match the sweep that writes it, or the console's "
        "staleness threshold is derived from a cadence nothing meets"
    )


def test_non_service_lifecycles_match_the_registry(mod):
    """The three-state set is duplicated from another repo's
    `observability_registry.py::LIFECYCLE_NEEDS_REASON`. If a fourth is added
    there, a row could declare itself off in a way this module reads as
    in-service, and the reconciler would silently under-report."""
    assert mod.NON_SERVICE_LIFECYCLES == frozenset(
        {"disabled", "deprecated", "retired"})


# ── alpha-engine-config-I7174: read-only alarm-action grading ────────────────
#
# `pause_reconcile.py` never mutates (test_the_module_cannot_write_anything
# above) — it only GRADES `paused_alarms` against live CloudWatch state,
# mirroring `automation_pause.py`'s own bidirectional check. The mutation
# itself lives only in automation_pause.py's enforce(), consistent with this
# module's documented invariant.

def _manifest_with_alarm(watches: dict[str, str] | None = None,
                          alarm_watches: list[str] | None = None,
                          reason: str = "r",
                          issue: str = "alpha-engine-config-I6984",
                          re_exam: str = "2999-01-01") -> dict:
    """A synthetic manifest with one `paused_alarms` entry.

    `issue`/`re_exam` default to a well-formed, far-future declaration
    (alpha-engine-config-I8047) so a test about the ALARM STATE directions is
    not also asserting the declaration's shape. Overriding either is how the
    declaration direction is proven RED — see
    `test_an_undeclared_owner_is_a_finding_in_the_reconciler` below.
    """
    m = _manifest(paused=watches or {})
    m["paused_alarms"] = {
        "_why": "prose, must be skipped",
        "probe-alarm": {"reason": reason, "watches": alarm_watches or [],
                        "issue": issue, "re_exam": re_exam},
    }
    return m


def test_alarm_unexpectedly_enabled_fires_when_armed_despite_justification(mod):
    m = _manifest_with_alarm(watches={"tick": "ruled off"}, alarm_watches=["tick"])
    findings = _reconcile(
        mod, manifest=m, rows={},
        triggers=[{"surface": "events", "name": "tick", "state": "DISABLED"}],
        alarm_actions_of=lambda name: True,
    )
    assert [(f["kind"], f["trigger"]) for f in findings
            if f["surface"] == "cloudwatch"] == [("alarm-unexpectedly-enabled", "probe-alarm")]


def test_alarm_stale_disabled_fires_after_its_watched_trigger_is_unpaused(mod):
    """The re-arm property, read-only: the trigger left `paused` (un-paused),
    the alarm is still silenced — check must flag it."""
    m = _manifest_with_alarm(watches={}, alarm_watches=["tick"])  # tick no longer paused
    findings = _reconcile(
        mod, manifest=m, rows={},
        triggers=[{"surface": "events", "name": "tick", "state": "ENABLED"}],
        alarm_actions_of=lambda name: False,
    )
    assert [(f["kind"], f["trigger"]) for f in findings
            if f["surface"] == "cloudwatch"] == [("alarm-stale-disabled", "probe-alarm")]


def test_alarm_grading_is_silent_when_state_matches_justification(mod):
    m = _manifest_with_alarm(watches={"tick": "ruled off"}, alarm_watches=["tick"])
    findings = _reconcile(
        mod, manifest=m, rows={},
        triggers=[{"surface": "events", "name": "tick", "state": "DISABLED"}],
        alarm_actions_of=lambda name: False,
    )
    assert not [f for f in findings if f["surface"] == "cloudwatch"]


def test_alarm_unexpectedly_enabled_does_not_fire_for_a_notbreaching_alarm(mod):
    """alpha-engine-config-I8712, induced: the exact live shape that was red on
    main — a `notBreaching` alarm, watched trigger paused (justified), live
    ActionsEnabled=True. Only a `breaching` alarm can false-page from the
    watched trigger's silence, so this is not drift: the live state was never
    wrong, and grading it as `alarm-unexpectedly-enabled` was the bug.
    """
    m = _manifest_with_alarm(watches={"tick": "ruled off"}, alarm_watches=["tick"])
    findings = _reconcile(
        mod, manifest=m, rows={},
        triggers=[{"surface": "events", "name": "tick", "state": "DISABLED"}],
        alarm_actions_of=lambda name: True,
        alarm_breaching_of=lambda name: False,
    )
    assert not [f for f in findings if f["surface"] == "cloudwatch"], findings


def test_alarm_stale_disabled_does_not_fire_for_a_notbreaching_alarm(mod):
    """The other direction of the same exemption: a notBreaching alarm left
    ActionsEnabled=False after its watched trigger un-pauses is not drift
    either — it never needed to be enabled to be correct, so it is not
    required to be re-enabled to stay correct."""
    m = _manifest_with_alarm(watches={}, alarm_watches=["tick"])  # tick no longer paused
    findings = _reconcile(
        mod, manifest=m, rows={},
        triggers=[{"surface": "events", "name": "tick", "state": "ENABLED"}],
        alarm_actions_of=lambda name: False,
        alarm_breaching_of=lambda name: False,
    )
    assert not [f for f in findings if f["surface"] == "cloudwatch"], findings


def test_alarm_missing_in_aws_fires_when_the_alarm_does_not_exist(mod):
    m = _manifest_with_alarm(watches={"tick": "ruled off"}, alarm_watches=["tick"])
    findings = _reconcile(
        mod, manifest=m, rows={},
        triggers=[{"surface": "events", "name": "tick", "state": "DISABLED"}],
        alarm_actions_of=lambda name: None,
    )
    assert [(f["kind"], f["trigger"]) for f in findings
            if f["surface"] == "cloudwatch"] == [("alarm-missing-in-aws", "probe-alarm")]


def test_declared_alarm_gaps_lists_only_currently_justified_entries(mod):
    still_watched = _manifest_with_alarm(watches={"tick": "ruled off"}, alarm_watches=["tick"],
                                          reason="watches tick")
    gaps = mod.declared_alarm_gaps(still_watched)
    assert [g["id"] for g in gaps] == ["probe-alarm"]
    assert "watches tick" in gaps[0]["detail"]

    lifted = _manifest_with_alarm(watches={}, alarm_watches=["tick"])
    assert mod.declared_alarm_gaps(lifted) == []


def test_declared_alarm_gaps_are_not_findings_and_do_not_flip_status(mod):
    """Property 2: the gap always renders, but never makes a correctly-paused
    run look like drift. A permanently-red row on a working pause is the exact
    page-on-compliance class this issue exists to remove."""
    m = _manifest_with_alarm(watches={"tick": "ruled off"}, alarm_watches=["tick"])
    findings = _reconcile(
        mod, manifest=m, rows={},
        triggers=[{"surface": "events", "name": "tick", "state": "DISABLED"}],
        alarm_actions_of=lambda name: False,
    )
    assert findings == []
    assert mod.declared_alarm_gaps(m) != []


def test_publish_renders_declared_gaps_without_forcing_attention_status(mod, monkeypatch):
    captured = {}

    class _FakeFcr:
        STATUS_OK = "ok"
        STATUS_ATTENTION = "attention"

        @staticmethod
        def build(**kw):
            captured.update(kw)
            return kw

        @staticmethod
        def emit(env, dry_run=False):
            return "emitted"

    import sys
    monkeypatch.setitem(sys.modules, "nousergon_lib", type(sys)("nousergon_lib"))
    monkeypatch.setattr(sys.modules["nousergon_lib"], "fleet_check_result", _FakeFcr, raising=False)

    gap = {"id": "probe-alarm", "kind": "declared-silenced-alarm", "detail": "watches tick"}
    mod.publish([], checked=3, declared_gaps=[gap])
    assert captured["status"] == "ok", "a declared gap alone must not flip status to attention"
    assert gap in captured["findings"], "the declared gap must still render on the console row"


# ── alpha-engine-config-I7547: runtime one-shots, and a legible verdict ──────

def test_a_self_deleting_one_shot_is_not_undeclared_scheduled_work(mod):
    """Induced: the exact resource that produced a false finding on 2026-08-17.

    `alpha-engine-arctic-migration-dispatcher` minted
    `arctic-migration-defer-0002-g1` at 11:09:22-07:00 to defer its own
    re-invocation; the reconciler reported it `undeclared-enabled` minutes
    later, and by the time it was described it no longer existed. Its name is
    generated per run, so no manifest entry could ever name it.
    """
    noted: list[dict] = []
    findings = mod.reconcile(
        manifest=_manifest(), rows={"other": _row("other")},
        triggers=[{"surface": "scheduler", "name": "arctic-migration-defer-0002-g1",
                   "state": "ENABLED"}],
        targets_of=lambda t: [], sf_invoked=set(), invocations_of=lambda cid: 0,
        alarm_actions_of=lambda n: None,
        is_ephemeral=lambda name, group=None: True, noted=noted,
    )
    assert findings == []
    assert [(n["kind"], n["id"]) for n in noted] == [
        ("ephemeral-one-shot", "arctic-migration-defer-0002-g1")]


def test_a_standing_schedule_is_still_a_finding(mod):
    """The exclusion is scoped to self-deleting one-shots, not to Scheduler.

    Without this, `is_ephemeral` returning True for everything would silently
    delete a whole finding class — the failure mode an exclusion always has.
    """
    findings = mod.reconcile(
        manifest=_manifest(), rows={},
        triggers=[{"surface": "scheduler", "name": "some-daily-job", "state": "ENABLED"}],
        targets_of=lambda t: [], sf_invoked=set(), invocations_of=lambda cid: 0,
        alarm_actions_of=lambda n: None,
        is_ephemeral=lambda name, group=None: False,
    )
    assert [(f["kind"], f["trigger"]) for f in findings] == [
        ("undeclared-enabled", "some-daily-job")]


def test_the_ephemeral_probe_is_not_called_on_a_clean_run(mod):
    """It costs one `get-schedule` per candidate, and a clean run has none."""
    calls: list[str] = []
    mod.reconcile(
        manifest=_manifest(kept={"kept-job": "ruled on"}), rows={},
        triggers=[{"surface": "scheduler", "name": "kept-job", "state": "ENABLED"}],
        targets_of=lambda t: [], sf_invoked=set(), invocations_of=lambda cid: 0,
        alarm_actions_of=lambda n: None,
        is_ephemeral=lambda name, group=None: calls.append(name) or True,
    )
    assert calls == []


def test_a_schedule_that_vanished_mid_run_is_proof_it_was_a_one_shot(mod, monkeypatch):
    """The race IS the evidence, so it must not raise. `list-schedules` saw it
    and `get-schedule` did not, which only self-deletion-after-firing explains;
    raising would turn any concurrent deferral into a broken detector."""
    def _boom(args):
        raise RuntimeError("aws scheduler get-schedule: An error occurred "
                           "(ResourceNotFoundException) when calling the GetSchedule operation")
    monkeypatch.setattr(mod, "_aws_json", _boom)
    assert mod.is_ephemeral_one_shot("sf-watch-defer-abc-g1") is True


def test_a_real_scheduler_failure_still_raises(mod, monkeypatch):
    """Only ResourceNotFound is evidence. An AccessDenied read as `ephemeral`
    would let this check grade itself green by losing its own access — the
    posture `_aws_json` already takes."""
    def _boom(args):
        raise RuntimeError("aws scheduler get-schedule: AccessDeniedException")
    monkeypatch.setattr(mod, "_aws_json", _boom)
    with pytest.raises(RuntimeError, match="AccessDenied"):
        mod.is_ephemeral_one_shot("sf-watch-defer-abc-g1")


# ── (group, name) is a Scheduler schedule's identity ─────────────────────────
# alpha-engine-config-I10009. `list-schedules` spans every schedule group;
# `get-schedule` resolves within ONE, defaulting to `default`. An
# enumerate-then-describe pass that drops the group lists schedules it can then
# never describe — measured live 2026-09-04, where the four `crucible-v2`-group
# schedules landed on 09-03 and every `pause-reconcile.yml` run since exited 2
# on `get-schedule --name alerts-sweep`.


def test_live_triggers_carries_each_schedules_group(mod, monkeypatch):
    """Without the group on the row there is nothing to pass downstream, and
    the bug reappears the next time someone makes a non-default group."""
    def _fake(cmd, key):
        if cmd[0] == "events":
            return iter([])
        return iter([
            {"Name": "alerts-sweep", "State": "ENABLED", "GroupName": "crucible-v2"},
            {"Name": "alpha-engine-weekday", "State": "ENABLED", "GroupName": "default"},
        ])
    monkeypatch.setattr(mod, "_paginate", _fake)
    by_name = {t["name"]: t for t in mod.live_triggers()}
    assert by_name["alerts-sweep"]["group"] == "crucible-v2"
    assert by_name["alpha-engine-weekday"]["group"] == "default"


def test_the_ephemeral_probe_passes_the_group_through(mod, monkeypatch):
    """The quiet half of the bug: `get-schedule` without `--group-name` raises
    ResourceNotFoundException for a non-default-group schedule, which this
    function reads as 'it deleted itself' and returns True for. So the bug
    does not only break the run — it silently excuses every non-default-group
    schedule from the register it is missing from."""
    seen: list[list[str]] = []
    monkeypatch.setattr(mod, "_aws_json", lambda args: seen.append(args) or {})
    mod.is_ephemeral_one_shot("alerts-sweep", "crucible-v2")
    assert seen[0] == ["scheduler", "get-schedule", "--name", "alerts-sweep",
                       "--group-name", "crucible-v2"]


def test_trigger_targets_passes_the_group_through(mod, monkeypatch):
    seen: list[list[str]] = []
    monkeypatch.setattr(mod, "_aws_json", lambda args: seen.append(args) or {})
    mod.trigger_targets({"surface": "scheduler", "name": "alerts-sweep",
                         "group": "crucible-v2"})
    assert "--group-name" in seen[0] and "crucible-v2" in seen[0]


def test_a_scheduler_row_with_no_group_falls_back_to_default(mod, monkeypatch):
    """Rows built before this change, and any hand-built row in a caller,
    must keep resolving rather than crash on a missing key."""
    seen: list[list[str]] = []
    monkeypatch.setattr(mod, "_aws_json", lambda args: seen.append(args) or {})
    mod.trigger_targets({"surface": "scheduler", "name": "alpha-engine-weekday"})
    assert seen[0][-2:] == ["--group-name", mod.DEFAULT_SCHEDULE_GROUP]


# ── the join key must be group-qualified too (alpha-engine-config-I9681) ────
# `live_triggers()` carries a non-default Scheduler group as a separate field
# (the two tests above), but `check()`'s direction A matched the BARE name
# against `paused`/`kept`/the registry — a different join key than
# `automation_pause.py::_live_triggers()` uses to populate those very sets
# (`"<group>/<name>"`, per `_split_scheduler_name`'s docstring). Measured
# live 2026-09-08: all four `crucible-v2`-group schedules (`heartbeat`,
# `weekly`, `alerts-sweep`, `data-daily`) — declared `not_paused` under keys
# `crucible-v2/heartbeat` etc. since 2026-09-04 (I9994) — were reported
# `undeclared-enabled` on every run because bare `"heartbeat"` is not in a
# `kept` set that only contains `"crucible-v2/heartbeat"`.

def test_a_kept_non_default_group_schedule_is_not_undeclared_enabled(mod):
    """The exact I9681 defect, induced: a `crucible-v2`-group schedule
    declared `not_paused` under its group-qualified key must not be flagged
    just because its live AWS name is bare."""
    findings = _reconcile(
        mod,
        manifest=_manifest(kept={"crucible-v2/heartbeat": "crucible v2 build, KEPT"}),
        rows={},
        triggers=[{"surface": "scheduler", "name": "heartbeat", "state": "ENABLED",
                   "group": "crucible-v2"}],
    )
    assert findings == []


def test_an_undeclared_non_default_group_schedule_still_fires_qualified(mod):
    """The other direction: a real gap in a non-default group must still be
    caught, and reported under the SAME qualified key automation_pause.json
    would need to declare — not the bare name, which nobody could add there."""
    findings = mod.reconcile(
        manifest=_manifest(), rows={},
        triggers=[{"surface": "scheduler", "name": "stowaway", "state": "ENABLED",
                   "group": "crucible-v2"}],
        targets_of=lambda t: [], sf_invoked=set(), invocations_of=lambda cid: 0,
        alarm_breaching_of=lambda name: True,
        is_ephemeral=lambda name, group=None: False,
    )
    assert [(f["kind"], f["trigger"]) for f in findings] == \
        [("undeclared-enabled", "crucible-v2/stowaway")]


def test_the_headline_distinguishes_drift_from_a_broken_detector(mod):
    """I7547 deliverable 3. GitHub renders both as `failure`; these do not.

    The literal failure this closes: four consecutive `failure` runs were four
    CORRECT detections and were read as one broken job, so a champion/challenger
    arm stayed missing five days.
    """
    drift = mod.headline([{"kind": "undeclared-dark", "trigger": "x", "surface": "events"}], 72)
    clear = mod.headline([], 72)
    broke = mod.headline([], 0, error="AccessDenied listing rules")
    assert drift.startswith("DRIFT") and "1 finding(s)" in drift and "72" in drift
    assert clear.startswith("clear") and "72" in clear
    assert broke.startswith("BROKE") and "AccessDenied" in broke
    assert len({drift, clear, broke}) == 3


def test_the_headline_is_one_line_so_it_can_be_a_job_name(mod):
    """`pause-reconcile.yml` uses it as `jobs.verdict.name`, and a newline there
    would truncate the verdict to nothing."""
    long_error = "line one of the failure\n" + "x" * 5000
    for text in (mod.headline([], 72), mod.headline([], 0, error=long_error),
                 mod.headline([{"kind": "k", "trigger": "t", "surface": "s"}], 1)):
        assert "\n" not in text and text


def test_the_workflow_fails_the_run_on_findings(mod):
    """The exit code IS the signal. I7547 says so in as many words: do not fix a
    noisy detector by making it exit 0."""
    text = WORKFLOW.read_text(encoding="utf-8")
    assert 'if [ "$VERDICT" != "clear" ]; then' in text and "exit 1" in text, (
        "the verdict job no longer fails on drift — a detect-only check that "
        "cannot go red reports nothing at all"
    )


def test_the_workflow_names_the_failing_job_after_the_verdict(mod):
    """The surface a reader lands on. `jobs.<id>.name` accepts the `needs`
    context, which is what carries the finding count onto the run page."""
    text = WORKFLOW.read_text(encoding="utf-8")
    assert "name: ${{ needs.reconcile.outputs.headline }}" in text
    assert "headline: ${{ steps.check.outputs.headline }}" in text, (
        "the reconcile job does not export the headline, so the verdict job's "
        "name resolves to empty and the count is invisible again"
    )


def test_a_breakage_publishes_an_error_row_rather_than_nothing(mod, monkeypatch):
    """Before I7547 the exit-2 path returned before `publish()`, so a check that
    lost its AWS access wrote NOTHING and the console kept rendering yesterday's
    row — the §8.3 collapse this module exists to detect, committed by the
    detector itself."""
    captured: dict = {}
    from nousergon_lib import fleet_check_result as fcr
    monkeypatch.setattr(fcr, "emit", lambda e, dry_run=False: captured.update(e))
    mod.publish_error("AccessDenied listing rules")
    assert captured["status"] == fcr.STATUS_ERROR != fcr.STATUS_ATTENTION
    assert "AccessDenied" in captured["summary"]
    assert captured["check_id"] == mod.CHECK_ID


def test_the_rendered_verdict_states_that_nothing_was_graded_on_a_breakage(mod):
    """A broken run's summary must not read like a clean one. `0 findings` is
    literally true and completely misleading when the comparison never ran."""
    broken = mod.render_markdown([], 0, 0, error="AccessDenied")
    clean = mod.render_markdown([], 72, 210)
    assert "NOT compared" in broken and "AccessDenied" in broken
    assert "No disagreement" in clean and "NOT compared" not in clean


def test_the_rendered_verdict_lists_findings_and_the_excluded_separately(mod):
    text = mod.render_markdown(
        [{"kind": "undeclared-dark", "trigger": "ghost", "surface": "events",
          "detail": "nobody declared it"}],
        72, 210,
        gaps=[{"id": "an-alarm", "kind": "declared-silenced-alarm", "detail": "silenced"}],
        noted=[{"id": "defer-0002-g1", "kind": "ephemeral-one-shot", "detail": "one-shot"}],
    )
    assert "these fail the run" in text
    assert "`ghost`" in text and "`an-alarm`" in text and "`defer-0002-g1`" in text
    assert text.index("Findings") < text.index("Declared gaps") < text.index("Excluded")


def test_github_output_is_written_as_single_line_key_values(mod, tmp_path):
    out = tmp_path / "gh_output"
    mod.write_github_output(out, [{"kind": "k", "trigger": "t", "surface": "s"}], 72)
    lines = out.read_text(encoding="utf-8").strip().splitlines()
    kv = dict(line.split("=", 1) for line in lines)
    assert len(lines) == 4 and kv["verdict"] == "drift" and kv["findings"] == "1"
    assert kv["checked"] == "72" and kv["headline"].startswith("DRIFT")
    mod.write_github_output(out, [], 72)
    assert "verdict=clear" in out.read_text(encoding="utf-8")


def test_the_run_summary_is_written_by_the_same_invocation_as_the_verdict(mod):
    """The old workflow ran the whole check a SECOND time to fill the summary,
    which doubled a ~100-call account enumeration and let the two disagree
    whenever live state moved between them — which is how a runtime-minted
    one-shot appeared in one and not the other."""
    text = WORKFLOW.read_text(encoding="utf-8")
    assert text.count("pause_reconcile.py --check") == 1, (
        "the reconciler runs more than once per workflow run; one run, one verdict"
    )
    assert "--markdown" in text and "--github-output" in text


def test_an_undeclared_owner_is_a_finding_in_the_reconciler(mod):
    """The reconciler carries the declaration verdict too — one report, not a
    second surface a reader has to know to check (alpha-engine-config-I8047)."""
    m = _manifest_with_alarm(watches={"tick": "ruled off"}, alarm_watches=["tick"],
                             issue="")
    findings = _reconcile(
        mod, manifest=m, rows={},
        triggers=[{"surface": "events", "name": "tick", "state": "DISABLED"}],
        alarm_actions_of=lambda name: False,
    )
    assert ("alarm-declaration-unowned", "probe-alarm") in [
        (f["kind"], f["trigger"]) for f in findings]


def test_an_undated_declaration_is_a_finding_in_the_reconciler(mod):
    m = _manifest_with_alarm(watches={"tick": "ruled off"}, alarm_watches=["tick"],
                             re_exam="soon")
    findings = _reconcile(
        mod, manifest=m, rows={},
        triggers=[{"surface": "events", "name": "tick", "state": "DISABLED"}],
        alarm_actions_of=lambda name: False,
    )
    assert ("alarm-declaration-undated", "probe-alarm") in [
        (f["kind"], f["trigger"]) for f in findings]


def test_a_declared_gap_row_names_its_owner_and_expiry(mod):
    """The console row is the surface a human reads; a declared gap that does
    not say what would end it is the prose the ruling replaced."""
    m = _manifest_with_alarm(watches={"tick": "ruled off"}, alarm_watches=["tick"])
    rows = mod.declared_alarm_gaps(m)
    assert rows, "the justified entry must render as a declared gap"
    assert "alpha-engine-config-I6984" in rows[0]["detail"]
    assert "2999-01-01" in rows[0]["detail"]




# ── alpha-engine-config-I8189: machine-readable declared-pause lane set ─────
#
# The groom pause manifest lives in THIS repo; the evaluator (crucible-
# evaluator's groom tile, a Lambda with no checkout) needs a machine-readable
# fact instead of inferring "paused" from absent artifacts (forbidden by
# observability-policy.md §8.3). This is the nousergon-data producer half —
# `publish_paused_lanes` publishes the set `automation_pause.paused_names()`
# already computes to a stable S3 key on the evaluator's existing read path.
# The consumer half lands separately in crucible-evaluator.

_KNOWN_GROOM_LANES = {
    "alpha-engine-groom-lane-reconciler-5min",
    "alpha-engine-groom-sweep-0000-daily",
    "alpha-engine-groom-sweep-0800-daily",
    "alpha-engine-groom-sweep-1600-daily",
    "alpha-engine-scheduled-groom-0400-daily",
    "alpha-engine-scheduled-groom-1200-daily",
    "alpha-engine-scheduled-groom-2000-daily",
    "alpha-engine-scheduled-groom-sun0900-weekly",
}


class _InMemoryS3:
    """Minimal in-memory S3 mock (put_object only — this module never reads
    this key back). No moto dep — mirrors
    tests/test_daily_append_schema_drift.py::_InMemoryS3 /
    tests/test_news_aggregates.py::_InMemoryS3, this repo's established
    convention for S3-touching tests (deliberately no moto dependency)."""

    def __init__(self) -> None:
        self.puts: list[dict] = []

    def put_object(self, *, Bucket, Key, Body, ContentType=None):
        self.puts.append({"Bucket": Bucket, "Key": Key, "Body": Body, "ContentType": ContentType})
        return {"ETag": "stub"}


def test_publish_paused_lanes_writes_the_exact_contract_shape(mod, monkeypatch):
    s3 = _InMemoryS3()
    monkeypatch.setattr(mod, "boto3", MagicMock(client=lambda *a, **k: s3))

    m = _manifest(
        paused={name: "groom pause (alpha-engine-config-I6617)" for name in _KNOWN_GROOM_LANES},
    )
    uri = mod.publish_paused_lanes(m, dry_run=False)

    assert uri == f"s3://{mod.REGISTRY_BUCKET}/{mod.PAUSED_LANES_KEY}"
    assert len(s3.puts) == 1
    put = s3.puts[0]
    assert put["Bucket"] == mod.REGISTRY_BUCKET
    assert put["Key"] == mod.PAUSED_LANES_KEY
    assert put["ContentType"] == "application/json"

    body = json.loads(put["Body"])
    assert body["schema_version"] == 1
    assert set(body.keys()) == {"schema_version", "generated_at", "paused"}
    # generated_at parses as a UTC ISO8601 timestamp.
    datetime.datetime.fromisoformat(body["generated_at"])
    assert body["paused"] == sorted(body["paused"]), "paused list must be sorted"


def test_publish_paused_lanes_matches_paused_names_including_the_eight_groom_lanes(mod, monkeypatch):
    s3 = _InMemoryS3()
    monkeypatch.setattr(mod, "boto3", MagicMock(client=lambda *a, **k: s3))

    m = _manifest(
        paused={name: "groom pause (alpha-engine-config-I6617)" for name in _KNOWN_GROOM_LANES},
    )
    mod.publish_paused_lanes(m, dry_run=False)

    body = json.loads(s3.puts[0]["Body"])
    assert set(body["paused"]) == mod.ap.paused_names(m)
    assert _KNOWN_GROOM_LANES.issubset(set(body["paused"])), (
        "all 8 known groom lanes must be present in the published paused set"
    )


def test_publish_paused_lanes_uses_the_live_manifest_by_default(mod, manifest):
    """Sanity check against the real, checked-in manifest (not a synthetic
    one): the 8 known groom lanes are actually paused there today."""
    assert _KNOWN_GROOM_LANES.issubset(mod.ap.paused_names(manifest))


def test_publish_paused_lanes_never_raises_on_a_publish_failure(mod, monkeypatch):
    class _BrokenS3:
        def put_object(self, **kw):
            raise RuntimeError("S3 is down")

    monkeypatch.setattr(mod, "boto3", MagicMock(client=lambda *a, **k: _BrokenS3()))

    m = _manifest(paused={"tick": "x"})
    result = mod.publish_paused_lanes(m, dry_run=False)  # must not raise
    assert result is None


def test_publish_paused_lanes_dry_run_does_not_write(mod, monkeypatch):
    s3 = _InMemoryS3()
    monkeypatch.setattr(mod, "boto3", MagicMock(client=lambda *a, **k: s3))

    m = _manifest(paused={"tick": "x"})
    result = mod.publish_paused_lanes(m, dry_run=True)
    assert result is None
    assert s3.puts == []


def test_main_calls_publish_paused_lanes_after_publish(mod, monkeypatch):
    """Wired into main() right after the existing publish() call, same
    dry_run handling — verified by call order, not by re-testing publish()."""
    calls: list[str] = []
    monkeypatch.setattr(mod, "live_triggers", lambda: [])
    monkeypatch.setattr(mod, "load_registry", lambda registry_dir=None: {})
    monkeypatch.setattr(mod, "reconcile", lambda **kw: [])
    monkeypatch.setattr(mod, "declared_alarm_gaps", lambda manifest=None: [])
    monkeypatch.setattr(mod, "publish", lambda *a, **kw: calls.append("publish"))
    monkeypatch.setattr(mod, "publish_paused_lanes", lambda *a, **kw: calls.append("publish_paused_lanes"))
    monkeypatch.setattr(sys, "argv", ["pause_reconcile.py", "--check", "--publish", "--dry-run"])

    mod.main()

    assert calls == ["publish", "publish_paused_lanes"], (
        "publish_paused_lanes must run right after publish(), same --publish/--dry-run gate"
    )


# ── the post-merge settle window (2026-10-03, #2022 / #2023 races) ───────────

def _trig(name, state, surface="scheduler", group="default"):
    t = {"surface": surface, "name": name, "state": state}
    if surface == "scheduler":
        t["group"] = group
    return t


class _Clock:
    def __init__(self):
        self.t = 0.0

    def __call__(self):
        return self.t

    def sleep(self, s):
        self.t += s


def test_settle_returns_the_snapshot_once_the_finding_trigger_moves(mod):
    """The 2026-10-03 shape: read DISABLED, then the on-merge deploy enables it."""
    first = [_trig("data-collection-daily-heal", "DISABLED", group="nousergon-data-collection")]
    finding = {"kind": "undeclared-dark", "surface": "scheduler",
               "trigger": "nousergon-data-collection/data-collection-daily-heal"}
    later = [_trig("data-collection-daily-heal", "ENABLED", group="nousergon-data-collection")]
    reads = iter([first, later])
    clock = _Clock()
    fresh = mod.settle(first, [finding], 300, enumerate_=lambda: next(reads),
                       sleep=clock.sleep, clock=clock, poll_s=30)
    assert fresh == later
    assert clock.t == 60, "stops polling as soon as every watched trigger moved"


def test_settle_never_regrades_a_trigger_that_does_not_move(mod):
    """A real finding is reported exactly as before — the window is a wait, not a filter."""
    first = [_trig("tick", "DISABLED")]
    finding = {"kind": "undeclared-dark", "surface": "scheduler", "trigger": "tick"}
    clock = _Clock()
    fresh = mod.settle(first, [finding], 300, enumerate_=lambda: list(first),
                       sleep=clock.sleep, clock=clock, poll_s=30)
    assert fresh is None
    assert clock.t == 300


def test_settle_is_off_without_a_budget_or_a_trigger_finding(mod):
    first = [_trig("tick", "DISABLED")]
    boom = lambda: (_ for _ in ()).throw(AssertionError("must not enumerate"))  # noqa: E731
    f = {"kind": "undeclared-dark", "surface": "scheduler", "trigger": "tick"}
    assert mod.settle(first, [f], 0, enumerate_=boom) is None
    alarm = {"kind": "alarm-stale-disabled", "surface": "cloudwatch", "trigger": "a"}
    assert mod.settle(first, [alarm], 300, enumerate_=boom) is None


def test_main_regrades_on_the_settled_snapshot_and_renders_the_transient(mod, monkeypatch, capsys):
    first = [_trig("tick", "DISABLED")]
    later = [_trig("tick", "ENABLED")]
    finding = {"kind": "undeclared-dark", "surface": "scheduler", "trigger": "tick", "detail": "d"}
    monkeypatch.setattr(mod, "live_triggers", lambda: first)
    monkeypatch.setattr(mod, "load_registry", lambda registry_dir=None: {})
    monkeypatch.setattr(mod, "reconcile",
                        lambda triggers, **kw: [finding] if triggers is first else [])
    monkeypatch.setattr(mod, "settle", lambda trigs, findings, budget: later)
    monkeypatch.setattr(mod, "declared_alarm_gaps", lambda manifest=None: [])
    monkeypatch.setattr(sys, "argv", ["pause_reconcile.py", "--check", "--settle", "300"])
    assert mod.main() == 0
    out = capsys.readouterr().out
    assert "converged-during-settle" in out and "scheduler:tick" in out


def test_main_does_not_settle_by_default(mod, monkeypatch):
    finding = {"kind": "undeclared-dark", "surface": "scheduler", "trigger": "tick", "detail": "d"}
    monkeypatch.setattr(mod, "live_triggers", lambda: [_trig("tick", "DISABLED")])
    monkeypatch.setattr(mod, "load_registry", lambda registry_dir=None: {})
    monkeypatch.setattr(mod, "reconcile", lambda **kw: [finding])
    monkeypatch.setattr(mod, "settle", lambda *a, **k: (_ for _ in ()).throw(AssertionError))
    monkeypatch.setattr(mod, "declared_alarm_gaps", lambda manifest=None: [])
    monkeypatch.setattr(sys, "argv", ["pause_reconcile.py", "--check"])
    assert mod.main() == 1


def test_the_workflow_settles_on_push_only(mod):
    text = WORKFLOW.read_text(encoding="utf-8")
    assert "--settle \"$SETTLE_SECONDS\"" in text
    assert "github.event_name == 'push' && '300' || '0'" in text, (
        "scheduled runs race no deploy; only a push grades a change its own "
        "on-merge deploy is still applying")
