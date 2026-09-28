"""Contract tests for infrastructure/overseer/arming.py (alpha-engine-config-I7056).

The property under test is a NEGATIVE one and it is the whole point: a paused
playbook and a broken playbook both emit nothing, and `observability-policy.md`
§8.3 forbids reporting either as the other. Every test below is ultimately about
that one collapse.

`overseer-policy.md` invariant 13 — a guard is not a guard until it has been
observed failing — is honoured concretely here: the two `undeclared-dark` tests
are constructed from the exact live condition this module found on its first run
(two sf-watch EventPattern rules DISABLED in AWS while `automation_pause.json`'s
`_reactive-notifier-rules` prose key claimed all six of its rules were kept).
"""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
ARMING_PATH = ROOT / "infrastructure" / "overseer" / "arming.py"
REGISTRY_PATH = ROOT / "infrastructure" / "overseer" / "playbooks.yaml"
MANIFEST_PATH = ROOT / "infrastructure" / "automation_pause.json"


@pytest.fixture(scope="module")
def arming():
    spec = importlib.util.spec_from_file_location("overseer_arming", ARMING_PATH)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="module")
def registry():
    return yaml.safe_load(REGISTRY_PATH.read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def manifest():
    return json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))


def _states(mapping):
    """A `live_state` stub. Anything unnamed is ABSENT, never silently ENABLED."""
    return lambda surface, name: mapping.get((surface, name))


# ── the join key itself ──────────────────────────────────────────────────────

def test_every_overseer_component_declares_triggers(registry):
    """Without `triggers`, arming is uncomputable and silence is unclassifiable.

    `wake:` prose cannot carry this: it is written for a human and no reader can
    join on it. This test is what stops the next playbook being added with only
    the prose.
    """
    missing = [n for n, s in registry["playbooks"].items() if not s.get("triggers")]
    missing += [e["name"] for e in registry.get("t1_automations", []) if not e.get("triggers")]
    assert not missing, (
        f"playbooks/T1 automations with no machine-readable `triggers`: {missing}. "
        "Declare them — use surface 'none' with a reason if there genuinely is no trigger."
    )


def test_every_aws_trigger_name_is_resolvable_in_the_pause_manifest(registry, manifest, arming):
    """The join must actually close, in the direction that matters.

    Every `events`/`scheduler` trigger the registry names must appear in
    automation_pause.json — in `paused`, `pending`, or `not_paused`. A name in
    neither is a trigger nobody has ruled on, which is exactly the state that
    makes DISABLED and MISSED indistinguishable.
    """
    import sys
    sys.path.insert(0, str(ROOT / "infrastructure"))
    import automation_pause as ap

    declared = ap.paused_names(manifest) | ap.kept_names(manifest)
    orphans = []
    for unit in arming.declared_units(registry):
        for t in unit["triggers"]:
            if t["surface"] in arming.JOINABLE_SURFACES and t["name"] not in declared:
                orphans.append(f"{unit['component_id']} -> {t['surface']}:{t['name']}")
    assert not orphans, (
        "AWS triggers named in playbooks.yaml but absent from automation_pause.json: "
        f"{orphans}. Each is a trigger whose off-ness nobody has decided."
    )


def test_non_aws_surfaces_state_a_reason(registry, arming):
    """An unjoinable trigger with no reason is a blank wearing a value's clothes."""
    bad = []
    for unit in arming.declared_units(registry):
        for t in unit["triggers"]:
            if t["surface"] not in arming.JOINABLE_SURFACES and not t.get("reason"):
                bad.append(f"{unit['component_id']} -> {t['surface']}")
    assert not bad, f"non-AWS triggers with no stated reason: {bad}"


# ── the three states the module exists to separate ───────────────────────────

def test_declared_pause_reads_as_paused_not_broken(arming, manifest):
    """State 1: paused by declaration. Expected silence, and NOT a finding."""
    reg = {"playbooks": {"p": {"triggers": [{"surface": "scheduler", "name": "sched-a"}]}}}
    m = dict(manifest)
    m["paused"] = {"events_rules": {}, "scheduler_schedules": {"sched-a": "ruled off"}}
    m["not_paused"] = {}
    m["pending"] = {}
    rec = arming.build_record(reg, m, _states({("scheduler", "sched-a"): "DISABLED"}))
    assert rec["units"][0]["arming"] == "paused-declared"
    assert arming.findings(rec) == [], "a deliberate operator disable is state, never a finding"


def test_enabled_and_idle_reads_as_armed_and_still_emits(arming, manifest):
    """State 2: enabled and idle. It emits every tick, so it is never silent."""
    reg = {"playbooks": {"p": {"triggers": [{"surface": "events", "name": "rule-a"}]}}}
    m = dict(manifest)
    m["paused"] = {"events_rules": {}, "scheduler_schedules": {}}
    m["not_paused"] = {"rule-a": "kept"}
    m["pending"] = {}
    rec = arming.build_record(reg, m, _states({("events", "rule-a"): "ENABLED"}))
    assert rec["units"][0]["arming"] == "armed"
    assert arming.findings(rec) == []
    # The emission itself: the unit appears in the record with a verdict even
    # though nothing ran. Absence of a run is never absence of a signal.
    assert rec["units"][0]["component_id"] == "overseer-p"
    assert rec["generated_at"]


def test_dark_with_no_declaration_is_a_finding_not_a_pause(arming, manifest):
    """State 3: off, and nobody decided it. THE case §8.3 forbids collapsing.

    Reconstructed from the live condition found on this module's first run.
    """
    reg = {"playbooks": {"sf-watch": {"triggers": [
        {"surface": "events", "name": "alpha-engine-sf-watch-spot-interruption"}]}}}
    m = dict(manifest)
    m["paused"] = {"events_rules": {}, "scheduler_schedules": {}}
    m["not_paused"] = {}
    m["pending"] = {}
    rec = arming.build_record(
        reg, m, _states({("events", "alpha-engine-sf-watch-spot-interruption"): "DISABLED"}))
    assert rec["units"][0]["arming"] == "undeclared-dark"
    found = arming.findings(rec)
    assert len(found) == 1 and found[0]["kind"] == "undeclared-dark"


def test_paused_and_dark_are_distinguishable_from_identical_silence(arming, manifest):
    """The load-bearing assertion: same live state, same (zero) runs, two verdicts.

    If this ever collapses to one answer, the module has stopped doing its job
    and every downstream state render is wrong in the quiet direction.
    """
    m = dict(manifest)
    m["paused"] = {"events_rules": {"declared": "ruled off"}, "scheduler_schedules": {}}
    m["not_paused"] = {}
    m["pending"] = {}
    live = _states({("events", "declared"): "DISABLED", ("events", "undeclared"): "DISABLED"})
    a = arming.build_record(
        {"playbooks": {"a": {"triggers": [{"surface": "events", "name": "declared"}]}}}, m, live)
    b = arming.build_record(
        {"playbooks": {"b": {"triggers": [{"surface": "events", "name": "undeclared"}]}}}, m, live)
    assert a["units"][0]["triggers"][0]["live"] == b["units"][0]["triggers"][0]["live"] == "DISABLED"
    assert a["units"][0]["arming"] == "paused-declared"
    assert b["units"][0]["arming"] == "undeclared-dark"


# ── the refinements that a playbook-level boolean would have got wrong ───────

def test_partial_arming_is_its_own_verdict(arming, manifest):
    """Measured 2026-08-12: alert-drain's four schedules were off and it still ran.

    Arming is a property of a TRIGGER. A playbook-level boolean would have read
    "paused" and been simply false.
    """
    reg = {"playbooks": {"alert-drain": {"triggers": [
        {"surface": "scheduler", "name": "sched-off"},
        {"surface": "events", "name": "rule-on"}]}}}
    m = dict(manifest)
    m["paused"] = {"events_rules": {}, "scheduler_schedules": {"sched-off": "ruled off"}}
    m["not_paused"] = {"rule-on": "kept"}
    m["pending"] = {}
    rec = arming.build_record(reg, m, _states(
        {("scheduler", "sched-off"): "DISABLED", ("events", "rule-on"): "ENABLED"}))
    assert rec["units"][0]["arming"] == "partially-armed"


def test_a_vanished_trigger_is_a_finding_not_an_absence(arming, manifest):
    """A declared trigger that does not exist live cannot wake anything."""
    reg = {"playbooks": {"p": {"triggers": [{"surface": "events", "name": "gone"}]}}}
    m = dict(manifest)
    m["paused"] = {"events_rules": {"gone": "ruled off"}, "scheduler_schedules": {}}
    m["not_paused"] = {}
    m["pending"] = {}
    rec = arming.build_record(reg, m, _states({}))
    assert rec["units"][0]["arming"] == "trigger-absent"
    assert arming.findings(rec)[0]["kind"] == "trigger-absent"


def test_a_unit_with_no_triggers_is_loud_not_blank(arming, manifest):
    """§8.3 has no fall-through: an uncomputable verdict is a finding, not a gap."""
    reg = {"playbooks": {"p": {}}}
    m = dict(manifest)
    m["paused"] = {"events_rules": {}, "scheduler_schedules": {}}
    m["not_paused"] = {}
    m["pending"] = {}
    rec = arming.build_record(reg, m, _states({}))
    assert rec["units"][0]["arming"] == "undeclared-no-triggers"
    assert arming.findings(rec)[0]["kind"] == "undeclared-no-triggers"


def test_undeclared_dark_outranks_every_other_verdict(arming, manifest):
    """One undeclared dark trigger makes every other reading on the unit unsafe."""
    reg = {"playbooks": {"p": {"triggers": [
        {"surface": "events", "name": "on"},
        {"surface": "events", "name": "dark"}]}}}
    m = dict(manifest)
    m["paused"] = {"events_rules": {}, "scheduler_schedules": {}}
    m["not_paused"] = {"on": "kept"}
    m["pending"] = {}
    rec = arming.build_record(reg, m, _states(
        {("events", "on"): "ENABLED", ("events", "dark"): "DISABLED"}))
    assert rec["units"][0]["arming"] == "undeclared-dark"


# ── posture ──────────────────────────────────────────────────────────────────

def test_the_record_covers_every_registered_unit_every_tick(arming, registry, manifest):
    """No unit may drop out of the record — a component that disappears from the
    surface is the failure a state vocabulary exists to make impossible (§8.3)."""
    rec = arming.build_record(registry, manifest, _states({}))
    ids = {u["component_id"] for u in rec["units"]}
    expected = {f"overseer-{n}" for n in registry["playbooks"]}
    expected |= {f"t1-{e['name']}" for e in registry.get("t1_automations", [])}
    assert ids == expected
    # The I7056 cohort was nine; alpha-engine-config-I11290 added the tenth,
    # `t1-vcpu-runaway-responder-verify`. A pin, deliberately: a unit added or
    # dropped without touching this line is the drift it exists to catch.
    assert len(ids) == 10, "the I7056 cohort (nine) plus t1-vcpu-runaway-responder-verify"


def test_the_record_names_the_ruling_behind_the_silence(arming, registry, manifest):
    """A DISABLED render must carry its reason, owner and date or it is an
    inference wearing a declaration's clothes."""
    rec = arming.build_record(registry, manifest, _states({}))
    assert rec["pause_ruling"]["by"] == "Brian"
    assert rec["pause_ruling"]["date"]
    assert rec["pause_ruling"]["statement"]


def test_the_module_cannot_act(arming):
    """Detection stays deterministic and non-agentic (overseer-policy.md §2, and
    invariant 3). Re-enabling scheduled work unattended is precisely what the
    ruling this module READS exists to prevent."""
    source = ARMING_PATH.read_text(encoding="utf-8")
    for verb in ("enable-rule", "disable-rule", "update-schedule", "put-rule",
                 "invoke", "run-instances", "send-command"):
        assert verb not in source, f"arming.py must not be able to {verb}"


def test_a_permissions_error_is_never_read_as_absence(arming, manifest):
    """Losing its own access must not let this module grade itself green."""
    def boom(surface, name):
        raise RuntimeError("AccessDenied")
    reg = {"playbooks": {"p": {"triggers": [{"surface": "events", "name": "x"}]}}}
    with pytest.raises(RuntimeError):
        arming.build_record(reg, manifest, boom)


def test_ci_runs_the_arming_report():
    """A checker nothing runs is a comment. Pin the wiring."""
    wf = (ROOT / ".github" / "workflows" / "sf-arn-drift-check.yml").read_text(encoding="utf-8")
    assert "infrastructure/overseer/arming.py" in wf


# ── the event-time leg: a blank where the answer was derivable (I9045) ───────

def _event_time_registry():
    """alert-drain's real shape: four paused schedules plus an event-time leg
    riding an EventBridge rule that is deliberately KEPT enabled."""
    return {"playbooks": {"alert-drain": {"triggers": [
        {"surface": "scheduler", "name": "drain-sched"},
        {"surface": "event-time",
         "ref": "freshness CRITICAL -> router",
         "reason": "the freshness Lambda invokes the router in-process",
         "depends_on": [{"surface": "events", "name": "freshness-rule"}]},
    ]}}}


def _paused_drain_manifest(manifest):
    m = dict(manifest)
    m["paused"] = {"events_rules": {}, "scheduler_schedules": {"drain-sched": "ruled off"}}
    m["not_paused"] = {"freshness-rule": "kept — detection only"}
    m["pending"] = {}
    return m


def test_an_event_time_leg_resolves_through_its_dependencies(arming, manifest):
    """The measured 2026-08-28 condition, and the reading that was wrong.

    Every schedule DISABLED, the drain running daily, and this leg reported
    `unjoinable` — so the playbook rolled up to `paused-declared`, i.e. "off by
    ruling", about a thing that ran every day of that week. With `depends_on`
    the leg resolves off the live rule and the playbook reads `partially-armed`.
    """
    rec = arming.build_record(
        _event_time_registry(), _paused_drain_manifest(manifest),
        _states({("scheduler", "drain-sched"): "DISABLED",
                 ("events", "freshness-rule"): "ENABLED"}))
    legs = rec["units"][0]["triggers"]
    assert legs[1]["verdict"] == "armed-via-dependency"
    assert rec["units"][0]["arming"] == "partially-armed"


def test_an_event_time_leg_whose_every_dependency_is_paused_reads_paused(arming, manifest):
    """The other direction. A declared pause is not a finding — reporting a
    deliberate operator disable as a defect is §8.3's collapse the other way."""
    m = _paused_drain_manifest(manifest)
    m["paused"]["events_rules"] = {"freshness-rule": "ruled off"}
    m["not_paused"] = {}
    rec = arming.build_record(
        _event_time_registry(), m,
        _states({("scheduler", "drain-sched"): "DISABLED",
                 ("events", "freshness-rule"): "DISABLED"}))
    assert rec["units"][0]["triggers"][1]["verdict"] == "paused-declared"
    assert rec["units"][0]["arming"] == "paused-declared"
    assert arming.findings(rec) == []


def test_an_event_time_leg_dark_with_nobody_s_decision_is_a_finding(arming, manifest):
    """`undeclared-dark` must survive the extra hop. A leg going dark because
    the rule it rides was disabled out of band is exactly as invisible as the
    direct case, and now exactly as loud."""
    m = _paused_drain_manifest(manifest)
    m["not_paused"] = {}
    rec = arming.build_record(
        _event_time_registry(), m,
        _states({("scheduler", "drain-sched"): "DISABLED",
                 ("events", "freshness-rule"): "DISABLED"}))
    assert rec["units"][0]["arming"] == "undeclared-dark"
    assert [f["kind"] for f in arming.findings(rec)] == ["undeclared-dark"]


def test_an_event_time_leg_whose_dependency_vanished_is_a_finding(arming, manifest):
    rec = arming.build_record(
        _event_time_registry(), _paused_drain_manifest(manifest),
        _states({("scheduler", "drain-sched"): "DISABLED"}))
    assert rec["units"][0]["arming"] == "trigger-absent"


def test_a_leg_with_no_dependencies_is_still_unjoinable(arming, manifest):
    """`depends_on` is optional and additive. A github-actions leg has no AWS
    state to read and must stay NAMED-with-a-reason, never assumed armed."""
    reg = {"playbooks": {"p": {"triggers": [
        {"surface": "github-actions", "ref": "owner/repo/.github/workflows/x.yml",
         "reason": "GitHub Actions carries no AWS trigger state to read"}]}}}
    rec = arming.build_record(reg, _paused_drain_manifest(manifest), _states({}))
    assert rec["units"][0]["triggers"][0]["verdict"] == "unjoinable"


def test_the_live_registry_declares_the_freshness_rules_as_the_drain_s_event_time_leg(
    registry,
):
    """Pins the real declaration, not a fixture: the linkage the AWS surface was
    missing must exist in the registry the reconcile derives descriptions from."""
    triggers = registry["playbooks"]["alert-drain"]["triggers"]
    event_time = [t for t in triggers if t["surface"] == "event-time"]
    assert len(event_time) == 1
    deps = {d["name"] for d in event_time[0]["depends_on"]}
    assert "alpha-engine-freshness-monitor-cron" in deps
