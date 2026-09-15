#!/usr/bin/env python3
"""The scheduled-automation pause must stay a record, not a comment.

Brian ruling 2026-08-07: every scheduled AWS trigger disabled except the weekly
SF (Saturday), the daily preopen and postclose SFs (Mon-Fri), the 24/7 dashboard
box, and the two cost-safety backstops protecting them.
``infrastructure/automation_pause.json`` is that ruling; ``automation_pause.py``
verifies it against live AWS.

The failure this file guards is the one a console-only disable already has: the
pause exists nowhere anyone can find, and nothing notices when it lifts. These
tests assert the properties that make it findable and self-checking:

  1. The manifest parses, and every paused entry carries a reason. An entry with
     an empty reason is a name nobody can evaluate for un-pausing.
  2. Nothing is both kept and paused, and the kept set still names the three
     pipelines the ruling preserved. A pause that quietly swallowed the weekly
     SF trigger would read identically to a correct one in every other surface.
  3. The four CloudFormation-owned rules that the manifest lists are ALSO pinned
     ``State: DISABLED`` in the template. deploy-infrastructure.yml re-applies
     the stack on every push to main, so a manifest entry alone would be undone
     by the next merge — the exact silent-revert class this file exists for.
  4. The check runs in CI, and it asserts the pause direction (paused ⇒
     DISABLED), which is the opposite of what check-schedule-drift.py asserts
     for everything else.
  5. check-schedule-drift.py exempts paused schedules from its ``disabled``
     finding by CONSULTING the manifest, not by a hardcoded list that would rot.
"""

from __future__ import annotations

import importlib.util
import json
import re
import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
INFRA = REPO_ROOT / "infrastructure"
MANIFEST_PATH = INFRA / "automation_pause.json"
MODULE_PATH = INFRA / "automation_pause.py"
CFN = INFRA / "cloudformation" / "alpha-engine-orchestration.yaml"
DRIFT_CHECKER = INFRA / "scheduler" / "check-schedule-drift.py"
WORKFLOW = REPO_ROOT / ".github" / "workflows" / "sf-arn-drift-check.yml"

KEPT_TRIGGERS = {
    "alpha-engine-saturday",           # weekly SF
    "alpha-engine-weekday",            # preopen SF
    "alpha-engine-eod-backstop-daily",  # postclose SF backstop
}


def _load_module():
    spec = importlib.util.spec_from_file_location("automation_pause", MODULE_PATH)
    mod = importlib.util.module_from_spec(spec)
    sys.modules["automation_pause"] = mod
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="module")
def manifest() -> dict:
    return json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def module():
    return _load_module()


@pytest.fixture(autouse=True)
def clean_trigger_world(module, manifest, monkeypatch, request):
    """A live-trigger enumeration that exactly matches the manifest's own
    declarations (alpha-engine-config-I9959).

    ``check()`` now calls ``trigger_coverage_findings()``, which calls
    ``_live_triggers()`` — a real ``aws events list-rules`` / ``aws scheduler
    list-schedules`` account-wide enumeration. Every test written before this
    existed calls ``module.check()`` with no idea that call exists, so without
    this stub every one of them would reach the network (and fail with
    NoCredentialsError in CI, which has none by design — the same problem
    ``classified_world`` solves one level up for the alarm scan it added).
    Autouse, so the NEXT live read added to ``check()`` cannot reintroduce this
    either.
    """
    if request.node.get_closest_marker("real_trigger_scan") is not None:
        return
    live: list[dict] = []
    for surface, name, _ in module.paused_entries(manifest):
        live.append({"surface": surface, "name": name, "state": "DISABLED"})
    for name in module.kept_names(manifest):
        live.append({"surface": "events", "name": name, "state": "ENABLED"})
    monkeypatch.setattr(module, "_live_triggers", lambda: live)


def test_manifest_parses_and_every_entry_has_a_reason(manifest):
    for surface in ("events_rules", "scheduler_schedules"):
        entries = manifest["paused"][surface]
        assert entries, f"{surface} is empty — the manifest claims a pause it does not record"
        for name, reason in entries.items():
            assert reason.strip(), f"{surface}:{name} has no reason; nobody can evaluate un-pausing it"


def test_ruling_is_attributed_and_dated(manifest):
    ruling = manifest["ruling"]
    for field in ("by", "date", "statement"):
        assert ruling.get(field, "").strip(), f"ruling.{field} is missing"


def test_kept_triggers_are_never_paused(manifest, module):
    paused = module.paused_names(manifest)
    overlap = KEPT_TRIGGERS & paused
    assert not overlap, f"the ruling keeps these but the manifest pauses them: {sorted(overlap)}"


def test_kept_set_is_documented(manifest):
    not_paused = manifest["not_paused"]
    for trigger in KEPT_TRIGGERS:
        assert trigger in not_paused, (
            f"{trigger} is kept by the ruling but not explained in not_paused — "
            "a keep with no stated reason is indistinguishable from an oversight"
        )


def test_paused_names_are_unique_across_surfaces(manifest, module):
    events = set(manifest["paused"]["events_rules"])
    scheduler = set(manifest["paused"]["scheduler_schedules"])
    assert not (events & scheduler), (
        f"same name on both surfaces: {sorted(events & scheduler)} — "
        "EventBridge rules and Scheduler schedules are different APIs, not aliases"
    )
    assert len(module.paused_entries(manifest)) == len(events) + len(scheduler)


def test_cfn_owned_paused_rules_are_disabled_in_the_template(manifest):
    """A manifest entry alone does not survive the next push to main."""
    cfn_src = CFN.read_text(encoding="utf-8")
    cfn_owned = [
        name
        for name, reason in manifest["paused"]["events_rules"].items()
        if "CloudFormation-managed" in reason
    ]
    assert cfn_owned, "expected the manifest to flag its CloudFormation-owned rules"

    for name in cfn_owned:
        m = re.search(rf"^\s*Name: {re.escape(name)}\s*$", cfn_src, re.MULTILINE)
        assert m, f"{name} is flagged CloudFormation-managed but is not in the template"
        block = cfn_src[m.end() : m.end() + 1200]
        state = re.search(r"^\s*State: (\w+)\s*$", block, re.MULTILINE)
        assert state, f"{name}: no State property found in its resource block"
        assert state.group(1) == "DISABLED", (
            f"{name} is paused in the manifest but State: {state.group(1)} in the "
            "template — deploy-infrastructure.yml would re-enable it on the next merge"
        )


def test_every_cfn_rule_flagged_in_the_manifest_is_actually_cfn_owned(manifest):
    """The inverse: a rule pinned DISABLED in the template must be in the manifest."""
    cfn_src = CFN.read_text(encoding="utf-8")
    paused_events = set(manifest["paused"]["events_rules"])
    for m in re.finditer(
        r"^\s*Name: ([\w-]+)\s*$.*?^\s*State: (\w+)\s*$",
        cfn_src,
        re.MULTILINE | re.DOTALL,
    ):
        name, state = m.group(1), m.group(2)
        if state == "DISABLED":
            assert name in paused_events, (
                f"{name} is DISABLED in the template but absent from "
                "automation_pause.json — an undocumented disable is the thing "
                "this manifest exists to prevent"
            )


def test_ci_runs_the_pause_check(module):
    wf = WORKFLOW.read_text(encoding="utf-8")
    assert "infrastructure/automation_pause.py --check" in wf, (
        "the pause check is not invoked by sf-arn-drift-check.yml — a checker "
        "nothing runs is the failure mode this repo already booked twice"
    )
    # It must not be gated on the PR-only branch: the invariant is about live
    # AWS, which no file-path filter can observe.
    step = wf.split("automation_pause.py --check")[0]
    tail = step.rsplit("- name:", 1)[-1]
    assert "if:" not in tail, "the pause check is conditionally skipped"


def test_ci_repairs_the_alarm_state_before_it_verifies_it():
    """Repair and verify are one unit, in that order (alpha-engine-config-I7023).

    `--check` carries no `continue-on-error` (pinned above, deliberately). So
    when `--enforce --alarms-only` ran AFTER it, the enforce step was skipped on
    exactly the runs where drift existed — the self-healing loop could only run
    when there was nothing to heal. Measured 2026-08-14 on run 31819251392:
    eleven `alarm-unexpectedly-enabled` findings, remediation never reached, and
    the alarms stayed armed until a human ran the same command by hand.
    """
    wf = WORKFLOW.read_text(encoding="utf-8")
    enforce = wf.index("automation_pause.py --enforce --alarms-only")
    check = wf.index("automation_pause.py --check")
    assert enforce < check, (
        "the alarm-action repair step runs after the check that can fail the "
        "job, so it is skipped precisely when it is needed"
    )


def test_shared_helper_matches_alarm_justified(manifest, module):
    """`_shared/pause.sh::alarm_actions_flag` reimplements `alarm_justified` in bash+python.

    Six provisioning scripts call `put-metric-alarm`, which is an upsert that
    RESETS ActionsEnabled — so each must consult the pause at write time, and
    they do it through that shared helper rather than through this module (they
    are bash, and sourcing one helper beats importing python in six places).
    Two implementations of one predicate drift silently: the helper would keep
    arming an alarm this module considers silenced, and `--check` would keep
    re-muting it, forever, with neither surface calling it a conflict.

    Compares the two over EVERY declared alarm plus a known-armed one.
    """
    import subprocess

    helper = REPO_ROOT / "infrastructure/lambdas/_shared/pause.sh"
    assert helper.is_file(), "the shared helper moved; update this test"

    names = [e["name"] for e in module.alarm_entries(manifest)]
    names.append("alpha-engine-pipeline-deadman-preopen-trading")  # never paused
    script = (f'set -euo pipefail\nsource "{helper}"\n'
              + "".join(f'alarm_actions_flag "{n}"\n' for n in names))
    out = subprocess.run(["bash", "-c", script], capture_output=True, text=True,
                         check=True).stdout.split()

    assert len(out) == len(names), f"helper emitted {len(out)} flags for {len(names)} alarms"
    for name, flag in zip(names, out):
        expected = "--no-actions-enabled" if any(
            e["name"] == name and module.alarm_justified(e, manifest)
            for e in module.alarm_entries(manifest)
        ) else "--actions-enabled"
        assert flag == expected, (
            f"{name}: helper says {flag}, alarm_justified() says {expected} — "
            f"the two implementations of one predicate have drifted"
        )


def test_every_alarm_provisioner_consults_the_pause(module):
    """One provisioner fixed is zero provisioners fixed.

    `setup_watch_plane_alarms.sh` was made pause-aware first, and within the
    hour a DIFFERENT provisioner re-armed the same eleven alarms — which is what
    showed this was never a one-script defect. Asserted by discovery, so a
    seventh script added later is covered without editing this test.
    """
    offenders = []
    for path in sorted(REPO_ROOT.glob("infrastructure/**/*.sh")):
        text = path.read_text(encoding="utf-8", errors="replace")
        calls = [ln for ln in text.splitlines()
                 if "aws cloudwatch put-metric-alarm" in ln
                 and not ln.lstrip().startswith("#")]
        if calls and 'alarm_actions_flag "' not in text:
            offenders.append(path.relative_to(REPO_ROOT).as_posix())
    assert not offenders, (
        f"{offenders} call put-metric-alarm without alarm_actions_flag — "
        f"put-metric-alarm RESETS ActionsEnabled, so each of these re-arms "
        f"every paused-component alarm it touches, on every run"
    )


def test_drift_checker_consults_the_manifest_not_a_hardcoded_list():
    src = DRIFT_CHECKER.read_text(encoding="utf-8")
    assert "automation_pause.paused_names()" in src, (
        "check-schedule-drift.py must read the manifest; a second hardcoded "
        "list of paused names would drift from the first one"
    )
    # The exemption must apply ONLY to the disabled finding — expression drift
    # still has to be reported for paused schedules, or the codified cron rots
    # while nobody is looking and un-pausing becomes a rewrite.
    assert re.search(
        r'if live\["state"\] != "ENABLED" and rule\["name"\] not in paused:', src
    ), "the pause exemption is not scoped to the `disabled` finding"


def test_manifest_drift_checker_also_exempts_paused_triggers():
    """The second live-state assertion. Missing it reddens the daily sweep.

    check-manifest-drift.py --live independently asserts ENABLED for all eight
    groom/sweep dispatch triggers in schedule-manifest.json — every one of which
    is paused. Exempting only check-schedule-drift.py would have left the daily
    sweep red for a state that is correct, which is the same noise this PR
    exists to prevent.
    """
    src = (INFRA / "scheduler" / "check-manifest-drift.py").read_text(encoding="utf-8")
    assert "automation_pause.paused_names()" in src, (
        "check-manifest-drift.py --live still asserts ENABLED for paused triggers"
    )
    assert 'live_rule["state"] != "ENABLED" and name not in paused' in src, (
        "the exemption is not scoped to the state half of live-mismatch — cron "
        "drift must still be reported for a paused entry"
    )


def test_every_groom_dispatch_trigger_is_accounted_for(manifest):
    """schedule-manifest.json and automation_pause.json must not disagree.

    Both name groom/sweep triggers. If one says a trigger dispatches and the
    other says it is off, the pair is worse than either alone — so assert the
    overlap is total rather than partial: every trigger in the groom manifest is
    paused, or none is.
    """
    groom = json.loads(
        (INFRA / "scheduler" / "schedule-manifest.json").read_text(encoding="utf-8")
    )
    names = {t["name"] for t in groom["triggers"]}
    paused = set(manifest["paused"]["scheduler_schedules"])
    overlap = names & paused
    assert overlap == names, (
        "partially-paused groom dispatch set — these fire while their siblings "
        f"are off: {sorted(names - paused)}. Either pause all of them or none."
    )


def test_module_exposes_both_directions(module):
    assert hasattr(module, "check"), "no --check implementation"
    assert hasattr(module, "enforce"), "no --enforce implementation"
    src = MODULE_PATH.read_text(encoding="utf-8")
    assert "unexpectedly-enabled" in src, (
        "the check must flag a paused trigger found ENABLED — without it the "
        "manifest records a pause that nothing verifies"
    )
    assert "missing-in-aws" in src, (
        "the check must flag a manifest entry with no live trigger, or the "
        "manifest rots into a list of names that cannot fail"
    )


def test_scheduler_disable_round_trips_the_full_spec():
    """update-schedule is a full replace: a partial write silently drops targets."""
    src = MODULE_PATH.read_text(encoding="utf-8")
    assert "--cli-input-json" in src, (
        "scheduler disable must round-trip the live spec; passing only --state "
        "to update-schedule wipes Target, FlexibleTimeWindow and the timezone"
    )
    for derived in ("Arn", "CreationDate", "LastModificationDate"):
        assert derived in src, f"{derived} is not stripped before update-schedule"


# ── deploy-time enforcement (alpha-engine-config-I6619) ──────────────────────
#
# The manifest records the pause and automation_pause.py --check verifies it,
# but until 2026-08-07 nothing stopped a redeploy from lifting it: neither
# `aws events put-rule` nor `aws scheduler create-schedule|update-schedule`
# has a "leave the state alone" option, and BOTH default to ENABLED when
# --state is omitted. Every deploy.sh reconciling its own triggers therefore
# silently re-enabled whatever it owned.
#
# Two shapes had to be fixed, and the second is the one worth a test: an
# OMITTED --state (defaults ENABLED) and a HARDCODED --state literal. The
# hardcoded ones were worse — `eod-backstop/deploy.sh` pinned `--state
# DISABLED` for a rule Brian explicitly KEPT, so redeploying it would have
# silently turned off the postclose SF backstop.

_WRITE_VERBS = (
    "aws events put-rule",
    "aws scheduler create-schedule",
    "aws scheduler update-schedule",
)
PAUSE_LIB = INFRA / "lambdas" / "_shared" / "pause.sh"


def _deploy_scripts() -> list[Path]:
    return sorted(INFRA.glob("lambdas/*/deploy.sh")) + sorted(INFRA.glob("*.sh"))


def _statements(text: str):
    """Yield whole logical statements, backslash continuations joined.

    Load-bearing: `--name` and `--state` routinely sit on different physical
    lines, so a line-at-a-time check would pass a statement that omits --state.
    """
    lines = text.splitlines(keepends=True)
    i = 0
    while i < len(lines):
        if any(v in lines[i] for v in _WRITE_VERBS) and not lines[i].lstrip().startswith("#"):
            j = i
            while lines[j].rstrip().endswith("\\") and j + 1 < len(lines):
                j += 1
            yield i + 1, "".join(lines[i:j + 1])
            i = j + 1
        else:
            i += 1


def test_pause_helper_exists():
    assert PAUSE_LIB.is_file(), (
        "infrastructure/lambdas/_shared/pause.sh is gone — every deploy.sh "
        "sourcing it will fail at deploy time"
    )


def test_every_trigger_write_derives_state_from_the_manifest():
    """No omitted --state (defaults ENABLED) and no hardcoded literal."""
    offenders = []
    for script in _deploy_scripts():
        for lineno, stmt in _statements(script.read_text(encoding="utf-8")):
            if "pause_state" in stmt:
                continue
            rel = script.relative_to(REPO_ROOT)
            reason = "hardcoded --state" if "--state" in stmt else "no --state (defaults ENABLED)"
            offenders.append(f"{rel}:{lineno} — {reason}")
    assert not offenders, (
        "these EventBridge writes do not derive --state from "
        "automation_pause.json, so a redeploy silently changes a trigger's "
        "state:\n  " + "\n  ".join(offenders)
    )


def test_every_script_that_calls_pause_state_also_sources_the_helper():
    missing = []
    for script in _deploy_scripts():
        text = script.read_text(encoding="utf-8")
        if "pause_state" in text and "_shared/pause.sh" not in text:
            missing.append(str(script.relative_to(REPO_ROOT)))
    assert not missing, (
        f"{missing} call pause_state without sourcing the helper — the deploy "
        "fails at runtime, after the Lambda code has already been pushed"
    )


def test_pause_helper_fails_open_not_closed():
    """A missing manifest must yield ENABLED, never DISABLED.

    The asymmetry is deliberate. A pause that silently SPREADS would stop the
    weekly SF with no signal that a config file caused it; a pause that
    silently LIFTS is caught by automation_pause.py --check the next morning.
    Only one of those failure modes has a detector.
    """
    src = PAUSE_LIB.read_text(encoding="utf-8")
    assert 'echo "ENABLED"' in src, "no fail-open branch for an unreadable manifest"
    marker = src.index("if [ ! -r")
    assert 'echo "ENABLED"' in src[marker:marker + 200], (
        "the unreadable-manifest branch does not return ENABLED"
    )


def test_pause_helper_resolves_both_surfaces():
    """Live behaviour, not just source inspection."""
    manifest = json.loads((INFRA / "automation_pause.json").read_text(encoding="utf-8"))
    paused_rule = sorted(manifest["paused"]["events_rules"])[0]
    paused_sched = sorted(manifest["paused"]["scheduler_schedules"])[0]

    def _state(name: str) -> str:
        out = subprocess.run(
            ["bash", "-c", f'source "{PAUSE_LIB}"; pause_state "{name}"'],
            capture_output=True, text=True, check=True,
        )
        return out.stdout.strip()

    assert _state(paused_rule) == "DISABLED", paused_rule
    assert _state(paused_sched) == "DISABLED", paused_sched
    for kept in ("alpha-engine-saturday", "alpha-engine-weekday",
                 "alpha-engine-eod-backstop-daily"):
        assert _state(kept) == "ENABLED", f"{kept} is kept by the ruling"
    assert _state("a-rule-nobody-has-heard-of") == "ENABLED"


def test_pending_entries_are_paused_at_write_time_but_not_required_live(manifest, module):
    """`pending` = paused, for triggers that do not exist live yet.

    alpha-engine-config-I6620. nousergon-data#1207 declares three schedules its
    deploy would create ENABLED mid-pause. They cannot go in `paused` — the
    check requires those to exist live — but they must still be born DISABLED.

    **An empty `pending` block is the SUCCESS state, not a regression**
    (corrected 2026-08-17, alpha-engine-config-I7547). This test used to assert
    the block was non-empty, which made it pass for exactly as long as there was
    an un-graduated trigger and turn red the moment the block did its job — the
    previous message even prescribed the graduation it then failed. All four
    entries were created live DISABLED on 2026-08-14 and moved up to `paused` on
    2026-08-17, so the population is now zero. The MECHANISM is therefore
    exercised on a synthetic manifest below, where it is always testable; the
    live block is graded on the invariants that hold at any population, and the
    bash leg SKIPS rather than passing vacuously when there is nothing pending
    (a green assertion written over an empty list proves nothing).
    """
    synthetic = {
        "ruling": {"by": "Brian", "date": "2026-08-07", "statement": "x"},
        "not_paused": {},
        "pending": {"_why": "prose", "a-trigger-not-yet-created": "lands after the ruling"},
        "paused": {"events_rules": {"a-live-paused-rule": "off"}, "scheduler_schedules": {}},
    }
    assert module.pending_names(synthetic) == {"a-trigger-not-yet-created"}
    # paused_names answers "is DISABLED deliberate?" and must include pending.
    assert "a-trigger-not-yet-created" in module.paused_names(synthetic)
    # paused_entries answers "does it exist live and is it off?" and must not.
    assert {n for _, n, _ in module.paused_entries(synthetic)} == {"a-live-paused-rule"}

    pending = {k for k in manifest.get("pending", {}) if not k.startswith("_")}

    # --check must NOT require them to exist live. That obligation is carried
    # by paused_entries(), NOT by paused_names() — the latter deliberately
    # includes pending so the drift checkers do not flag a correctly-pending
    # trigger. See test_paused_names_includes_pending_but_check_does_not.
    entry_names = {name for _, name, _ in module.paused_entries(manifest)}
    assert not (pending & entry_names), (
        "a name is in BOTH pending and paused — --check would demand it exist "
        "live while the pending block exists precisely because it does not"
    )

    def _state(name: str) -> str:
        out = subprocess.run(
            ["bash", "-c", f'source "{PAUSE_LIB}"; pause_state "{name}"'],
            capture_output=True, text=True, check=True,
        )
        return out.stdout.strip()

    if not pending:
        pytest.skip(
            "the pending block is empty — every entry graduated to `paused` once it "
            "existed live, which is the block working. Skipped rather than passed: a "
            "loop over an empty list is a green assertion that proves nothing, and the "
            "born-DISABLED property has no live subject to assert it against right now. "
            "The Python-side mechanism is covered synthetically above."
        )
    for name in sorted(pending):
        assert _state(name) == "DISABLED", f"{name} would be created ENABLED"


def test_pending_notes_are_not_treated_as_trigger_names():
    """`_`-prefixed keys are prose, not triggers."""
    out = subprocess.run(
        ["bash", "-c", f'source "{PAUSE_LIB}"; pause_state "_why"'],
        capture_output=True, text=True, check=True,
    )
    assert out.stdout.strip() == "ENABLED"


def test_paused_names_includes_pending_but_check_does_not(manifest, module):
    """The two questions differ, and conflating them failed a deploy.

    2026-08-07: `pending` was wired into the bash helper but not here, so
    expense-collector correctly created its schedule DISABLED and its own
    post-deploy assertion (check-schedule-drift.py, which reads paused_names)
    then failed the deploy for the state it had just been told to write.

      paused_names()   -> "is DISABLED deliberate?"  MUST include pending
      paused_entries() -> "does it exist live and is it off?"  MUST NOT

    Graded on a SYNTHETIC manifest (corrected 2026-08-17,
    alpha-engine-config-I7547). It previously required the live `pending` block
    to be non-empty, which tied a test of two functions' semantics to a
    transient population and turned red when the block was correctly emptied.
    The distinction these two functions draw is a property of the code and is
    testable whether or not anything is pending today — and it is worth MORE
    when nothing is, because that is when a regression in it would go unnoticed.
    """
    synthetic = {
        "ruling": {"by": "Brian", "date": "2026-08-07", "statement": "x"},
        "not_paused": {},
        "pending": {"_why": "prose", "not-yet-live": "lands after the ruling"},
        "paused": {"events_rules": {"live-and-off": "off"}, "scheduler_schedules": {}},
    }
    pending = module.pending_names(synthetic)
    assert pending == {"not-yet-live"}

    names = module.paused_names(synthetic)
    assert pending <= names, (
        "paused_names() excludes pending, so every drift checker will report a "
        "correctly-pending trigger as drift and fail the deploy that created it"
    )

    entry_names = {name for _, name, _ in module.paused_entries(synthetic)}
    assert not (pending & entry_names), (
        "paused_entries() includes a pending name — automation_pause.py --check "
        "would demand it exist live, which is exactly why pending is separate"
    )

    # And the same invariant over the LIVE manifest, which holds at any
    # population including zero: the two blocks never name the same trigger.
    live_pending = module.pending_names(manifest)
    live_entries = {name for _, name, _ in module.paused_entries(manifest)}
    assert not (live_pending & live_entries)


def test_pending_notes_are_not_returned_as_names(module, manifest):
    assert not any(n.startswith("_") for n in module.pending_names(manifest))


# ── the kept half is now asserted, not merely documented (config#6613) ────────
#
# Until 2026-08-11 `not_paused` was prose: `check()` iterated only the paused
# entries, so a kept trigger that silently flipped to DISABLED produced no
# finding. That is the failure this file's own docstring names — "an entry that
# can never fail is not a record, it is a comment" — applied to the paused half
# and not the kept one. The expense collector is what made it matter: it is the
# only guard against the provider-credit exhaustion that took every autonomous
# lane dark for three days, and it had been cold since 2026-08-07.


def test_kept_names_excludes_prose_keys(module, manifest):
    names = module.kept_names(manifest)
    for key in manifest["not_paused"]:
        if key.startswith("_"):
            assert key not in names, f"{key} is prose and must not be probed as a trigger"
    assert names, "not_paused holds no real trigger names — the check would assert nothing"


def test_every_prose_key_uses_the_underscore_marker(manifest):
    # A prose key without the marker becomes a `kept-but-missing` finding, and a
    # trigger name WITH the marker silently drops out of the check. Both fail
    # quietly, so the convention is pinned here rather than left to review.
    #
    # A group-qualified Scheduler name (alpha-engine-config-I9994) lives
    # outside the default group, e.g. "crucible-v2/heartbeat" — the fleet's
    # own "alpha-"/"DO-NOT-DELETE-" prefix convention is a v1-pipeline habit,
    # not an AWS naming rule, so a non-default-group name is graded against
    # the ACTUAL EventBridge Scheduler naming constraint
    # (``[\.\-_A-Za-z0-9]+``, alphanumeric plus ``.-_``) instead of a
    # hand-listed prefix list that would need editing for every future group.
    aws_name_re = re.compile(r"^[.\-_A-Za-z0-9]+$")
    for key, reason in manifest["not_paused"].items():
        if key.startswith("_"):
            continue
        if "/" in key:
            group, _, bare = key.partition("/")
            assert group and aws_name_re.match(bare), (
                f"not_paused key {key!r} does not look like a group-qualified live "
                "trigger name (\"<group>/<name>\"); if it is a grouping label, "
                "prefix it with '_' as the pending block does"
            )
        else:
            # Same reasoning for a default-bus rule or default-group schedule:
            # graded against the AWS naming constraint, not a prefix list.
            # `crucible-v2-spot-interruption-redispatch` (2026-09-15) is a real
            # v2 EventBridge rule the old "alpha-" prefix test refused, so a
            # correct classification could not land. Prose still fails here —
            # it carries spaces.
            assert aws_name_re.match(key) and len(key) <= 64, (
                f"not_paused key {key!r} does not look like a live trigger name; if "
                "it is a grouping label, prefix it with '_' as the pending block does"
            )
        assert reason.strip(), f"{key} is kept with no stated reason"


def test_check_reports_a_kept_trigger_that_went_disabled(module, monkeypatch):
    kept = sorted(module.kept_names())[0]

    def fake(surface, name):
        if name == kept:
            return "DISABLED" if surface == "events" else None
        return "DISABLED"      # every paused entry is correctly off

    monkeypatch.setattr(module, "_live_state", fake)
    # config-I7174: check() now also reads live alarm-action state, which is an
    # AWS call. CI runs without credentials by design, so stub it here as the
    # alarm-aware tests already do — otherwise these two assert a trigger
    # finding and fail on a CloudWatch NoCredentials error instead.
    monkeypatch.setattr(module, "_alarm_actions_enabled", lambda name: False)
    findings = module.check()
    kinds = {(f["trigger"], f["kind"]) for f in findings}
    assert (kept, "kept-but-disabled") in kinds, (
        f"a kept trigger sitting DISABLED produced no finding: {findings}"
    )


def test_check_reports_a_kept_trigger_that_vanished(module, monkeypatch):
    kept = sorted(module.kept_names())[0]

    def fake(surface, name):
        if name == kept:
            return None        # exists on neither surface
        return "DISABLED"

    monkeypatch.setattr(module, "_live_state", fake)
    # config-I7174: check() now also reads live alarm-action state, which is an
    # AWS call. CI runs without credentials by design, so stub it here as the
    # alarm-aware tests already do — otherwise these two assert a trigger
    # finding and fail on a CloudWatch NoCredentials error instead.
    monkeypatch.setattr(module, "_alarm_actions_enabled", lambda name: False)
    findings = module.check()
    kinds = {(f["trigger"], f["kind"]) for f in findings}
    assert (kept, "kept-but-missing") in kinds, (
        f"a kept trigger that no longer exists produced no finding: {findings}"
    )


def test_check_is_silent_when_kept_triggers_are_enabled(module, monkeypatch):
    monkeypatch.setattr(
        module, "_live_state",
        lambda surface, name: "ENABLED" if name in module.kept_names() else "DISABLED")
    # Every paused_alarms entry watches only paused/pending triggers (asserted
    # elsewhere), so it is justified here; matching live state is already
    # ActionsEnabled=false.
    monkeypatch.setattr(module, "_alarm_actions_enabled", lambda name: False)
    assert module.check() == []


def test_enforce_never_touches_a_kept_trigger(module, monkeypatch):
    # enforce() may only ever DISABLE a trigger. If it learned to re-enable
    # one, this script would start scheduled work unattended — the one thing
    # the ruling forbids. (Alarm-action state is a different, safe-to-act-on
    # asymmetry — see test_enforce_can_both_disable_and_enable_alarm_actions.)
    disabled: list[str] = []
    monkeypatch.setattr(module, "_live_state", lambda surface, name: "ENABLED")
    monkeypatch.setattr(module, "_disable", lambda surface, name: disabled.append(name))
    # Already matching its justification (True, since watches are declared
    # paused regardless of the live-state monkeypatch above): no alarm AWS call.
    monkeypatch.setattr(module, "_alarm_actions_enabled", lambda name: False)
    monkeypatch.setattr(
        module, "_set_alarm_actions",
        lambda name, enabled: pytest.fail(f"unexpected alarm mutation on {name}"))
    module.enforce()
    assert not (set(disabled) & module.kept_names()), (
        f"enforce() disabled a deliberately-kept trigger: "
        f"{sorted(set(disabled) & module.kept_names())}"
    )


def test_the_expense_collector_is_kept_and_names_its_ruling(manifest, module):
    name = "alpha-engine-expense-collector-twicedaily"
    assert name in module.kept_names(manifest), (
        "the balance/spend collector is the only guard against the provider-credit "
        "exhaustion of config#6613; it must be in not_paused, not paused"
    )
    reason = manifest["not_paused"][name]
    assert "2026-08-11" in reason and "6613" in reason, (
        "an un-pause must name the ruling that authorised it, or the next pause "
        "audit cannot tell an exception from drift"
    )


def test_reconcile_monthly_is_paused_not_pending(manifest, module):
    # It exists live (verified DISABLED 2026-08-11), and `pending` asserts
    # neither existence nor state — only `paused` does.
    name = "alpha-engine-expense-collector-reconcile-monthly"
    assert name in manifest["paused"]["scheduler_schedules"]
    assert name not in module.pending_names(manifest)


# ── alpha-engine-config-I7174: paused_alarms owns alarm-action state ─────────
#
# A paused component's absence-alarm (treat_missing_data: breaching) cannot
# tell "gated off by declaration" from "upstream died" — nine alarms fired
# `OVERSEER BACKSTOP` pages on 2026-08-13 for exactly that reason. These tests
# assert: every declared alarm names a real reason and only checkable trigger
# names (never a name buried in prose, the `_reactive-notifier-rules` defect
# repeated); justification is derived live from paused_names(), never cached;
# check() is bidirectional (unexpectedly-enabled AND stale-disabled); and
# enforce() can act in BOTH directions for alarms specifically, unlike triggers.

# The nine declared when the block was created (alpha-engine-config-I7174).
# Asserted as a SUBSET below, never as equality. The original test demanded
# equality, which meant the manifest could not gain an entry without a test
# edit — and the four alarms it was missing were precisely the ones still paging
# on 2026-08-14. A frozen expected-set turns "the register grew" into a failure
# and "the register is incomplete" into a pass, which is backwards.
ORIGINAL_ALARM_NAMES = {
    "alpha-engine-watch-plane-alert-drain-liveness-probe-invocations-floor",
    "alpha-engine-watch-plane-canary-replay-liveness-probe-invocations-floor",
    "alpha-engine-watch-plane-ci-watch-liveness-probe-invocations-floor",
    "alpha-engine-watch-plane-sf-watch-liveness-probe-errors",
    "alpha-engine-watch-plane-sf-watch-liveness-probe-throttles",
    "alpha-engine-ssm-reachability-probe-dead",
    "alpha-engine-ssm-reachability-probe-unreachable",
    "alpha-engine-watch-plane-overseer-intake-age",
    "alpha-engine-watch-plane-overseer-intake-dlq-depth",
}


def test_the_original_pause_caused_alarms_are_still_declared(manifest, module):
    """Declared SOMEWHERE — `paused_alarms` or `armed_alarms`, either is a
    declaration.

    This used to require membership in `paused_alarms` specifically, which
    made a legitimate un-pause impossible: re-arming an alarm moves its entry
    to `armed_alarms` by design, and the assertion read that move as the
    alarm having lost its declaration. Hit live on 2026-08-21
    (alpha-engine-config-I8110) when the four alert-drain schedules came back
    and their three alarms were re-armed with them.

    It is the same defect the comment above ORIGINAL_ALARM_NAMES already
    records one axis over: a frozen set that turns a correct change into a
    failure. The property worth guarding is that an alarm silenced under
    I7174 never becomes UNCLASSIFIED — which is exactly what the two blocks
    together express, and what `--check` grades as `alarm-undeclared`.
    """
    names = ({e["name"] for e in module.alarm_entries(manifest)}
             | set(module.armed_alarm_names()))
    assert ORIGINAL_ALARM_NAMES <= names, (
        f"an alarm silenced under I7174 lost its declaration entirely — it is "
        f"in neither paused_alarms nor armed_alarms: "
        f"{ORIGINAL_ALARM_NAMES - names}"
    )


def test_the_four_alarms_that_paged_on_2026_08_14_are_declared(manifest, module):
    """The instance this issue was opened for (alpha-engine-config-I7023).

    These four have exactly the same shape as the nine above — breaching alarms
    on probes whose every trigger is paused — and were left out because the
    original list was built from what was firing that morning rather than from
    the class.
    """
    names = {e["name"] for e in module.alarm_entries(manifest)}
    for name in (
        "alpha-engine-watch-plane-overseer-liveness-probe-errors",
        "alpha-engine-watch-plane-overseer-liveness-probe-throttles",
        "alpha-engine-watch-plane-overseer-liveness-probe-invocations-floor",
        "alpha-engine-watch-plane-sf-watch-liveness-probe-invocations-floor",
    ):
        assert name in names, f"{name} is undeclared and will page again"


def test_the_three_deliberately_armed_alarms_are_not_declared(manifest, module):
    # These are real and unrelated to the pause; declaring them here would
    # silence an alarm that is correctly paging.
    armed = {
        "alpha-engine-dashboard-health-problems",
        "alpha-engine-eval-quality-regression",
        "router-degraded-mode-drill-uncovered-class",
    }
    names = {e["name"] for e in module.alarm_entries(manifest)}
    assert not (armed & names), f"an alarm that must stay armed is declared paused: {armed & names}"


def test_every_alarm_entry_has_a_reason_and_nonempty_watches(manifest, module):
    for entry in module.alarm_entries(manifest):
        assert entry["reason"].strip(), f"{entry['name']} has no reason"
        assert entry["watches"], (
            f"{entry['name']} watches nothing — a declaration that names no "
            "checkable trigger can never be graded"
        )


def test_every_watched_name_is_a_real_manifest_trigger(manifest, module):
    # Watches must resolve against paused_names() (paused ∪ pending). A name
    # that resolves against nothing is exactly the `_reactive-notifier-rules`
    # defect: a trigger name inside a value no checker reads.
    checkable = module.paused_names(manifest)
    for entry in module.alarm_entries(manifest):
        unknown = [w for w in entry["watches"] if w not in checkable]
        assert not unknown, f"{entry['name']} watches undeclared trigger(s): {unknown}"


def test_ci_watch_reclaim_legs_are_declared_for_its_alarm_to_watch(manifest, module):
    # The property is that both legs are DECLARED somewhere the alarm's
    # justification resolves — paused_names() is paused ∪ pending — not that
    # they sit in one particular block. Which block is a fact about AWS, and
    # it changed: measured 2026-08-13, neither rule exists live (the whole
    # ci-watch-liveness-probe component is codified in its deploy.sh and never
    # bootstrapped), so `paused` would make --check red with [missing-in-aws]
    # forever. `pending` is the block for exactly that, and pinning the
    # earlier block here turned a correct manifest correction into a red test.
    declared = module.paused_names(manifest)
    for name in ("alpha-engine-ci-watch-spot-interruption",
                 "alpha-engine-ci-watch-instance-terminated"):
        assert name in declared, (
            f"{name} is declared in neither `paused` nor `pending` — the "
            f"ci-watch alarm entry watches it, so its silencing would rest on "
            f"a name no checker reads"
        )


def test_alarm_justified_derives_live_from_paused_names_not_a_cached_flag(module):
    """The core un-pause property: lifting a pause changes the verdict on the
    NEXT read, with no second field to edit."""
    entry = {"name": "x", "reason": "r", "watches": ["some-trigger"]}
    still_paused = {"ruling": {"date": "2026-08-07"}, "not_paused": {}, "pending": {},
                     "paused": {"events_rules": {"some-trigger": "r"},
                                "scheduler_schedules": {}}}
    lifted = {"ruling": {"date": "2026-08-07"}, "not_paused": {}, "pending": {},
              "paused": {"events_rules": {}, "scheduler_schedules": {}}}
    assert module.alarm_justified(entry, still_paused) is True
    assert module.alarm_justified(entry, lifted) is False


def test_alarm_justified_requires_ALL_watched_triggers_paused(module):
    entry = {"name": "x", "reason": "r", "watches": ["a", "b"]}
    partial = {"ruling": {"date": "2026-08-07"}, "not_paused": {}, "pending": {},
               "paused": {"events_rules": {"a": "r"}, "scheduler_schedules": {}}}
    assert module.alarm_justified(entry, partial) is False


@pytest.fixture(autouse=True)
def classified_world(module, monkeypatch, request):
    """A live CloudWatch in which every breaching alarm is already classified.

    `check()` scans live alarms for coverage, so without this a test would shell
    out to `aws cloudwatch describe-alarms` and grade against whatever the real
    account happens to hold — passing on a developer laptop with credentials and
    failing in CI, which has none by design. Returns the dict so a test can
    mutate it to induce a coverage finding.

    AUTOUSE deliberately. I7174 added a live alarm read to `check()` and patched
    the three tests that noticed, one at a time; I7023 added another and the
    same three broke again the same way. The fixture makes the whole module
    unable to reach AWS, so the next live read added to `check()` cannot
    reintroduce this — the failure mode is a test file that silently depends on
    ambient credentials, not any one call.
    """
    live = {e["name"]: False for e in module.alarm_entries()}
    live.update({name: True for name in module.armed_alarm_names()})
    if request.node.get_closest_marker("real_alarm_scan") is None:
        # ONE stub, at the single primitive both populations derive from
        # (alpha-engine-config-I8047). Stubbing `_live_breaching_alarms` alone
        # left `_live_silenced_alarms` reaching the network the moment it was
        # added — the fixture's docstring above predicted exactly that, and the
        # fix is to stub the enumeration rather than each view of it.
        monkeypatch.setattr(
            module, "_live_alarm_actions",
            lambda: {n: {"enabled": e, "breaching": True} for n, e in live.items()})
    return live


def test_check_flags_a_justified_alarm_left_armed(
        module, manifest, monkeypatch, classified_world):
    """The bug this issue fixes, induced: paused trigger, ActionsEnabled=true."""
    monkeypatch.setattr(module, "_live_state", lambda surface, name: "DISABLED")
    # ActionsEnabled=true is graded here (alarm-unexpectedly-enabled) only for
    # a BREACHING alarm (alpha-engine-config-I8712) — `classified_world`
    # already marks every entry `breaching: True` by default, so setting
    # `enabled: True` on the live dict it backs is what induces the finding.
    declared = {e["name"] for e in module.alarm_entries(manifest)}
    assert declared, "paused_alarms is empty — this test would prove nothing"
    classified_world.update({name: True for name in declared})
    findings = module.check()
    kinds = {(f["trigger"], f["kind"]) for f in findings}
    # Read from the manifest's CURRENT paused_alarms, not a frozen name list.
    # With every trigger forced DISABLED, every entry in that block is
    # justified, so every one must be flagged — that is the property. Naming
    # three specific alarms made this fail the moment those three were
    # legitimately re-armed (alpha-engine-config-I8110).
    for name in declared:
        assert (name, "alarm-unexpectedly-enabled") in kinds, findings


def test_check_flags_a_stale_disabled_alarm_after_its_pause_lifts(
        module, monkeypatch, classified_world):
    """The failure this issue exists to prevent: pause lifted, alarm never re-armed."""
    lifted = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
    del lifted["paused"]["scheduler_schedules"]["alpha-engine-sf-watch-liveness-0645-daily"]
    del lifted["paused"]["scheduler_schedules"]["alpha-engine-sf-watch-liveness-1445-daily"]
    monkeypatch.setattr(module, "load_manifest", lambda: lifted)
    monkeypatch.setattr(module, "_live_state", lambda surface, name: "DISABLED")
    monkeypatch.setattr(module, "_alarm_actions_enabled", lambda name: False)
    findings = module.check()
    kinds = {(f["trigger"], f["kind"]) for f in findings}
    assert ("alpha-engine-watch-plane-sf-watch-liveness-probe-errors",
            "alarm-stale-disabled") in kinds, findings
    assert ("alpha-engine-watch-plane-sf-watch-liveness-probe-throttles",
            "alarm-stale-disabled") in kinds, findings


def test_check_is_silent_on_alarms_whose_state_matches_justification(
        module, monkeypatch, classified_world):
    monkeypatch.setattr(module, "_live_state", lambda surface, name: "DISABLED")
    monkeypatch.setattr(module, "_alarm_actions_enabled", lambda name: False)
    findings = module.check()
    assert not [f for f in findings if f["surface"] == "cloudwatch"], findings


# ── Completeness: every breaching alarm is classified (config-I7023) ────────


def test_every_armed_alarm_entry_carries_a_reason(manifest, module):
    for name in module.armed_alarm_names(manifest):
        reason = manifest["armed_alarms"][name].get("reason", "")
        assert reason.strip(), f"{name} is declared armed with no reason"


def test_no_alarm_is_declared_both_paused_and_armed(manifest, module):
    """The two blocks are a partition, not two lists that may overlap.

    An alarm in both is a contradiction the reconciler would resolve silently in
    favour of whichever block it reads first.
    """
    paused = {e["name"] for e in module.alarm_entries(manifest)}
    both = paused & module.armed_alarm_names(manifest)
    assert not both, f"declared as both silenced and armed: {sorted(both)}"


def test_an_undeclared_breaching_alarm_is_a_finding(module, monkeypatch,
                                                    classified_world):
    """The defect this check exists for: a new absence-alarm nobody classified."""
    monkeypatch.setattr(module, "_live_state", lambda surface, name: "DISABLED")
    monkeypatch.setattr(module, "_alarm_actions_enabled", lambda name: False)
    classified_world["alpha-engine-brand-new-probe-dead"] = True
    kinds = {(f["trigger"], f["kind"]) for f in module.check()}
    assert ("alpha-engine-brand-new-probe-dead", "alarm-undeclared") in kinds


def test_a_hand_muted_armed_alarm_is_a_finding(module, monkeypatch, classified_world):
    """A detector muted with no declaration is indistinguishable from a healthy one."""
    monkeypatch.setattr(module, "_live_state", lambda surface, name: "DISABLED")
    monkeypatch.setattr(module, "_alarm_actions_enabled", lambda name: False)
    victim = sorted(module.armed_alarm_names())[0]
    classified_world[victim] = False
    kinds = {(f["trigger"], f["kind"]) for f in module.check()}
    assert (victim, "armed-but-silenced") in kinds


def test_an_armed_alarm_that_vanished_is_a_finding(module, monkeypatch,
                                                   classified_world):
    monkeypatch.setattr(module, "_live_state", lambda surface, name: "DISABLED")
    monkeypatch.setattr(module, "_alarm_actions_enabled", lambda name: False)
    victim = sorted(module.armed_alarm_names())[0]
    del classified_world[victim]
    kinds = {(f["trigger"], f["kind"]) for f in module.check()}
    assert (victim, "armed-missing-in-aws") in kinds


@pytest.mark.real_alarm_scan
def test_describe_alarms_pagination_is_followed(module, monkeypatch):
    """A truncated first page would silently shrink the population being graded.

    Opts out of the autouse stub — this is the one test whose subject IS the
    live enumeration. It fakes `_aws` instead, so it still never reaches the
    network.
    """
    pages = [
        ('{"a": [{"n": "a1", "e": true, "b": "breaching"}], "t": "tok"}', None),
        ('{"a": [{"n": "a2", "e": false, "b": "breaching"}], "t": null}', "tok"),
    ]
    seen: list[str | None] = []

    def fake_aws(args):
        token = args[args.index("--starting-token") + 1] if "--starting-token" in args else None
        seen.append(token)
        body = next(b for b, t in pages if t == token)
        return 0, body, ""

    monkeypatch.setattr(module, "_aws", fake_aws)
    assert module._live_breaching_alarms() == {"a1": True, "a2": False}
    assert seen == [None, "tok"], "the second page was never requested"


def test_enforce_can_both_disable_and_enable_alarm_actions(module, monkeypatch):
    """Unlike triggers, enforce() may act in BOTH directions for alarms — this
    is the mechanism that re-arms an alarm the same run a pause lifts, with no
    separate AWS CLI command."""
    lifted = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
    del lifted["paused"]["scheduler_schedules"]["alpha-engine-sf-watch-liveness-0645-daily"]
    del lifted["paused"]["scheduler_schedules"]["alpha-engine-sf-watch-liveness-1445-daily"]
    monkeypatch.setattr(module, "load_manifest", lambda: lifted)
    monkeypatch.setattr(module, "_live_state", lambda surface, name: "DISABLED")
    monkeypatch.setattr(module, "_alarm_actions_enabled", lambda name: False)
    acted: list[tuple[str, bool]] = []
    monkeypatch.setattr(
        module, "_set_alarm_actions",
        lambda name, enabled: acted.append((name, enabled)))
    module.enforce(alarms_only=True)
    acted_names = {n for n, _ in acted}
    assert "alpha-engine-watch-plane-sf-watch-liveness-probe-errors" in acted_names
    assert ("alpha-engine-watch-plane-sf-watch-liveness-probe-errors", True) in acted, (
        "the un-paused entry's alarm must be RE-ENABLED, not disabled again"
    )


def test_enforce_alarms_only_never_disables_a_trigger(module, monkeypatch):
    disabled: list[str] = []
    monkeypatch.setattr(module, "_live_state", lambda surface, name: "ENABLED")
    monkeypatch.setattr(module, "_disable", lambda surface, name: disabled.append(name))
    monkeypatch.setattr(module, "_alarm_actions_enabled", lambda name: False)
    monkeypatch.setattr(module, "_set_alarm_actions", lambda name, enabled: None)
    module.enforce(alarms_only=True)
    assert disabled == [], "enforce(alarms_only=True) touched a trigger"


def test_ci_reconciles_alarm_actions_alarms_only(module):
    wf = WORKFLOW.read_text(encoding="utf-8")
    assert "automation_pause.py --enforce --alarms-only" in wf, (
        "the daily/on-push sweep does not reconcile alarm-action state, so an "
        "un-pause never re-arms its alarm without a hand-run command"
    )


def test_alarms_are_silenced_not_deleted(module):
    # Property 1: history/config survive. The mutation surface must never
    # contain a delete verb for an alarm.
    import inspect
    src = inspect.getsource(module)
    assert "delete-alarms" not in src, (
        "a paused alarm must be silenced (disable-alarm-actions), never deleted"
    )


# ── the suppression's REASON, not its resource (alpha-engine-config-I8047) ───
#
# Brian ruled option (b) on 2026-08-21: the fourteen alarms he disabled by hand
# on 2026-08-13/14 stay muted, and the mute becomes a DECLARED, SELF-EXPIRING
# suppression rather than an undeclared one. `watches` already grades the
# resource condition. These grade the two things that make the REASON
# gradeable, which is the condition that actually rots (-I8090).


def test_every_paused_alarm_names_an_owning_issue_and_a_re_exam_date(manifest, module):
    """The data half of the ruling. Without both fields the sweep that grades
    them against the clock and the tracker is satisfied vacuously."""
    for entry in module.alarm_entries(manifest):
        assert module.DECLARATION_ISSUE_RE.match(entry["issue"]), (
            f"{entry['name']} carries issue={entry['issue']!r}; a suppression whose "
            f"owner cannot be resolved can never be found stale"
        )
        assert module.DECLARATION_DATE_RE.match(entry["re_exam"]), (
            f"{entry['name']} carries re_exam={entry['re_exam']!r}; a suppression "
            f"with no expiry outlives its justification silently"
        )


def test_the_fourteen_hand_muted_alarms_are_all_declared(manifest, module):
    """The exact set measured live on 2026-08-21 via
    `describe-alarms --query 'MetricAlarms[?ActionsEnabled==`false`]'`, and the
    set CloudTrail shows Brian disabling on 2026-08-13/14. Pinned literally so a
    silent shrink of the declaration set is a test failure rather than a smaller
    number in a report."""
    hand_muted = {
        "alpha-engine-ssm-reachability-probe-dead",
        "alpha-engine-ssm-reachability-probe-unreachable",
        "alpha-engine-watch-plane-alert-drain-liveness-probe-invocations-floor",
        "alpha-engine-watch-plane-canary-replay-liveness-probe-invocations-floor",
        "alpha-engine-watch-plane-ci-watch-liveness-probe-invocations-floor",
        "alpha-engine-watch-plane-overseer-intake-age",
        "alpha-engine-watch-plane-overseer-intake-dlq-depth",
        "alpha-engine-watch-plane-overseer-intake-dlq-severe-content",
        "alpha-engine-watch-plane-overseer-liveness-probe-errors",
        "alpha-engine-watch-plane-overseer-liveness-probe-invocations-floor",
        "alpha-engine-watch-plane-overseer-liveness-probe-throttles",
        "alpha-engine-watch-plane-sf-watch-liveness-probe-errors",
        "alpha-engine-watch-plane-sf-watch-liveness-probe-invocations-floor",
        "alpha-engine-watch-plane-sf-watch-liveness-probe-throttles",
    }
    # Declared in EITHER block. The invariant the ruling is protecting is that
    # a suppression never becomes UNDECLARED — not that these fourteen stay
    # suppressed forever. Re-arming an alarm moves its entry to
    # `armed_alarms`, and that move is the count going down BECAUSE THE
    # CONDITION CLEARED, which is the one way this test must not forbid.
    #
    # Requiring `paused_alarms` specifically made a correct re-arm red: on
    # 2026-08-21 the four alert-drain schedules came back and their four
    # alarms were re-armed live and moved here (alpha-engine-config-I8110).
    #
    # The teeth are unchanged, because the only route out of `paused_alarms`
    # is a declaration in `armed_alarms`, and `--check` then grades that live
    # as `armed-but-silenced` if the alarm is in fact still muted. A silent
    # shrink still cannot happen; it just is not spelled with a frozen block
    # name any more.
    paused = {e["name"] for e in module.alarm_entries(manifest)}
    armed = set(module.armed_alarm_names())
    undeclared = hand_muted - paused - armed
    assert not undeclared, (
        f"a hand-muted alarm lost its declaration entirely — in neither "
        f"paused_alarms nor armed_alarms: {sorted(undeclared)}"
    )


def test_no_alarm_outside_the_measured_set_was_declared(manifest, module):
    """The other direction, and the one the ruling is emphatic about. Declaring
    a suppression is how a count goes down without a condition clearing;
    `alpha-engine-eval-quality-regression` (latched 106 days, ARMED, the live
    surface for -I7038) and `alpha-engine-saturday-sf-failed` (true, first
    correct reading in 53 days) must never appear here."""
    declared = {e["name"] for e in module.alarm_entries(manifest)}
    for never in ("alpha-engine-eval-quality-regression",
                  "alpha-engine-saturday-sf-failed",
                  "alpha-engine-weekday-sf-failed",
                  "alpha-engine-eod-sf-failed",
                  "router-degraded-mode-drill-uncovered-class",
                  "alpha-engine-director-plan-latency"):
        assert never not in declared, (
            f"{never} is an ARMED alarm reporting a real condition; declaring it "
            f"suppressed would clear a count by silencing the finding"
        )


def _mutated(field: str, value):
    """The manifest with one paused_alarms entry's declaration broken."""
    m = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
    m["paused_alarms"]["alpha-engine-watch-plane-sf-watch-liveness-probe-errors"][field] = value
    return m


def test_a_declaration_with_no_owning_issue_is_a_finding(module):
    """RED proof 1 — an unowned declaration cannot be graded against the
    tracker, so it can never be found stale."""
    findings = module.declaration_findings(_mutated("issue", ""))
    assert ("alpha-engine-watch-plane-sf-watch-liveness-probe-errors",
            "alarm-declaration-unowned") in {(f["trigger"], f["kind"]) for f in findings}


def test_a_declaration_with_a_free_text_owner_is_a_finding(module):
    """A free-text owner is the defect one step earlier: it LOOKS owned. Same
    shape as the `Pause-owner:` line -I7524 had to make explicit after prose
    matching declared 40 of 48 triggers owned and 0 latched."""
    findings = module.declaration_findings(_mutated("issue", "tracked by Brian"))
    assert "alarm-declaration-unowned" in {f["kind"] for f in findings}


def test_a_declaration_with_no_re_exam_date_is_a_finding(module):
    """RED proof 2 — no date means nothing can ever be past it."""
    findings = module.declaration_findings(_mutated("re_exam", ""))
    assert ("alpha-engine-watch-plane-sf-watch-liveness-probe-errors",
            "alarm-declaration-undated") in {(f["trigger"], f["kind"]) for f in findings}


def test_a_re_exam_that_is_not_a_real_date_is_a_finding(module):
    findings = module.declaration_findings(_mutated("re_exam", "2026-02-31"))
    assert "alarm-declaration-undated" in {f["kind"] for f in findings}


def test_the_live_manifest_produces_no_declaration_findings(module):
    """The GREEN direction, asserted explicitly: every one of the fourteen
    carries a well-formed declaration today."""
    assert module.declaration_findings() == []


def test_declaration_findings_never_move_live_state(module, monkeypatch):
    """A suppression whose reason expired must be REPORTED, never auto-re-armed.

    `enforce()` re-enables an alarm whose `watches` justification lapsed, and
    that is safe: the same manifest edit that restores the trigger makes it
    happen. An expiry is different — it is a calendar tick, and re-arming the
    response plane's own liveness alarms on one would undo a ruling of Brian's
    unattended (`principles.md` 2.5). So the declaration predicate is
    deliberately absent from `alarm_justified()` and therefore from `enforce()`.
    """
    broken = _mutated("re_exam", "1999-01-01")
    monkeypatch.setattr(module, "load_manifest", lambda: broken)
    monkeypatch.setattr(module, "_live_state", lambda surface, name: "DISABLED")
    monkeypatch.setattr(module, "_alarm_actions_enabled", lambda name: False)
    acted: list = []
    monkeypatch.setattr(module, "_set_alarm_actions",
                        lambda name, enabled: acted.append((name, enabled)))
    module.enforce(alarms_only=True)
    assert acted == [], (
        "an expired declaration re-armed an alarm unattended — that is Brian's "
        "ruling to revisit, not the enforcer's"
    )


# ── an UNDECLARED mute, of any alarm, is still a defect ─────────────────────


def test_a_hand_muted_undeclared_alarm_is_a_finding(module, monkeypatch):
    """RED proof 4 — the half of the ruling that must not weaken."""
    world = {e["name"]: {"enabled": False, "breaching": True}
             for e in module.alarm_entries()}
    world.update({n: {"enabled": True, "breaching": True}
                  for n in module.armed_alarm_names()})
    world["alpha-engine-some-new-detector"] = {"enabled": False, "breaching": False}
    monkeypatch.setattr(module, "_live_alarm_actions", lambda: world)
    kinds = {(f["trigger"], f["kind"]) for f in module.alarm_coverage_findings()}
    assert ("alpha-engine-some-new-detector", "alarm-undeclared-silence") in kinds, kinds


def test_the_undeclared_silence_scan_is_not_limited_to_breaching_alarms(module, monkeypatch):
    """The precise gap -I8047 closes. Nine of the fourteen alarms Brian muted
    are `notBreaching`; the pre-existing completeness scan enumerated only
    `TreatMissingData=breaching`, so an undeclared mute of any of them was
    invisible to every check in the fleet."""
    world = {e["name"]: {"enabled": False, "breaching": True}
             for e in module.alarm_entries()}
    world.update({n: {"enabled": True, "breaching": True}
                  for n in module.armed_alarm_names()})
    world["alpha-engine-quiet-mute"] = {"enabled": False, "breaching": False}
    monkeypatch.setattr(module, "_live_alarm_actions", lambda: world)
    findings = module.alarm_coverage_findings()
    assert {"alpha-engine-quiet-mute"} == {
        f["trigger"] for f in findings if f["kind"] == "alarm-undeclared-silence"}
    assert not [f for f in findings if f["kind"] == "alarm-undeclared"], (
        "the breaching scan must not also report it — one mute, one finding"
    )


def test_an_armed_alarm_is_never_reported_as_an_undeclared_silence(module, monkeypatch):
    """`armed-but-silenced` already owns the armed block; two findings for one
    condition is how a report gets skimmed."""
    world = {e["name"]: {"enabled": False, "breaching": True}
             for e in module.alarm_entries()}
    world.update({n: {"enabled": False, "breaching": True}
                  for n in module.armed_alarm_names()})
    monkeypatch.setattr(module, "_live_alarm_actions", lambda: world)
    findings = module.alarm_coverage_findings()
    assert not [f for f in findings if f["kind"] == "alarm-undeclared-silence"]
    assert {f["kind"] for f in findings} == {"armed-but-silenced"}


# ── Completeness: every live trigger CLASSIFIED, not merely every declared
# one (alpha-engine-config-I9959) ────────────────────────────────────────────
#
# `check()`'s other findings all iterate the manifest's OWN hand-listed names
# (`paused_entries()`, `kept_names()`), so a live, ENABLED trigger named in
# NEITHER block was invisible to every one of them — the check ran daily,
# reported, and could not see the case it most needed to. Found by hand on
# 2026-09-03 for exactly two triggers (alpha-engine-preflight-sweep-daily,
# alpha-engine-preopen-deploy-readiness-probe; alpha-engine-config-I9937) —
# nothing would have found a third.


def test_trigger_coverage_is_silent_when_live_matches_the_manifest_exactly(
        module, manifest):
    """The GREEN direction: the live world `clean_trigger_world` builds from
    the manifest itself must produce no trigger-side finding."""
    findings = module.trigger_coverage_findings(manifest)
    assert not [f for f in findings
                if f["kind"] in ("trigger-undeclared", "trigger-out-of-scope")]


def test_an_undeclared_enabled_trigger_is_a_finding(module, manifest):
    """The defect this check exists for: a new trigger nobody classified."""
    live = [{"surface": "events", "name": "alpha-engine-saturday", "state": "ENABLED"},
            {"surface": "events", "name": "alpha-engine-brand-new-undeclared-rule",
             "state": "ENABLED"}]
    findings = module.trigger_coverage_findings(manifest, triggers=live)
    kinds = {(f["trigger"], f["kind"]) for f in findings}
    assert ("alpha-engine-brand-new-undeclared-rule", "trigger-undeclared") in kinds
    assert not any(f["trigger"] == "alpha-engine-saturday" for f in findings)


def test_an_undeclared_disabled_trigger_is_not_a_finding(module, manifest):
    """Only ENABLED is graded here — a disabled, undeclared trigger is dormant
    and carries no scheduled-work risk; `missing-in-aws`/`kept-but-missing`
    already cover the declared half of this axis."""
    live = [{"surface": "scheduler", "name": "some-random-disabled-thing",
             "state": "DISABLED"}]
    assert module.trigger_coverage_findings(manifest, triggers=live) == []


def test_an_aws_managed_rule_is_reported_out_of_scope_not_silently_dropped(
        module, manifest):
    live = [{"surface": "events",
             "name": "DO-NOT-DELETE-AmazonInspectorEcrManagedRule",
             "state": "ENABLED"}]
    findings = module.trigger_coverage_findings(manifest, triggers=live)
    kinds = {(f["trigger"], f["kind"]) for f in findings}
    assert ("DO-NOT-DELETE-AmazonInspectorEcrManagedRule", "trigger-out-of-scope") in kinds
    assert not any(f["kind"] == "trigger-undeclared" for f in findings), (
        "an AWS-managed rule must never ALSO read as undeclared — one "
        "condition, one finding"
    )


def test_check_surfaces_trigger_coverage_findings(module, monkeypatch, manifest):
    """The wiring: `check()` must actually call the new completeness scan,
    not just have it exist as a dead function."""
    monkeypatch.setattr(
        module, "_live_state",
        lambda surface, name: "DISABLED" if name in module.paused_names(manifest) else "ENABLED")
    monkeypatch.setattr(module, "_alarm_actions_enabled", lambda name: False)
    monkeypatch.setattr(module, "_live_triggers", lambda: [
        {"surface": "events", "name": "alpha-engine-mystery-rule", "state": "ENABLED"},
    ])
    kinds = {(f["trigger"], f["kind"]) for f in module.check()}
    assert ("alpha-engine-mystery-rule", "trigger-undeclared") in kinds


# ── §7.4 RED proof: the finding must fail without the fix, and clear when the
# manifest is restored (alpha-engine-config-I9959) ───────────────────────────


def test_removing_a_kept_entrys_declaration_from_a_fixture_manifest_is_caught(
        module, manifest):
    """Mutates a FIXTURE COPY only — never MANIFEST_PATH / the real file."""
    import copy

    fixture = copy.deepcopy(manifest)
    victim = "alpha-engine-preflight-sweep-daily"
    assert victim in fixture["not_paused"], (
        "fixture setup is wrong: the victim must be a real, currently-declared "
        "kept trigger for this to be a meaningful RED proof"
    )
    del fixture["not_paused"][victim]
    assert victim not in fixture["not_paused"], (
        "the deletion did not change the fixture — a demonstration that "
        "mutated nothing would prove nothing"
    )
    # The rest of the manifest is untouched — same object graph, one key gone.
    assert fixture["paused"] == manifest["paused"]

    live = [{"surface": "events", "name": victim, "state": "ENABLED"}]

    # RED: with the entry gone, the live-enabled trigger must be reported.
    red = module.trigger_coverage_findings(fixture, triggers=live)
    red_kinds = {(f["trigger"], f["kind"]) for f in red}
    assert (victim, "trigger-undeclared") in red_kinds, (
        f"removing {victim!r}'s declaration did not surface it as undeclared: {red}"
    )

    # GREEN: the ORIGINAL (unmutated) manifest still declares it — restoring
    # the entry (i.e. going back to `manifest` instead of `fixture`) must
    # clear the finding for the same live state.
    green = module.trigger_coverage_findings(manifest, triggers=live)
    assert not [f for f in green if f["trigger"] == victim], (
        f"the unmutated manifest still declares {victim!r}, but it was still "
        f"reported: {green}"
    )


# ── the enumeration itself: pagination and fail-loud (alpha-engine-config-I9959) ─


@pytest.mark.real_trigger_scan
def test_live_triggers_pagination_is_followed(module, monkeypatch):
    """A truncated first page would silently shrink the population an
    undeclared trigger could hide behind — the `aws --no-paginate` failure
    mode (first page only, no marker) this loop must never reproduce."""
    pages = {
        ("events", None): '{"Rules": [{"Name": "r1", "State": "ENABLED"}], "NextToken": "tok1"}',
        ("events", "tok1"): '{"Rules": [{"Name": "r2", "State": "DISABLED"}], "NextToken": null}',
        ("scheduler", None): '{"Schedules": [{"Name": "s1", "State": "ENABLED"}], "NextToken": null}',
    }
    seen: list[tuple[str, str | None]] = []

    def fake_aws(args):
        surface = "events" if "list-rules" in args else "scheduler"
        token = args[args.index("--next-token") + 1] if "--next-token" in args else None
        seen.append((surface, token))
        return 0, pages[(surface, token)], ""

    monkeypatch.setattr(module, "_aws", fake_aws)
    triggers = module._live_triggers()
    got = {(t["surface"], t["name"], t["state"]) for t in triggers}
    assert got == {
        ("events", "r1", "ENABLED"),
        ("events", "r2", "DISABLED"),
        ("scheduler", "s1", "ENABLED"),
    }
    assert ("events", "tok1") in seen, "the second page of events:list-rules was never requested"


@pytest.mark.real_trigger_scan
def test_live_triggers_raises_loud_on_an_aws_failure(module, monkeypatch):
    """A partial listing must never be reported as 'every trigger checked' —
    RAISE, don't return whatever page came back."""
    monkeypatch.setattr(module, "_aws", lambda args: (255, "", "AccessDeniedException"))
    with pytest.raises(RuntimeError, match="AccessDeniedException"):
        module._live_triggers()


def test_scope_carve_out_matches_pause_reconcile(module):
    """One exclusion, declared once each in two files that must not drift.

    `pause_reconcile.py`'s own account-wide enumeration excludes AWS-managed
    rules under `_AWS_MANAGED_PREFIX`; this module's `_live_triggers()`-based
    completeness scan excludes the same population under
    `AWS_MANAGED_TRIGGER_PREFIX`. Two literals for one exclusion is exactly
    how the `_reactive-notifier-rules` class of defect (a value no checker
    reads) reappears one level up — pinned here so the two cannot diverge
    silently.
    """
    pytest.importorskip("boto3")
    pytest.importorskip("yaml")
    spec = importlib.util.spec_from_file_location("pause_reconcile", INFRA / "pause_reconcile.py")
    pause_reconcile = importlib.util.module_from_spec(spec)
    sys.modules.setdefault("pause_reconcile", pause_reconcile)
    spec.loader.exec_module(pause_reconcile)
    assert module.AWS_MANAGED_TRIGGER_PREFIX == pause_reconcile._AWS_MANAGED_PREFIX


# ── Scheduler group qualification (alpha-engine-config-I9994) ───────────────
#
# `get-schedule`/`update-schedule` only ever look in ONE Scheduler group
# (`--group-name`, default "default") — unlike `list-schedules`, which
# enumerates every group unasked. `_live_triggers()` reports a non-default
# schedule as "<group>/<name>"; `_live_state()`/`_disable()` must split that
# back apart and pass `--group-name`, or a real, live, ENABLED schedule in a
# non-default group reads as `kept-but-missing` / cannot be found at all —
# exactly the finding class this issue was filed to close for the crucible-v2
# group's four schedules.


def test_scheduler_name_splitting(module):
    assert module._split_scheduler_name("heartbeat") == ("default", "heartbeat")
    assert module._split_scheduler_name("crucible-v2/heartbeat") == ("crucible-v2", "heartbeat")


def test_live_state_passes_group_name_for_a_qualified_scheduler_name(module, monkeypatch):
    """RED without the fix: `get-schedule --name heartbeat` (no --group-name)
    404s for a schedule that lives in a non-default group — this asserts the
    group is actually passed through, not just that a name is split somewhere."""
    seen = []

    def fake_aws(args):
        seen.append(args)
        return 0, "ENABLED", ""

    monkeypatch.setattr(module, "_aws", fake_aws)
    state = module._live_state("scheduler", "crucible-v2/heartbeat")
    assert state == "ENABLED"
    (args,) = seen
    assert "--name" in args and args[args.index("--name") + 1] == "heartbeat", (
        f"the bare name must be passed to get-schedule, not the qualified form: {args}"
    )
    assert "--group-name" in args and args[args.index("--group-name") + 1] == "crucible-v2", (
        f"a non-default-group schedule must pass --group-name or AWS looks in "
        f"the wrong group: {args}"
    )


def test_live_state_omits_group_name_for_the_default_group(module, monkeypatch):
    """The overwhelming majority of this manifest's scheduler entries are
    unqualified default-group names — this must keep working exactly as
    before, with no --group-name flag added where none was ever needed."""
    seen = []

    def fake_aws(args):
        seen.append(args)
        return 0, "DISABLED", ""

    monkeypatch.setattr(module, "_aws", fake_aws)
    module._live_state("scheduler", "alpha-engine-stop-trading")
    (args,) = seen
    assert "--group-name" not in args


def test_live_state_never_asks_events_for_a_group_qualified_name(module, monkeypatch):
    """A "/" is not a legal EventBridge rule name — describe-rule would 400
    (ValidationException), not 404, so `kept_names()`'s try-both-surfaces loop
    must recognize this case without ever calling AWS."""
    def fail_if_called(args):
        raise AssertionError(f"describe-rule must not be called for a group-qualified "
                              f"name: {args}")

    monkeypatch.setattr(module, "_aws", fail_if_called)
    assert module._live_state("events", "crucible-v2/heartbeat") is None


def test_live_triggers_qualifies_a_non_default_scheduler_group(module, monkeypatch):
    """RED without the fix: `_live_triggers()` reported the bare `Name` for
    every schedule regardless of `GroupName`, so a crucible-v2 schedule and a
    same-named default-group schedule would have been indistinguishable to
    every consumer of this list — including `trigger_coverage_findings()`,
    which matches purely on the reported name."""
    pages = {
        ("scheduler", None): json.dumps({
            "Schedules": [
                {"Name": "heartbeat", "State": "ENABLED", "GroupName": "crucible-v2"},
                {"Name": "alpha-engine-stop-trading", "State": "ENABLED", "GroupName": "default"},
            ],
            "NextToken": None,
        }),
        ("events", None): '{"Rules": [], "NextToken": null}',
    }

    def fake_aws(args):
        surface = "events" if "list-rules" in args else "scheduler"
        return 0, pages[(surface, None)], ""

    monkeypatch.setattr(module, "_aws", fake_aws)
    names = {t["name"] for t in module._live_triggers()}
    assert "crucible-v2/heartbeat" in names, (
        "a non-default-group schedule must be reported group-qualified"
    )
    assert "alpha-engine-stop-trading" in names, (
        "a default-group schedule must stay unqualified — this manifest's "
        "existing entries must not need a mass rewrite"
    )
    assert "heartbeat" not in names, (
        "the bare, unqualified name must never appear for a non-default-group "
        "schedule — it would silently match a same-named default-group entry"
    )


def test_group_qualified_kept_schedule_is_not_a_kept_but_missing_finding(
        module, monkeypatch):
    """End-to-end RED/GREEN proof at the `check()` level for the actual defect
    class this issue closes: a crucible-v2 schedule declared `not_paused` must
    resolve live via _live_state, not read as vanished."""
    import copy

    fixture = copy.deepcopy(module.load_manifest())
    fixture["not_paused"]["crucible-v2/heartbeat"] = "test fixture entry"

    def fake_live_state(surface, name):
        if name == "crucible-v2/heartbeat":
            # Only the scheduler surface, with the group split correctly, can
            # see this schedule — exactly what the live AWS API does.
            group, bare = module._split_scheduler_name(name)
            if surface == "scheduler" and group == "crucible-v2" and bare == "heartbeat":
                return "ENABLED"
            return None
        return "DISABLED" if name in module.paused_names(fixture) else "ENABLED"

    monkeypatch.setattr(module, "load_manifest", lambda: fixture)
    monkeypatch.setattr(module, "_live_state", fake_live_state)
    monkeypatch.setattr(module, "_alarm_actions_enabled", lambda name: False)
    monkeypatch.setattr(module, "_live_triggers", lambda: [])

    findings = module.check()
    bad = [f for f in findings if f["trigger"] == "crucible-v2/heartbeat"]
    assert bad == [], (
        f"a live, ENABLED, group-qualified kept schedule was reported wrong: {bad}"
    )
