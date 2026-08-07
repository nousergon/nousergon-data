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
    """
    pending = {k for k in manifest.get("pending", {}) if not k.startswith("_")}
    assert pending, "the pending block is empty; if #1207's schedules now exist live, move them to paused"

    # --check must NOT require them to exist live.
    assert not (pending & module.paused_names(manifest)), (
        "a name is in BOTH pending and paused — --check would demand it exist "
        "live while the pending block exists precisely because it does not"
    )

    def _state(name: str) -> str:
        out = subprocess.run(
            ["bash", "-c", f'source "{PAUSE_LIB}"; pause_state "{name}"'],
            capture_output=True, text=True, check=True,
        )
        return out.stdout.strip()

    for name in sorted(pending):
        assert _state(name) == "DISABLED", f"{name} would be created ENABLED"


def test_pending_notes_are_not_treated_as_trigger_names():
    """`_`-prefixed keys are prose, not triggers."""
    out = subprocess.run(
        ["bash", "-c", f'source "{PAUSE_LIB}"; pause_state "_why"'],
        capture_output=True, text=True, check=True,
    )
    assert out.stdout.strip() == "ENABLED"
