"""The cadence run's pit_parity disable is deliberate, scoped, and tracked.

Brian ruled 2026-08-13 that the scheduled weekly run should stop executing
pit_parity so the pipeline can complete. The flag lives in the ONE place that
is the declared source of truth for the cadence trigger —
`SaturdayTrigger.Targets[saturday-pipeline].Input` in
`infrastructure/cloudformation/alpha-engine-orchestration.yaml`
(`deploy_step_function.sh:342` records that the deploy scripts no longer touch
EventBridge).

Three properties, and the third is the one that keeps this honest:

1. **The flag is present**, so a CFN edit cannot silently re-enable a stage
   that has failed 11 of 13 attempts without someone seeing this test change.
2. **One flag is sufficient.** `CheckSkipParity` routes to
   `MarkParityVerdictUnknownByCadence` (alpha-engine-config-I11103's Pass state,
   which stamps `$.parity_verdict_unknown` and changes nothing about the
   topology) and then to `CheckSkipEvaluator`, which is past
   `PitParityCompare` — compare is reachable only via the Parallel's exit
   path. If a future edit makes compare independently reachable, `skip_parity`
   alone would leave it running with no passes to compare and it would emit
   verdict UNKNOWN forever. This test asserts the topology that makes one flag
   enough.
3. **The disable names its re-enable issue.** A skip with no tracked path back
   is a silent removal. `alpha-engine-config-I7309` carries the blockers
   (the walk-forward pass cannot finish inside any budget, and places zero
   orders) and the closes-when.

Scope check: this constrains the CADENCE trigger only. An operator
`StartExecution` input without the flag still runs the full parity branch —
that is the loop for working I7309.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
_CFN = _REPO_ROOT / "infrastructure" / "cloudformation" / "alpha-engine-orchestration.yaml"
_SF = _REPO_ROOT / "infrastructure" / "step_function.json"

REENABLE_ISSUE = "I7309"


def _saturday_target_input() -> str:
    """The literal JSON body of the Saturday trigger's target Input.

    Parsed textually rather than with a YAML loader: the value is a `!Sub`
    block scalar, and the CFN intrinsic tags are not resolvable by a plain
    `yaml.safe_load`.
    """
    text = _CFN.read_text()
    start = text.index("SaturdayTrigger:")
    # the next resource at the same indent level bounds this one
    body = text[start:]
    m = re.search(r"\n          Input: !Sub \|\n(.*?)\n\n", body, re.S)
    assert m, "SaturdayTrigger target Input block not found — CFN shape changed"
    return m.group(1)


def test_cadence_input_carries_skip_parity() -> None:
    payload = json.loads(_saturday_target_input())
    assert payload.get("skip_parity") is True, (
        "the Saturday cadence trigger no longer disables pit_parity. If this "
        f"is a deliberate re-enable, close alpha-engine-config-{REENABLE_ISSUE} "
        "and delete this test — do not just flip the flag."
    )


def test_cadence_input_still_carries_its_identity_fields() -> None:
    """The disable must not have disturbed anything else in this Input —
    `pipeline_role` in particular selects the canonical weekly run on the
    reporting surface."""
    payload = json.loads(_saturday_target_input())
    assert payload["pipeline_role"] == "weekly"
    assert "sns_topic_arn" in payload
    assert "ec2_instance_id" not in payload, (
        "config#2248: this Input must NOT pin a launcher box — the SF's own "
        "DispatchWeeklyFreshnessSpot populates it from a fresh spot"
    )


def test_skip_parity_alone_also_bypasses_the_compare_stage() -> None:
    """Property 2: one flag is enough only while compare sits downstream of
    the Parallel's exit. If that changes, this fails and a second flag
    (`skip_pit_parity_compare`) is required — otherwise compare runs with no
    pass artifacts and emits UNKNOWN every week."""
    doc = json.loads(_SF.read_text())

    def find(states: dict, name: str):
        for key, state in states.items():
            if key == name:
                return state
            if "States" in state:
                hit = find(state["States"], name)
                if hit:
                    return hit
            for branch in state.get("Branches", []) or []:
                hit = find(branch.get("States", {}), name)
                if hit:
                    return hit
        return None

    gate = find(doc["States"], "CheckSkipParity")
    assert gate, "CheckSkipParity disappeared"
    targets = [c["Next"] for c in gate["Choices"]]
    assert targets == ["MarkParityVerdictUnknownByCadence"], (
        f"CheckSkipParity's skip route now goes to {targets} instead of "
        "MarkParityVerdictUnknownByCadence — verify it still bypasses "
        "PitParityCompare, or add skip_pit_parity_compare to the cadence Input"
    )
    assert gate["Default"] == "ParityParallel"

    marker = find(doc["States"], "MarkParityVerdictUnknownByCadence")
    assert marker, "MarkParityVerdictUnknownByCadence disappeared"
    assert marker["Next"] == "CheckSkipEvaluator", (
        "alpha-engine-config-I11103's marker must not change the skip "
        "topology — it stamps $.parity_verdict_unknown and continues straight "
        "to CheckSkipEvaluator, which is past PitParityCompare"
    )


def test_disable_names_its_reenable_issue() -> None:
    """Property 3: a skip with no tracked path back is a silent removal."""
    text = _CFN.read_text()
    idx = text.index("SaturdayTrigger:")
    window = text[idx:idx + 6000]
    assert REENABLE_ISSUE in window, (
        "the skip_parity comment no longer names the tracked re-enable issue "
        f"(alpha-engine-config-{REENABLE_ISSUE}) — without it the disable is "
        "indistinguishable from a permanent removal nobody owns"
    )
