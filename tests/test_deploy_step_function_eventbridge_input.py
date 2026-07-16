"""Pins the EventBridge cron-rule contract.

Two source-of-truth invariants are enforced here:

1. **CFN is canonical** for EventBridge rules + targets. The deploy
   scripts (``infrastructure/deploy_step_function{,_daily}.sh``) MUST
   NOT contain ``aws events put-rule`` or ``aws events put-targets``.
   Both rules + their targets are defined in
   ``infrastructure/cloudformation/alpha-engine-orchestration.yaml``.

2. **Each cron rule has exactly ONE target.** EventBridge dispatches
   a rule's trigger event to every target, so a duplicate target
   silently fans the cron into N parallel SF executions.

Why these chokepoints (2026-05-26 incident):

PR #317 (33c3753, 2026-05-25 evening) added ``pipeline_role`` tagging
to all three SF cron triggers and stamped the change in BOTH
source-of-truth paths — the CFN template AND the deploy scripts —
with DIFFERENT target IDs (``Id="1"`` from the scripts, ``Id="...-
pipeline"`` from CFN). EventBridge couldn't dedupe (different IDs =
different targets), so every weekday cron firing fanned to TWO
parallel SF executions. Both spawned MorningEnrich on the same
trading instance via SSM SendCommand, both connected ArcticDB, and
both reached ``daily_append``'s ``update_batch`` / ``write_batch``
phase, where the ArcticDB C++ engine emitted 321 unique-symbol
``E_NON_INCREASING_INDEX_VERSION`` (code 5090) races
("This is most likely due to parallel writes to the same symbol").
The 5%-threshold daily_append gate hard-failed both runs at 35.6%
error rate (905 tickers, n_err=322). Trading didn't happen on 5/26.

PR #317's existing chokepoint tests validated target *input
contents* (``pipeline_role`` value, ``enable_standalone_scanner``
value) but not target *uniqueness* — so CI passed. The new
``TestCFNTargetUniqueness`` + ``TestDeployScriptsHaveNoEventBridgeWrites``
classes below close that gap so the next duplicate-target attempt
fails at PR time.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest


_REPO_ROOT = Path(__file__).resolve().parent.parent
_INFRA = _REPO_ROOT / "infrastructure"
_CFN_ORCHESTRATION_PATH = _INFRA / "cloudformation" / "alpha-engine-orchestration.yaml"

_DEPLOY_SCRIPTS = (
    "deploy_step_function.sh",
    "deploy_step_function_daily.sh",
)

# Each cron-rule CFN block runs from its name marker to the next
# top-level name marker. Pairing each rule with its known successor
# lets us slice the block deterministically without a YAML parser
# (CFN's ``!Sub`` / ``!Ref`` tags require a custom loader).
# FridayShellRunTrigger retired 2026-05-29 (ROADMAP L4055) — superseded by the
# event-driven alpha-engine-eod-success-friday-shell-trigger Lambda. SaturdayTrigger
# now flows directly into WeekdayPipelineSchedule.
_TRIGGER_SUCCESSORS = {
    "SaturdayTrigger": "WeekdayPipelineSchedule",
    # alpha-engine-config-I2545: ModelZooSundayTrigger was inserted between
    # WeekdayPipelineSchedule and ResearchAlerts in the CFN template — update this
    # chain in the SAME PR as any future insertion between two entries here
    # (an unregistered gap silently merges the two neighboring blocks and
    # masks a genuine multi-target regression, exactly the failure mode
    # this successor-chain exists to prevent).
    "WeekdayPipelineSchedule": "ModelZooSundayTrigger",
    "ModelZooSundayTrigger": "ResearchAlerts",
}


@pytest.fixture(scope="module")
def orchestration_text() -> str:
    return _CFN_ORCHESTRATION_PATH.read_text()


def _trigger_block(text: str, name: str) -> str:
    """Extract a single cron-trigger resource block from the CFN text
    (``AWS::Events::Rule`` or, since config#2413, ``AWS::Scheduler::Schedule``)."""
    head = text.split(f"{name}:", 1)
    assert len(head) == 2, f"{name} block not found in orchestration CFN"
    successor = _TRIGGER_SUCCESSORS[name]
    return head[1].split(f"{successor}:", 1)[0]


# ── Substrate gate 1: scripts must not touch EventBridge ──────────────────


class TestDeployScriptsHaveNoEventBridgeWrites:
    """Single source of truth: CFN owns EventBridge rules + targets;
    deploy scripts only update the SF state machine JSON.

    Codified 2026-05-26 after PR #317's dual-write pattern caused the
    duplicate-target incident (see module docstring). The script side
    of the dual write is what created the second target; removing it
    here prevents recurrence.
    """

    @staticmethod
    def _executable_lines(text: str) -> str:
        """Return the script with comment + blank lines stripped so the
        substring scan below only looks at executable statements. Bash
        comments start with ``#`` after any leading whitespace.
        Documentation blocks describing the prohibited commands stay in
        place (they're informative), but don't trigger the test."""
        kept = []
        for line in text.splitlines():
            stripped = line.lstrip()
            if not stripped or stripped.startswith("#"):
                continue
            kept.append(line)
        return "\n".join(kept)

    @pytest.mark.parametrize("script", _DEPLOY_SCRIPTS)
    def test_no_put_targets(self, script):
        text = self._executable_lines((_INFRA / script).read_text())
        assert "aws events put-targets" not in text, (
            f"infrastructure/{script} contains an executable "
            f"`aws events put-targets` call. EventBridge rules + targets "
            f"are CFN-canonical "
            f"(infrastructure/cloudformation/alpha-engine-orchestration.yaml). "
            f"If you need to change a target, edit the CFN template and run "
            f"`aws cloudformation deploy --template-file "
            f"infrastructure/cloudformation/alpha-engine-orchestration.yaml ...`. "
            f"Calling put-targets from the deploy script created the "
            f"2026-05-26 duplicate-target incident."
        )

    @pytest.mark.parametrize("script", _DEPLOY_SCRIPTS)
    def test_no_put_rule(self, script):
        text = self._executable_lines((_INFRA / script).read_text())
        assert "aws events put-rule" not in text, (
            f"infrastructure/{script} contains an executable "
            f"`aws events put-rule` call. EventBridge rules are "
            f"CFN-canonical "
            f"(infrastructure/cloudformation/alpha-engine-orchestration.yaml). "
            f"Edit the CFN ``AWS::Events::Rule`` resource and re-deploy "
            f"the stack."
        )


# ── Substrate gate 2: each cron rule has exactly ONE target ───────────────


class TestCFNTargetUniqueness:
    """EventBridge dispatches a rule's trigger event to every target.
    A duplicate target silently fans the cron into N parallel SF
    executions, which then race at any shared downstream resource
    (the 2026-05-26 ArcticDB race; see module docstring).

    Constraint: every cron-triggered trigger resource in the
    orchestration CFN template has EXACTLY ONE target. If you have a
    legitimate reason to fan one trigger to multiple targets, document
    the rationale + update this test in the same PR.

    Two resource shapes are in play here:

    - ``AWS::Events::Rule`` (e.g. SaturdayTrigger) declares targets as
      a ``Targets:`` list of ``- Id: ...`` entries — EventBridge fans
      the trigger event to EVERY entry, so we count ``- Id:`` markers
      and require exactly 1.
    - ``AWS::Scheduler::Schedule`` (e.g. WeekdayPipelineSchedule, migrated
      config#2413) declares a single ``Target:`` mapping — the CFN
      schema only permits one Target per Schedule, so "exactly one
      target" is structurally guaranteed and there is no list to
      count. We just assert the ``Target:`` block exists and contains
      no second ``Arn:``/``- Id:`` list (which would indicate someone
      hand-rolled a second target shape into the same block).
    """

    @pytest.mark.parametrize("trigger", list(_TRIGGER_SUCCESSORS.keys()))
    def test_exactly_one_target_per_trigger(self, orchestration_text, trigger):
        block = _trigger_block(orchestration_text, trigger)
        resource_type_match = re.search(r"Type:\s*(\S+)", block)
        assert resource_type_match, f"{trigger} block missing ``Type:``"
        resource_type = resource_type_match.group(1)

        if resource_type == "AWS::Scheduler::Schedule":
            # Singular ``Target:`` mapping — the schema permits only one,
            # so uniqueness is structural. Just confirm it's present and
            # that the plural ``Targets:`` (Events::Rule) shape wasn't
            # left behind by a partial migration.
            assert re.search(r"^\s*Target:\s*$", block, re.MULTILINE), (
                f"{trigger} is an AWS::Scheduler::Schedule but has no "
                f"``Target:`` block."
            )
            assert not re.search(r"^\s*Targets:\s*$", block, re.MULTILINE), (
                f"{trigger} is an AWS::Scheduler::Schedule but still has "
                f"a plural ``Targets:`` block — looks like a leftover "
                f"from the AWS::Events::Rule shape."
            )
            return

        # ``Targets:`` is followed by one or more ``- Id: ...`` entries
        # at the same indent. Count the ``- Id:`` markers between
        # ``Targets:`` and the end of the block.
        targets_split = block.split("Targets:", 1)
        assert len(targets_split) == 2, (
            f"{trigger} block missing ``Targets:`` section"
        )
        targets_body = targets_split[1]
        id_count = len(re.findall(r"^\s*- Id:\s*\S+", targets_body, re.MULTILINE))
        assert id_count == 1, (
            f"{trigger} declares {id_count} targets; must be exactly 1. "
            f"EventBridge fans triggers to every target — a second target "
            f"on this rule will spawn parallel SF executions on every "
            f"firing (see the 2026-05-26 duplicate-target incident in the "
            f"module docstring). If multi-target is intentional here, "
            f"update this test in the same PR with the rationale."
        )


# ── Content checks: pipeline_role + scanner activation (CFN side) ─────────


class TestOrchestrationCFNPipelineRoles:
    """The cron rules' Input fields come from the CFN template (now
    the sole source of truth — see ``TestDeployScriptsHaveNoEvent``-
    ``BridgeWrites``). Both cron rules must carry their canonical
    ``pipeline_role`` tag so page 25 / Slack / CLI consumers filter
    smoke / recovery / operator-replay executions out of the cadence
    section.

    WeekdayPipelineSchedule (config#2413, AWS::Scheduler::Schedule) nests its
    payload inside the ``aws-sdk:sfn:startExecution`` call shape, so
    the inner JSON is backslash-escaped (``\\"pipeline_role\\"``)
    inside the outer ``Input`` string. The pattern below tolerates an
    optional backslash before each quote so it matches both the plain
    (SaturdayTrigger, AWS::Events::Rule) and escaped (WeekdayPipelineSchedule)
    forms.
    """

    _PIPELINE_ROLE_RE = r'\\?"pipeline_role\\?"\s*:\s*\\?"{}\\?"'

    def test_saturday_trigger_has_weekly_role(self, orchestration_text):
        block = _trigger_block(orchestration_text, "SaturdayTrigger")
        assert re.search(
            self._PIPELINE_ROLE_RE.format("weekly"),
            block,
        ), 'SaturdayTrigger Input must carry pipeline_role="weekly".'

    def test_weekday_trigger_has_daily_role(self, orchestration_text):
        block = _trigger_block(orchestration_text, "WeekdayPipelineSchedule")
        assert re.search(
            self._PIPELINE_ROLE_RE.format("daily"),
            block,
        ), 'WeekdayPipelineSchedule Input must carry pipeline_role="daily".'


class TestSaturdayCFNTargetHasNoScannerGateFlag:
    """The ``CheckEnableStandaloneScanner`` SF Choice gate was removed
    2026-05-28 — Scanner runs unconditionally per
    ``feedback_observe_mode_unconditional_gates_govern_cutover``. The
    EB target Input MUST NOT carry ``enable_standalone_scanner``: the
    flag is dead substrate, and re-introducing it primes a future
    re-add of the Choice gate (the test that pinned its presence is
    what kept the gate alive). The Phase 4/5 consumer-cutover flag
    (Research / RAG reading ``candidates.json``) belongs at the
    consumer side, not on the producer-side EB target.
    """

    def test_no_enable_standalone_scanner_in_input(self, orchestration_text):
        block = _trigger_block(orchestration_text, "SaturdayTrigger")
        assert "enable_standalone_scanner" not in block, (
            "SaturdayTrigger Input must NOT carry enable_standalone_scanner. "
            "Scanner runs unconditionally as of 2026-05-28; the flag is "
            "dead substrate. Re-introducing it primes a re-add of the "
            "removed Choice gate. See "
            "feedback_observe_mode_unconditional_gates_govern_cutover."
        )
