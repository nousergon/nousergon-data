"""config#2281 — the account/SNS-topic ARN must not proliferate through the
weekly definition.

`sns_topic_arn` is an execution input: `InitializeInput` JsonMerges the
caller's value over a hardcoded floor default, and `NormalizeFailureContext`
re-floors it on the failure path. Those two floors are the ONLY sanctioned
copies of the literal (each carries a PROVENANCE comment naming why it is
retained: a bare manual console start must still alert). The authoritative
source is the caller — the EventBridge schedule's CFN-managed Input, which
lives in alpha-engine-config's alpha-engine-orchestration.yaml.

Pinned here:
  1. every `sns:publish` state reads `TopicArn.$: $.sns_topic_arn` — never a
     hardcoded TopicArn;
  2. the topic literal appears in EXACTLY the two declared floor sites
     (a third copy = a new hand-carried account/region literal, the exact
     drift class config#2281 retired);
  3. both floors carry the SAME literal (lockstep) and their provenance
     comments.
"""
from __future__ import annotations

import json
import pathlib
import re

_WEEKLY = pathlib.Path(__file__).parent.parent / "infrastructure" / "step_function.json"

_TOPIC_LITERAL = "arn:aws:sns:us-east-1:711398986525:alpha-engine-alerts"
_FLOOR_STATES = ("InitializeInput", "NormalizeFailureContext")


def _definition() -> dict:
    return json.loads(_WEEKLY.read_text())


def _iter_states(states):
    for name, state in states.items():
        yield name, state
        if state.get("Type") == "Parallel":
            for branch in state.get("Branches", []):
                yield from _iter_states(branch["States"])
        if state.get("Type") == "Map":
            iterator = state.get("Iterator") or state.get("ItemProcessor")
            if iterator:
                yield from _iter_states(iterator["States"])


def test_every_publish_state_reads_topic_from_execution_context():
    offenders = []
    publishers = 0
    for name, state in _iter_states(_definition()["States"]):
        if state.get("Resource") != "arn:aws:states:::sns:publish":
            continue
        publishers += 1
        params = state.get("Parameters", {})
        if "TopicArn" in params or params.get("TopicArn.$") != "$.sns_topic_arn":
            offenders.append(name)
    assert publishers >= 5, f"walker regressed: only {publishers} publish states found"
    assert not offenders, (
        f"config#2281: publish state(s) {offenders} do not read "
        "TopicArn.$ = $.sns_topic_arn — hardcoding a topic re-introduces the "
        "hand-carried account/region literal"
    )


def test_topic_literal_appears_only_in_the_two_declared_floors():
    definition = _definition()
    states = definition["States"]
    for floor in _FLOOR_STATES:
        merged = states[floor]["Parameters"]["merged.$"]
        assert _TOPIC_LITERAL in merged, f"{floor} lost its sns_topic_arn floor"
        assert "config#2281" in states[floor].get("Comment", ""), (
            f"{floor}'s floor literal lost its provenance comment"
        )
    # Count copies OUTSIDE Comments: strip every Comment value first, so
    # provenance prose may name the literal without tripping the guard.
    def _strip_comments(node):
        if isinstance(node, dict):
            return {k: _strip_comments(v) for k, v in node.items() if k != "Comment"}
        if isinstance(node, list):
            return [_strip_comments(v) for v in node]
        return node

    functional_text = json.dumps(_strip_comments(definition))
    occurrences = functional_text.count(_TOPIC_LITERAL)
    assert occurrences == len(_FLOOR_STATES), (
        f"config#2281: the topic literal appears {occurrences}x functionally "
        f"(expected {len(_FLOOR_STATES)}: the InitializeInput + "
        "NormalizeFailureContext floors). A new copy is a hand-carried "
        "account/region literal — read $.sns_topic_arn instead."
    )
