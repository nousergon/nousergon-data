"""The orchestration stack declares no CloudWatch alarm
(alpha-engine-config-I9009, step 2).

Every alarm this stack used to declare is codified in
``nous-ergon-ops/infrastructure/cloudwatch/alarms/<AlarmName>.json``, and that
tree is the only applier (alpha-engine-config-I7339). While both declared the
same alarm, every push to main re-stamped this stack and CloudFormation
reasserted its copy over the codified one, so an edit made only in
nous-ergon-ops was reverted within hours (alpha-engine-config-I7996).

Step 1 (nousergon-data#2288) deployed ``DeletionPolicy: Retain`` on all 14
before step 2 removed them, so the stack update released the live alarms
instead of deleting them. This test keeps the stack out of the alarm business:
a new alarm goes in nous-ergon-ops, not here.
"""

from __future__ import annotations

from pathlib import Path

import yaml

_CFN = Path(__file__).resolve().parents[1] / "infrastructure" / "cloudformation" / "alpha-engine-orchestration.yaml"


class _CfnLoader(yaml.SafeLoader):
    """SafeLoader that reads CloudFormation short-form tags (``!Ref``,
    ``!Sub``, ``!GetAtt`` ...) as plain values instead of failing."""


def _any_tag(loader: yaml.SafeLoader, _suffix: str, node: yaml.Node) -> object:
    if isinstance(node, yaml.ScalarNode):
        return loader.construct_scalar(node)
    if isinstance(node, yaml.SequenceNode):
        return loader.construct_sequence(node)
    return loader.construct_mapping(node)


_CfnLoader.add_multi_constructor("!", _any_tag)


def _resources() -> dict[str, dict]:
    template = yaml.load(_CFN.read_text(encoding="utf-8"), Loader=_CfnLoader)
    return template.get("Resources") or {}


def test_the_template_still_parses_to_resources():
    """Guards the guard: an empty parse would pass the check below vacuously."""
    resources = _resources()
    assert resources, "no Resources parsed from the orchestration template"
    assert any(
        isinstance(res, dict) and res.get("Type") == "AWS::StepFunctions::StateMachine"
        for res in resources.values()
    ), "the orchestration template parsed without its state machines"


def test_the_stack_declares_no_cloudwatch_alarm():
    alarms = sorted(
        f"{logical_id} ({res.get('Properties', {}).get('AlarmName')})"
        for logical_id, res in _resources().items()
        if isinstance(res, dict) and res.get("Type") == "AWS::CloudWatch::Alarm"
    )
    assert not alarms, (
        "CloudWatch alarms are codified in nous-ergon-ops/infrastructure/"
        "cloudwatch/alarms/ and applied only from there (alpha-engine-config-"
        "I7339, I9009). Declaring one in this stack makes it dual-owned, and "
        "every push to main would reassert this copy over the codified one. "
        "Move it to nous-ergon-ops:\n  " + "\n  ".join(alarms)
    )
