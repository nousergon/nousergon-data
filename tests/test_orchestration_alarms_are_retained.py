"""Every CloudWatch alarm the orchestration stack still owns carries
``DeletionPolicy: Retain`` (alpha-engine-config-I9009, step 1).

All of these alarms are also codified in
``nous-ergon-ops/infrastructure/cloudwatch/alarms/``, which owns them going
forward (alpha-engine-config-I7339). Step 2 removes them from this stack, and
a CloudFormation stack update that drops a resource DELETES it unless the
resource was already deployed with ``DeletionPolicy: Retain``. Without this,
the handoff would delete each live alarm and leave it absent until the next
nous-ergon-ops apply, which is a control removed by CI.

So Retain has to be live before any removal PR merges. This test holds it in
place, including for an alarm resource added here later.
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


def _alarm_resources() -> dict[str, dict]:
    template = yaml.load(_CFN.read_text(encoding="utf-8"), Loader=_CfnLoader)
    return {
        logical_id: res
        for logical_id, res in (template.get("Resources") or {}).items()
        if isinstance(res, dict) and res.get("Type") == "AWS::CloudWatch::Alarm"
    }


def test_the_template_still_parses_to_alarm_resources():
    """Guards the guard: an empty set would pass the check below vacuously."""
    assert _alarm_resources(), "no AWS::CloudWatch::Alarm resources found"


def test_every_alarm_resource_is_retained_on_delete_and_replace():
    missing = sorted(
        f"{logical_id} ({res.get('Properties', {}).get('AlarmName')})"
        for logical_id, res in _alarm_resources().items()
        if res.get("DeletionPolicy") != "Retain" or res.get("UpdateReplacePolicy") != "Retain"
    )
    assert not missing, (
        "these alarm resources would be DELETED by the stack update that hands "
        "them to nous-ergon-ops (alpha-engine-config-I9009). Give each "
        "`DeletionPolicy: Retain` and `UpdateReplacePolicy: Retain`:\n  " + "\n  ".join(missing)
    )
