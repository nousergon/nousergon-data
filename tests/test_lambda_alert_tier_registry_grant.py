"""A Lambda that publishes alerts must be able to read the delivery-tier registry.

`alerts.publish` reads `s3://alpha-engine-research/overseer/alert_tier_registry.json`
to decide each alert's tier. When the read fails it pages on every emission
(fail-upward), so a missing grant turns every routine alert into a page.
On 2026-10-23 `alpha-engine-pipeline-watchdog-role` hit AccessDenied there,
and six other publishing Lambdas had no grant either.

Derived, never listed: every `infrastructure/lambdas/*/index.py` that calls
`alerts.publish(` must carry an `iam-policy.json` allowing `s3:GetObject` on
that object, so a new publisher is covered the day it is added.
"""
from __future__ import annotations

import fnmatch
import json
import re
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
LAMBDAS = REPO / "infrastructure" / "lambdas"
REGISTRY = "arn:aws:s3:::alpha-engine-research/overseer/alert_tier_registry.json"
_PUBLISH = re.compile(r"\balerts\.publish\(")

PUBLISHERS = sorted(
    p.parent.name
    for p in LAMBDAS.glob("*/index.py")
    if _PUBLISH.search(p.read_text(encoding="utf-8"))
)


def _as_list(v):
    return v if isinstance(v, list) else [v]


def _grants_registry_read(policy: dict) -> bool:
    for s in policy.get("Statement", []):
        if s.get("Effect") != "Allow" or "Condition" in s:
            continue
        if not any(fnmatch.fnmatchcase("s3:GetObject", a) for a in _as_list(s.get("Action", []))):
            continue
        if any(fnmatch.fnmatchcase(REGISTRY, r) for r in _as_list(s.get("Resource", []))):
            return True
    return False


def test_publishers_found():
    assert "pipeline-watchdog" in PUBLISHERS


@pytest.mark.parametrize("name", PUBLISHERS)
def test_publisher_can_read_alert_tier_registry(name):
    policy_path = LAMBDAS / name / "iam-policy.json"
    assert policy_path.exists(), f"{name} calls alerts.publish but has no iam-policy.json"
    policy = json.loads(policy_path.read_text(encoding="utf-8"))
    assert _grants_registry_read(policy), (
        f"{name} calls alerts.publish but its iam-policy.json does not allow "
        f"s3:GetObject on {REGISTRY}; every alert it sends would page."
    )
