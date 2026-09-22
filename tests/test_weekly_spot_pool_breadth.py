"""The weekly launcher spot's capacity surface (alpha-engine-config-I7133).

`spot_dispatch.launch_with_fallback` rotates instance_type x subnet on a
capacity error, so the number of DISTINCT pools it can fall through is what
decides whether a capacity dip is survivable. The pool was 4 types of adjacent
generations in two families.

Measured 2026-08-12: 3 of 11 recent spot requests in this account died
`instance-terminated-no-capacity`, one of them mid-`DataPhase1` on the
scheduled weekly run (config-I7119).

config-I7119 makes a mid-run reclaim RECOVERABLE. This pins the properties
that make it RARER — recovery still costs a relaunch, a re-bootstrap and the
stage's runtime, so it is the floor rather than the goal.

Two further properties were added by alpha-engine-config-I11427, mirroring
`tests/test_shell_launcher_spot_pool_breadth.py` (the SHELL half of the same
defect, alpha-engine-config-I11412) rather than inventing a second shape:

*   **Order.** `launch_with_fallback` hands the list to
    `krepis.ec2_spot.launch`, which walks instance_type x subnet IN ORDER, and
    the on-demand rung buys the FIRST entry. August 2026: 456.8 on-demand
    `BoxUsage:c5.large` hours alongside 489.0 `SpotUsage:c5.large` hours in a
    744-hour month — a ~48% escalation rate, billed at whatever leads this
    list. A reorder is invisible to every other test in this repo and to code
    review, which is exactly why it gets an assertion.
*   **IAM lockstep.** The issue that spawned this said the Lambda's
    `ec2:RunInstances` grant was `Resource: "*"` with no condition. That was
    true when the breadth tests were written and is now STALE:
    alpha-engine-config-I11227 replaced it with
    `RunInstancesInstanceScopedByTagAndType`, which enumerates
    `ec2:InstanceType` in this directory's `iam-policy.json`. A default naming
    a type that statement omits raises `UnauthorizedOperation`, which
    `krepis.ec2_spot.launch` classifies as a NON-capacity error and re-raises
    as `SpotLaunchError` WITHOUT rotating — turning a survivable capacity dip
    into a hard weekly-SF failure. Nothing held the two in step; this does.

These assert PROPERTIES, not a literal list: adding a type should not require
editing a test, but adding an arm64 or a 1-vCPU type must fail loudly.
"""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import pytest

_LAMBDA_DIR = (
    Path(__file__).resolve().parents[1]
    / "infrastructure"
    / "lambdas"
    / "weekly-freshness-spot-dispatcher"
)
_DISPATCHER = _LAMBDA_DIR / "index.py"
_IAM_POLICY = _LAMBDA_DIR / "iam-policy.json"
_RUN_INSTANCES_SID = "RunInstancesInstanceScopedByTagAndType"

# x86_64, 2 vCPU, >= 4 GiB — the floor is c5.large, which already runs this
# workload successfully. arm64 families are EXCLUDED on purpose: the dispatcher
# pins an x86_64 AL2023 AMI, so an arm64 type fails the architecture check at
# launch. That failure is the reason this is a test and not a comment.
#: Membership here means "x86_64, 2 vCPU, >= 4 GiB" — i.e. SAFE TO LAUNCH if
#: the role is authorised for it. It does NOT mean authorised: that is the
#: separate IAM-lockstep assertion below. The generation-7 c/m/r families were
#: verified 2 vCPU / 4096 MiB / x86_64 against `ec2:DescribeInstanceTypes` by
#: nous-ergon-ops-PR1388 (alpha-engine-config-I11412).
_X86_2VCPU_FAMILIES = {
    "c5", "c5a", "c5n", "c6i", "c6a", "c6in", "c7i", "c7a",
    "m5", "m5a", "m5n", "m6i", "m6a", "m7i", "m7a",
    "r5", "r5a", "r5n", "r6i", "r6a", "r7i", "r7a",
}
_ARM64_FAMILIES = {"c6g", "c7g", "m6g", "m7g", "r6g", "r7g", "t4g", "c8g", "m8g"}

#: The generation digit is the second character of the family: c5a -> 5.
#: "Current" is generation 6 or newer; generation 5 is the previous generation
#: whose head position produced the measured on-demand escalation.
_CURRENT_GENERATION_FLOOR = 6


@pytest.fixture(scope="module")
def mod():
    spec = importlib.util.spec_from_file_location(
        "weekly_freshness_spot_dispatcher", _DISPATCHER
    )
    m = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = m
    spec.loader.exec_module(m)
    return m


def _families(types: list[str]) -> set[str]:
    return {t.split(".", 1)[0] for t in types}


def _generation(instance_type: str) -> int:
    return int(instance_type.split(".", 1)[0][1])


def _iam_allowed_instance_types() -> list[str]:
    """The `ec2:InstanceType` values the role's RunInstances statement allows."""
    assert _IAM_POLICY.is_file(), (
        f"{_IAM_POLICY} does not exist — this guard is checking nothing"
    )
    policy = json.loads(_IAM_POLICY.read_text())
    matching = [
        s for s in policy["Statement"] if s.get("Sid") == _RUN_INSTANCES_SID
    ]
    assert len(matching) == 1, (
        f"expected exactly one `{_RUN_INSTANCES_SID}` statement in "
        f"{_IAM_POLICY.name}, found {len(matching)} — the instance-type "
        f"condition was renamed or removed (alpha-engine-config-I11227), and "
        f"this lockstep guard can no longer see what the role may launch"
    )
    allowed = matching[0]["Condition"]["StringEquals"]["ec2:InstanceType"]
    return list(allowed) if isinstance(allowed, list) else [allowed]


def test_the_pool_spans_enough_distinct_capacity_pools(mod):
    """Pool COUNT is the mechanism — one type per family is one pool."""
    types = mod.INSTANCE_TYPES
    assert len(types) >= 8, (
        f"only {len(types)} instance types; launch_with_fallback has too few "
        f"pools to rotate through on a capacity dip: {types}"
    )
    assert len(_families(types)) >= 5, (
        f"types cluster into too few families {sorted(_families(types))} — "
        f"capacity events correlate WITHIN a family, so 10 types across 2 "
        f"families is not 10 pools"
    )


def test_every_type_is_x86_64(mod):
    """The AMI is x86_64 AL2023; an arm64 type fails at launch, not at review."""
    arm = _families(mod.INSTANCE_TYPES) & _ARM64_FAMILIES
    assert not arm, (
        f"arm64 families {sorted(arm)} in the pool, but WEEKLY_SPOT_AMI_ID is "
        f"x86_64 — every rotation onto one of these fails the architecture check"
    )


def test_every_type_is_a_known_2vcpu_x86_family_at_or_above_the_floor(mod):
    """c5.large (2 vCPU / 4 GiB) is the measured floor — it already runs this
    workload. A smaller type would fail somewhere inside a multi-hour stage."""
    unknown = _families(mod.INSTANCE_TYPES) - _X86_2VCPU_FAMILIES
    assert not unknown, (
        f"unrecognised families {sorted(unknown)}; add them to "
        f"_X86_2VCPU_FAMILIES only after confirming x86_64 + >= c5.large specs"
    )
    sizes = {t.split(".", 1)[1] for t in mod.INSTANCE_TYPES}
    assert sizes == {"large"}, (
        f"mixed sizes {sorted(sizes)} — a rotation onto a smaller size changes "
        f"the workload's resources silently mid-pipeline"
    )


def test_the_subnets_span_multiple_azs(mod):
    """Type rotation is only half the surface; a single AZ re-enters the same
    physical capacity pool however many types are tried."""
    assert len(mod.SUBNETS) >= 3, f"too few subnets to rotate: {mod.SUBNETS}"


def test_the_pool_is_env_overridable(mod, monkeypatch):
    """The override is the operator's escape hatch during a capacity event —
    it must not have been hardcoded away while widening the default."""
    src = _DISPATCHER.read_text()
    assert 'os.environ.get(\n        "WEEKLY_SPOT_INSTANCE_TYPES"' in src or (
        '"WEEKLY_SPOT_INSTANCE_TYPES"' in src and "os.environ.get" in src
    )


def test_on_demand_fallback_is_still_reachable(mod):
    """Widening the pool must not become a REPLACEMENT for the on-demand
    escape: capacity can be exhausted across every pool at once."""
    src = _DISPATCHER.read_text()
    assert "force_on_demand" in src
    assert "launch_with_fallback" in src


def test_the_default_leads_with_a_current_generation_type(mod):
    """The COST property. `krepis.ec2_spot.launch` walks this list IN ORDER and
    `launch_with_fallback` buys the FIRST entry on the on-demand rung, so the
    head is where every launch concentrates and what every escalation is
    billed as — a previous-generation head is a bill, not a style question."""
    head = mod.INSTANCE_TYPES[0]
    assert _generation(head) >= _CURRENT_GENERATION_FLOOR, (
        f"the default leads with {head}, a generation-{_generation(head)} type. "
        f"August 2026 measured 456.8 on-demand BoxUsage:c5.large hours against "
        f"489.0 spot hours in a 744-hour month because a previous-generation "
        f"type led this rotation (alpha-engine-config-I11427)."
    )


def test_the_default_spans_multiple_generations(mod):
    """Capacity correlates WITHIN a generation as well as within a family, so
    ten types of one generation is not ten independent pools."""
    generations = {_generation(t) for t in mod.INSTANCE_TYPES}
    assert len(generations) >= 2, (
        f"only generations {sorted(generations)} in the pool — the 2026-08 "
        f"exhaustion was a generation-wide event, not a family-wide one"
    )


def test_the_default_is_not_refused_by_the_roles_own_iam_allow_list(mod):
    """The statement `RunInstancesInstanceScopedByTagAndType` enumerates
    `ec2:InstanceType`. A default naming a type it omits raises
    `UnauthorizedOperation`, which `krepis.ec2_spot.launch` treats as a
    NON-capacity error and re-raises as `SpotLaunchError` WITHOUT rotating —
    so an unauthorised entry does not cost a rung, it fails the whole launch
    at exactly the capacity dip the pool was widened for."""
    allowed = set(_iam_allowed_instance_types())
    refused = sorted(set(mod.INSTANCE_TYPES) - allowed)
    assert not refused, (
        f"the WEEKLY_SPOT_INSTANCE_TYPES default names {refused}, which this "
        f"directory's iam-policy.json does NOT authorise. Add them to the "
        f"`{_RUN_INSTANCES_SID}` condition AND run `deploy.sh --apply-iam` "
        f"(AWS_PROFILE=ne-admin) — the CI auto-deploy path is code-only and "
        f"will not apply the policy (alpha-engine-config-I11427)."
    )


def test_the_iam_allow_list_grants_nothing_the_default_cannot_use(mod):
    """Least privilege in the other direction: a type authorised but never
    rotated onto is a standing grant with no caller, and it is how the
    condition silently regrows toward the `Resource: "*"` it replaced."""
    unused = sorted(set(_iam_allowed_instance_types()) - set(mod.INSTANCE_TYPES))
    assert not unused, (
        f"iam-policy.json authorises {unused}, which the rotation default never "
        f"names — drop them from the `{_RUN_INSTANCES_SID}` condition, or add "
        f"them to the default if the widening was the intent"
    )
