"""The SHELL launchers' spot candidate set (alpha-engine-config-I11412).

`tests/test_weekly_spot_pool_breadth.py` pins these properties for the
weekly-freshness-spot-dispatcher **Lambda**. The two shell launchers in
`infrastructure/` carry a SECOND, independent candidate list that
`krepis.ec2_spot.launch` walks, and nothing pinned it — so the Lambda's pool
could be widened and audited while the shell path silently kept rotating
through four adjacent-generation pools. This is the shell half, deliberately
the same shape.

Three properties, and the third is the one this file exists for:

1. **Launch-safety.** Every type in the rotation default is x86_64, 2 vCPU and
   `.large` — the AMI is x86_64 AL2023 and `c5.large` is the measured floor
   that already runs this workload.
2. **Breadth.** Enough DISTINCT pools to rotate through. Capacity events
   correlate within a family and within a generation, so the count that matters
   is families x generations, not types.
3. **Order.** `krepis.ec2_spot.launch` walks types x subnets IN ORDER and
   `launch_with_fallback` buys the FIRST entry on the on-demand rung, so the
   head of the list is both where spot launches concentrate and what an
   escalation is BILLED as. August 2026: 456.8 on-demand `BoxUsage:c5.large`
   hours alongside 489.0 `SpotUsage:c5.large` hours in a 744-hour month, a 48%
   escalation rate, because a previous-generation type led the rotation. A
   reorder is invisible to every other test in this repo and to code review,
   which is exactly why it gets an assertion.

Plus the self-consistency guard: a rotation default naming a type the file's
OWN `ALLOWED_INSTANCE_TYPES` constant refuses makes the launcher reject every
launch before it reaches AWS.

Properties, not a literal list: adding a type must not require editing this
file, but adding an arm64 type, a smaller size, or a previous-generation head
must fail loudly.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

_INFRA = Path(__file__).resolve().parents[1] / "infrastructure"
_LAUNCHERS = ["_spot_common.sh", "spot_data_weekly.sh"]

#: x86_64, 2 vCPU, >= 4 GiB. arm64 families are EXCLUDED on purpose: the
#: launchers pin an x86_64 AL2023 AMI, so an arm64 type fails the architecture
#: check at launch. That failure is the reason this is a test and not a comment.
_X86_2VCPU_FAMILIES = {
    "c5", "c5a", "c5n", "c6i", "c6a", "c6in", "c7i", "c7a",
    "m5", "m5a", "m5n", "m6i", "m6a", "m7i", "m7a",
    "r5", "r5a", "r5n", "r6i", "r6a", "r7i", "r7a",
}
_ARM64_FAMILIES = {"c6g", "c7g", "c8g", "m6g", "m7g", "m8g", "r6g", "r7g", "t4g"}

#: The generation digit is the second character of the family: c5a -> 5.
#: "Current" is gen 6 or newer; gen 5 is the previous generation whose
#: exhaustion produced the measured on-demand escalation.
_CURRENT_GENERATION_FLOOR = 6

_ROTATION = re.compile(
    r'^\s*INSTANCE_TYPES="\$\{INSTANCE_TYPES:-(?P<val>[^}"]*)\}"', re.MULTILINE
)
_CONSTANT = re.compile(r'^ALLOWED_INSTANCE_TYPES="(?P<val>[^"]*)"', re.MULTILINE)


def _text(name: str) -> str:
    path = _INFRA / name
    assert path.is_file(), f"{path} does not exist — this guard is checking nothing"
    return path.read_text()


def _rotation(name: str) -> list[str]:
    m = _ROTATION.search(_text(name))
    assert m, f"infrastructure/{name} carries no INSTANCE_TYPES rotation default"
    return [t.strip() for t in m.group("val").split(",") if t.strip()]


def _allowed(name: str) -> list[str]:
    m = _CONSTANT.search(_text(name))
    assert m, f"infrastructure/{name} carries no ALLOWED_INSTANCE_TYPES constant"
    return [t.strip() for t in m.group("val").split(",") if t.strip()]


def _family(t: str) -> str:
    return t.split(".", 1)[0]


def _generation(t: str) -> int:
    return int(_family(t)[1])


@pytest.mark.parametrize("launcher", _LAUNCHERS)
def test_every_rotation_type_is_a_known_x86_2vcpu_large(launcher: str):
    types = _rotation(launcher)
    families = {_family(t) for t in types}
    arm = families & _ARM64_FAMILIES
    assert not arm, (
        f"infrastructure/{launcher}: arm64 families {sorted(arm)} in the rotation, "
        f"but the AMI is x86_64 AL2023 — every rotation onto one fails the "
        f"architecture check at launch, not at review"
    )
    unknown = families - _X86_2VCPU_FAMILIES
    assert not unknown, (
        f"infrastructure/{launcher}: unrecognised families {sorted(unknown)}; add "
        f"them here only after confirming x86_64 + 2 vCPU + >= c5.large specs "
        f"against ec2:DescribeInstanceTypes"
    )
    sizes = {t.split(".", 1)[1] for t in types}
    assert sizes == {"large"}, (
        f"infrastructure/{launcher}: mixed sizes {sorted(sizes)} — a rotation onto "
        f"a different size changes the workload's resources silently mid-pipeline"
    )


@pytest.mark.parametrize("launcher", _LAUNCHERS)
def test_the_rotation_spans_enough_distinct_capacity_pools(launcher: str):
    types = _rotation(launcher)
    assert len(types) == len(set(types)), (
        f"infrastructure/{launcher}: duplicate types in the rotation {types} — a "
        f"repeat re-enters the same pool instead of adding one"
    )
    assert len(types) >= 6, (
        f"infrastructure/{launcher}: only {len(types)} types; launch_with_fallback "
        f"has too few pools to rotate through on a capacity dip: {types}"
    )
    assert len({_family(t) for t in types}) >= 5, (
        f"infrastructure/{launcher}: types cluster into too few families "
        f"{sorted({_family(t) for t in types})} — capacity correlates WITHIN a "
        f"family, so N types across 2 families is not N pools"
    )
    assert len({_generation(t) for t in types}) >= 3, (
        f"infrastructure/{launcher}: only generations "
        f"{sorted({_generation(t) for t in types})} — capacity also correlates "
        f"within a generation, which is what the 2026-08 exhaustion measured"
    )


@pytest.mark.parametrize("launcher", _LAUNCHERS)
def test_the_rotation_leads_with_a_current_generation_type(launcher: str):
    """The cost property. The head of the list is what an on-demand escalation
    buys, so a previous-generation head is a bill, not a style question."""
    head = _rotation(launcher)[0]
    assert _generation(head) >= _CURRENT_GENERATION_FLOOR, (
        f"infrastructure/{launcher}: the rotation leads with {head}, a generation-"
        f"{_generation(head)} type. krepis.ec2_spot.launch walks this list IN "
        f"ORDER and launch_with_fallback buys the FIRST entry on the on-demand "
        f"rung, so this is where every launch concentrates and what every "
        f"escalation is billed as (alpha-engine-config-I11412)."
    )


@pytest.mark.parametrize("launcher", _LAUNCHERS)
def test_the_rotation_default_is_not_refused_by_the_files_own_allow_list(launcher: str):
    """`spot_assert_instance_types_allowed "$INSTANCE_TYPES"` runs on the
    rotation default as well as on an operator override, so a default naming a
    type the constant omits makes the launcher refuse EVERY launch."""
    allowed = set(_allowed(launcher))
    refused = sorted(set(_rotation(launcher)) - allowed)
    assert not refused, (
        f"infrastructure/{launcher}: its own ALLOWED_INSTANCE_TYPES would refuse "
        f"{refused} from its own rotation default — every launch exits 2 before "
        f"reaching AWS"
    )


def test_both_shell_launchers_agree_on_the_rotation():
    """spot_data_weekly.sh does not source _spot_common.sh; the two lists are
    maintained in parallel, which is a fork waiting to happen."""
    rotations = {name: _rotation(name) for name in _LAUNCHERS}
    distinct = {tuple(v) for v in rotations.values()}
    assert len(distinct) == 1, (
        "the two shell launchers' rotation defaults have diverged — a capacity "
        "or cost fix applied to one silently misses the other:\n"
        + "\n".join(f"  {k}: {v}" for k, v in rotations.items())
    )
