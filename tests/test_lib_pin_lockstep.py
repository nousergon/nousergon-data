"""Pin every lib-install surface to the same nousergon-lib version.

(The dist was renamed ``alpha-engine-lib`` → ``nousergon-lib`` at v0.60.0;
the historical incidents below predate the rename and reference the old
``nousergon_lib`` import name accordingly — kept verbatim as the
drift-class record.)

The Dockerfile strips nousergon-lib from ``requirements.txt`` before
``pip install`` (see the ``grep -vE ...nousergon-lib`` line in the
Dockerfile RUN block) and instead installs the lib via a hardcoded
``pip install "nousergon-lib@vX.Y.Z"`` line ABOVE that grep. So
bumping ``requirements.txt`` alone does NOT propagate to the Lambda
image — the Dockerfile's hardcoded pin wins. The slim
``requirements-daily-news.txt`` (standalone daily-news collector on the
dashboard box) carries its own copy of the pin and its header demands
lockstep with ``requirements.txt`` — so it is guarded here too.
``.github/workflows/deploy-infrastructure.yml`` also carries its own
hardcoded ``pip install`` pin for its drift-check alerting step
(``nousergon_lib.alerts``) and is guarded here for the same reason
(alpha-engine-config#2999: this file drifted a full version behind
``requirements.txt`` undetected until this test covered it).

Some Lambdas have deliberate exemptions documented in their requirements.txt
comments. These must move in lockstep within their exemption group (e.g., all
spot-dispatch Lambdas stay together) and MUST NOT silently drift from their
documented version without a named contract reason.

This drift class has bitten production multiple times:

  - 2026-05-06 (research): requirements.txt bumped @v0.4.0 → @v0.5.1
    but Dockerfile kept v0.3.0; Research Lambda canary failed with
    ``ModuleNotFoundError: nousergon_lib.agent_schemas``.
  - 2026-05-12 (predictor): requirements.txt → v0.12.0 but
    requirements-lambda.txt stayed v0.9.1; predictor canary failed
    with ``ModuleNotFoundError: nousergon_lib.secrets``.
  - 2026-05-12 (data, this repo): requirements.txt → v0.12.0 in PR
    #221 but Dockerfile kept v0.3.0 (a 9-version-old pin); data
    Lambda canary failed at 17:22 UTC with the same
    ``nousergon_lib.secrets`` ModuleNotFoundError.

This test re-greps all three files on every CI run so a future single-file
bump fails here, not in a canary.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent

_REQUIREMENTS_PIN_RE = re.compile(
    r"nousergon-lib\[[^\]]*\]\s*@\s*git\+https://github\.com/nousergon/nousergon-lib@(v[0-9]+\.[0-9]+\.[0-9]+)"
)
_DOCKERFILE_PIN_RE = re.compile(
    r'"nousergon-lib\[[^\]]*\]\s*@\s*git\+https://github\.com/nousergon/nousergon-lib@(v[0-9]+\.[0-9]+\.[0-9]+)"'
)
_LAMBDA_PIN_RE = re.compile(
    r"nousergon-lib(?:\[[^\]]*\])?\s*@\s*git\+https://github\.com/nousergon/nousergon-lib@(v[0-9]+\.[0-9]+\.[0-9]+)"
)


@dataclass
class _Exemption:
    """A deliberate pin exemption with a named kind and contract reason.

    ``floor``
        The Lambda needs at least this version.  A FLOOR SHALL NOT LAG THE
        ROOT PIN: if root >= floor already, the floor is redundant and the
        Lambda must track root like every other Lambda.  ``floor`` is an
        attestation record — it documents *why* the Lambda was pinned, but
        once root has moved past it the attestation is satisfied and the
        exemption is no longer needed (alpha-engine-config-I4802).

    ``ceiling``
        The Lambda genuinely cannot use the root pin — there is a concrete
        incompatibility.  REQUIRES ``re_exam`` naming a date when the
        incompatibility will be re-evaluated, and the reason MUST name the
        specific incompatibility rather than a general "needs vX for
        feature Y" (which is a floor).

    ``group``
        A set of Lambdas that must move in lockstep with each other,
        separately from the root pin (e.g. the spot-dispatch cluster).
        REQUIRES ``re_exam`` naming a date when the group's pin will be
        re-evaluated against root.  All group members MUST assert the
        same version.
    """

    kind: str
    version: str
    reason: str
    re_exam: str | None = None  # required for ceiling and group
    members: tuple[str, ...] | None = None  # for group: declares the cluster

    def __post_init__(self) -> None:
        assert self.kind in ("floor", "ceiling", "group"), (
            f"_Exemption.kind must be 'floor', 'ceiling', or 'group', got {self.kind!r}"
        )
        if self.kind in ("ceiling", "group"):
            assert self.re_exam is not None, (
                f"{self.kind} exemption requires a re_exam date"
            )
        if self.kind == "group":
            assert self.members is not None and len(self.members) >= 2, (
                "group exemption requires >= 2 members"
            )


# Lambda exemptions: deliberate pins outside the root lockstep guard.
# Key: lambda directory name, Value: structured _Exemption.
#
# Most historical exemptions were FLOORS ("bumped to vX for feature Y")
# but were enforced as EQUALITY — silently freezing each Lambda at the
# version of its last feature need.  The floor entries below have been
# resolved by bumping the Lambda to the root pin and removing the
# exemption (alpha-engine-config-I4802).  Only genuine group exemptions
# (lockstep clusters that cannot track root independently) remain.
#
# A floor is not an exemption: root >= floor already satisfies it.
# scheduled-groom-dispatcher: see test_groom_dispatcher_is_not_exempted
_LAMBDA_PIN_EXEMPTIONS: dict[str, _Exemption] = {
    # EMPTY, and that is the target state (alpha-engine-config-I5751).
    #
    # The "Spot-dispatch lockstep group" that lived here — arctic-migration,
    # canary-replay, alert-drain, ci-watch and sf-watch-spot — was removed
    # 2026-07-30 after being frozen at v0.124.5 for nineteen lib versions.
    #
    # Its stated reason was that the five share the nousergon_lib.spot_dispatch
    # chokepoint and so "stay in lockstep with them, not with root". That reason
    # argues for bumping them AS A UNIT. It was enforced as not bumping them at
    # all, and the difference cost something measurable: nousergon-lib#274 added
    # launch-provenance tags to spot_dispatch itself, and the five Lambdas
    # grouped around that module were the only five that would not have received
    # it (alpha-engine-config-I5727).
    #
    # Measured before removing it, rather than assumed: across v0.124.5..v0.124.24
    # the ONLY commit touching spot_dispatch.py or ec2_spot.py is #274, and all
    # five Lambdas import spot_dispatch and nothing else from the library. The
    # exemption therefore protected them from no change at all — the nineteen
    # versions of drift were entirely in modules they do not import — while
    # withholding the one change that was for them.
    #
    # A `group` kind still exists in _Exemption and is still legitimate: it means
    # "these move together". If a future cluster genuinely cannot track root, it
    # goes here WITH a contract reason naming what breaks — not "shares a
    # chokepoint", which is an argument for coupling their bumps, not for
    # freezing them.
}


def _read_pin(filename: str, regex: re.Pattern[str]) -> str:
    text = (_REPO_ROOT / filename).read_text()
    match = regex.search(text)
    assert match is not None, (
        f"could not find nousergon-lib pin in {filename}"
    )
    return match.group(1)


def test_requirements_and_dockerfile_pins_match():
    req_pin = _read_pin("requirements.txt", _REQUIREMENTS_PIN_RE)
    docker_pin = _read_pin("Dockerfile", _DOCKERFILE_PIN_RE)
    daily_news_pin = _read_pin("requirements-daily-news.txt", _REQUIREMENTS_PIN_RE)
    deploy_infra_pin = _read_pin(
        ".github/workflows/deploy-infrastructure.yml", _LAMBDA_PIN_RE
    )
    assert req_pin == docker_pin == daily_news_pin == deploy_infra_pin, (
        f"nousergon-lib pin drift: requirements.txt={req_pin!r}, "
        f"Dockerfile={docker_pin!r}, requirements-daily-news.txt={daily_news_pin!r}, "
        f".github/workflows/deploy-infrastructure.yml={deploy_infra_pin!r}. "
        f"All four must move in lockstep — the Dockerfile strips lib from "
        f"requirements.txt before pip install, so requirements-only bumps "
        f"don't propagate to the Lambda image, the slim daily-news file "
        f"carries an independent copy of the pin, and the deploy-infrastructure "
        f"workflow's drift-check step installs its own copy directly."
    )


def test_lambda_pins_match_or_are_explicitly_exempted():
    root_pin = _read_pin("requirements.txt", _REQUIREMENTS_PIN_RE)
    lambdas_dir = _REPO_ROOT / "infrastructure" / "lambdas"
    seen_group_members: dict[str, set[str]] = {}  # kind → set of lambda names

    for req_file in sorted(lambdas_dir.glob("*/requirements.txt")):
        lambda_name = req_file.parent.name
        text = req_file.read_text()
        match = _LAMBDA_PIN_RE.search(text)

        if match is None:
            continue

        lambda_pin = match.group(1)

        if lambda_name in _LAMBDA_PIN_EXEMPTIONS:
            ex = _LAMBDA_PIN_EXEMPTIONS[lambda_name]

            if ex.kind == "floor":
                # Floor: pin must be >= recorded version AND must track root.
                # A floor that lags root is a defect — the floor is already
                # satisfied, so the Lambda should track root like any other.
                assert _version_tuple(lambda_pin) >= _version_tuple(ex.version), (
                    f"{lambda_name}: floor exemption requires pin >= {ex.version}, "
                    f"got {lambda_pin!r} (reason: {ex.reason})"
                )
                assert lambda_pin == root_pin, (
                    f"{lambda_name}: floor exemption pin {lambda_pin!r} lags root "
                    f"pin {root_pin!r} — the floor ({ex.version}) is already "
                    f"satisfied by root, so this Lambda must track root directly "
                    f"(alpha-engine-config-I4802). Reason: {ex.reason}"
                )
            elif ex.kind == "ceiling":
                assert lambda_pin == ex.version, (
                    f"{lambda_name}: ceiling exemption pin {lambda_pin!r} does not "
                    f"match {ex.version!r} (reason: {ex.reason}). Re-exam: {ex.re_exam}"
                )
                assert ex.re_exam is not None, (
                    f"{lambda_name}: ceiling exemption requires re_exam date"
                )
            elif ex.kind == "group":
                assert lambda_pin == ex.version, (
                    f"{lambda_name}: group exemption pin {lambda_pin!r} does not "
                    f"match declared version {ex.version!r} — all group members "
                    f"must be in lockstep. Reason: {ex.reason}"
                )
                assert ex.members is not None, (
                    f"{lambda_name}: group exemption requires members"
                )
                assert lambda_name in ex.members, (
                    f"{lambda_name}: not listed in its own group's members"
                )
                for m in ex.members:
                    seen_group_members.setdefault(ex.kind, set()).add(m)
        else:
            assert (
                lambda_pin == root_pin
            ), f"{lambda_name}: pin {lambda_pin!r} must match root pin {root_pin!r}, or be added to _LAMBDA_PIN_EXEMPTIONS with a contract reason"

    # Ensure every declared group member actually has a requirements.txt
    for kind, members in seen_group_members.items():
        for m in members:
            req_file = lambdas_dir / m / "requirements.txt"
            assert req_file.exists(), (
                f"{m}: declared in a {kind} exemption but "
                f"{req_file.relative_to(_REPO_ROOT)} does not exist"
            )


# --------------------------------------------------------------------------- #
# Tier→model conformance floor (groom-sweep-policy §2.3 / §5).
#
# The groom dispatcher's launch decisions come from the *pinned lib*, not from
# the Lambda's own code — `nousergon_lib.groom_eligibility.TIER_MODELS` is the
# single owner of the tier→model assignment. So the policy's tier table is only
# true in production if the pinned lib is new enough to contain it.
# --------------------------------------------------------------------------- #

#: First nousergon-lib release where TIER_MODELS["high"] == "deepseek-v4-pro"
#: (nousergon-lib#252). Below this, live high-tier grooms dispatch claude-sonnet-5,
#: violating groom-sweep-policy §5 (tier table) and §7 (no Claude for groom traffic).
_TIER_MODEL_FLOOR = (0, 124, 16)


def _version_tuple(pin: str) -> tuple[int, int, int]:
    return tuple(int(part) for part in pin.lstrip("v").split("."))


def test_groom_dispatcher_pin_can_express_the_policy_tier_table():
    """The dispatcher must bundle a lib new enough to know high == deepseek-v4-pro.

    This is the check groom-sweep-policy §2.3 demands for the §5 tier table: it
    fails if the pin regresses below the release that carries the assignment,
    however that regression happens (manual edit, revived exemption, bad merge).
    """
    pin = _read_pin(
        "infrastructure/lambdas/scheduled-groom-dispatcher/requirements.txt",
        _LAMBDA_PIN_RE,
    )
    assert _version_tuple(pin) >= _TIER_MODEL_FLOOR, (
        f"scheduled-groom-dispatcher pins nousergon-lib {pin}, which predates "
        f"TIER_MODELS['high'] = 'deepseek-v4-pro'. Live complexity:high grooms "
        f"would dispatch claude-sonnet-5, violating groom-sweep-policy §5 and §7."
    )


def test_groom_dispatcher_is_not_exempted_from_the_root_pin():
    """Regression guard for the removed exemption.

    Re-adding `scheduled-groom-dispatcher` to `_LAMBDA_PIN_EXEMPTIONS` would
    reinstate equality-pinning and let the lib go stale silently again. If it ever
    genuinely needs a CEILING (a real incompatibility with root, not a floor),
    that is a deliberate change that must also update this test and say why.
    """
    assert "scheduled-groom-dispatcher" not in _LAMBDA_PIN_EXEMPTIONS, (
        "the groom dispatcher must track the root pin — its historical exemption "
        "reasons were all floors, and equality-pinning them froze the lib"
    )
