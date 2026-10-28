"""`armed_alarms` classifies the BREACHING half of a split pair, never the other.

alpha-engine-config-I9121. `alpha-engine-config-I8118` split several alarms in
two, one latched state per meaning: a `-dead` / `-no-datapoint` half that owns
SILENCE and therefore carries ``TreatMissingData=breaching``, and a
`-violation` / `-latency` half that owns the CONDITION and was changed to
``notBreaching`` so absence could never be read as the condition.

`automation_pause.json`'s `armed_alarms` block was not updated with that split.
Its population is defined as the alarms whose live treatment is ``breaching``
— the ones that can latch ALARM from silence alone — so the three leftover
CONDITION-half entries could never appear in the live breaching set and emitted
``[armed-missing-in-aws]`` on every single run. Measured 2026-08-28 against
live CloudWatch, that plus two unclassified breaching alarms was the whole
reason `pause-check-alert.yml` had failed 33 of 33 runs and `sf-arn-drift-check
.yml` was red on main.

The failure mode is silent in both directions and neither direction is visible
from this repo alone — the alarms are created in `nous-ergon-ops`. So the
pairing is pinned here: for every split pair, the breaching half MUST be
declared and the notBreaching half MUST NOT be.

FOLDED PAIRS (alpha-engine-config-I11792, Brian's "Cut redundant" ruling
2026-10-15). The two exposure-probe pairs were folded back into one alarm each:
the `-violation` count is published on every tick, so `-violation` became
``breaching`` and carries silence itself, and `-dead` was deleted. For those the
rule inverts: the `-violation` half MUST be declared armed and the `-dead` half
MUST sit in ``retired_alarms`` (never ``armed_alarms``), naming its restore point.

RETIRED SPLIT PAIRS (alpha-engine-config-I11792, 2026-10-28). The Director's
silence half, `alpha-engine-director-plan-no-datapoint`, was retired as
REDUNDANT: the Director stage is terminal for the weekly SF, so a missing run
pages through sf-telegram-notifier, and the freshness monitor's
`director_action_plan` row catches a plan never written. Its condition half
still must never be declared armed, and the retired half must sit in
``retired_alarms``, never ``armed_alarms``.
"""

from __future__ import annotations

import importlib.util
import json
import pathlib

import pytest

REPO = pathlib.Path(__file__).resolve().parent.parent
MANIFEST_PATH = REPO / "infrastructure" / "automation_pause.json"

#: ``(silence half — breaching, must be declared; condition half — notBreaching,
#: must not be)``. Every treatment here was read off live CloudWatch on
#: 2026-08-28 with ``aws cloudwatch describe-alarms``, and each alarm's own
#: AlarmDescription states the same split in prose.
I8118_PAIRS: list[tuple[str, str]] = []

#: ``(retired silence half, condition half — notBreaching, must not be
#: declared)`` — see the module docstring's RETIRED SPLIT PAIRS paragraph.
RETIRED_SPLIT_PAIRS = [
    (
        "alpha-engine-director-plan-no-datapoint",
        "alpha-engine-director-plan-latency",
    ),
]

#: ``(retired silence half, folded alarm now breaching and armed)`` — see the
#: module docstring's FOLDED PAIRS paragraph.
FOLDED_PAIRS = [
    (
        "alpha-engine-console-exposure-probe-dead",
        "alpha-engine-console-exposure-probe-violation",
    ),
    (
        "alpha-engine-router-exposure-probe-dead",
        "alpha-engine-router-exposure-probe-violation",
    ),
]

#: Breaching alarms that are not half of a pair but were born unclassified.
STANDALONE_BREACHING = ["alpha-engine-stage-coverage-sweep-dead"]


@pytest.fixture(scope="module")
def manifest() -> dict:
    return json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def module():
    spec = importlib.util.spec_from_file_location(
        "automation_pause_i9121", REPO / "infrastructure" / "automation_pause.py"
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def classification_findings(manifest: dict, module) -> list[str]:
    """Every way the split-pair classification can be wrong. Pure, so the
    seeded regressions below can grade a mutated manifest."""
    declared = set(module.armed_alarm_names(manifest))
    findings: list[str] = []
    for breaching, not_breaching in I8118_PAIRS:
        if breaching not in declared:
            findings.append(
                f"{breaching} is the breaching half of an I8118 pair and is not "
                "declared — automation_pause.py --check will report it as "
                "[alarm-undeclared] on every run"
            )
        if not_breaching in declared:
            findings.append(
                f"{not_breaching} is notBreaching live and can never appear in the "
                "breaching population, so declaring it here can only produce "
                "[armed-missing-in-aws] forever"
            )
    retired_names = set(module.retired_alarm_names(manifest))
    for silence, not_breaching in RETIRED_SPLIT_PAIRS:
        if silence in declared or silence not in retired_names:
            findings.append(
                f"{silence} was retired as redundant on 2026-10-28; it belongs in "
                "retired_alarms, never armed_alarms"
            )
        if not_breaching in declared:
            findings.append(
                f"{not_breaching} is notBreaching live and can never appear in the "
                "breaching population, so declaring it here can only produce "
                "[armed-missing-in-aws] forever"
            )
    for name in STANDALONE_BREACHING:
        if name not in declared:
            findings.append(f"{name} is breaching live and is not classified")
    retired = set(module.retired_alarm_names(manifest))
    for dead, folded in FOLDED_PAIRS:
        if folded not in declared:
            findings.append(
                f"{folded} carries silence since the 2026-10-15 fold "
                "(TreatMissingData=breaching) and is not declared armed"
            )
        if dead in declared or dead not in retired:
            findings.append(
                f"{dead} was folded into {folded} and deleted; it belongs in "
                "retired_alarms, never armed_alarms"
            )
    return findings


def test_each_split_pair_declares_the_breaching_half_only(manifest, module):
    assert classification_findings(manifest, module) == []


def test_the_condition_half_prose_is_kept_under_the_underscore_marker(manifest):
    """Removing an entry outright would delete the reasoning with it.

    The repo's own convention for "keep the prose, drop the classification" is
    the underscore prefix `armed_alarm_names()` skips — the same device
    `_alpha-engine-watch-plane-overseer-intake-dlq-age` already uses for
    exactly this reason. Reconstructing WHY an entry left is the whole point.
    """
    for _, not_breaching in I8118_PAIRS + RETIRED_SPLIT_PAIRS:
        key = "_" + not_breaching
        assert key in manifest["armed_alarms"], (
            f"{not_breaching} was removed without keeping its record; re-add it "
            f"as {key!r} with the original text quoted"
        )
        assert "I8118" in manifest["armed_alarms"][key]["reason"], (
            f"{key} does not say WHY it stopped being a classification"
        )


def test_every_active_armed_entry_names_a_plausible_alarm(manifest):
    for key, entry in manifest["armed_alarms"].items():
        if key.startswith("_"):
            continue
        assert entry.get("reason", "").strip(), f"{key} is declared armed with no reason"


def test_re_declaring_a_condition_half_is_rejected(manifest, module):
    """Seeded regression — the guard must fail on the shape it exists for."""
    seeded = json.loads(json.dumps(manifest))
    seeded["armed_alarms"]["alpha-engine-director-plan-latency"] = {"reason": "x"}
    findings = classification_findings(seeded, module)
    assert any("can never appear in the breaching population" in f for f in findings), findings


def test_dropping_a_breaching_half_is_rejected(manifest, module):
    """Seeded regression in the other direction: an undeclared breaching alarm
    is the `[alarm-undeclared]` finding that started this."""
    seeded = json.loads(json.dumps(manifest))
    del seeded["armed_alarms"]["alpha-engine-stage-coverage-sweep-dead"]
    findings = classification_findings(seeded, module)
    assert any("is breaching live and is not classified" in f for f in findings), findings
