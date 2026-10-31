"""The declared cadence and every surface derived from it cannot disagree
(alpha-engine-config-I7249, sf-pipeline-policy §2.6).

§2.6's test is: "can an operator, reading ONE named file, answer 'when does
this run next, and what pages if it doesn't?' If two files must be read, the
declaration does not exist yet."

``infrastructure/preflight_sweep_cadence.json`` is that file. These tests are
what stop it from becoming a comment — they assert the CloudFormation
schedule, the deadman's evaluation window and the console rows' staleness
threshold are all the SAME value it declares, so the manifest cannot drift
into being a description of what someone intended.
"""

from __future__ import annotations

import json
import math
import pathlib

import pytest
import yaml

REPO = pathlib.Path(__file__).resolve().parent.parent
CADENCE_PATH = REPO / "infrastructure" / "preflight_sweep_cadence.json"
CFN_PATH = REPO / "infrastructure" / "cloudformation" / "alpha-engine-orchestration.yaml"
DESCRIPTOR_PATH = REPO / "registry.d" / "ae-preflight-sweep.yaml"

RULE = "PreflightSweepDailyTrigger"


@pytest.fixture(scope="module")
def cadence() -> dict:
    return json.loads(CADENCE_PATH.read_text())


@pytest.fixture(scope="module")
def template() -> dict:
    class _Loader(yaml.SafeLoader):
        pass

    # CloudFormation short forms (!Ref, !GetAtt, !Sub) are not plain YAML.
    def _tag(loader, node):
        if isinstance(node, yaml.ScalarNode):
            return loader.construct_scalar(node)
        if isinstance(node, yaml.SequenceNode):
            return loader.construct_sequence(node)
        return loader.construct_mapping(node)

    for tag in ("!Ref", "!GetAtt", "!Sub", "!Join", "!Select", "!ImportValue", "!Equals",
                "!If", "!Not", "!And", "!Or", "!FindInMap", "!Split", "!Base64", "!Cidr"):
        _Loader.add_constructor(tag, _tag)
    _Loader.add_multi_constructor("!", lambda l, s, n: _tag(l, n))
    return yaml.load(CFN_PATH.read_text(), Loader=_Loader)


def test_the_declaration_is_self_consistent(cadence):
    assert cadence["sweep_cadence"] in cadence["allowed_values"]
    assert cadence["cadence_minutes"] > 0
    assert cadence["staleness_factor"] >= 1


def test_the_schedule_does_not_hard_require_the_lambda_to_exist(template):
    """AWS::Lambda::Permission validates its FunctionName; an Events::Rule
    target ARN does not. Declaring the permission in this stack would mean an
    un-bootstrapped Lambda fails the WHOLE orchestration stack update on the
    next merge — reddening deploy-infrastructure for every unrelated commit.
    The deadman going red is the correct detector instead."""
    resources = template["Resources"]
    permissions = [
        name
        for name, body in resources.items()
        if body.get("Type") == "AWS::Lambda::Permission"
        and "PreflightSweep" in name
    ]
    assert permissions == [], (
        "the preflight-sweep invoke permission belongs in deploy.sh --bootstrap, "
        "not in this stack"
    )


def test_the_cloudformation_schedule_is_the_declared_cron(cadence, template):
    rule = template["Resources"][RULE]["Properties"]
    assert rule["ScheduleExpression"] == cadence["cron_utc"]


def test_a_daily_declaration_means_the_rule_is_enabled(cadence, template):
    rule = template["Resources"][RULE]["Properties"]
    if cadence["sweep_cadence"] == "daily":
        assert rule["State"] == "ENABLED"


# The deadman alarm (alpha-engine-preflight-sweep-no-run) left this stack for
# its codified owner, nous-ergon-ops/infrastructure/cloudwatch/alarms/
# alpha-engine-preflight-sweep-no-run.json (alpha-engine-config-I9009). This
# public repo cannot read that file in CI, so the window it encodes is pinned
# here: a cadence change that fails this test needs the matching edit to that
# file in the same arc. Its other properties (TreatMissingData: breaching, the
# PreflightSweepRunCompleted subject, OKActions) are asserted on the file in
# nous-ergon-ops.
CODIFIED_DEADMAN_PERIOD_S = 86400
CODIFIED_DEADMAN_EVALUATION_PERIODS = 2


def test_the_codified_deadman_window_is_derived_from_the_declared_cadence(cadence):
    assert cadence["cadence_minutes"] * 60 == CODIFIED_DEADMAN_PERIOD_S, (
        "cadence_minutes changed: update Period in nous-ergon-ops "
        "infrastructure/cloudwatch/alarms/alpha-engine-preflight-sweep-no-run.json "
        "to match, then this constant"
    )
    # staleness_factor 1.5 rounded up to whole periods = 2. A single late run
    # must not page; a dead sweep must.
    assert math.ceil(cadence["staleness_factor"]) == CODIFIED_DEADMAN_EVALUATION_PERIODS


def test_the_stack_no_longer_declares_the_deadman(template):
    """The deadman's definition has one home. A copy here would be reasserted
    over the codified one on every push (alpha-engine-config-I9009)."""
    assert "PreflightSweepDeadman" not in template["Resources"]


def test_the_console_descriptor_declares_the_same_cadence(cadence):
    descriptor = yaml.safe_load(DESCRIPTOR_PATH.read_text())
    bindings = [descriptor["runs"], descriptor["metrics"]]
    for binding in bindings:
        assert binding["cadence_minutes"] == cadence["cadence_minutes"], (
            "a console row whose declared cadence differs from the sweep's own "
            "would render STALE early or late — either way on a threshold "
            "nobody declared"
        )


def test_the_descriptor_names_the_component_id_every_surface_uses(cadence):
    descriptor = yaml.safe_load(DESCRIPTOR_PATH.read_text())
    assert descriptor["component_id"] == "ae-preflight-sweep"
    # `deprecated` since 2026-10-31 (alpha-engine-config-I12842): the sweep
    # still runs, over the v1 weekly SF whose trigger is disabled. A declared
    # decision must carry its reason and re-exam; it never goes quietly.
    assert descriptor["lifecycle"] in ("in-service", "deprecated")
    if descriptor["lifecycle"] != "in-service":
        assert descriptor["lifecycle_reason"]
        assert descriptor["lifecycle_reexam"]
    assert descriptor["owner"]


# ── The template must stay deployable ────────────────────────────────────────

# CloudFormation's S3-sourced --template-url ceiling (alpha-engine-config-I7250).
# deploy-infrastructure.sh used to pass `--template-body file://` for BOTH
# validate-template and create/update-stack, capped at the 51200-byte inline
# ceiling. Measured 2026-08-13 while adding the preflight-sweep resources:
# main was already at 48015 of 51200 bytes, under 7% headroom, with nothing
# anywhere reporting that — and that workflow runs on EVERY push to main with
# no path filter, so crossing the ceiling doesn't fail the PR that did it, it
# fails every subsequent merge's infra deploy for authors who never touched
# infrastructure/.
#
# I7250 moved the deploy to `aws s3 cp` + `--template-url`, which raises the
# real ceiling to 460800 bytes (CloudFormation's S3-template limit). This
# guard is retargeted at that higher ceiling, MINUS a margin — asserting
# against the raw 460800-byte hard limit would recreate the exact defect one
# order of magnitude later: a template that silently grows to within a few
# hundred bytes of a wall nothing warns about until it's crossed. The margin
# below (60800 bytes, ~13%) is deliberately generous relative to the 459-byte
# headroom that triggered this issue.
CFN_TEMPLATE_URL_HARD_LIMIT = 460800
CFN_TEMPLATE_URL_MARGIN = 60800
CFN_TEMPLATE_URL_GUARD_CEILING = CFN_TEMPLATE_URL_HARD_LIMIT - CFN_TEMPLATE_URL_MARGIN


def test_the_orchestration_template_still_fits_the_inline_deploy_limit():
    size = len(CFN_PATH.read_bytes())
    assert size <= CFN_TEMPLATE_URL_GUARD_CEILING, (
        f"{CFN_PATH.name} is {size} bytes, within {CFN_TEMPLATE_URL_MARGIN} "
        f"bytes of CloudFormation's {CFN_TEMPLATE_URL_HARD_LIMIT}-byte "
        "--template-url ceiling (deploy-infrastructure.sh uploads the "
        "template to S3 and deploys via --template-url — I7250). Trim the "
        "template, or raise CFN_TEMPLATE_URL_MARGIN here with a written "
        "rationale — never delete this guard."
    )
