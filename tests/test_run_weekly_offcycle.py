"""Pins the off-cycle weekly runner's input contracts + safety ordering.

``infrastructure/run_weekly_offcycle.sh`` fires the Saturday pipeline
off-schedule. Its whole value is that an off-cycle run is byte-identical to
the scheduled one, so these tests pin the two production input contracts the
script must reproduce:

  * ``shell`` input == the ``alpha-engine-eod-success-friday-shell-trigger``
    Lambda input (``shell_run=true`` + ``pipeline_role="shell-run"``).
  * ``full`` input == the ``alpha-engine-saturday`` EventBridge cron target
    input (``pipeline_role="weekly"``, NO ``shell_run``).

If the live CFN cron input or the Lambda input drifts, the corresponding
assertion here fails — forcing the runner to be re-synced rather than
silently firing a stale contract.

It also pins the ``full``-path safety invariant: the auto re-enable schedule
is created BEFORE the cron rule is disabled (so a failure never leaves the
weekly cadence silently dead).
"""

from __future__ import annotations

import ast
import json
import os
import re
import subprocess
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parent.parent
_INFRA = _REPO_ROOT / "infrastructure"
_RUNNER = _INFRA / "run_weekly_offcycle.sh"
_CFN = _INFRA / "cloudformation" / "alpha-engine-orchestration.yaml"
_LAMBDA = _INFRA / "lambdas" / "eod-success-friday-shell-trigger" / "index.py"


@pytest.fixture(scope="module")
def runner_text() -> str:
    return _RUNNER.read_text()


def _dry_run_input(verb: str) -> dict:
    """Run the script in --dry-run and parse the EXPANDED execution input.

    Tests the real shell-variable expansion (catches a mistyped constant),
    not just the static heredoc text. The --dry-run path only touches ``aws``
    inside guarded ``if`` conditions, so it works with no AWS creds / no CLI.
    """
    proc = subprocess.run(
        ["bash", str(_RUNNER), verb, "--dry-run"],
        capture_output=True,
        text=True,
        env={**os.environ, "AWS_REGION": "us-east-1", "ACCOUNT_ID": "711398986525"},
    )
    assert proc.returncode == 0, f"{verb} --dry-run failed:\n{proc.stderr}"
    m = re.search(r"^\s*input:\s*(\{.*\})\s*$", proc.stdout, re.MULTILINE)
    assert m, f"no 'input:' line in {verb} --dry-run output:\n{proc.stdout}"
    return json.loads(m.group(1))


def _builder_body(text: str, fn_name: str) -> str:
    """Slice a shell function body ``fn_name() { ... }`` from the script."""
    m = re.search(rf"{fn_name}\(\)\s*\{{(.*?)\n\}}", text, re.DOTALL)
    assert m, f"{fn_name}() not found in runner"
    return m.group(1)


def test_runner_exists_and_executable() -> None:
    assert _RUNNER.exists(), "run_weekly_offcycle.sh missing"
    assert os.access(_RUNNER, os.X_OK), "run_weekly_offcycle.sh must be executable"


def test_shell_input_contract() -> None:
    obj = _dry_run_input("shell")
    assert obj["shell_run"] is True
    assert obj["pipeline_role"] == "shell-run"
    # config#2248: ec2_instance_id is intentionally ABSENT — the weekly SF's
    # own CheckSpotDispatchNeeded/DispatchWeeklyFreshnessSpot states populate
    # it from a fresh ephemeral spot; this script no longer hardcodes the
    # always-on dashboard box id.
    assert "ec2_instance_id" not in obj
    assert obj["sns_topic_arn"].endswith(":alpha-engine-alerts-muted")


def test_full_input_contract() -> None:
    obj = _dry_run_input("full")
    assert obj["pipeline_role"] == "weekly"
    assert "shell_run" not in obj, "full weekly run must NOT set shell_run"
    assert "ec2_instance_id" not in obj  # config#2248 — see test_shell_input_contract
    assert obj["sns_topic_arn"].endswith(":alpha-engine-alerts-muted")


def test_full_input_matches_live_cron_target() -> None:
    """The ``full`` builder must reproduce the CFN SaturdayTrigger Input."""
    cfn = _CFN.read_text()
    block = cfn.split("SaturdayTrigger:", 1)[1].split("WeekdayPipelineSchedule:", 1)[0]
    # Narrow to the Input: !Sub | heredoc (the surrounding region carries a
    # comment mentioning "shell_run mode" that is not part of the target input).
    after_input = block.split("Input:", 1)[1]
    # The JSON closes with a "}" on its own (dedented) line, so this stops
    # correctly at the first line-leading "}" after "Input:".
    m = re.search(r".*?\n\s*\}", after_input, re.DOTALL)
    assert m, "could not isolate the SaturdayTrigger Input JSON"
    input_block = m.group(0)
    assert '"pipeline_role": "weekly"' in input_block
    assert "shell_run" not in input_block
    # config#2248: the live cron Input no longer carries ec2_instance_id
    # either — both sides of the contract (CFN + this offcycle runner) went
    # through the dispatch path together.
    assert "ec2_instance_id" not in input_block
    assert _dry_run_input("full")["pipeline_role"] == "weekly"

    # alpha-engine-config-I9756 deliverable 3. This test's docstring has
    # always said the builder "must reproduce the CFN SaturdayTrigger Input",
    # and until 2026-09-04 it checked three keys and never `sns_topic_arn` —
    # so when the CFN target was repointed to AlertsMutedTopic and this
    # script was not, the test stayed green across the drift it names.
    # (The very next helper's docstring warns about hand-checking NAMED keys;
    # this assertion is that warning applied to the test above it.)
    #
    # Compared by SNS logical resource id rather than by ARN: the CFN side is
    # a `!Sub` reference (`${AlertsMutedTopic}`) and the script side is an
    # interpolated literal, so the only thing both sides spell the same way
    # is the topic NAME.
    cfn_topic = re.search(r'"sns_topic_arn":\s*"\$\{(\w+)\}"', input_block)
    assert cfn_topic, "SaturdayTrigger Input declares no sns_topic_arn"
    expected_name = {
        "AlertsTopic": "alpha-engine-alerts",
        "AlertsMutedTopic": "alpha-engine-alerts-muted",
    }[cfn_topic.group(1)]
    assert _dry_run_input("full")["sns_topic_arn"].endswith(":" + expected_name), (
        "the offcycle runner routes the weekly SF somewhere the live cron "
        "target does not; every execution it starts is graded for routing by "
        "crucible/gate.py::_clause_old_alerts_muted"
    )
    assert _dry_run_input("shell")["sns_topic_arn"].endswith(":" + expected_name)


def _lambda_shell_input() -> dict:
    """The dict literal `_build_shell_run_input` returns, parsed from source.

    Derived with `ast` rather than substring-matched. The previous version of
    the lockstep test below hand-checked two NAMED keys, so a field added to
    one side and not the other was invisible to it — which is exactly how
    `skip_parity` came to be on the Saturday cadence input and absent from
    this one for a day (alpha-engine-config-I7309). Comparing the whole key
    set is the only form of this assertion that actually holds.
    """
    tree = ast.parse(_LAMBDA.read_text())
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == "_build_shell_run_input":
            for sub in ast.walk(node):
                if isinstance(sub, ast.Dict):
                    out = {}
                    for k, v in zip(sub.keys, sub.values):
                        assert isinstance(k, ast.Constant), (
                            "_build_shell_run_input has a non-literal key; this "
                            "derivation needs updating"
                        )
                        out[k.value] = (
                            v.value if isinstance(v, ast.Constant) else "<non-literal>"
                        )
                    return out
    raise AssertionError(
        "_build_shell_run_input not found (or returns no dict literal) in "
        f"{_LAMBDA.name} — re-derive this helper before trusting the lockstep test"
    )


def test_shell_input_matches_lambda() -> None:
    """The ``shell`` builder must reproduce the friday-shell Lambda input —
    KEY FOR KEY, not just on the two keys someone remembered to list.

    ``sns_topic_arn`` is compared by presence only: the Lambda reads it from
    the process environment and the runner from its own literal, so the two
    VALUES legitimately differ in source form while naming the same topic.
    """
    lam_input = _lambda_shell_input()
    obj = _dry_run_input("shell")

    assert set(obj) == set(lam_input), (
        "the off-cycle `shell` input and the friday-shell Lambda input have "
        f"diverged: runner-only={sorted(set(obj) - set(lam_input))}, "
        f"lambda-only={sorted(set(lam_input) - set(obj))}. The whole point of "
        "the shell verb is that an off-cycle rehearsal is identical to the "
        "automatic one; a key on one side only means the two rehearse "
        "different pipelines."
    )
    for key in sorted(set(obj) - {"sns_topic_arn"}):
        assert obj[key] == lam_input[key], (
            f"{key}: runner={obj[key]!r} but Lambda={lam_input[key]!r}"
        )

    # The two values every caller of this contract depends on.
    assert obj["shell_run"] is True
    assert obj["pipeline_role"] == "shell-run"


#: Brian ruling 2026-08-14 ("disable it in the meantime"): no AUTOMATIC launch
#: path may run pit_parity until alpha-engine-config-I7309 clears. An operator
#: StartExecution passing {"skip_parity": false} still overrides it — that
#: override is the working loop for I7309 and is deliberately NOT closed here.
_NO_AUTOMATIC_PARITY_RULING = "alpha-engine-config-I7309"


def test_no_automatic_launch_path_runs_parity() -> None:
    """Checked across ALL THREE automatic inputs at once, not one at a time.

    Parity was disabled on the Saturday cadence trigger on 2026-08-13 and left
    enabled on the Friday rehearsal, so the rehearsal ran a stage production
    skips — and died in it (execution friday-shell-2026-08-14-validate-i7386,
    States.DataLimitExceeded inside ParityParallel) after clearing every stage
    Saturday actually runs. A per-path assertion would have passed in that
    state; this one does not.
    """
    lam_input = _lambda_shell_input()
    assert lam_input.get("skip_parity") is True, (
        "the friday-shell trigger Lambda no longer sets skip_parity — the "
        "rehearsal would run a stage the cadence run skips "
        f"({_NO_AUTOMATIC_PARITY_RULING})"
    )
    for verb in ("shell", "full"):
        obj = _dry_run_input(verb)
        assert obj.get("skip_parity") is True, (
            f"run_weekly_offcycle.sh `{verb}` input no longer sets skip_parity "
            f"({_NO_AUTOMATIC_PARITY_RULING})"
        )
    cfn = _CFN.read_text()
    assert '"skip_parity": true' in cfn, (
        "the Saturday cadence trigger's CFN Input no longer sets skip_parity "
        f"({_NO_AUTOMATIC_PARITY_RULING})"
    )


def test_full_targets_correct_state_machine(runner_text: str) -> None:
    assert "ne-weekly-freshness-pipeline" in runner_text


def test_full_suppresses_then_starts_safely(runner_text: str) -> None:
    """Fail-loud ordering: schedule re-enable, THEN disable cron, THEN start."""
    body = _builder_body(runner_text, "do_full")
    i_schedule = body.index("schedule_reenable")
    i_disable = body.index("disable-rule")
    i_start = body.index("start_execution")
    assert i_schedule < i_disable < i_start, (
        "do_full must schedule the auto re-enable before disabling the cron, "
        "and disable the cron before starting the execution"
    )


def test_reenable_role_scoped_to_saturday_rule(runner_text: str) -> None:
    """The scheduler role grants events:EnableRule on the Saturday rule ONLY."""
    assert "events:EnableRule" in runner_text
    # Parse the role's trust policy and compare the principal exactly, rather
    # than substring-matching a hostname in the script text (CodeQL
    # py/incomplete-url-substring-sanitization, alpha-engine-config-I3042).
    m = re.search(r"trust='(\{.*?\})'", runner_text)
    assert m, "run_weekly_offcycle.sh must declare the re-enable role's trust policy"
    trust = json.loads(m.group(1))
    services = [stmt["Principal"]["Service"] for stmt in trust["Statement"]]
    assert services == ["scheduler.amazonaws.com"]
    # The inline policy Resource is the single Saturday rule ARN.
    assert "rule/${SATURDAY_RULE}" in runner_text or "SATURDAY_RULE_ARN" in runner_text


def test_reenable_schedule_self_deletes(runner_text: str) -> None:
    assert "--action-after-completion DELETE" in runner_text
