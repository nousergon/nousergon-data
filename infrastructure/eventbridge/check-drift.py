#!/usr/bin/env python3
"""check-drift.py — Diff codified EventBridge rule event-patterns (SF ARNs)
against live AWS state.

**Background (alpha-engine-config#1464).** The 2026-06-29 Step Function
rename (config#1381: ``alpha-engine-{saturday,weekday}`` → ``ne-weekly-
freshness-pipeline`` / ``ne-preopen-trading-pipeline``) silently orphaned
several EventBridge rules that reference an SF ARN/name in their
``EventPattern`` — they kept matching the OLD name, so they simply stopped
firing (no error, no alert). The two CFN-managed cron rules
(``SaturdayTrigger`` / ``WeekdayTrigger`` in
``infrastructure/cloudformation/alpha-engine-orchestration.yaml``) are safe
from this because ``deploy-infrastructure.yml`` reconciles them on every
push to main. The bitten rules were all managed OUTSIDE CloudFormation by
individual Lambda ``deploy.sh`` scripts under ``infrastructure/lambdas/*/``
— those only reconcile the live rule when an OPERATOR manually re-runs
``deploy.sh`` (some only on ``--bootstrap``, i.e. never again after first
create). A source-level rename that isn't followed by a manual redeploy
leaves live AWS silently stale. This script is the CI backstop: it doesn't
require anyone to remember to redeploy, it just fails loudly when source and
live disagree.

**Source of truth.** Each ``infrastructure/lambdas/<name>/deploy.sh`` that
wires an EventBridge rule off Step Functions status-change events embeds its
own ``EVENT_PATTERN=$(cat <<EOF ... EOF)`` heredoc + ``RULE_NAME="..."``
literal — those two are, by construction, the SAME values the script would
push to AWS with ``aws events put-rule``. This script discovers every such
deploy.sh (glob + regex, no hardcoded rule registry to keep in sync), fills
in the two placeholders the heredoc uses (``${REGION}`` / ``${ACCOUNT_ID}``,
using each script's own literal fallback defaults), and diffs the resulting
JSON against ``aws events describe-rule --name <rule>``.

Drift cases (all exit non-zero):
  * Live rule's EventPattern differs from the codified heredoc
    (content-drift) — reported with the specific stateMachineArn sets on
    each side when that's where the difference is, since that's the
    class of drift this guard exists to catch.
  * Rule not found on AWS at all                (missing-in-aws)
  * A discovered EVENT_PATTERN block isn't valid JSON once substituted
    (source-error — the deploy.sh itself is broken)

JSON is compared after normalization (sorted object keys, order-independent
array comparison), so cosmetic differences (whitespace, array order) don't
trip the check.

Usage:
  ./infrastructure/eventbridge/check-drift.py             # check every discovered rule
  ./infrastructure/eventbridge/check-drift.py --rule NAME  # check one rule (by RULE_NAME)

Requires AWS creds with events:DescribeRule on the target rules. Locally:
any admin profile. In CI: intended to reuse the same OIDC role as
``iam-drift-check.yml`` (``github-actions-iam-drift-check``), which will
need `events:DescribeRule` added to its policy — see this repo's PR for
config#1464 for the exact IAM diff (that role is codified in
crucible-executor, not this repo).
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent.resolve()
REPO_ROOT = SCRIPT_DIR.parent.parent
LAMBDAS_DIR = REPO_ROOT / "infrastructure" / "lambdas"

# Fallback defaults mirrored from each deploy.sh's own
# `REGION="${AWS_REGION:-us-east-1}"` / `ACCOUNT_ID="${ACCOUNT_ID:-711398986525}"`
# lines — used ONLY to resolve the `${REGION}`/`${ACCOUNT_ID}` placeholders
# inside a script's EVENT_PATTERN heredoc so it can be parsed as JSON and
# compared. The live rule lookup itself always uses the ambient AWS CLI
# region/credentials, not these constants.
DEFAULT_REGION = "us-east-1"
DEFAULT_ACCOUNT_ID = "711398986525"

_EVENT_PATTERN_RE = re.compile(
    r"EVENT_PATTERN=\$\(cat <<EOF\n(.*?)\nEOF\n\)", re.DOTALL
)
_RULE_NAME_RE = re.compile(r'^RULE_NAME="([^"]+)"', re.MULTILINE)


def _discover_codified_rules() -> list[dict]:
    """Scan every `infrastructure/lambdas/*/deploy.sh` for an EVENT_PATTERN
    heredoc keyed on `stateMachineArn`. Returns one entry per rule found,
    each either `{"rule_name", "source_file", "expected_pattern"}` or, if
    the heredoc doesn't parse, `{"rule_name", "source_file", "error"}`.

    Deliberately a scan, not a hardcoded table — mirrors
    `infrastructure/iam/check-drift.py`'s glob-every-codified-file
    philosophy, so a new Lambda wiring an SF-status EventBridge rule is
    picked up automatically without editing this script.
    """
    rules: list[dict] = []
    for deploy_sh in sorted(LAMBDAS_DIR.glob("*/deploy.sh")):
        text = deploy_sh.read_text()
        pattern_match = _EVENT_PATTERN_RE.search(text)
        if not pattern_match:
            continue
        pattern_src = pattern_match.group(1)
        if "stateMachineArn" not in pattern_src:
            continue

        rule_match = _RULE_NAME_RE.search(text)
        if not rule_match:
            rules.append({
                "rule_name": f"<unknown in {deploy_sh.name}>",
                "source_file": deploy_sh,
                "error": (
                    f"{deploy_sh} has an EVENT_PATTERN keyed on "
                    f"stateMachineArn but no RULE_NAME=\"...\" literal — "
                    f"can't determine which live rule to check"
                ),
            })
            continue
        rule_name = rule_match.group(1)

        resolved = pattern_src.replace("${REGION}", DEFAULT_REGION).replace(
            "${ACCOUNT_ID}", DEFAULT_ACCOUNT_ID
        )
        try:
            expected_pattern = json.loads(resolved)
        except json.JSONDecodeError as exc:
            rules.append({
                "rule_name": rule_name,
                "source_file": deploy_sh,
                "error": (
                    f"EVENT_PATTERN in {deploy_sh} is not valid JSON after "
                    f"substituting REGION/ACCOUNT_ID placeholders ({exc})"
                ),
            })
            continue

        rules.append({
            "rule_name": rule_name,
            "source_file": deploy_sh,
            "expected_pattern": expected_pattern,
        })
    return rules


def _aws_events(*args: str, allow_missing: bool = False) -> dict | list | str | None:
    """Call `aws events ...` and return the parsed JSON output.

    If `allow_missing` and the CLI fails with ResourceNotFoundException,
    return None instead of aborting — the caller turns that into a
    missing-in-aws drift finding rather than a hard failure, since "the
    rule doesn't exist" is itself a legitimate (if extreme) drift case.
    """
    result = subprocess.run(
        ["aws", "events", *args, "--output", "json"],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        if allow_missing and "ResourceNotFoundException" in result.stderr:
            return None
        sys.stderr.write(
            f"AWS CLI failed: aws events {' '.join(args)}\n"
            f"stderr: {result.stderr}\n"
        )
        sys.exit(2)
    return json.loads(result.stdout) if result.stdout.strip() else {}


def _canonical(obj):
    """Recursively sort dict keys and (order-independently) list elements
    so cosmetic reordering doesn't register as drift."""
    if isinstance(obj, dict):
        return {k: _canonical(obj[k]) for k in sorted(obj)}
    if isinstance(obj, list):
        canon_items = [_canonical(v) for v in obj]
        try:
            return sorted(canon_items, key=lambda x: json.dumps(x, sort_keys=True))
        except TypeError:
            return canon_items
    return obj


def _canonical_json(obj) -> str:
    return json.dumps(_canonical(obj), sort_keys=True, separators=(",", ":"))


def _extract_state_machine_arns(pattern: dict) -> set[str]:
    """Pull the `detail.stateMachineArn` allow-list out of an EventPattern.
    Handles both list and bare-string shapes (AWS accepts either)."""
    detail = pattern.get("detail", {}) if isinstance(pattern, dict) else {}
    arns = detail.get("stateMachineArn", [])
    if isinstance(arns, str):
        return {arns}
    if isinstance(arns, list):
        return set(arns)
    return set()


def _check_rule(rule: dict) -> list[str]:
    """Return list of drift findings for one codified rule. Empty means clean."""
    rule_name = rule["rule_name"]

    if "error" in rule:
        return [f"{rule_name}: {rule['error']}"]

    expected_pattern = rule["expected_pattern"]
    source_rel = rule["source_file"].relative_to(REPO_ROOT)

    desc = _aws_events("describe-rule", "--name", rule_name, allow_missing=True)
    if desc is None:
        return [
            f"{rule_name}: codified in {source_rel} but rule not found on "
            f"AWS (run that lambda's deploy.sh --bootstrap, or re-run "
            f"deploy.sh if it was deleted out of band)"
        ]

    live_pattern_raw = desc.get("EventPattern")
    if not live_pattern_raw:
        return [
            f"{rule_name}: live rule exists but has no EventPattern "
            f"(codified in {source_rel} expects one keyed on stateMachineArn)"
        ]

    try:
        live_pattern = json.loads(live_pattern_raw)
    except json.JSONDecodeError as exc:
        return [f"{rule_name}: live EventPattern is not valid JSON ({exc})"]

    if _canonical_json(expected_pattern) == _canonical_json(live_pattern):
        return []

    expected_arns = _extract_state_machine_arns(expected_pattern)
    live_arns = _extract_state_machine_arns(live_pattern)
    if expected_arns != live_arns:
        return [
            f"{rule_name}: live EventPattern's stateMachineArn set differs "
            f"from {source_rel} (content drift)\n"
            f"      codified: {sorted(expected_arns)}\n"
            f"      live:     {sorted(live_arns)}"
        ]

    return [
        f"{rule_name}: live EventPattern differs from {source_rel} "
        f"(stateMachineArn set matches; some other field — e.g. the "
        f"status filter — has drifted)"
    ]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--rule", help="Check one rule by RULE_NAME (default: every discovered rule)"
    )
    args = parser.parse_args()

    rules = _discover_codified_rules()

    if args.rule:
        rules = [r for r in rules if r["rule_name"] == args.rule]
        if not rules:
            sys.stderr.write(
                f"ERROR: no codified EVENT_PATTERN found for rule "
                f"'{args.rule}' under {LAMBDAS_DIR}/*/deploy.sh\n"
            )
            return 2

    if not rules:
        print(
            f"No codified stateMachineArn-keyed EventBridge rules found "
            f"under {LAMBDAS_DIR}/*/deploy.sh — nothing to check."
        )
        return 0

    total_findings: list[str] = []
    for rule in rules:
        total_findings.extend(_check_rule(rule))

    if total_findings:
        print(f"EventBridge SF-ARN drift detected ({len(total_findings)} finding(s)):")
        for f in total_findings:
            print(f"  - {f}")
        return 1

    rule_names = ", ".join(r["rule_name"] for r in rules)
    print(f"OK: no EventBridge SF-ARN drift for {rule_names}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
