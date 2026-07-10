#!/usr/bin/env python3
"""check-drift.py — Diff codified Step Function `LoggingConfiguration` against
live AWS state.

**Background (alpha-engine-config#1464).** `LoggingConfiguration` survives a
plain `update-state-machine --definition ...` call (that's a partial update;
an omitted `loggingConfiguration` is left unchanged) but is DROPPED whenever
a Step Function gets recreated rather than updated — e.g. a CloudFormation
replacement triggered by a `StateMachineName` change. The 2026-06-29 `ne-*`
rename (config#1381) did exactly that: the two CFN-managed SFs
(`ne-weekly-freshness-pipeline`, `ne-preopen-trading-pipeline`) came back
with NO execution logging, silently breaking the L274 MutexConflict
CloudWatch metric-filter chain (config#729) until a later PR restored the
CFN `LoggingConfiguration` block. Nothing caught the gap in between — this
script is the CI backstop.

**Source of truth is split across two files** (this system's actual layout,
not a hypothetical one):

  * `infrastructure/cloudformation/alpha-engine-orchestration.yaml` —
    declares `LoggingConfiguration` for the two CFN-owned state machines
    (`SaturdayPipeline` / `ne-weekly-freshness-pipeline` and
    `WeekdayPipeline` / `ne-preopen-trading-pipeline`). The CFN comment on
    those resources spells out why this is safe from the deploy-vs-drift
    trap: the deploy script's `update-state-machine` call passes only
    `--definition`, so it never wipes this CFN-set config.
  * `infrastructure/deploy-infrastructure.sh` — the EOD SF
    (`ne-postclose-trading-pipeline`) is NOT in CloudFormation (script-
    managed); its `EOD_LOGGING_CONFIG` literal there is the source of truth,
    passed explicitly on every `update-state-machine` / `create-state-machine`
    call. The backlog-groom SF (`alpha-engine-groom-pipeline`) is also
    script-managed but its `update_or_create` call deliberately omits a
    logging arg ("preserving its current no-logging behavior exactly", per
    that script's comment) — so this guard's codified expectation for groom
    is "no logging enabled", and it flags drift if that ever changes without
    a matching source update.

This script parses both files (regex-based textual extraction — CFN's
`!Ref`/`!GetAtt`/`!Sub` intrinsics aren't valid YAML for a stock loader, and
this repo's own test suite already made the same "slice the text, don't
pretend to fully parse CFN" call — see
`tests/test_deploy_step_function_eventbridge_input.py`) into one expected
`LoggingConfiguration` per state machine, then diffs each against
`aws stepfunctions describe-state-machine --query loggingConfiguration`.

Drift cases (all exit non-zero):
  * Live `level` differs from codified (e.g. ERROR expected, live OFF —
    exactly the recreate-drops-logging bug class)
  * Live `includeExecutionData` differs from codified
  * Live log group name differs from codified (comparing just the
    `log-group:<name>:` component, not full ARN, so this doesn't false-
    positive on account/region differences between this script's parsing
    defaults and the live account)
  * A codified state machine isn't found on AWS at all (missing-in-aws)
  * The source files don't parse the way this script expects (source-error)

Usage:
  ./infrastructure/step-functions/check-drift.py               # check every codified SF
  ./infrastructure/step-functions/check-drift.py --name NAME   # check one (by SF name)

Requires AWS creds with states:DescribeStateMachine on the target state
machines. In CI: intended to reuse the same OIDC role as
`iam-drift-check.yml` (`github-actions-iam-drift-check`), which will need
`states:DescribeStateMachine` added to its policy — see this repo's PR for
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
CFN_TEMPLATE = REPO_ROOT / "infrastructure" / "cloudformation" / "alpha-engine-orchestration.yaml"
DEPLOY_INFRA_SH = REPO_ROOT / "infrastructure" / "deploy-infrastructure.sh"

# Fallback defaults used only to build the ARN this script queries AWS with
# (mirrors infrastructure/eventbridge/check-drift.py's same constants).
DEFAULT_REGION = "us-east-1"
DEFAULT_ACCOUNT_ID = "711398986525"

_TOP_LEVEL_KEY_RE = re.compile(r"^  ([A-Za-z0-9]+):\s*$", re.MULTILINE)


def _cfn_resource_blocks(text: str) -> dict[str, str]:
    """Split the CFN template body into {logical_id: block_text}, where
    block_text runs from one top-level (2-space-indented) key to the next.
    Deliberately not a YAML parser — see module docstring."""
    matches = list(_TOP_LEVEL_KEY_RE.finditer(text))
    blocks: dict[str, str] = {}
    for i, m in enumerate(matches):
        start = m.end()
        end = matches[i + 1].start() if i + 1 < len(matches) else len(text)
        blocks[m.group(1)] = text[start:end]
    return blocks


def _discover_expected_from_cfn() -> list[dict]:
    """Extract expected LoggingConfiguration for the two CFN-owned SFs."""
    if not CFN_TEMPLATE.is_file():
        return [{
            "sf_name": "<cfn-template-missing>",
            "source_file": CFN_TEMPLATE,
            "error": f"{CFN_TEMPLATE} not found",
        }]

    text = CFN_TEMPLATE.read_text()
    blocks = _cfn_resource_blocks(text)
    results: list[dict] = []

    for logical_id, block in blocks.items():
        if "Type: AWS::StepFunctions::StateMachine" not in block:
            continue

        name_match = re.search(r"StateMachineName:\s*(\S+)", block)
        if not name_match:
            results.append({
                "sf_name": f"<{logical_id}>",
                "source_file": CFN_TEMPLATE,
                "error": f"{logical_id} in {CFN_TEMPLATE.name} has no StateMachineName",
            })
            continue
        sf_name = name_match.group(1)

        if "LoggingConfiguration:" not in block:
            results.append({
                "sf_name": sf_name,
                "source_file": CFN_TEMPLATE,
                "expected_level": "OFF",
                "expected_include_execution_data": None,
                "expected_log_group_name": None,
            })
            continue

        logging_block = block.split("LoggingConfiguration:", 1)[1]
        level_match = re.search(r"Level:\s*(\S+)", logging_block)
        include_match = re.search(r"IncludeExecutionData:\s*(\S+)", logging_block)
        getatt_match = re.search(r"LogGroupArn:\s*!GetAtt\s+(\w+)\.Arn", logging_block)

        if not (level_match and include_match and getatt_match):
            results.append({
                "sf_name": sf_name,
                "source_file": CFN_TEMPLATE,
                "error": (
                    f"{logical_id} in {CFN_TEMPLATE.name} has a "
                    f"LoggingConfiguration block this script couldn't fully "
                    f"parse (expected Level: / IncludeExecutionData: / "
                    f"LogGroupArn: !GetAtt X.Arn)"
                ),
            })
            continue

        log_group_logical_id = getatt_match.group(1)
        log_group_block = blocks.get(log_group_logical_id, "")
        log_group_name_match = re.search(r"LogGroupName:\s*(\S+)", log_group_block)
        if not log_group_name_match:
            results.append({
                "sf_name": sf_name,
                "source_file": CFN_TEMPLATE,
                "error": (
                    f"{logical_id} references LogGroupArn !GetAtt "
                    f"{log_group_logical_id}.Arn but {log_group_logical_id} "
                    f"has no resolvable LogGroupName in {CFN_TEMPLATE.name}"
                ),
            })
            continue

        results.append({
            "sf_name": sf_name,
            "source_file": CFN_TEMPLATE,
            "expected_level": level_match.group(1),
            "expected_include_execution_data": include_match.group(1).lower() == "true",
            "expected_log_group_name": log_group_name_match.group(1),
        })

    return results


def _discover_expected_from_deploy_script() -> list[dict]:
    """Extract expected LoggingConfiguration for the script-managed SFs
    (EOD = explicit logging config; groom = deliberately no logging)."""
    if not DEPLOY_INFRA_SH.is_file():
        return [{
            "sf_name": "<deploy-infrastructure-sh-missing>",
            "source_file": DEPLOY_INFRA_SH,
            "error": f"{DEPLOY_INFRA_SH} not found",
        }]

    text = DEPLOY_INFRA_SH.read_text()
    results: list[dict] = []

    # --- EOD: explicit LoggingConfiguration ---------------------------------
    eod_arn_match = re.search(
        r'EOD_ARN="arn:aws:states:\$REGION:\$\{ACCOUNT_ID\}:stateMachine:([\w-]+)"',
        text,
    )
    eod_log_group_match = re.search(r'EOD_LOG_GROUP_NAME="([^"]+)"', text)
    eod_config_match = re.search(r"EOD_LOGGING_CONFIG='(\{.*?\})'", text)

    if eod_arn_match and eod_log_group_match and eod_config_match:
        eod_name = eod_arn_match.group(1)
        eod_log_group_name = eod_log_group_match.group(1)
        # The logGroupArn value inside EOD_LOGGING_CONFIG is itself a shell
        # variable substitution (`...logGroupArn":"'"$EOD_LOG_GROUP_ARN"'"}`),
        # not a literal — pull level/includeExecutionData directly out of the
        # literal JSON text, and trust EOD_LOG_GROUP_NAME (extracted above,
        # itself a plain literal) for the log group name.
        level_match = re.search(r'"level":"([^"]+)"', eod_config_match.group(1))
        include_match = re.search(
            r'"includeExecutionData":(true|false)', eod_config_match.group(1)
        )
        if level_match and include_match:
            results.append({
                "sf_name": eod_name,
                "source_file": DEPLOY_INFRA_SH,
                "expected_level": level_match.group(1),
                "expected_include_execution_data": include_match.group(1) == "true",
                "expected_log_group_name": eod_log_group_name,
            })
        else:
            results.append({
                "sf_name": eod_name,
                "source_file": DEPLOY_INFRA_SH,
                "error": (
                    f"EOD_LOGGING_CONFIG in {DEPLOY_INFRA_SH.name} didn't "
                    f"parse as expected (level/includeExecutionData)"
                ),
            })
    else:
        results.append({
            "sf_name": "<eod-sf-unresolved>",
            "source_file": DEPLOY_INFRA_SH,
            "error": (
                f"Couldn't find EOD_ARN / EOD_LOG_GROUP_NAME / "
                f"EOD_LOGGING_CONFIG literals in {DEPLOY_INFRA_SH.name} — "
                f"has the EOD deploy plumbing been refactored?"
            ),
        })

    # --- Groom: deliberately no logging (update_or_create omits the arg) ---
    groom_arn_match = re.search(
        r'GROOM_ARN="arn:aws:states:\$REGION:\$\{ACCOUNT_ID\}:stateMachine:([\w-]+)"',
        text,
    )
    groom_call_match = re.search(
        r'update_or_create\s+"\$GROOM_ARN"\s+"\$GROOM_STAMPED"\s+"[\w-]+"\s+"[^"]+"\s*(".*")?\s*$',
        text,
        re.MULTILINE,
    )
    if groom_arn_match:
        groom_name = groom_arn_match.group(1)
        has_logging_arg = bool(groom_call_match and groom_call_match.group(1))
        if has_logging_arg:
            results.append({
                "sf_name": groom_name,
                "source_file": DEPLOY_INFRA_SH,
                "error": (
                    f"groom SF's update_or_create call in "
                    f"{DEPLOY_INFRA_SH.name} now passes a logging arg — "
                    f"this script's groom-has-no-logging assumption is "
                    f"stale, update it rather than trusting this result"
                ),
            })
        else:
            results.append({
                "sf_name": groom_name,
                "source_file": DEPLOY_INFRA_SH,
                "expected_level": "OFF",
                "expected_include_execution_data": None,
                "expected_log_group_name": None,
            })

    return results


def _discover_expected_logging_configs() -> list[dict]:
    return _discover_expected_from_cfn() + _discover_expected_from_deploy_script()


def _aws_stepfunctions(*args: str, allow_missing: bool = False):
    result = subprocess.run(
        ["aws", "stepfunctions", *args, "--output", "json"],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        if allow_missing and (
            "StateMachineDoesNotExist" in result.stderr
            or "ResourceNotFoundException" in result.stderr
        ):
            return None
        sys.stderr.write(
            f"AWS CLI failed: aws stepfunctions {' '.join(args)}\n"
            f"stderr: {result.stderr}\n"
        )
        sys.exit(2)
    return json.loads(result.stdout) if result.stdout.strip() else {}


def _live_log_group_name(logging_config: dict) -> str | None:
    destinations = logging_config.get("destinations") or []
    if not destinations:
        return None
    arn = destinations[0].get("cloudWatchLogsLogGroup", {}).get("logGroupArn", "")
    # arn:aws:logs:<region>:<account>:log-group:<name>:* — pull just <name>,
    # so this comparison is immune to region/account differences between
    # this script's DEFAULT_REGION/DEFAULT_ACCOUNT_ID and the live account.
    m = re.search(r"log-group:(.+?):(\*|\d+)?$", arn)
    return m.group(1) if m else arn or None


def _check_sf(entry: dict) -> list[str]:
    sf_name = entry["sf_name"]

    if "error" in entry:
        return [f"{sf_name}: {entry['error']}"]

    source_rel = entry["source_file"].relative_to(REPO_ROOT)
    arn = (
        f"arn:aws:states:{DEFAULT_REGION}:{DEFAULT_ACCOUNT_ID}:"
        f"stateMachine:{sf_name}"
    )

    desc = _aws_stepfunctions(
        "describe-state-machine", "--state-machine-arn", arn, allow_missing=True
    )
    if desc is None:
        return [
            f"{sf_name}: codified in {source_rel} but state machine not "
            f"found on AWS (has it been renamed/recreated without updating "
            f"the source, or vice versa?)"
        ]

    live_logging = desc.get("loggingConfiguration", {}) or {}
    live_level = live_logging.get("level", "OFF")
    live_include = live_logging.get("includeExecutionData")
    live_log_group = _live_log_group_name(live_logging)

    findings: list[str] = []

    expected_level = entry["expected_level"]
    if live_level != expected_level:
        findings.append(
            f"{sf_name}: LoggingConfiguration.level drift — codified "
            f"{source_rel} expects '{expected_level}', live is "
            f"'{live_level}'"
        )
        # If level itself has drifted to OFF, the includeExecutionData /
        # log-group comparisons below are meaningless (AWS omits them) —
        # skip the noise and report just the one root-cause finding.
        return findings

    expected_include = entry["expected_include_execution_data"]
    if expected_include is not None and live_include != expected_include:
        findings.append(
            f"{sf_name}: LoggingConfiguration.includeExecutionData drift — "
            f"codified {source_rel} expects {expected_include}, live is "
            f"{live_include}"
        )

    expected_log_group = entry["expected_log_group_name"]
    if expected_log_group is not None and live_log_group != expected_log_group:
        findings.append(
            f"{sf_name}: LoggingConfiguration log group drift — codified "
            f"{source_rel} expects '{expected_log_group}', live is "
            f"'{live_log_group}'"
        )

    return findings


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--name", help="Check one state machine by name (default: every codified one)"
    )
    args = parser.parse_args()

    entries = _discover_expected_logging_configs()

    if args.name:
        entries = [e for e in entries if e["sf_name"] == args.name]
        if not entries:
            sys.stderr.write(
                f"ERROR: no codified LoggingConfiguration found for state "
                f"machine '{args.name}'\n"
            )
            return 2

    if not entries:
        print("No codified state machines found — nothing to check.")
        return 0

    total_findings: list[str] = []
    for entry in entries:
        total_findings.extend(_check_sf(entry))

    if total_findings:
        print(f"SF LoggingConfiguration drift detected ({len(total_findings)} finding(s)):")
        for f in total_findings:
            print(f"  - {f}")
        return 1

    sf_names = ", ".join(e["sf_name"] for e in entries)
    print(f"OK: no LoggingConfiguration drift for {sf_names}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
