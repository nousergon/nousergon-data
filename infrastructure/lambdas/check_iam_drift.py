#!/usr/bin/env python3
"""check_iam_drift.py — Diff codified Lambda exec-role IAM policies against live AWS.

Each `infrastructure/lambdas/<name>/iam-policy.json` is applied to AWS ONLY
when a human runs `deploy.sh --bootstrap` (config#2825: the auto-deploy-on-merge
workflow runs deploy.sh with NO flags, code-only, by design — CI deliberately
lacks iam:CreateRole/iam:PutRolePolicy, the fleet-wide policy that has averted
IAM-clobber incidents, see infrastructure/iam/README.md "Single-writer rule").
That means a merged edit to iam-policy.json has NO live effect until someone
remembers to re-run --bootstrap by hand — drift regrows silently, exactly the
class nousergon-data-PR784's full-coverage sweep surfaced on 2026-07-17
(alpha-engine-config#2825).

This script closes the DETECTION gap the same way infrastructure/iam/check-drift.py
already does for the cross-cutting orchestration roles: read-only, CI-run, no
live mutation. It deliberately does NOT auto-apply — that stays a human decision
via reapply_iam_policy.sh (this directory), per the single-writer rule above.

The (role name, policy name) pair for each lambda is parsed from its own
deploy.sh (`ROLE_NAME="..."` / `POLICY_NAME="..."` — the FIRST occurrence in
the file, which is always the Lambda exec role; the SF/Scheduler exec roles
that some deploy.sh scripts also bootstrap are out of scope here, matching
config#2825's own scope) rather than hardcoded, so a rename in deploy.sh can't
silently desync the check.

Read access to a covered role's inline policy comes from the shared
`github-actions-iam-drift-check` OIDC role (defined in alpha-engine,
infrastructure/iam/github-actions-iam-drift-check/iam-readonly.json). Not
every lambda role is in that grant yet (newer additions land there over time)
— an AccessDenied for a specific role is reported as SKIPPED, not a failure,
so this check stays actionable (real drift among covered roles) instead of
red for reasons outside a single PR's control.

Usage:
  ./infrastructure/lambdas/check_iam_drift.py            # check every lambda
  ./infrastructure/lambdas/check_iam_drift.py --lambda scheduled-groom-dispatcher
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from pathlib import Path

LAMBDAS_DIR = Path(__file__).parent.resolve()

ROLE_NAME_RE = re.compile(r'^ROLE_NAME="([^"]*)"', re.MULTILINE)
POLICY_NAME_RE = re.compile(r'^POLICY_NAME="([^"]*)"', re.MULTILINE)

# Lambdas with a KNOWN, separately-tracked reason their live role can't match
# source yet (e.g. an operator ruling still pending). Reported as PENDING, not
# a drift failure, so the check stays a signal for NEW/unnoticed drift instead
# of permanently red on something already on someone's plate. Remove the entry
# once the tracking issue resolves.
KNOWN_PENDING = {
    "weekly-schedule-adjuster": (
        "never bootstrapped — alpha-engine-config#2825 deploy-vs-remove ruling "
        "due 2026-07-19"
    ),
}


def _aws_iam(*args: str):
    result = subprocess.run(
        ["aws", "iam", *args, "--output", "json"],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        if "AccessDenied" in result.stderr:
            raise PermissionError(result.stderr.strip())
        sys.stderr.write(
            f"AWS CLI failed: aws iam {' '.join(args)}\nstderr: {result.stderr}\n"
        )
        sys.exit(2)
    return json.loads(result.stdout) if result.stdout.strip() else {}


def _canonical_json(doc: dict) -> str:
    return json.dumps(doc, sort_keys=True, separators=(",", ":"))


def _discover_lambdas() -> list[Path]:
    return sorted(
        d for d in LAMBDAS_DIR.iterdir()
        if d.is_dir() and (d / "iam-policy.json").is_file() and (d / "deploy.sh").is_file()
    )


def _role_and_policy_name(deploy_sh: Path) -> tuple[str, str] | None:
    text = deploy_sh.read_text()
    role_match = ROLE_NAME_RE.search(text)
    policy_match = POLICY_NAME_RE.search(text)
    if not role_match or not policy_match:
        return None
    return role_match.group(1), policy_match.group(1)


def _check_lambda(lambda_dir: Path) -> tuple[list[str], list[str], list[str]]:
    """Return (drift_findings, skips, pending) for one lambda dir."""
    name = lambda_dir.name
    names = _role_and_policy_name(lambda_dir / "deploy.sh")
    if names is None:
        return ([f"{name}: could not parse ROLE_NAME/POLICY_NAME from deploy.sh"], [], [])
    role_name, policy_name = names

    if name in KNOWN_PENDING:
        return ([], [], [f"{name} ({role_name}): {KNOWN_PENDING[name]}"])

    try:
        source_doc = json.loads((lambda_dir / "iam-policy.json").read_text())
    except json.JSONDecodeError as exc:
        return ([f"{name} ({role_name}): source JSON invalid ({exc})"], [], [])

    try:
        aws_resp = _aws_iam(
            "get-role-policy", "--role-name", role_name, "--policy-name", policy_name,
        )
    except PermissionError:
        return ([], [f"{name} ({role_name}): SKIPPED — no read access (add to alpha-engine infrastructure/iam/github-actions-iam-drift-check/iam-readonly.json)"], [])

    aws_doc = aws_resp.get("PolicyDocument")
    if not aws_doc:
        return (
            [f"{name} ({role_name}/{policy_name}): codified but not found on the "
             f"live role (never bootstrapped, or bootstrapped under a different "
             f"policy name — run reapply_iam_policy.sh {name})"],
            [],
            [],
        )

    if _canonical_json(source_doc) != _canonical_json(aws_doc):
        return (
            [f"{name} ({role_name}/{policy_name}): source differs from live "
             f"(content drift — run reapply_iam_policy.sh {name} to push, or "
             f"codify live back into source if live is correct)"],
            [],
            [],
        )

    return ([], [], [])


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--lambda", dest="lambda_name", help="Check one lambda (default: every lambda with an iam-policy.json)")
    args = parser.parse_args()

    if args.lambda_name:
        lambda_dirs = [LAMBDAS_DIR / args.lambda_name]
        if not (lambda_dirs[0] / "iam-policy.json").is_file():
            sys.stderr.write(f"ERROR: {lambda_dirs[0]}/iam-policy.json not found\n")
            return 2
    else:
        lambda_dirs = _discover_lambdas()

    if not lambda_dirs:
        print(f"No lambda iam-policy.json files found under {LAMBDAS_DIR} — nothing to check.")
        return 0

    all_findings: list[str] = []
    all_skips: list[str] = []
    all_pending: list[str] = []
    for lambda_dir in lambda_dirs:
        findings, skips, pending = _check_lambda(lambda_dir)
        all_findings.extend(findings)
        all_skips.extend(skips)
        all_pending.extend(pending)

    if all_pending:
        print(f"PENDING ({len(all_pending)}, tracked separately):")
        for p in all_pending:
            print(f"  - {p}")

    if all_skips:
        print(f"SKIPPED ({len(all_skips)}, no read access yet):")
        for s in all_skips:
            print(f"  - {s}")

    if all_findings:
        print(f"IAM drift detected ({len(all_findings)} finding(s)):")
        for f in all_findings:
            print(f"  - {f}")
        return 1

    checked = len(lambda_dirs) - len(all_skips) - len(all_pending)
    print(f"OK: no IAM drift for {checked}/{len(lambda_dirs)} lambda exec-role polic{'y' if checked == 1 else 'ies'} (read-accessible, non-pending ones)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
