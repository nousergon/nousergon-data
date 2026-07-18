#!/usr/bin/env python3
"""check-drift.py — Diff codified IAM inline + trust policies against live AWS state.

Walks every `infrastructure/iam/<role>.json` in this directory and compares
against `aws iam get-role-policy --role-name <role> --policy-name <role>-policy`
(the `{role}-policy` naming convention enforced by apply.sh). Also walks every
`infrastructure/iam/<role>.trust.json` snapshot and compares it against the
role's live `AssumeRolePolicyDocument` (config#2826) — the trust document a
role was created with can drift silently since it's normally asserted once at
bootstrap time in a deploy script, not re-checked on every deploy.

Drift cases (all exit non-zero):
  * Source JSON differs from AWS document              (content-drift)
  * AWS role has no inline policy by that name          (missing-in-aws)
  * AWS role's live trust doc differs from the snapshot (trust-drift)

JSON is compared after normalization (sorted keys, no extra whitespace),
so cosmetic-only differences in indentation or key order don't trip the check.

Usage:
  ./infrastructure/iam/check-drift.py             # check every codified role
  ./infrastructure/iam/check-drift.py --role X    # check one role (both .json and .trust.json if present)

Requires AWS creds with iam:ListRolePolicies + iam:GetRolePolicy +
iam:GetRole on the target roles. Locally: any admin profile. In CI: the
OIDC role `github-actions-iam-drift-check` (alpha-engine repo owns it;
trust policy allows this repo too).
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent.resolve()


def _aws_iam(*args: str) -> dict | list | str:
    """Call aws iam ... and return the parsed JSON output."""
    result = subprocess.run(
        ["aws", "iam", *args, "--output", "json"],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        sys.stderr.write(
            f"AWS CLI failed: aws iam {' '.join(args)}\n"
            f"stderr: {result.stderr}\n"
        )
        sys.exit(2)
    return json.loads(result.stdout) if result.stdout.strip() else {}


def _canonical_json(doc: dict) -> str:
    """Canonical JSON for byte-stable comparison: sorted keys, no extra ws."""
    return json.dumps(doc, sort_keys=True, separators=(",", ":"))


def _check_role(role_file: Path) -> list[str]:
    """Return list of drift findings for one role. Empty means clean."""
    role_name = role_file.stem
    policy_name = f"{role_name}-policy"
    findings: list[str] = []

    try:
        source_doc = json.loads(role_file.read_text())
    except json.JSONDecodeError as exc:
        return [f"{role_name}: source JSON invalid ({exc})"]

    aws_resp = _aws_iam(
        "get-role-policy",
        "--role-name", role_name,
        "--policy-name", policy_name,
    )
    aws_doc = aws_resp.get("PolicyDocument")

    if not aws_doc:
        return [
            f"{role_name}: codified in source but inline policy "
            f"'{policy_name}' not found on AWS role (run apply.sh to push)"
        ]

    if _canonical_json(source_doc) != _canonical_json(aws_doc):
        findings.append(
            f"{role_name}/{policy_name}: source document differs from "
            f"AWS document (content drift)"
        )

    return findings


def _check_trust_role(trust_file: Path) -> list[str]:
    """Return list of trust-drift findings for one role. Empty means clean."""
    role_name = trust_file.name[: -len(".trust.json")]

    try:
        source_doc = json.loads(trust_file.read_text())
    except json.JSONDecodeError as exc:
        return [f"{role_name}: trust snapshot JSON invalid ({exc})"]

    aws_resp = _aws_iam("get-role", "--role-name", role_name)
    aws_doc = aws_resp.get("Role", {}).get("AssumeRolePolicyDocument")

    if not aws_doc:
        return [
            f"{role_name}: trust snapshot codified in source but role not "
            f"found on AWS (or has no trust document)"
        ]

    if _canonical_json(source_doc) != _canonical_json(aws_doc):
        findings = [
            f"{role_name}: trust snapshot differs from AWS "
            f"AssumeRolePolicyDocument (trust-drift — run apply.sh to push)"
        ]
    else:
        findings = []

    return findings


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--role", help="Check one role (default: every codified role)"
    )
    args = parser.parse_args()

    if args.role:
        role_files = [SCRIPT_DIR / f"{args.role}.json"]
        if not role_files[0].is_file():
            role_files = []
        trust_files = [SCRIPT_DIR / f"{args.role}.trust.json"]
        if not trust_files[0].is_file():
            trust_files = []
        if not role_files and not trust_files:
            sys.stderr.write(
                f"ERROR: neither {args.role}.json nor {args.role}.trust.json "
                f"found in {SCRIPT_DIR}\n"
            )
            return 2
    else:
        # *.trust.json are assume-role (trust) policy snapshots, not inline
        # permission documents — they have no `{stem}-policy` inline policy to
        # diff, so they're checked separately via _check_trust_role below.
        role_files = sorted(
            f for f in SCRIPT_DIR.glob("*.json") if not f.name.endswith(".trust.json")
        )
        trust_files = sorted(SCRIPT_DIR.glob("*.trust.json"))

    if not role_files and not trust_files:
        print(f"No codified role files found under {SCRIPT_DIR} — nothing to check.")
        return 0

    total_findings: list[str] = []
    for role_file in role_files:
        total_findings.extend(_check_role(role_file))
    for trust_file in trust_files:
        total_findings.extend(_check_trust_role(trust_file))

    if total_findings:
        print(f"IAM drift detected ({len(total_findings)} finding(s)):")
        for f in total_findings:
            print(f"  - {f}")
        return 1

    checked_names = sorted(
        {f.stem for f in role_files} | {f.name[: -len(".trust.json")] for f in trust_files}
    )
    print(f"OK: no IAM drift for {', '.join(checked_names)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
