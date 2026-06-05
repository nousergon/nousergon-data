#!/usr/bin/env python3
"""check-drift.py — Diff codified IAM inline policies against live AWS state.

Walks every `infrastructure/iam/<role>.json` in this directory and compares
against `aws iam get-role-policy --role-name <role> --policy-name <role>-policy`
(the `{role}-policy` naming convention enforced by apply.sh).

Drift cases (all exit non-zero):
  * Source JSON differs from AWS document       (content-drift)
  * AWS role has no inline policy by that name  (missing-in-aws)

JSON is compared after normalization (sorted keys, no extra whitespace),
so cosmetic-only differences in indentation or key order don't trip the check.

Usage:
  ./infrastructure/iam/check-drift.py             # check every codified role
  ./infrastructure/iam/check-drift.py --role X    # check one role

Requires AWS creds with iam:ListRolePolicies + iam:GetRolePolicy on the
target roles. Locally: any admin profile. In CI: the OIDC role
`github-actions-iam-drift-check` (alpha-engine repo owns it; trust policy
allows this repo too).
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


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--role", help="Check one role (default: every codified role)"
    )
    args = parser.parse_args()

    if args.role:
        role_files = [SCRIPT_DIR / f"{args.role}.json"]
        if not role_files[0].is_file():
            sys.stderr.write(f"ERROR: {role_files[0]} not found\n")
            return 2
    else:
        # *.trust.json are assume-role (trust) policy snapshots, not inline
        # permission documents — they have no `{stem}-policy` inline policy to
        # diff, so exclude them from the drift sweep (mirrors apply.sh).
        role_files = sorted(
            f for f in SCRIPT_DIR.glob("*.json") if not f.name.endswith(".trust.json")
        )

    if not role_files:
        print(f"No codified role files found under {SCRIPT_DIR} — nothing to check.")
        return 0

    total_findings: list[str] = []
    for role_file in role_files:
        findings = _check_role(role_file)
        total_findings.extend(findings)

    if total_findings:
        print(f"IAM drift detected ({len(total_findings)} finding(s)):")
        for f in total_findings:
            print(f"  - {f}")
        return 1

    role_names = ", ".join(f.stem for f in role_files)
    print(f"OK: no IAM drift for {role_names}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
