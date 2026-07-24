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
  ./infrastructure/iam/check-drift.py --post-merge  # auto-apply drifted roles then re-check

Requires AWS creds with iam:ListRolePolicies + iam:GetRolePolicy +
iam:GetRole on the target roles. Locally: any admin profile. In CI: the
OIDC role `github-actions-iam-drift-check` (alpha-engine repo owns it;
trust policy allows this repo too).

--post-merge (config#3495 → config#3697): a PR that codifies IAM is,
structurally, always "drifted" pre-merge — the codified state is the NEW
desired state, live AWS still has the OLD one, and nothing has run
apply.sh yet. Blocking PR checks on this comparison guarantees red on
every IAM PR. --post-merge instead: find drift as normal, run
apply.sh <role> for each DRIFTED role only (never touching clean roles),
then re-check just those roles. Residual drift after apply is REAL
unexpected drift (e.g. an apply or trust-apply failure) and still fails
the check. Requires write creds (iam:PutRolePolicy +
iam:UpdateAssumeRolePolicy + iam:AttachRolePolicy) in addition to the
read set above — a separate, more privileged OIDC role than the
PR/schedule path uses.
"""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent.resolve()
APPLY_SCRIPT = SCRIPT_DIR / "apply.sh"


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


def _normalize_unordered_lists(node):
    """Recursively sort string-lists — IAM treats Action/Resource/
    Principal.Service (and every other policy-document string array) as
    UNORDERED sets, and AWS returns them in arbitrary, unstable order.
    First bitten 2026-07-22: GetRole started returning
    ["scheduler.amazonaws.com","events.amazonaws.com"] for a trust doc
    codified in the opposite order — same set, red drift finding, every PR
    blocked. Only lists that are entirely strings are sorted; mixed/object
    lists (e.g. Statement arrays) keep their order — Statement order is
    semantically meaningful for readers even though IAM ORs them, and the
    codified file controls it."""
    if isinstance(node, dict):
        return {k: _normalize_unordered_lists(v) for k, v in node.items()}
    if isinstance(node, list):
        if node and all(isinstance(x, str) for x in node):
            return sorted(node)
        return [_normalize_unordered_lists(x) for x in node]
    return node


def _canonical_json(doc: dict) -> str:
    """Canonical JSON for byte-stable comparison: sorted keys, sorted
    string-set arrays (see _normalize_unordered_lists), no extra ws."""
    return json.dumps(
        _normalize_unordered_lists(doc), sort_keys=True, separators=(",", ":")
    )


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
            f"AWS document (content drift)\n"
            f"    source: {_canonical_json(source_doc)}\n"
            f"    aws:    {_canonical_json(aws_doc)}"
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
            f"AssumeRolePolicyDocument (trust-drift — run apply.sh to push)\n"
            f"    source: {_canonical_json(source_doc)}\n"
            f"    aws:    {_canonical_json(aws_doc)}"
        ]
    else:
        findings = []

    return findings


def _apply_role(role_name: str) -> subprocess.CompletedProcess:
    """Run apply.sh for one drifted role. Caller re-checks afterward.

    apply.sh takes a bare role name (no flag) and handles both the inline
    policy (<role>-policy) and trust document (update-assume-role-policy)
    for that role when both <role>.json and <role>.trust.json exist.
    """
    bash_bin = shutil.which("bash")
    if not bash_bin:
        sys.stderr.write("bash not found on PATH\n")
        sys.exit(1)
    return subprocess.run(
        [bash_bin, str(APPLY_SCRIPT), role_name],
        capture_output=True,
        text=True,
        check=False,
    )


def _reconcile_post_merge(
    drifted_role_names: set[str],
    role_file_map: dict[str, Path],
    trust_file_map: dict[str, Path],
) -> list[str]:
    """Apply + re-check each drifted role. Returns residual (real) findings.

    For each drifted role, runs apply.sh <role>, then re-runs _check_role
    and/or _check_trust_role (whichever files exist for that role). A role
    that is clean after apply is resolved; residual drift after a
    successful apply (or an apply failure) is reported.
    """
    residual: list[str] = []
    for role_name in sorted(drifted_role_names):
        print(f"Auto-applying {role_name}...")
        result = _apply_role(role_name)
        sys.stdout.write(result.stdout)
        sys.stderr.write(result.stderr)
        if result.returncode != 0:
            residual.append(
                f"{role_name}: apply.sh failed (exit {result.returncode}) — "
                f"drift NOT resolved, see apply output above"
            )
            continue

        # Re-check inline policy if codified
        if role_name in role_file_map:
            recheck = _check_role(role_file_map[role_name])
            if recheck:
                residual.extend(
                    f"{role_name}: inline-policy drift persists after "
                    f"auto-apply — {f}" for f in recheck
                )
        # Re-check trust doc if codified
        if role_name in trust_file_map:
            recheck = _check_trust_role(trust_file_map[role_name])
            if recheck:
                residual.extend(
                    f"{role_name}: trust-doc drift persists after "
                    f"auto-apply — {f}" for f in recheck
                )

        if role_name not in {_role_from_finding(r) for r in residual}:
            print(f"  resolved: {role_name}")

    return residual


def _role_from_finding(finding: str) -> str:
    """Extract role name from a finding string like 'role-name: ...'."""
    return finding.split(":")[0]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--role", help="Check one role (default: every codified role)"
    )
    parser.add_argument(
        "--post-merge",
        action="store_true",
        help=(
            "On drift, run apply.sh for each drifted role and re-check "
            "before failing (config#3495 → config#3697) — see module docstring"
        ),
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

    # Build lookup maps so _reconcile_post_merge can re-check specific roles.
    role_file_map: dict[str, Path] = {f.stem: f for f in role_files}
    trust_file_map: dict[str, Path] = {
        f.name[: -len(".trust.json")]: f for f in trust_files
    }

    total_findings: list[str] = []
    drifted_role_names: set[str] = set()
    for role_file in role_files:
        findings = _check_role(role_file)
        if findings:
            drifted_role_names.add(role_file.stem)
        total_findings.extend(findings)
    for trust_file in trust_files:
        findings = _check_trust_role(trust_file)
        if findings:
            drifted_role_names.add(trust_file.name[: -len(".trust.json")])
        total_findings.extend(findings)

    if total_findings:
        if args.post_merge:
            residual = _reconcile_post_merge(
                drifted_role_names, role_file_map, trust_file_map
            )
            if residual:
                print(
                    f"IAM drift persists after auto-apply "
                    f"({len(residual)} finding(s)):"
                )
                for f in residual:
                    print(f"  - {f}")
                return 1
            print(
                f"OK: auto-applied and reconciled drift for "
                f"{', '.join(sorted(drifted_role_names))}"
            )
            return 0

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
