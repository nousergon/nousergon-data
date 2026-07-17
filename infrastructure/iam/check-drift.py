#!/usr/bin/env python3
"""check-drift.py — Diff codified IAM inline policies against live AWS state.

Covers two families of codified role:

  1. **Orchestration roles** — every `infrastructure/iam/<role>.json` in this
     directory, compared against
     `aws iam get-role-policy --role-name <role> --policy-name <role>-policy`
     (the `{role}-policy` naming convention enforced by apply.sh).

  2. **Lambda exec roles** (config#2340 surface 3) — every
     `infrastructure/lambdas/<name>/iam-policy.json`. These are already tracked
     files and already applied by each lambda's `deploy.sh --bootstrap`; the one
     missing leg was drift-check coverage. Rather than move the files (which
     would churn every lambda's deploy path), we discover each lambda's PRIMARY
     role IN PLACE: the authoritative `ROLE_NAME=` / `POLICY_NAME=` assignments
     at the top of its `deploy.sh` (the source of truth for what
     `put-role-policy ... --policy-document file://iam-policy.json` applies), and
     drift-check `(ROLE_NAME, POLICY_NAME, iam-policy.json)`.

     A tracked `iam-policy.json` whose deploy.sh does NOT define both names is a
     COVERAGE GAP and fails the check — a policy file can never be silently
     un-drift-checked (the config#2340 "untracked policy → outage" class).

     Scope note: secondary scheduler/canary roles (`SCHED_ROLE_NAME`,
     `CANARY_SCHED_ROLE_NAME`, ...) apply INLINE policy documents (heredoc vars,
     not `iam-policy.json` files), so they have no file to diff and are out of
     scope here; lifting those inline policies into files is the documented
     surface-3 follow-up (see infrastructure/iam/README.md).

Drift cases (all exit non-zero):
  * Source JSON differs from AWS document       (content-drift)
  * AWS role has no inline policy by that name  (missing-in-aws)
  * A tracked lambda iam-policy.json is undiscoverable (coverage-gap)

JSON is compared after normalization (sorted keys, no extra whitespace),
so cosmetic-only differences in indentation or key order don't trip the check.

Usage:
  ./infrastructure/iam/check-drift.py             # every codified role (orch + lambda)
  ./infrastructure/iam/check-drift.py --role X    # one orchestration role
  ./infrastructure/iam/check-drift.py --lambdas-only

Requires AWS creds with iam:ListRolePolicies + iam:GetRolePolicy on the
target roles. Locally: any admin profile. In CI: the OIDC role
`github-actions-iam-drift-check` (alpha-engine repo owns it; trust policy
allows this repo too; its iam-readonly Resource list must include every role
checked here — extended for the lambda roles in the paired crucible-executor PR).
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from pathlib import Path
from typing import NamedTuple

SCRIPT_DIR = Path(__file__).parent.resolve()
LAMBDAS_DIR = SCRIPT_DIR.parent / "lambdas"
LAMBDA_POLICY_FILENAME = "iam-policy.json"


class RolePolicy(NamedTuple):
    """One (role, inline-policy, source-file) triple to drift-check."""

    role_name: str
    policy_name: str
    source_file: Path
    origin: str  # human label for output, e.g. "lambdas/freshness-monitor"


# Per-role error classes that must surface as FINDINGS, not kill the run:
# one missing/unreadable role would otherwise mask drift results for every
# other role in the same invocation.
_EXPECTED_AWS_ERRORS = ("NoSuchEntity", "AccessDenied", "AccessDeniedException")


def _aws_iam(*args: str) -> dict | list | str:
    """Call aws iam ... and return the parsed JSON output.

    NoSuchEntity/AccessDenied come back as ``{"__aws_error__": <code>}`` so
    the caller can report a per-role finding and keep checking the remaining
    roles; any other CLI failure still fails the whole run loudly (exit 2).
    """
    result = subprocess.run(
        ["aws", "iam", *args, "--output", "json"],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        for code in _EXPECTED_AWS_ERRORS:
            if code in result.stderr:
                return {"__aws_error__": code}
        sys.stderr.write(
            f"AWS CLI failed: aws iam {' '.join(args)}\n"
            f"stderr: {result.stderr}\n"
        )
        sys.exit(2)
    return json.loads(result.stdout) if result.stdout.strip() else {}


def _canonical_json(doc: dict) -> str:
    """Canonical JSON for byte-stable comparison: sorted keys, no extra ws."""
    return json.dumps(doc, sort_keys=True, separators=(",", ":"))


def _parse_shell_assignment(text: str, var: str) -> str | None:
    """First top-level `VAR="value"` assignment in a shell script (or None).

    Anchored at line start so it matches the authoritative top-of-file
    definition and not a `--role-name "${VAR}"` reference. Pure / testable.
    """
    m = re.search(rf'^{re.escape(var)}="([^"]*)"', text, re.MULTILINE)
    return m.group(1) if m else None


def _orchestration_role_policies() -> list[RolePolicy]:
    """The `infrastructure/iam/<role>.json` orchestration roles."""
    # *.trust.json are assume-role (trust) snapshots, not inline permission
    # documents — no `{stem}-policy` to diff, so excluded (mirrors apply.sh).
    out: list[RolePolicy] = []
    for f in sorted(SCRIPT_DIR.glob("*.json")):
        if f.name.endswith(".trust.json"):
            continue
        out.append(RolePolicy(f.stem, f"{f.stem}-policy", f, "infrastructure/iam"))
    return out


def discover_lambda_role_policies(
    lambdas_dir: Path = LAMBDAS_DIR,
) -> tuple[list[RolePolicy], list[str]]:
    """Discover each lambda's primary file-backed role from its deploy.sh.

    Returns ``(role_policies, coverage_gaps)``. A ``coverage_gap`` is a tracked
    ``iam-policy.json`` we could not map to a role — an actionable drift-check
    hole that must fail the sweep. Pure except for the filesystem read; the
    parsing is unit-tested.
    """
    role_policies: list[RolePolicy] = []
    gaps: list[str] = []
    if not lambdas_dir.is_dir():
        return role_policies, gaps

    for pol in sorted(lambdas_dir.glob(f"*/{LAMBDA_POLICY_FILENAME}")):
        lam_dir = pol.parent
        origin = f"lambdas/{lam_dir.name}"
        deploy = lam_dir / "deploy.sh"
        if not deploy.is_file():
            gaps.append(
                f"{origin}/{LAMBDA_POLICY_FILENAME} is tracked but has no "
                f"deploy.sh to derive its role — cannot drift-check"
            )
            continue
        text = deploy.read_text()
        role_name = _parse_shell_assignment(text, "ROLE_NAME")
        policy_name = _parse_shell_assignment(text, "POLICY_NAME")
        if not role_name or not policy_name:
            missing = " and ".join(
                v for v, got in (("ROLE_NAME", role_name), ("POLICY_NAME", policy_name))
                if not got
            )
            gaps.append(
                f"{origin}/deploy.sh does not define {missing} at top level — "
                f"cannot map {LAMBDA_POLICY_FILENAME} to a role for drift-check"
            )
            continue
        role_policies.append(RolePolicy(role_name, policy_name, pol, origin))
    return role_policies, gaps


def _check_policy(rp: RolePolicy) -> list[str]:
    """Return drift findings for one (role, policy, file). Empty means clean."""
    try:
        source_doc = json.loads(rp.source_file.read_text())
    except json.JSONDecodeError as exc:
        return [f"{rp.role_name} [{rp.origin}]: source JSON invalid ({exc})"]

    aws_resp = _aws_iam(
        "get-role-policy",
        "--role-name", rp.role_name,
        "--policy-name", rp.policy_name,
    )
    aws_error = aws_resp.get("__aws_error__") if isinstance(aws_resp, dict) else None
    if aws_error == "NoSuchEntity":
        return [
            f"{rp.role_name} [{rp.origin}]: role/policy does not exist on AWS "
            f"(codified but never deployed — run deploy.sh --bootstrap, or "
            f"remove the codified files if the lambda was superseded)"
        ]
    if aws_error in ("AccessDenied", "AccessDeniedException"):
        return [
            f"{rp.role_name} [{rp.origin}]: CI role cannot read this role — "
            f"extend github-actions-iam-drift-check's iam-readonly Resource "
            f"list to include it"
        ]
    aws_doc = aws_resp.get("PolicyDocument")

    if not aws_doc:
        return [
            f"{rp.role_name} [{rp.origin}]: codified in source but inline policy "
            f"'{rp.policy_name}' not found on AWS role (run apply.sh / deploy.sh "
            f"--bootstrap to push)"
        ]

    if _canonical_json(source_doc) != _canonical_json(aws_doc):
        return [
            f"{rp.role_name}/{rp.policy_name} [{rp.origin}]: source document "
            f"differs from AWS document (content drift)"
        ]
    return []


# Back-compat shim: the original single-file entrypoint, kept so `--role X`
# and any external caller of `_check_role` keep working.
def _check_role(role_file: Path) -> list[str]:
    return _check_policy(
        RolePolicy(role_file.stem, f"{role_file.stem}-policy", role_file, "infrastructure/iam")
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--role", help="Check one orchestration role (infrastructure/iam/<role>.json)"
    )
    parser.add_argument(
        "--lambdas-only",
        action="store_true",
        help="Check only the lambda exec roles (infrastructure/lambdas/*/iam-policy.json)",
    )
    args = parser.parse_args()

    targets: list[RolePolicy] = []
    gaps: list[str] = []

    if args.role:
        rf = SCRIPT_DIR / f"{args.role}.json"
        if not rf.is_file():
            sys.stderr.write(f"ERROR: {rf} not found\n")
            return 2
        targets = [RolePolicy(args.role, f"{args.role}-policy", rf, "infrastructure/iam")]
    else:
        if not args.lambdas_only:
            targets.extend(_orchestration_role_policies())
        lambda_targets, gaps = discover_lambda_role_policies()
        targets.extend(lambda_targets)

    if not targets and not gaps:
        print(f"No codified role files found under {SCRIPT_DIR} — nothing to check.")
        return 0

    total_findings: list[str] = list(gaps)
    for rp in targets:
        total_findings.extend(_check_policy(rp))

    if total_findings:
        print(f"IAM drift detected ({len(total_findings)} finding(s)):")
        for f in total_findings:
            print(f"  - {f}")
        return 1

    print(
        f"OK: no IAM drift across {len(targets)} codified role(s) "
        f"({sum(1 for t in targets if t.origin.startswith('lambdas/'))} lambda + "
        f"{sum(1 for t in targets if t.origin == 'infrastructure/iam')} orchestration)"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
