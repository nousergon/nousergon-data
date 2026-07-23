#!/usr/bin/env python3
"""Check all lambda iam-policy.json files for GetObject-without-ListBucket.

S3 returns 403 AccessDenied instead of 404 NoSuchKey for ``GetObject`` on a
nonexistent key when the caller lacks ``s3:ListBucket``. Any Lambda that:
(a) does ``GetObject`` on a possibly-absent key and (b) distinguishes
NoSuchKey from other errors will fail-loud on the benign absent-key path.

This check flags every ``iam-policy.json`` that grants ``s3:GetObject`` without
a matching prefix-scoped ``s3:ListBucket`` statement for the same bucket.

Add ``# skip-if-absent-key-impossible`` on the first line of the policy file
to exempt roles whose code only reads keys guaranteed to exist.
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent.parent.parent
LAMBDAS_DIR = REPO_ROOT / "infrastructure" / "lambdas"
SKIP_MARKER = "# skip-if-absent-key-impossible"


def _find_iam_policy_files() -> list[Path]:
    """Return all ``iam-policy.json`` files under the lambdas dir."""
    return sorted(LAMBDAS_DIR.rglob("iam-policy.json"))


def _has_skip_marker(path: Path) -> bool:
    """True if the first line of the file is the skip marker."""
    try:
        first_line = path.read_text().strip().split("\n")[0].strip()
        return first_line == SKIP_MARKER
    except (OSError, IndexError):
        return False


def _parse_policy(path: Path) -> dict | None:
    """Parse the JSON policy doc, or return None on parse error."""
    try:
        return json.loads(path.read_text())
    except json.JSONDecodeError as exc:
        print(f"ERROR: {path.relative_to(REPO_ROOT)} — invalid JSON: {exc}")
        return None


def _has_getobject_without_listbucket(policy: dict) -> list[str]:
    """Return list of SIDs with GetObject but no matching ListBucket.

    Checks that for every bucket/prefix referenced by ``s3:GetObject``, there
    is a corresponding ``s3:ListBucket`` statement on the same bucket with a
    ``s3:prefix`` condition scoped to (or broader than) the GetObject's
    resource prefix. Checks all statements regardless of Sid.
    """
    issues: list[str] = []

    # Collect all ListBucket statements keyed by (bucket, prefix).
    list_bucket_prefixes: set[tuple[str, str]] = set()
    for stmt in policy.get("Statement", []):
        actions = stmt.get("Action", [])
        if isinstance(actions, str):
            actions = [actions]
        if "s3:ListBucket" not in actions:
            continue

        resource = stmt.get("Resource", "")
        condition = stmt.get("Condition", {})
        string_like = condition.get("StringLike", {})
        prefix_conditions = string_like.get("s3:prefix", [])

        bucket = _bucket_from_arn(resource)
        if isinstance(prefix_conditions, str):
            prefix_conditions = [prefix_conditions]
        for prefix in prefix_conditions:
            list_bucket_prefixes.add((bucket, prefix))

    # Check each GetObject statement.
    for stmt in policy.get("Statement", []):
        actions = stmt.get("Action", [])
        if isinstance(actions, str):
            actions = [actions]
        # Only flag pure GetObject (no ListBucket) statements.
        if "s3:GetObject" not in actions:
            continue
        if "s3:ListBucket" in actions:
            continue

        resource = stmt.get("Resource", "")
        bucket = _bucket_from_arn(resource)
        get_prefix = _prefix_from_arn(resource)

        sid = stmt.get("Sid", "(unnamed)")

        if get_prefix is None:
            # Wildcard GetObject on the whole bucket — definitely needs ListBucket
            issues.append(sid)
            continue

        # Check if ANY ListBucket prefix covers this GetObject prefix.
        has_coverage = any(
            _prefix_matches(list_prefix, get_prefix)
            for b, list_prefix in list_bucket_prefixes
            if b == bucket
        )

        if not has_coverage:
            issues.append(sid)

    return issues


def _bucket_from_arn(resource: str) -> str:
    """Extract bucket name from an S3 ARN."""
    if isinstance(resource, list):
        resource = resource[0] if resource else ""
    m = re.match(r"arn:aws:s3:::([^/]+)", resource)
    return m.group(1) if m else ""


def _prefix_from_arn(resource: str) -> str | None:
    """Extract the object prefix from an S3 object ARN, or None if bucket-level."""
    if isinstance(resource, list):
        resource = resource[0] if resource else ""
    m = re.match(r"arn:aws:s3:::[^/]+/(.+)", resource)
    return m.group(1) if m else None


def _prefix_matches(pattern: str, target: str) -> bool:
    """True if ``target`` matches the IAM StringLike ``pattern``.

    Supports ``*`` (match any chars) and ``?`` (match one char) wildcards,
    matching IAM condition semantics.
    """
    import fnmatch
    return fnmatch.fnmatch(target, pattern)


def main() -> int:
    violations: list[tuple[Path, list[str]]] = []

    for policy_path in _find_iam_policy_files():
        if _has_skip_marker(policy_path):
            continue

        policy = _parse_policy(policy_path)
        if policy is None:
            violations.append((policy_path, ["parse_error"]))
            continue

        issues = _has_getobject_without_listbucket(policy)
        if issues:
            violations.append((policy_path, issues))

    if not violations:
        print("OK: all iam-policy.json files have matching ListBucket statements")
        return 0

    for path, issues in violations:
        rel = path.relative_to(REPO_ROOT)

        if issues == ["parse_error"]:
            print(f"ERROR: {rel} — failed to parse")
        else:
            print(f"ERROR: {rel} — GetObject without ListBucket in SID(s):")
            for sid in issues:
                print(f"  - {sid}")

    return 1


if __name__ == "__main__":
    sys.exit(main())
