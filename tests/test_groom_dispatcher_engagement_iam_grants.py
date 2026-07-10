"""
tests/test_groom_dispatcher_engagement_iam_grants.py — the scheduled-groom-
dispatcher Lambda's codified IAM policy must cover every S3 surface its
pre-boot enumeration reads.

Regression target: config#2142 (2026-07-10) — the fresh-skip-aware
enumeration shipped in the config#2038 arc (`_load_recent_engagements` →
``list_objects_v2(Prefix="groom/{date}/")`` + ``get_object`` on the run
artifacts) WITHOUT matching IAM statements. The role's only ``s3:ListBucket``
grant was condition-scoped to ``claude_code_usage/*`` (the pace gate), so the
engagement scan hit AccessDenied on every trigger from ship (2026-07-08) to
2026-07-10 — swallowed by a "non-fatal, skip nothing" fallback, silently
disabling fresh-skip and inflating every advertised per-tier count.

This is the static policy/code-drift half of the fix (mirrors
test_groom_sf_iam_lambda_grants.py's role for the groom SF role); the
runtime half is that `_load_recent_engagements` now RAISES and the trigger
handler pages ops-health. The policy file is applied idempotently by
deploy.sh (`aws iam put-role-policy`), so guarding the file guards the role.
"""

import fnmatch
import json
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
POLICY_FILE = (REPO_ROOT / "infrastructure" / "lambdas"
               / "scheduled-groom-dispatcher" / "iam-policy.json")

RESEARCH_BUCKET_ARN = "arn:aws:s3:::alpha-engine-research"

# One representative key per S3 read surface index.py's pre-boot phases touch.
# Adding a new S3 read to index.py without extending the policy AND this map
# is exactly the config#2142 gap — keep them in lockstep.
READ_SURFACES = {
    # pace gate (_pace_gate_status)
    "claude_code_usage": "claude_code_usage/groom/2026-07-10.json",
    # engagement scan (_load_recent_engagements, config#1893/#2038)
    "groom_run_artifacts": "groom/2026-07-10/abc123.json",
}


def _statements() -> list[dict]:
    doc = json.loads(POLICY_FILE.read_text())
    return doc["Statement"]


def _actions(stmt: dict) -> set[str]:
    a = stmt.get("Action", [])
    return {a} if isinstance(a, str) else set(a)


def _resources(stmt: dict) -> list[str]:
    r = stmt.get("Resource", [])
    return [r] if isinstance(r, str) else list(r)


def _prefix_patterns(stmt: dict) -> list[str]:
    """s3:prefix patterns from the statement's condition ('' -> unconditioned)."""
    cond = stmt.get("Condition")
    if not cond:
        return ["*"]
    patterns: list[str] = []
    for op, kv in cond.items():
        if not op.startswith(("StringLike", "StringEquals")):
            continue
        for key, val in kv.items():
            if key.lower() == "s3:prefix":
                patterns.extend([val] if isinstance(val, str) else val)
    return patterns


def _list_bucket_allows_prefix(key: str) -> bool:
    for stmt in _statements():
        if stmt.get("Effect") != "Allow" or "s3:ListBucket" not in _actions(stmt):
            continue
        if RESEARCH_BUCKET_ARN not in _resources(stmt):
            continue
        # ListObjectsV2 sends the *prefix* as the s3:prefix context key — match
        # the key's prefix chain against the statement's patterns.
        prefix = key.rsplit("/", 1)[0] + "/"
        if any(fnmatch.fnmatch(prefix, pat) or fnmatch.fnmatch(key, pat)
               for pat in _prefix_patterns(stmt)):
            return True
    return False


def _get_object_allows_key(key: str) -> bool:
    obj_arn = f"{RESEARCH_BUCKET_ARN}/{key}"
    for stmt in _statements():
        if stmt.get("Effect") != "Allow" or "s3:GetObject" not in _actions(stmt):
            continue
        if any(fnmatch.fnmatch(obj_arn, res) for res in _resources(stmt)):
            return True
    return False


def test_every_read_surface_has_list_bucket_grant():
    missing = {name: key for name, key in READ_SURFACES.items()
               if not _list_bucket_allows_prefix(key)}
    assert not missing, (
        f"iam-policy.json grants no s3:ListBucket covering: {missing} — "
        "index.py's pre-boot enumeration will AccessDenied at run time "
        "(config#2142 regression)."
    )


def test_every_read_surface_has_get_object_grant():
    missing = {name: key for name, key in READ_SURFACES.items()
               if not _get_object_allows_key(key)}
    assert not missing, (
        f"iam-policy.json grants no s3:GetObject covering: {missing} — "
        "index.py's pre-boot enumeration will AccessDenied at run time "
        "(config#2142 regression)."
    )


def test_list_bucket_grants_stay_prefix_scoped():
    """Deliberate ceiling: never widen ListBucket to the whole research
    bucket unconditioned — grants stay prefix-scoped per read surface."""
    for stmt in _statements():
        if "s3:ListBucket" not in _actions(stmt):
            continue
        assert _prefix_patterns(stmt) != ["*"], (
            f"unconditioned s3:ListBucket in statement {stmt.get('Sid')!r} — "
            "scope it with an s3:prefix condition."
        )
