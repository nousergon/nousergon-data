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
    # engagement scan (_load_recent_engagements, config#1893/#2038).
    # claude_code_usage was removed 2026-07-14 with the pre-boot pace gate
    # (usage pacing dismantled) — the dispatcher no longer reads usage docs.
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


# ── config#2152: write surfaces (queue manifests + decision records) ─────────

WRITE_SURFACES = {
    "decision_records": "groom/decisions/2026-07-10/trigger-1900.json",
    "queue_manifests": "groom/queues/2026-07-10/trigger-1900-high-only.json",
    # alpha-engine-config-I5229. This surface existed in index.py from
    # config#3173 and was never added here, so nothing failed when the grant
    # was absent. Measured 2026-07-30: EVERY dispatch-ledger PutObject had been
    # AccessDenied since 2026-07-24, swallowed by a non-fatal warning, leaving
    # the ledger empty and the lane reconciler reporting a healthy
    # "open_expectations: 0" because its producer was dead — §2.4's
    # absence-of-a-signal failure in the component built to detect it.
    #
    # It is now load-bearing for availability, not just observability: the
    # post-launch write is FAIL-LOUD (§2.7 "registration failure is itself a
    # paging condition") and terminates the just-launched box on failure. A
    # missing grant here does not degrade the groom, it stops it.
    "dispatch_ledger": (
        "groom/_control/dispatch-ledger/2026-07-10/"
        "9d2004971a8e4b39ad554a47cd80ae39.json"
    ),
}


def _put_object_allows_key(key: str) -> bool:
    obj_arn = f"{RESEARCH_BUCKET_ARN}/{key}"
    for stmt in _statements():
        if stmt.get("Effect") != "Allow" or "s3:PutObject" not in _actions(stmt):
            continue
        if any(fnmatch.fnmatch(obj_arn, res) for res in _resources(stmt)):
            return True
    return False


def test_every_write_surface_has_put_object_grant():
    missing = {name: key for name, key in WRITE_SURFACES.items()
               if not _put_object_allows_key(key)}
    assert not missing, (
        f"iam-policy.json grants no s3:PutObject covering: {missing} — "
        "index.py's trigger/manifest writes will AccessDenied at run time "
        "(config#2142/#2152 gap class)."
    )


# ── I5229: the reconciler's own grants must match live ARN shapes ────────────


def test_send_task_failure_is_unscoped_because_iam_permits_nothing_narrower():
    """`states:SendTaskFailure` must be granted on `"*"` — narrower is broken.

    The action takes a task TOKEN, not a resource, and Step Functions supports
    NO resource-level permissions for it. Measured 2026-07-30 with
    `iam simulate-custom-policy`:

        Resource "*"                                        -> allowed
        Resource "execution:alpha-engine-groom-dispatch:*"   -> implicitDeny

    — and the ARN-scoped result is `implicitDeny` even for the *correct* ARN.
    So an execution-scoped grant does not restrict the action, it silently
    DISABLES it, and the reconciler would detect a dead lane and then be denied
    when reporting it: the one thing the feature exists to do.

    This test exists because the grant shipped as
    `execution:groom-dispatch:*` (also missing the `alpha-engine-` prefix) and
    a first attempt to fix it merely corrected the prefix — which would still
    have been implicitDeny, while a naive fnmatch-based test asserting "the
    grant matches a real execution ARN" would have PASSED. Assert the thing IAM
    actually evaluates, not the thing that looks tighter.

    The real bound is the token: a caller can only fail a task whose token it
    holds, and those tokens are minted by this Lambda's own dispatch path.
    """
    unscoped = [
        stmt for stmt in _statements()
        if stmt.get("Effect") == "Allow"
        and "states:SendTaskFailure" in _actions(stmt)
        and "*" in _resources(stmt)
    ]
    assert unscoped, (
        'states:SendTaskFailure must be granted with Resource "*" — Step '
        "Functions supports no resource-level permissions for it, so any ARN "
        "scoping evaluates implicitDeny and disables the reconciler's only "
        "means of reporting a dead lane."
    )
