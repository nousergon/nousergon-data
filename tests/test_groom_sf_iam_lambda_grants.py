"""
tests/test_groom_sf_iam_lambda_grants.py — groom-dispatch SF role IAM grant
must cover every Lambda the groom SF invokes.

`alpha-engine-groom-dispatch` (infrastructure/step_function_groom.json) runs
under its OWN execution role, `alpha-engine-groom-sf-role` — a separate role
from `alpha-engine-step-functions-role` covered by
tests/test_sf_iam_lambda_grants.py (config#1613/#2010: SF role grants are
per-role, not fleet-wide; conflating them was an explicit gotcha called out
when this class of gap was scoped).

The groom-sf-role's codified policy
(infrastructure/lambdas/scheduled-groom-dispatcher/sf-execution-iam-policy.json)
is applied idempotently by deploy.sh on every deploy — this test is the
policy/SF-defn-drift half of that pair (mirrors
test_sf_iam_lambda_grants.py's role for the main SF role): it catches the
case where a future edit adds a second Lambda invocation to the groom SF
without extending the single-Lambda `InvokeGroomDispatcherLambda` grant.

Regression target: config#2010 (2026-07-08) — the main SF role's
`alpha-engine-data-spot-dispatcher` and `alpha-engine-scheduled-groom-
dispatcher` grants both went missing after their Lambdas were added to their
respective SF definitions without a matching IAM update, causing
AccessDeniedException at run time. The main-role half is now guarded by
test_sf_iam_lambda_grants.py + the PR-gating pytest CI run + the
iam-drift-check.yml live-AWS drift check; this file closes the equivalent
gap for the groom SF's own role.
"""

import json
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
GROOM_LAMBDA_DIR = REPO_ROOT / "infrastructure" / "lambdas" / "scheduled-groom-dispatcher"

SF_FILE = REPO_ROOT / "infrastructure" / "step_function_groom.json"
ROLE_POLICY = GROOM_LAMBDA_DIR / "sf-execution-iam-policy.json"


def _collect_invoked_lambda_arns(sf_doc: dict) -> set[str]:
    """Extract every Lambda ARN any Task state in the SF would invoke.

    Same two invocation-shape handling as test_sf_iam_lambda_grants.py
    (bare-ARN Resource + FunctionName param, short-name fallback), without
    the Parallel-branch walk since the groom SF defn has no Parallel states
    today — kept as a plain top-level walk to match the SF's actual shape
    rather than speculatively generalizing.
    """
    found: set[str] = set()
    for state in sf_doc.get("States", {}).values():
        if state.get("Type") != "Task":
            continue
        resource = state.get("Resource", "")
        if "lambda:invoke" not in resource and "lambda:Invoke" not in resource:
            continue
        params = state.get("Parameters", {})
        fn = params.get("FunctionName") or params.get("FunctionName.$")
        if not fn or not isinstance(fn, str):
            continue
        if fn.startswith("arn:aws:lambda:"):
            base = ":".join(fn.split(":")[:7])
            found.add(base)
        else:
            short = fn.split(":")[0]
            found.add(f"arn:aws:lambda:us-east-1:711398986525:function:{short}")
    return found


def _policy_lambda_patterns() -> list[str]:
    doc = json.loads(ROLE_POLICY.read_text())
    out: list[str] = []
    for stmt in doc.get("Statement", []):
        actions = stmt.get("Action")
        actions_list = [actions] if isinstance(actions, str) else (actions or [])
        if "lambda:InvokeFunction" not in actions_list:
            continue
        resources = stmt.get("Resource")
        resources_list = [resources] if isinstance(resources, str) else (resources or [])
        for r in resources_list:
            out.append(r.rstrip("*") if r.endswith("*") else r)
    return out


def _arn_matches_any(arn: str, patterns: list[str]) -> bool:
    return any(arn.startswith(p) for p in patterns)


def test_groom_sf_invoked_lambdas_have_iam_grant():
    """Every Lambda the groom-dispatch SF invokes must be grantable under
    its own codified execution-role policy."""
    if not SF_FILE.exists():
        pytest.skip(f"{SF_FILE.name} missing — repo layout drift?")
    if not ROLE_POLICY.exists():
        pytest.skip(f"{ROLE_POLICY.name} missing — repo layout drift?")

    invoked = _collect_invoked_lambda_arns(json.loads(SF_FILE.read_text()))
    assert invoked, (
        f"{SF_FILE.name}: no Lambda invocations found — has the groom SF "
        "definition moved or been restructured?"
    )

    patterns = _policy_lambda_patterns()
    assert patterns, (
        "No lambda:InvokeFunction resources found in "
        f"{ROLE_POLICY.relative_to(REPO_ROOT)} — has the policy moved?"
    )

    missing = sorted(arn for arn in invoked if not _arn_matches_any(arn, patterns))
    assert not missing, (
        f"{SF_FILE.name} invokes Lambdas not granted by alpha-engine-groom-sf-role's "
        f"policy. Add their ARNs to {ROLE_POLICY.relative_to(REPO_ROOT)}'s "
        "lambda:InvokeFunction Resource list:\n"
        + "\n".join(f"  - {arn}" for arn in missing)
    )
