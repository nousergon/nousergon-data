"""
tests/test_sf_iam_lambda_grants.py — SF role IAM grants must cover every
Lambda the SF invokes.

Static check that walks the Saturday + weekday + EOD SF defns, extracts
every `lambda:invoke` Lambda ARN, then asserts each ARN is grantable
under one of the patterns in
infrastructure/iam/alpha-engine-step-functions-role.json's
`lambda:InvokeFunction` resource list.

Pattern matching: the codified policy uses trailing-`*` wildcards (e.g.
`arn:aws:lambda:...:alpha-engine-research-eval-judge*`). A SF Lambda
ARN matches a policy pattern when its prefix (the part before the
trailing `*`) is a prefix of the SF ARN.

Regression target: 2026-05-07 SF runs after the agent-justification
triple shipped (2026-05-06) silently caught
`Lambda.AWSLambdaException: AccessDenied` from RationaleClustering,
ReplayConcordance, and Counterfactual states because the codified
policy was last edited before those Lambdas were added to the SF.
The SF state-level Catch[States.ALL] swallowed the errors and the
operator had no surface to see them — the Lambdas just silently
never wrote their S3 outputs (~3 weeks of missing data on the
agent-justification triple).

Per the asymmetric-IAM-grant antipattern memory: 5th instance of
this class. The codified-policy + check-drift loop catches policy/AWS
divergence; this test catches policy/SF-defn divergence — the OTHER
half of the symmetric grant.
"""

import json
import re
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
IAM_DIR = REPO_ROOT / "infrastructure" / "iam"
INFRA_DIR = REPO_ROOT / "infrastructure"

SF_FILES = [
    INFRA_DIR / "step_function.json",
    INFRA_DIR / "step_function_daily.json",
    INFRA_DIR / "step_function_eod.json",
]
ROLE_POLICY = IAM_DIR / "alpha-engine-step-functions-role.json"


def _collect_invoked_lambda_arns(sf_doc: dict) -> set[str]:
    """Extract every Lambda ARN any Task state in the SF would invoke.

    Two invocation patterns we care about:
      (1) "Resource": "arn:aws:states:::lambda:invoke" with
          Parameters.FunctionName = full ARN or short name + ":alias"
      (2) "Resource": "arn:aws:states:::aws-sdk:lambda:invoke" (less
          common; same shape)

    Returns the set of fully-qualified ARNs (or short names that we
    convert to ARNs assuming us-east-1 + the canonical account).

    Recursively walks Parallel branches so Lambda invocations nested
    inside a Parallel state (e.g. the ResearchPredictorParallel Branch
    A chain: Research → DataPhase2 → eval-judge chain →
    RationaleClustering → ReplayConcordance → Counterfactual →
    AggregateCosts) are not silently skipped. Pre-2026-05-26 the walker
    only iterated top-level States; the aggregate-costs Lambda was
    added 2026-05-25 (L1146.B) inside Branch A and shipped without an
    IAM grant because this walker missed it.
    """
    found: set[str] = set()

    def _walk(states: dict) -> None:
        for state_name, state in states.items():
            if state.get("Type") == "Parallel":
                for branch in state.get("Branches", []):
                    _walk(branch.get("States", {}))
                continue
            if state.get("Type") != "Task":
                continue
            resource = state.get("Resource", "")
            if (
                "lambda:invoke" not in resource
                and "lambda:Invoke" not in resource
            ):
                continue
            params = state.get("Parameters", {})
            fn = params.get("FunctionName") or params.get("FunctionName.$")
            if not fn or not isinstance(fn, str):
                continue
            if fn.startswith("arn:aws:lambda:"):
                arn = fn.split(":")[6] if fn.count(":") >= 6 else fn
                # Normalize to full ARN minus alias suffix for prefix matching
                base = ":".join(fn.split(":")[:7])
                found.add(base)
            else:
                # Short name with optional ":alias" — assume canonical region/acct
                short = fn.split(":")[0]
                found.add(
                    f"arn:aws:lambda:us-east-1:711398986525:function:{short}"
                )

    _walk(sf_doc.get("States", {}))
    return found


def _policy_lambda_patterns() -> list[str]:
    """Return the list of resource patterns from the role's
    lambda:InvokeFunction statement, normalized to drop trailing `*`."""
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
            if r.endswith("*"):
                out.append(r.rstrip("*"))
            else:
                out.append(r)
    return out


def _arn_matches_any(arn: str, patterns: list[str]) -> bool:
    """True if any pattern is a prefix of the ARN (after wildcard strip)."""
    return any(arn.startswith(p) for p in patterns)


@pytest.mark.parametrize("sf_path", SF_FILES, ids=lambda p: p.name)
def test_every_invoked_lambda_has_iam_grant(sf_path: Path):
    """For each SF defn, every Lambda its Task states would invoke must
    be grantable under the codified IAM policy."""
    if not sf_path.exists():
        pytest.skip(f"{sf_path.name} missing — repo layout drift?")

    sf_doc = json.loads(sf_path.read_text())
    invoked = _collect_invoked_lambda_arns(sf_doc)
    if not invoked:
        pytest.skip(f"{sf_path.name}: no Lambda invocations found")

    patterns = _policy_lambda_patterns()
    assert patterns, (
        "No lambda:InvokeFunction resources found in "
        "alpha-engine-step-functions-role.json — has the policy moved?"
    )

    missing = sorted(arn for arn in invoked if not _arn_matches_any(arn, patterns))
    assert not missing, (
        f"{sf_path.name} invokes Lambdas not granted by the SF role IAM "
        "policy. Add their ARN patterns to "
        "infrastructure/iam/alpha-engine-step-functions-role.json's "
        "lambda:InvokeFunction Resource list (with trailing `*` for "
        "alias support):\n"
        + "\n".join(f"  - {arn}*" for arn in missing)
    )
