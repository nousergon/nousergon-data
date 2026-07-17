#!/usr/bin/env python3
"""check-lambda-existence.py — Verify every Lambda a codified Step Function
`lambda:invoke` state targets actually exists live on AWS.

**Background (alpha-engine-config#1464, 2026-07-08 EOD incident).**
config#1767 Phase 2 (nousergon-data#643) merged SF wiring that invoked
`alpha-engine-data-spot-dispatcher` from `step_function_eod.json`, plus the
matching IAM invoke grant. The IAM grant reached live AWS days late
(config#1446); once THAT was fixed, the SF still 404'd — the Lambda itself
had never been deployed (no `deploy.sh` shipped for it; fixed in
nousergon-data#698). Fail-open masked the failure at the SF step, and it
surfaced two hops downstream as a hard EODReconcile failure on a missing
SPY close instead of failing loud at the source.

This is the same codified-but-not-live drift class the sibling
`check-drift.py` scripts in this directory cover (EventBridge stateMachineArn,
SF LoggingConfiguration) — generalized from "grant not applied" /
"logging dropped" to "referenced resource never deployed". A SF definition
can reference a `FunctionName` that is syntactically valid and IAM-permitted
but simply does not exist, and nothing catches that until the state actually
executes in production.

**Source of truth.** Every `infrastructure/step_function_*.json` file in this
directory's parent — the same files `check-definition-drift.py` treats as
canonical. This script walks each definition's `States` tree (including
nested `Iterator`/`ItemProcessor` maps and `Parallel` branches) for any state
whose `Resource` is (or is a `.waitForTaskToken`/`.sync`-suffixed variant of)
`arn:aws:states:::lambda:invoke`, and extracts `Parameters.FunctionName`.

`FunctionName` values in this fleet's SF JSON appear in three shapes — all
normalized to a bare function name before the existence check:
  * a bare name                      (`alpha-engine-scheduled-groom-dispatcher`)
  * a full ARN
    (`arn:aws:lambda:us-east-1:711398986525:function:alpha-engine-ssm-liveness-poller`)
  * a name with a version/alias qualifier (`alpha-engine-predictor-inference:live`)

Drift cases (all exit non-zero):
  * A referenced Lambda does not exist on AWS at all (`ResourceNotFoundException`
    from `lambda:get-function`) — the exact 2026-07-08 incident class.
  * A codified SF definition file is missing or fails to parse (source-error).

Usage:
  ./infrastructure/step-functions/check-lambda-existence.py             # check every codified SF
  ./infrastructure/step-functions/check-lambda-existence.py --name NAME # check one (by SF name)

Requires AWS creds with `lambda:GetFunction` on the referenced functions
(read-only). In CI: intended to reuse the same OIDC role as
`iam-drift-check.yml` / `sf-arn-drift-check.yml`
(`github-actions-iam-drift-check`), which will need `lambda:GetFunction`
added to its policy — see this repo's PR for config#1464 for the exact IAM
diff (that role is codified in `crucible-executor`, not this repo).
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent.resolve()
REPO_ROOT = SCRIPT_DIR.parent.parent

# repo definition file -> live state machine name. Mirrors
# check-definition-drift.py's SF_DEFINITIONS mapping (this script defines its
# own copy rather than importing that hyphenated-filename module, matching
# how check-drift.py in this same directory is also self-contained).
SF_DEFINITIONS: tuple[dict, ...] = (
    {"sf_name": "ne-weekly-freshness-pipeline", "definition_file": "step_function.json"},
    {"sf_name": "ne-preopen-trading-pipeline", "definition_file": "step_function_daily.json"},
    {"sf_name": "ne-postclose-trading-pipeline", "definition_file": "step_function_eod.json"},
    {"sf_name": "alpha-engine-groom-dispatch", "definition_file": "step_function_groom.json"},
)

_LAMBDA_INVOKE_RESOURCE_RE = re.compile(
    r"^arn:aws:states:::lambda:invoke(\.(waitForTaskToken|sync|sync:2))?$"
)
# Bare-name shape Lambda function names are allowed to take (letters, digits,
# - and _, up to 64 chars) — used only to sanity-check extraction, not to
# validate AWS's actual rules.
_FULL_ARN_RE = re.compile(r"^arn:aws:lambda:[\w-]+:\d+:function:([^:]+)(:.+)?$")


def _load_definition(definition_file: str) -> tuple[dict | None, str | None]:
    """Load one SF definition JSON. Returns (definition, error)."""
    path = REPO_ROOT / "infrastructure" / definition_file
    if not path.is_file():
        return None, f"{path} not found"
    try:
        return json.loads(path.read_text()), None
    except json.JSONDecodeError as exc:
        return None, f"{path} is not valid JSON: {exc}"


def _normalize_function_name(raw: str) -> str:
    """Reduce a FunctionName value (bare name, full ARN, or name/ARN with a
    :version-or-alias qualifier) to the bare function name `lambda
    get-function --function-name` accepts."""
    m = _FULL_ARN_RE.match(raw)
    if m:
        return m.group(1)
    # Bare name, possibly with a :qualifier suffix (e.g. "name:live").
    return raw.split(":", 1)[0]


def _walk_states(states: dict) -> list[dict]:
    """Recursively collect every Task state whose Resource targets
    lambda:invoke (any .waitForTaskToken/.sync suffix), across Map
    (Iterator/ItemProcessor) and Parallel (Branches) nesting."""
    found: list[dict] = []
    for state_name, state in states.items():
        if state.get("Type") == "Task":
            resource = state.get("Resource", "")
            if _LAMBDA_INVOKE_RESOURCE_RE.match(resource):
                params = state.get("Parameters", {}) or {}
                function_name = params.get("FunctionName")
                found.append(
                    {"state_name": state_name, "function_name": function_name}
                )
        # Map state (older "Iterator" key, newer "ItemProcessor" key).
        for nested_key in ("Iterator", "ItemProcessor"):
            nested = state.get(nested_key)
            if isinstance(nested, dict) and "States" in nested:
                found.extend(_walk_states(nested["States"]))
        # Parallel state.
        for branch in state.get("Branches", []) or []:
            if "States" in branch:
                found.extend(_walk_states(branch["States"]))
    return found


def _discover_referenced_functions(sf_name: str, definition_file: str) -> list[dict]:
    """Return one entry per lambda:invoke state discovered in the codified
    definition, or a single error entry if the file can't be loaded/parsed."""
    definition, error = _load_definition(definition_file)
    if error is not None:
        return [{"sf_name": sf_name, "definition_file": definition_file, "error": error}]

    states = definition.get("States", {})
    invokes = _walk_states(states)

    results: list[dict] = []
    for inv in invokes:
        function_name = inv["function_name"]
        if not function_name or not isinstance(function_name, str):
            results.append({
                "sf_name": sf_name,
                "definition_file": definition_file,
                "state_name": inv["state_name"],
                "error": (
                    f"state {inv['state_name']!r} targets lambda:invoke but "
                    f"has no (or non-string) Parameters.FunctionName"
                ),
            })
            continue
        results.append({
            "sf_name": sf_name,
            "definition_file": definition_file,
            "state_name": inv["state_name"],
            "function_name": function_name,
            "normalized_name": _normalize_function_name(function_name),
        })
    return results


def _aws_lambda_get_function(function_name: str) -> bool:
    """True if the function exists live. False on ResourceNotFoundException.
    Any other AWS CLI failure (auth, throttling, etc.) is fatal — this guard
    must never report a false-clean pass because of an unrelated AWS error."""
    result = subprocess.run(
        ["aws", "lambda", "get-function", "--function-name", function_name,
         "--output", "json"],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode == 0:
        return True
    if "ResourceNotFoundException" in result.stderr:
        return False
    sys.stderr.write(
        f"AWS CLI failed: aws lambda get-function --function-name {function_name}\n"
        f"stderr: {result.stderr}\n"
    )
    sys.exit(2)


def _check_sf(entry: dict) -> list[str]:
    sf_name = entry["sf_name"]
    definition_file = entry["definition_file"]
    findings: list[str] = []

    references = _discover_referenced_functions(sf_name, definition_file)
    for ref in references:
        if "error" in ref:
            state_bit = f" (state {ref['state_name']!r})" if "state_name" in ref else ""
            findings.append(f"{sf_name}{state_bit}: {ref['error']}")
            continue

        if not _aws_lambda_get_function(ref["normalized_name"]):
            findings.append(
                f"{sf_name}: state {ref['state_name']!r} in {definition_file} "
                f"targets lambda:invoke FunctionName={ref['function_name']!r} "
                f"(resolved: {ref['normalized_name']!r}) but that Lambda does "
                f"not exist on AWS (ResourceNotFoundException) — the SF will "
                f"404 the moment this state executes. Deploy the function "
                f"before/with this SF change, or remove the reference."
            )

    return findings


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--name", help="Check one state machine by name (default: every codified one)"
    )
    args = parser.parse_args()

    entries = list(SF_DEFINITIONS)

    if args.name:
        entries = [e for e in entries if e["sf_name"] == args.name]
        if not entries:
            sys.stderr.write(
                f"ERROR: no codified definition mapping for state machine "
                f"'{args.name}'\n"
            )
            return 2

    total_findings: list[str] = []
    for entry in entries:
        total_findings.extend(_check_sf(entry))

    if total_findings:
        print(f"SF lambda:invoke existence drift detected ({len(total_findings)} finding(s)):")
        for f in total_findings:
            print(f"  - {f}")
        return 1

    sf_names = ", ".join(e["sf_name"] for e in entries)
    print(f"OK: every lambda:invoke FunctionName referenced by {sf_names} exists live")
    return 0


if __name__ == "__main__":
    sys.exit(main())
