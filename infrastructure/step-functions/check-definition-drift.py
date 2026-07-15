#!/usr/bin/env python3
"""check-definition-drift.py — Diff the codified Step Function DEFINITIONS
(repo `infrastructure/step_function*.json`) against live AWS state AND the
S3 staged copies CloudFormation reads from.

**Background (alpha-engine-config#2273).** The weekly SF definition existed
as THREE copies with no reconciliation: the repo file (source of truth), the
S3 object CFN's ``DefinitionS3Location`` references (read at stack-create /
resource-replacement time), and the live state machine. Historically
``deploy_step_function.sh`` updated the live machine from the LOCAL file
without refreshing the S3 object — so a stale S3 copy sat armed, and any
future CFN restamp/replacement would silently ROLL BACK the live definition
to whatever the S3 object held. config#2273 codified the single-writer
contract (every deploy path uploads the stamped repo bytes to the CFN key
before ``update-state-machine`` from those same bytes); this script is the
drift BACKSTOP that pages when any of the three copies diverges anyway
(out-of-band console edit, aborted deploy, drive-by S3 write).

Sibling of `infrastructure/step-functions/check-drift.py` (the
LoggingConfiguration drift guard, config#1464) — same shape: standalone,
regex/JSON parsing of repo sources, live state via the AWS CLI, exit 0/1/2.

**Normalization.** Deploys stamp the top-level ``Comment`` with a
``[git:<sha>] `` prefix (see deploy-infrastructure.sh); the repo file is
unstamped. The comparison strips that stamp from both sides and compares
canonical JSON (sorted keys) — so a stamp-only difference is NOT drift, but
any real Comment/state/field change is.

Drift cases (all exit non-zero):
  * Live definition differs from the repo file (normalized) — the live
    machine was written from something other than the repo HEAD bytes.
  * S3 staged copy differs from the repo file (normalized) — the CFN
    read-source is stale; a CFN restamp would deploy those stale bytes.
  * A codified state machine isn't found on AWS at all (missing-in-aws).
  * The S3 staged object is missing entirely.
  * A repo definition file is missing or malformed JSON (source-error).

Usage:
  ./infrastructure/step-functions/check-definition-drift.py               # every codified SF
  ./infrastructure/step-functions/check-definition-drift.py --name NAME   # one (by SF name)

Requires AWS creds with states:DescribeStateMachine on the target state
machines and s3:GetObject on s3://alpha-engine-research/infrastructure/*.
Wiring: standalone by design, callable from liveness sweeps / operator
sessions — mirrors its check-drift.py siblings rather than adding a new
GHA schedule (fleet direction is EC2-spot sweeps, not GHA-hosted crons).
Shape-guarded by tests/test_sf_definition_check_drift.py (mocked CLI — no
real AWS access in CI).
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
INFRA_DIR = REPO_ROOT / "infrastructure"

# Fallback defaults used only to build the ARN this script queries AWS with
# (mirrors step-functions/check-drift.py's same constants).
DEFAULT_REGION = "us-east-1"
DEFAULT_ACCOUNT_ID = "711398986525"

# The S3 bucket/prefix every deploy path stages definitions to — MUST match
# deploy-infrastructure.sh ($BUCKET) and the CFN DefinitionS3Location keys.
S3_BUCKET = "alpha-engine-research"
S3_PREFIX = "infrastructure/"

# repo definition file -> live state machine name. Mirrors the ARN mapping in
# deploy-infrastructure.sh step 3. A renamed/removed file or SF fails loud
# below (source-error / missing-in-aws) rather than silently dropping out.
SF_DEFINITIONS: tuple[dict, ...] = (
    {"sf_name": "ne-weekly-freshness-pipeline", "definition_file": "step_function.json"},
    {"sf_name": "ne-preopen-trading-pipeline", "definition_file": "step_function_daily.json"},
    {"sf_name": "ne-postclose-trading-pipeline", "definition_file": "step_function_eod.json"},
    {"sf_name": "alpha-engine-groom-pipeline", "definition_file": "step_function_groom.json"},
    # alpha-engine-config-I2544/I2545: advisory + Sunday-modelzoo child SFs,
    # split out of step_function.json (config#2273 single-writer contract
    # applies to these two files identically — see deploy_step_function.sh).
    {"sf_name": "ne-weekly-advisory-pipeline", "definition_file": "step_function_advisory.json"},
    {"sf_name": "ne-modelzoo-sunday-pipeline", "definition_file": "step_function_modelzoo.json"},
)

_GIT_STAMP_RE = re.compile(r"^\[git:[0-9a-fA-F]{7,40}\]\s*")


def _normalized_dict(definition: dict) -> dict:
    """Deep copy with the git-stamp stripped from the top-level Comment —
    the ONLY tolerated difference between the repo file and deployed copies."""
    d = json.loads(json.dumps(definition))  # deep copy — never mutate input
    comment = d.get("Comment")
    if isinstance(comment, str):
        d["Comment"] = _GIT_STAMP_RE.sub("", comment)
    return d


def _normalize(definition: dict) -> str:
    """Canonical form for comparison: git-stamp stripped from the top-level
    Comment, keys sorted, whitespace-free dump."""
    return json.dumps(_normalized_dict(definition), sort_keys=True, separators=(",", ":"))


def _diff_summary(expected: dict, actual: dict) -> str:
    """Human-oriented pointer at WHERE two definitions diverge (top-level
    keys; differing state names when States is the divergent key). Callers
    pass stamp-stripped (_normalized_dict) copies so the git stamp never
    reads as a Comment divergence."""
    expected, actual = _normalized_dict(expected), _normalized_dict(actual)
    parts: list[str] = []
    keys = sorted(set(expected) | set(actual))
    for key in keys:
        if json.dumps(expected.get(key), sort_keys=True) == json.dumps(actual.get(key), sort_keys=True):
            continue
        if key == "States" and isinstance(expected.get(key), dict) and isinstance(actual.get(key), dict):
            exp_states, act_states = expected[key], actual[key]
            differing = sorted(
                name
                for name in set(exp_states) | set(act_states)
                if json.dumps(exp_states.get(name), sort_keys=True)
                != json.dumps(act_states.get(name), sort_keys=True)
            )
            shown = ", ".join(differing[:5]) + (" …" if len(differing) > 5 else "")
            parts.append(f"States ({len(differing)} differing: {shown})")
        else:
            parts.append(key)
    return "; ".join(parts) if parts else "<no top-level divergence found — nested/ordering?>"


def _aws_cli(*args: str, allow_missing_patterns: tuple[str, ...] = ()):
    """Run an AWS CLI command; return raw stdout, None when the error matches
    an allow_missing pattern, or hard-exit 2 on any other failure (a broken
    CLI/creds state must never read as 'no drift')."""
    result = subprocess.run(
        ["aws", *args],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        if any(pat in result.stderr for pat in allow_missing_patterns):
            return None
        sys.stderr.write(
            f"AWS CLI failed: aws {' '.join(args)}\n"
            f"stderr: {result.stderr}\n"
        )
        sys.exit(2)
    return result.stdout


def _fetch_live_definition(sf_name: str) -> dict | None:
    """Live definition dict, or None when the state machine doesn't exist."""
    arn = f"arn:aws:states:{DEFAULT_REGION}:{DEFAULT_ACCOUNT_ID}:stateMachine:{sf_name}"
    out = _aws_cli(
        "stepfunctions",
        "describe-state-machine",
        "--state-machine-arn",
        arn,
        "--output",
        "json",
        allow_missing_patterns=("StateMachineDoesNotExist", "ResourceNotFoundException"),
    )
    if out is None:
        return None
    desc = json.loads(out)
    return json.loads(desc["definition"])


def _fetch_s3_definition(key: str) -> dict | None:
    """S3 staged definition dict, or None when the object doesn't exist."""
    out = _aws_cli(
        "s3",
        "cp",
        f"s3://{S3_BUCKET}/{key}",
        "-",
        allow_missing_patterns=("Not Found", "NoSuchKey", "404", "does not exist"),
    )
    if out is None:
        return None
    return json.loads(out)


def _check_sf(entry: dict) -> list[str]:
    sf_name = entry["sf_name"]
    definition_path = INFRA_DIR / entry["definition_file"]
    source_rel = definition_path.relative_to(REPO_ROOT)

    if not definition_path.is_file():
        return [
            f"{sf_name}: codified definition {source_rel} not found — has the "
            f"file been renamed without updating SF_DEFINITIONS in this script?"
        ]
    try:
        repo_def = json.loads(definition_path.read_text())
    except json.JSONDecodeError as exc:
        return [f"{sf_name}: {source_rel} is not valid JSON ({exc})"]

    repo_norm = _normalize(repo_def)
    findings: list[str] = []

    # ── live vs repo ─────────────────────────────────────────────────────
    live_def = _fetch_live_definition(sf_name)
    if live_def is None:
        findings.append(
            f"{sf_name}: codified in {source_rel} but state machine not found "
            f"on AWS (renamed/recreated without updating the source, or vice "
            f"versa?)"
        )
    elif _normalize(live_def) != repo_norm:
        findings.append(
            f"{sf_name}: definition drift (LIVE vs {source_rel}) — the live "
            f"state machine was not written from the repo bytes. Diverges at: "
            f"{_diff_summary(repo_def, live_def)}"
        )

    # ── S3 staged copy vs repo ───────────────────────────────────────────
    s3_key = f"{S3_PREFIX}{entry['definition_file']}"
    s3_def = _fetch_s3_definition(s3_key)
    if s3_def is None:
        findings.append(
            f"{sf_name}: staged copy s3://{S3_BUCKET}/{s3_key} is missing — "
            f"a CFN stack-create/replacement would fail (or read nothing); "
            f"run a deploy to restore it."
        )
    else:
        try:
            s3_drifted = _normalize(s3_def) != repo_norm
        except (TypeError, ValueError) as exc:
            findings.append(f"{sf_name}: s3://{S3_BUCKET}/{s3_key} unparseable ({exc})")
            s3_drifted = False
        if s3_drifted:
            findings.append(
                f"{sf_name}: definition drift (S3 staged copy vs {source_rel}) "
                f"— s3://{S3_BUCKET}/{s3_key} is stale; a future CFN "
                f"restamp/replacement would silently roll the live definition "
                f"back to those bytes (config#2273). Diverges at: "
                f"{_diff_summary(repo_def, s3_def)}"
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
        print(f"SF definition drift detected ({len(total_findings)} finding(s)):")
        for f in total_findings:
            print(f"  - {f}")
        return 1

    sf_names = ", ".join(e["sf_name"] for e in entries)
    print(f"OK: repo, live, and S3 staged definitions all match for {sf_names}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
