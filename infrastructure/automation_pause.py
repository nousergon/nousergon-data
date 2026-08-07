#!/usr/bin/env python3
"""automation_pause.py — enforce and verify the scheduled-automation pause.

**Background (Brian ruling, 2026-08-07).** Every scheduled AWS trigger in this
account was disabled except the weekly Step Function (Saturday), the daily
preopen and postclose Step Functions (Mon-Fri), the 24/7 dashboard box, and the
two cost-safety backstops that protect them. ``infrastructure/automation_pause.json``
is the record of that ruling and the source of truth for which triggers are
intentionally off.

**Why this file exists at all.** Disabling a rule in the console is not a
decision anyone can find later, and it does not survive the machinery that
re-asserts ``ENABLED``:

  * ``deploy-infrastructure.yml`` re-applies the CloudFormation stack on EVERY
    push to main. MEASURED 2026-08-07: a successful run 49 seconds after the
    live disable did NOT revert the four CFN-owned rules — an unchanged
    template yields an empty changeset, and CloudFormation touches nothing. The
    revert risk is the NEXT template edit to those resources, which would
    rewrite them from a template still claiming ``State: ENABLED``. So the
    template edit is not urgency, it is truth: those four are pinned
    ``State: DISABLED`` in
    ``infrastructure/cloudformation/alpha-engine-orchestration.yaml`` so the
    source stops asserting something the operator has decided against.
  * ``infrastructure/lambdas/*/deploy.sh --reconcile-schedules`` recreates its
    Scheduler rules with the AWS default state (``ENABLED``) whenever that
    Lambda is redeployed. Those call sites are NOT yet pause-aware (see the
    delta note in this repo's PR): the pause survives them by detection, not
    prevention — ``--check`` runs in the daily drift sweep and goes red the next
    morning naming the exact ``--enforce`` command.

The check is deliberately two-directional. A pause that silently lifted and a
manifest entry for a rule that no longer exists are both findings: an entry that
can never fail is not a record, it is a comment.

Usage:
  ./infrastructure/automation_pause.py --check     # verify; exit 1 on any finding
  ./infrastructure/automation_pause.py --enforce   # re-disable anything that came back
  ./infrastructure/automation_pause.py --check --json

``--check`` needs ``events:DescribeRule`` + ``scheduler:GetSchedule``; ``--enforce``
additionally needs ``events:DisableRule`` + ``scheduler:UpdateSchedule``. CI runs
``--check`` only, under the read-only ``github-actions-iam-drift-check`` role.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

MANIFEST = Path(__file__).parent.resolve() / "automation_pause.json"
REGION = "us-east-1"


def load_manifest(path: Path = MANIFEST) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def paused_entries(manifest: dict | None = None) -> list[tuple[str, str, str]]:
    """Return [(surface, name, reason)] for every paused trigger.

    ``surface`` is ``"events"`` (an EventBridge rule) or ``"scheduler"`` (an
    EventBridge Scheduler schedule) — they are different APIs, not aliases.
    """
    m = manifest if manifest is not None else load_manifest()
    out: list[tuple[str, str, str]] = []
    for name, reason in sorted(m["paused"]["events_rules"].items()):
        out.append(("events", name, reason))
    for name, reason in sorted(m["paused"]["scheduler_schedules"].items()):
        out.append(("scheduler", name, reason))
    return out


def paused_names(manifest: dict | None = None) -> set[str]:
    """Every intentionally-paused trigger name, both surfaces."""
    return {name for _, name, _ in paused_entries(manifest)}


def _aws(args: list[str]) -> tuple[int, str, str]:
    proc = subprocess.run(
        ["aws"] + args + ["--region", REGION], capture_output=True, text=True, check=False
    )
    return proc.returncode, proc.stdout.strip(), proc.stderr.strip()


def _live_state(surface: str, name: str) -> str | None:
    """Return the live State, or None if the trigger does not exist.

    Any failure that is NOT a genuine not-found is raised. A permissions error
    read as absence would let this check grade itself green by losing its own
    access — the same failure mode check-schedule-drift.py guards against.
    """
    if surface == "events":
        rc, out, err = _aws(
            ["events", "describe-rule", "--name", name, "--query", "State", "--output", "text"]
        )
        not_found = "ResourceNotFoundException" in err
    else:
        rc, out, err = _aws(
            ["scheduler", "get-schedule", "--name", name, "--query", "State", "--output", "text"]
        )
        not_found = "ResourceNotFoundException" in err
    if rc != 0:
        if not_found:
            return None
        raise RuntimeError(f"aws {surface} describe/get {name}: {err}")
    return out


def _disable(surface: str, name: str) -> None:
    if surface == "events":
        rc, _, err = _aws(["events", "disable-rule", "--name", name])
        if rc != 0:
            raise RuntimeError(f"aws events disable-rule --name {name}: {err}")
        return
    # Scheduler has no disable verb: update-schedule is a full replace, so the
    # live spec must be round-tripped or every unspecified attribute is lost.
    rc, out, err = _aws(["scheduler", "get-schedule", "--name", name, "--output", "json"])
    if rc != 0:
        raise RuntimeError(f"aws scheduler get-schedule --name {name}: {err}")
    spec = json.loads(out)
    for derived in ("Arn", "CreationDate", "LastModificationDate", "ResponseMetadata"):
        spec.pop(derived, None)
    spec["State"] = "DISABLED"
    rc, _, err = _aws(["scheduler", "update-schedule", "--cli-input-json", json.dumps(spec)])
    if rc != 0:
        raise RuntimeError(f"aws scheduler update-schedule --name {name}: {err}")


def check() -> list[dict]:
    findings: list[dict] = []
    for surface, name, reason in paused_entries():
        state = _live_state(surface, name)
        if state is None:
            findings.append({
                "trigger": name, "surface": surface, "kind": "missing-in-aws",
                "detail": (
                    "listed as paused but does not exist live — it was deleted, or "
                    "renamed. Remove the entry from automation_pause.json."
                ),
            })
        elif state != "DISABLED":
            findings.append({
                "trigger": name, "surface": surface, "kind": "unexpectedly-enabled",
                "detail": (
                    f"paused on 2026-08-07 but live state is {state} — a deploy or a "
                    f"console edit lifted the pause. Reason it is paused: {reason}. "
                    f"Fix: ./infrastructure/automation_pause.py --enforce"
                ),
            })
    return findings


def enforce() -> list[str]:
    acted: list[str] = []
    for surface, name, _ in paused_entries():
        state = _live_state(surface, name)
        if state is not None and state != "DISABLED":
            _disable(surface, name)
            acted.append(f"{surface}:{name}")
    return acted


def main() -> int:
    ap = argparse.ArgumentParser(description="scheduled-automation pause check / enforce")
    mode = ap.add_mutually_exclusive_group(required=True)
    mode.add_argument("--check", action="store_true", help="verify the pause holds")
    mode.add_argument("--enforce", action="store_true", help="re-disable anything enabled")
    ap.add_argument("--json", action="store_true", help="machine-readable output")
    args = ap.parse_args()

    entries = paused_entries()

    try:
        if args.enforce:
            acted = enforce()
            if args.json:
                print(json.dumps({"re_disabled": acted}, indent=2))
            else:
                print(f"automation pause — {len(entries)} paused trigger(s)")
                for a in acted:
                    print(f"  re-disabled {a}")
                if not acted:
                    print("  ✓ nothing had come back; no action taken")
            return 0

        findings = check()
    except RuntimeError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2

    if args.json:
        print(json.dumps({"checked": len(entries), "findings": findings}, indent=2))
    else:
        print(f"automation pause — {len(entries)} paused trigger(s) checked")
        if not findings:
            print("  ✓ every paused trigger exists live and is DISABLED")
        for f in findings:
            print(f"  ✗ [{f['kind']}] {f['surface']}:{f['trigger']}")
            print(f"      {f['detail']}")

    return 1 if findings else 0


if __name__ == "__main__":
    sys.exit(main())
