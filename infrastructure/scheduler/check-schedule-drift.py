#!/usr/bin/env python3
"""check-schedule-drift.py — Diff codified EventBridge Scheduler rules against
live AWS state.

**Background (alpha-engine-config-I5805).** Every ``infrastructure/lambdas/
<name>/deploy.sh`` that owns a Scheduler rule declares it as literal shell
arrays — ``SCHED_NAMES`` / ``SCHED_CRONS``, and in the groom dispatcher's case
also ``SWEEP_SCHED_NAMES`` / ``SWEEP_SCHED_CRONS`` and a singular
``RECON_SCHED_NAME`` / ``RECON_SCHED_CRON``. Until 2026-07-31 those arrays were
only pushed to AWS from inside ``--bootstrap``, a flag no automation passed. The
consequence was a whole class of silent no-ops: ``nousergon-data#1179`` gave the
PR sweep three independent daily schedules, merged green, deployed green, and
created nothing. The sweep stayed coupled to groom health for as long as nobody
happened to look.

The structural fix is ``deploy.sh --reconcile-schedules`` running on every merge.
This script is the backstop for what that cannot see: a rule edited or disabled
in the console, a deploy that half-succeeded, a rule that was never created
because its repo has no deploy workflow at all. It is deliberately a SEPARATE
mechanism from the reconciler — a reconciler that reports on itself is not a
detector (``groom-sweep-policy.md`` §2.2).

Modelled on ``infrastructure/eventbridge/check-drift.py``: discovery by glob +
regex over the deploy scripts, no hardcoded rule registry to keep in sync, so a
rule added to a deploy.sh is covered by this check the moment it lands.

Drift cases (all exit non-zero):
  * ``missing-in-aws``    — codified rule does not exist live. This is the
                            I5805 case and the reason the script exists.
  * ``expression-drift``  — live ``ScheduleExpression`` differs from source.
  * ``disabled``          — rule exists but ``State != ENABLED``. A disabled
                            schedule is indistinguishable from a working one in
                            every surface except this check.
  * ``orphaned``          — a live rule under a declared ``SCHED_PREFIX`` that
                            source no longer declares. The prune loop in
                            deploy.sh should have removed it; if one survives,
                            something is still firing that nobody codifies.
  * ``source-error``      — the arrays in a deploy.sh do not pair up (a name
                            with no cron, or vice versa). The script is broken.

Usage:
  ./infrastructure/scheduler/check-schedule-drift.py             # every discovered rule
  ./infrastructure/scheduler/check-schedule-drift.py --rule NAME # one rule
  ./infrastructure/scheduler/check-schedule-drift.py --json      # machine-readable

Requires AWS creds with ``scheduler:GetSchedule`` and ``scheduler:ListSchedules``.
In CI this runs twice, under two different identities and for two different
reasons: in ``deploy-scheduled-groom-dispatcher.yml`` as the post-deploy outcome
assertion (``github-actions-lambda-deploy``), and in ``sf-arn-drift-check.yml``
on the daily out-of-band sweep (``github-actions-iam-drift-check``).
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
LAMBDAS_DIR = REPO_ROOT / "infrastructure" / "lambdas"

# Bash array literals: NAME=(\n  "a"\n  "b"\n). The leading \s* is load-bearing,
# not defensive: SWEEP_SCHED_NAMES is declared INSIDE an `if` block and so is
# indented. Anchoring at column 0 found the four groom rules and silently missed
# the three sweep ones — which is precisely the "zero found reads as does-not-
# exist" failure this whole check exists to catch, reproduced inside the checker.
_ARRAY_RE_TMPL = r"^\s*{name}=\(\n(.*?)\n\s*\)"
# Individual quoted entries inside an array body.
_ENTRY_RE = re.compile(r'^\s*"([^"]+)"\s*$', re.MULTILINE)
# Singular scalar assignments (the reconciler rule).
_SCALAR_RE_TMPL = r'^\s*{name}="([^"]+)"'
# SCHED_PREFIX="alpha-engine-scheduled-groom-"
_PREFIX_RE = re.compile(r'^\s*SCHED_PREFIX="([^"]+)"', re.MULTILINE)

# (names_var, crons_var) pairs to discover. Order matters only for output.
_ARRAY_PAIRS = [
    ("SCHED_NAMES", "SCHED_CRONS"),
    ("SWEEP_SCHED_NAMES", "SWEEP_SCHED_CRONS"),
]
_SCALAR_PAIRS = [
    ("RECON_SCHED_NAME", "RECON_SCHED_CRON"),
]


def _extract_array(text: str, var: str) -> list[str] | None:
    """Return the quoted entries of a bash array literal, or None if absent."""
    m = re.search(
        _ARRAY_RE_TMPL.format(name=re.escape(var)), text, re.DOTALL | re.MULTILINE
    )
    if not m:
        return None
    return _ENTRY_RE.findall(m.group(1))


def _extract_scalar(text: str, var: str) -> str | None:
    m = re.search(_SCALAR_RE_TMPL.format(name=re.escape(var)), text, re.MULTILINE)
    return m.group(1) if m else None


def discover_codified_rules() -> tuple[list[dict], list[dict], set[str]]:
    """Scan every `infrastructure/lambdas/*/deploy.sh` for Scheduler rules.

    Returns (rules, source_errors, prefixes) where each rule is
    {"name", "expression", "source_file"}.
    """
    rules: list[dict] = []
    errors: list[dict] = []
    prefixes: set[str] = set()

    for deploy_sh in sorted(LAMBDAS_DIR.glob("*/deploy.sh")):
        text = deploy_sh.read_text(encoding="utf-8")
        rel = str(deploy_sh.relative_to(REPO_ROOT))

        for names_var, crons_var in _ARRAY_PAIRS:
            names = _extract_array(text, names_var)
            crons = _extract_array(text, crons_var)
            if names is None and crons is None:
                continue
            if names is None or crons is None:
                errors.append({
                    "source_file": rel,
                    "kind": "source-error",
                    "detail": (
                        f"{names_var} and {crons_var} must both be present; "
                        f"found {names_var}={'yes' if names is not None else 'no'}, "
                        f"{crons_var}={'yes' if crons is not None else 'no'}"
                    ),
                })
                continue
            if len(names) != len(crons):
                errors.append({
                    "source_file": rel,
                    "kind": "source-error",
                    "detail": (
                        f"{names_var} has {len(names)} entries but {crons_var} "
                        f"has {len(crons)} — they are paired by index"
                    ),
                })
                continue
            for name, cron in zip(names, crons):
                rules.append({
                    "name": name, "expression": cron, "source_file": rel,
                })

        for name_var, cron_var in _SCALAR_PAIRS:
            name = _extract_scalar(text, name_var)
            cron = _extract_scalar(text, cron_var)
            if name is None and cron is None:
                continue
            if name is None or cron is None:
                errors.append({
                    "source_file": rel,
                    "kind": "source-error",
                    "detail": f"{name_var} and {cron_var} must both be present",
                })
                continue
            rules.append({"name": name, "expression": cron, "source_file": rel})

        prefix = _PREFIX_RE.search(text)
        if prefix:
            prefixes.add(prefix.group(1))

    return rules, errors, prefixes


def _aws(args: list[str]) -> tuple[int, str, str]:
    proc = subprocess.run(["aws"] + args, capture_output=True, text=True, check=False)
    return proc.returncode, proc.stdout.strip(), proc.stderr.strip()


def _live_schedule(name: str) -> dict | None:
    """Return {"expression", "state"} for a live rule, or None if absent."""
    rc, out, err = _aws([
        "scheduler", "get-schedule", "--name", name,
        "--query", "[ScheduleExpression,State]", "--output", "text",
    ])
    if rc != 0:
        if "ResourceNotFoundException" in err:
            return None
        # Anything else — AccessDenied, throttling, a bad region — must NOT be
        # reported as "missing". A permissions failure that reads as absence
        # would make this check grade itself green by losing its own access.
        raise RuntimeError(f"aws scheduler get-schedule --name {name}: {err}")
    parts = out.split("\t")
    if len(parts) != 2:
        raise RuntimeError(f"unexpected get-schedule output for {name}: {out!r}")
    return {"expression": parts[0], "state": parts[1]}


def _live_names_under(prefix: str) -> list[str]:
    rc, out, err = _aws([
        "scheduler", "list-schedules", "--name-prefix", prefix,
        "--query", "Schedules[].Name", "--output", "text",
    ])
    if rc != 0:
        raise RuntimeError(
            f"aws scheduler list-schedules --name-prefix {prefix}: {err}"
        )
    return out.split() if out else []


def check(rule_filter: str | None = None) -> tuple[list[dict], int]:
    rules, findings, prefixes = discover_codified_rules()
    if rule_filter:
        rules = [r for r in rules if r["name"] == rule_filter]
        if not rules:
            print(f"No codified rule named {rule_filter!r}", file=sys.stderr)
            return findings, 0

    codified_names = {r["name"] for r in rules}

    for rule in rules:
        live = _live_schedule(rule["name"])
        if live is None:
            findings.append({
                "rule": rule["name"], "kind": "missing-in-aws",
                "source_file": rule["source_file"],
                "detail": f"codified as {rule['expression']} but does not exist live",
            })
            continue
        if live["expression"] != rule["expression"]:
            findings.append({
                "rule": rule["name"], "kind": "expression-drift",
                "source_file": rule["source_file"],
                "detail": f"source={rule['expression']} live={live['expression']}",
            })
        if live["state"] != "ENABLED":
            findings.append({
                "rule": rule["name"], "kind": "disabled",
                "source_file": rule["source_file"],
                "detail": f"live state is {live['state']}, expected ENABLED",
            })

    # Orphan sweep, only over prefixes source actually claims to own. A live
    # rule outside every declared prefix is not this check's business.
    if not rule_filter:
        for prefix in sorted(prefixes):
            for live_name in _live_names_under(prefix):
                if live_name not in codified_names:
                    findings.append({
                        "rule": live_name, "kind": "orphaned",
                        "source_file": f"(none — under declared prefix {prefix})",
                        "detail": (
                            "live rule under a codified prefix that no deploy.sh "
                            "declares; the prune loop should have removed it"
                        ),
                    })

    return findings, len(rules)


def main() -> int:
    ap = argparse.ArgumentParser(description="EventBridge Scheduler drift check")
    ap.add_argument("--rule", help="check a single rule by name")
    ap.add_argument("--json", action="store_true", help="machine-readable output")
    args = ap.parse_args()

    try:
        findings, checked = check(args.rule)
    except RuntimeError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2

    if args.json:
        print(json.dumps({"checked": checked, "findings": findings}, indent=2))
    else:
        print(f"scheduler drift — {checked} codified rule(s) checked")
        if not findings:
            print(
                "  ✓ every codified rule exists live, ENABLED, "
                "with a matching expression"
            )
        for f in findings:
            print(f"  ✗ [{f['kind']}] {f.get('rule', f['source_file'])}")
            print(f"      {f['detail']}")
            print(f"      source: {f['source_file']}")

    return 1 if findings else 0


if __name__ == "__main__":
    sys.exit(main())
