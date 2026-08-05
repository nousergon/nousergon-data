#!/usr/bin/env python3
"""check-manifest-drift.py — Reconcile schedule-manifest.json against the
codified deploy.sh declarations, and (optionally) against live AWS.

**Background (alpha-engine-config-I6461, groom-sweep-policy.md §2.9/§4.8).**
`check-schedule-drift.py` already proves *codified deploy.sh rules == live
AWS state*. This script is a SIBLING (not a duplicate — see that script's own
"don't duplicate" gotcha): it proves *schedule-manifest.json == codified
deploy.sh rules*. Composed, the two together prove manifest == live, without
this script re-implementing a single AWS call for the fast path.

Two modes:

  * Default (no ``--live``): AWS-free. Compares the manifest against
    ``check_schedule_drift.discover_codified_rules()`` only. Cheap enough to
    run on every PR that touches the manifest or a deploy.sh.
  * ``--live``: additionally calls ``check_schedule_drift._live_schedule()``
    for every manifest entry, so a manifest entry that is codified but not
    actually live (or live but disabled) is caught directly, not only via the
    separate ``check-schedule-drift.py`` invocation. Reserved for the daily
    fleet-wide sweep (mirrors the scoped-PR/unscoped-daily split
    ``TestDeployAssertionIsScopedToItsOwnDispatcher`` documents for the sibling
    script).

Drift cases (all exit non-zero):
  * ``undeclared-in-manifest`` — a rule deploy.sh codifies has no manifest
    entry. Every dispatch trigger must be named in one place (I6461
    deliverable 1).
  * ``manifest-orphan``        — a manifest entry names a trigger no deploy.sh
    declares (removed cadence, never removed from the manifest).
  * ``cron-mismatch``          — the manifest's cron/rate differs from the
    codified one (the manifest drifted from its own source of truth).
  * ``incomplete-manifest-entry`` — a manifest entry is missing ``reason``,
    ``actuation_tier``, or (when ``actuation_tier == "irreversible"``) a
    non-null ``attended_window``. This is the §2.9 "every dispatch traces to a
    stated reason" + §4.8 "irreversible actions carry a declared window"
    obligation, checked mechanically.
  * ``live-mismatch`` (``--live`` only) — the manifest's cron differs from the
    live AWS schedule, or the live rule is not ENABLED.

Usage:
  ./infrastructure/scheduler/check-manifest-drift.py             # AWS-free
  ./infrastructure/scheduler/check-manifest-drift.py --live      # + live AWS
  ./infrastructure/scheduler/check-manifest-drift.py --json
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent.resolve()
REPO_ROOT = SCRIPT_DIR.parent.parent
MANIFEST_PATH = SCRIPT_DIR / "schedule-manifest.json"
_DRIFT_CHECKER_PATH = SCRIPT_DIR / "check-schedule-drift.py"

_REQUIRED_FIELDS = ("name", "cron", "source_file", "reason", "actuation_tier")
_VALID_TIERS = {"reversible", "irreversible"}

# The manifest today covers ONLY the groom dispatcher's own triggers — the
# issue this script closes (alpha-engine-config-I6461) is scoped to §2.9/§4.8
# as measured against THIS dispatcher's six daily dispatches + reconciler, not
# every deploy.sh in the fleet. Scoping the comparison to the same source file
# mirrors check-schedule-drift.py's own --source-file convention
# (TestDeployAssertionIsScopedToItsOwnDispatcher in test_scheduler_reconcile_
# is_ci_runnable.py) — an unscoped comparison would flag every OTHER
# dispatcher's rules (expense-collector, overseer-liveness, sf-watch-reclaim-
# sweep) as "undeclared-in-manifest", which is not this manifest's job any
# more than the groom deploy's own drift assertion is. A future manifest
# covering another dispatcher passes its own --source-file.
_DEFAULT_SOURCE_FILE = (
    "infrastructure/lambdas/scheduled-groom-dispatcher/deploy.sh"
)


def _load_drift_checker():
    spec = importlib.util.spec_from_file_location(
        "check_schedule_drift", _DRIFT_CHECKER_PATH
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def load_manifest(path: Path = MANIFEST_PATH) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def check(
    live: bool = False,
    manifest_path: Path = MANIFEST_PATH,
    source_file: str | None = _DEFAULT_SOURCE_FILE,
) -> tuple[list[dict], int]:
    checker = _load_drift_checker()
    manifest = load_manifest(manifest_path)
    triggers = manifest.get("triggers", [])

    findings: list[dict] = []

    # ── Field completeness (no AWS needed) ──────────────────────────────────
    for t in triggers:
        missing = [f for f in _REQUIRED_FIELDS if not t.get(f)]
        if missing:
            findings.append({
                "rule": t.get("name", "<unnamed>"), "kind": "incomplete-manifest-entry",
                "detail": f"missing required field(s): {missing}",
            })
            continue
        if t["actuation_tier"] not in _VALID_TIERS:
            findings.append({
                "rule": t["name"], "kind": "incomplete-manifest-entry",
                "detail": f"actuation_tier={t['actuation_tier']!r} not in {_VALID_TIERS}",
            })
        if t["actuation_tier"] == "irreversible" and not t.get("attended_window"):
            findings.append({
                "rule": t["name"], "kind": "incomplete-manifest-entry",
                "detail": "irreversible tier requires a non-null attended_window",
            })

    # ── Manifest vs codified deploy.sh rules (AWS-free) ─────────────────────
    codified_rules, source_errors, _ = checker.discover_codified_rules()
    if source_file:
        codified_rules = [r for r in codified_rules if r["source_file"] == source_file]
        source_errors = [e for e in source_errors if e["source_file"] == source_file]
    if source_errors:
        findings.extend({**e, "kind": "source-error"} for e in source_errors)

    codified_by_name = {r["name"]: r for r in codified_rules}
    manifest_by_name = {t["name"]: t for t in triggers if t.get("name")}

    for name, rule in codified_by_name.items():
        if name not in manifest_by_name:
            findings.append({
                "rule": name, "kind": "undeclared-in-manifest",
                "detail": f"deploy.sh codifies {name} ({rule['expression']}) but "
                          f"schedule-manifest.json has no entry for it",
            })

    for name, t in manifest_by_name.items():
        if name not in codified_by_name:
            findings.append({
                "rule": name, "kind": "manifest-orphan",
                "detail": "schedule-manifest.json names a trigger no deploy.sh codifies",
            })
            continue
        codified_cron = codified_by_name[name]["expression"]
        if t["cron"] != codified_cron:
            findings.append({
                "rule": name, "kind": "cron-mismatch",
                "detail": f"manifest={t['cron']!r} codified={codified_cron!r}",
            })

    # ── Manifest vs live AWS (optional) ─────────────────────────────────────
    if live:
        for name, t in manifest_by_name.items():
            live_rule = checker._live_schedule(name)
            if live_rule is None:
                findings.append({
                    "rule": name, "kind": "live-mismatch",
                    "detail": "manifest entry has no live AWS schedule",
                })
                continue
            if live_rule["expression"] != t["cron"]:
                findings.append({
                    "rule": name, "kind": "live-mismatch",
                    "detail": f"manifest={t['cron']!r} live={live_rule['expression']!r}",
                })
            if live_rule["state"] != "ENABLED":
                findings.append({
                    "rule": name, "kind": "live-mismatch",
                    "detail": f"live state is {live_rule['state']}, expected ENABLED",
                })

    return findings, len(triggers)


def main() -> int:
    ap = argparse.ArgumentParser(description="schedule-manifest.json drift check")
    ap.add_argument("--live", action="store_true",
                     help="also compare each manifest entry against live AWS "
                          "(reserved for the daily fleet-wide sweep)")
    ap.add_argument("--source-file", default=_DEFAULT_SOURCE_FILE,
                     help="scope the codified-rule comparison to this deploy.sh "
                          "(repo-relative path); pass '' to compare fleet-wide")
    ap.add_argument("--json", action="store_true", help="machine-readable output")
    args = ap.parse_args()

    try:
        findings, checked = check(live=args.live, source_file=args.source_file or None)
    except RuntimeError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2

    if args.json:
        print(json.dumps({"checked": checked, "live": args.live, "findings": findings}, indent=2))
    else:
        print(f"manifest drift — {checked} manifest entr{'y' if checked == 1 else 'ies'} "
              f"checked{' (incl. live AWS)' if args.live else ''}")
        if not findings:
            print("  ✓ manifest matches codified deploy.sh declarations"
                  + (" and live AWS state" if args.live else ""))
        for f in findings:
            print(f"  ✗ [{f['kind']}] {f.get('rule', f.get('source_file', '?'))}")
            print(f"      {f['detail']}")

    return 1 if findings else 0


if __name__ == "__main__":
    sys.exit(main())
