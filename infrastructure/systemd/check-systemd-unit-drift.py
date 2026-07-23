#!/usr/bin/env python3
"""check-systemd-unit-drift.py — Diff installed systemd units against the
repo copies in `infrastructure/systemd/` (config#2352).

**Background.** `daily-news.{service,timer}` and `metron-intraday.{service,
timer}` are version-tracked but only ever reach `/etc/systemd/system/` via a
manual (or, after config#2352's deploy-on-merge workflows/boot-pull sync)
install-script run. Both delivery paths can still drift from the repo in
practice:

  * daily-news: the deploy-on-merge workflow (deploy-daily-news-units.yml)
    could fail silently, or someone could hand-edit the on-box unit file
    without touching the repo.
  * metron-intraday: moved from ae-trading to ae-dashboard
    (alpha-engine-config#1768 Phase 1, 2026-07-21) — now relies on the
    (always-on) dashboard box picking up the repo copy at its next
    boot-pull run, same "queue on merge, apply on next boot" convergence
    model as the trading box used before the move (2026-07-13 operator
    ruling on config#2352). A box that stays up unusually long, or a
    boot-pull systemd-sync regression, could leave it stale for longer
    than the "pages within a day" acceptance bar.

This script is a same-box, read-only comparison — it runs AS a systemd timer
ON the box that owns the units (no cross-box SSM reach, no new IAM grant:
see the config#2352 PR description for why the alternative, a GHA-side SSM
hash-pull, would have needed a new SendCommand grant on the trading-box
instance that github-actions-lambda-deploy does not currently have).
Reports divergence via flow-doctor, mirroring every other on-box self-report
in this repo (e.g. scripts/run_daily_news_standalone.sh's fail-loud git
sync). Sibling in shape (repo-vs-live diff, exit 0/1/2) to
../step-functions/check-drift.py and check-definition-drift.py, but LOCAL
file compare instead of an AWS API call — there is no "live AWS state" for
a systemd unit, only "what's on this box's disk".

Usage:
  ./infrastructure/systemd/check-systemd-unit-drift.py               # every unit this box installs
  ./infrastructure/systemd/check-systemd-unit-drift.py --unit NAME   # one unit (e.g. daily-news.timer)
  ./infrastructure/systemd/check-systemd-unit-drift.py --report      # on divergence, also flow-doctor report

Exit codes: 0 clean, 1 drift found, 2 config/source error.
"""

from __future__ import annotations

import argparse
import hashlib
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent.resolve()
REPO_ROOT = SCRIPT_DIR.parent.parent
INSTALLED_DIR = Path("/etc/systemd/system")

# Which units THIS box is expected to have installed. Both daily-news and
# metron-intraday units live in the repo, but a given box only ever installs
# the ones relevant to it (dashboard box: both daily-news AND, as of
# alpha-engine-config#1768 Phase 1, metron-intraday too — trading no longer
# hosts it) — install-daily-news.sh / install-metron-intraday.sh are
# each other's only callers of the corresponding pair. This script probes
# whichever of the two pairs is ALREADY present on disk (a box that has
# never installed a unit is not "drifted", it's simply not that unit's
# host) rather than hardcoding a per-box unit list here.
ALL_UNITS: tuple[str, ...] = (
    "daily-news.service",
    "daily-news.timer",
    "metron-intraday.service",
    "metron-intraday.timer",
)


def _sha256(path: Path) -> str | None:
    try:
        return hashlib.sha256(path.read_bytes()).hexdigest()
    except FileNotFoundError:
        return None


def check_unit(name: str) -> tuple[str, str]:
    """Returns (status, detail). status in {"clean", "drift", "not-installed", "source-error"}."""
    repo_path = SCRIPT_DIR / name
    installed_path = INSTALLED_DIR / name

    if not repo_path.is_file():
        return "source-error", f"{name}: no repo copy at {repo_path}"

    installed_hash = _sha256(installed_path)
    if installed_hash is None:
        return "not-installed", f"{name}: not present on this box ({installed_path})"

    repo_hash = _sha256(repo_path)
    if installed_hash != repo_hash:
        return "drift", f"{name}: installed ({installed_hash[:12]}) != repo ({repo_hash[:12]})"

    return "clean", f"{name}: OK"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--unit", help="check a single unit by filename (e.g. daily-news.timer)")
    parser.add_argument(
        "--report",
        action="store_true",
        help="on drift, also flow-doctor report (in addition to the exit code + stdout)",
    )
    args = parser.parse_args()

    units = [args.unit] if args.unit else list(ALL_UNITS)

    findings = []
    saw_installed = False
    exit_code = 0
    for name in units:
        status, detail = check_unit(name)
        print(f"[{status}] {detail}")
        if status == "source-error":
            exit_code = max(exit_code, 2)
        elif status == "drift":
            saw_installed = True
            findings.append(detail)
            exit_code = max(exit_code, 1)
        elif status == "clean":
            saw_installed = True

    if not saw_installed and exit_code == 0:
        # Every probed unit was "not-installed" — this box hosts neither
        # pair. Not an error (a box legitimately hosting neither unit
        # would otherwise always fail); the daily install scripts are the
        # enforcement point for "should this unit exist here at all".
        print("No tracked units installed on this box — nothing to compare.")
        return 0

    if findings:
        print(f"DRIFT: {len(findings)} unit(s) diverged from repo.", file=sys.stderr)
        if args.report:
            _flow_doctor_report(findings)
    else:
        print("Systemd unit drift check PASSED (installed units match repo).")

    return exit_code


def _flow_doctor_report(findings: list[str]) -> None:
    try:
        sys.path.insert(0, str(REPO_ROOT))
        import flow_doctor

        fd = flow_doctor.init(config_path=str(REPO_ROOT / "flow-doctor.yaml"))
        fd.report(
            RuntimeError(f"systemd unit drift: {'; '.join(findings)}"),
            severity="error",
            context={"site": "check-systemd-unit-drift", "findings": findings},
        )
    except Exception as e:  # pragma: no cover - best-effort side channel
        print(f"[check-systemd-unit-drift] flow-doctor report failed: {e}", file=sys.stderr)


if __name__ == "__main__":
    sys.exit(main())
