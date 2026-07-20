"""Guard the krepis floor on the fleet's primary alerting Lambdas.

krepis 0.15.0 (tagged 2026-07-17T15:52 UTC) added ``fleet_events`` + the
overseer-bus chokepoint emission. Every Lambda's ``requirements.txt`` had
only pinned a much older floor (``krepis>=0.10.2`` etc.), so nothing forced
a rebuild when 0.15.0 shipped — four of the fleet's primary alerting
Lambdas silently kept shipping pre-fleet_events krepis and contributed ZERO
events to the overseer bus (alpha-engine-config#2907):

  - alpha-engine-sf-telegram-notifier   — krepis 0.10.3 (deployed 7/05)
  - alpha-engine-pipeline-watchdog      — krepis 0.10.2 (7/04)
  - alpha-engine-saturday-integrity-sentinel — krepis 0.10.2 (7/04)
  - alpha-engine-freshness-monitor      — krepis 0.14.0 (7/16, one short)

This test re-checks the floor on every CI run so a future requirements.txt
edit that silently drops the floor again fails here, not in a fleet-events
coverage audit months later.

Scope note: this covers exactly the four Lambdas config#2907 found broken,
not every fleet Lambda that happens to depend on krepis. Several other
flow-doctor-extra Lambdas (ci-watch-dispatcher, sf-watch-liveness-probe,
saturday-sf-watch-dispatcher, overseer-liveness-probe, sf-watch-spot-dispatcher)
were confirmed by config#2907's own deployed-zip audit to already be running
krepis>=0.15.0 in production despite a lower requirements.txt floor — their
actual currently-deployed dist-info is fine, but their floor pins were not
re-verified/bumped as part of this fix (some, like overseer-liveness-probe,
carry their own separately-tracked follow-up: alpha-engine-config-I2846).
Extending this guard fleet-wide is a natural fast-follow, not this issue's
scope — add lambdas to ``_KREPIS_FLOOR_REQUIRED`` as they're brought in.
"""

from __future__ import annotations

import re
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
_LAMBDAS_DIR = _REPO_ROOT / "infrastructure" / "lambdas"

# First krepis version emitting fleet_events + overseer-bus chokepoint events.
KREPIS_FLEET_EVENTS_MIN_VERSION = "0.15.0"

_KREPIS_PIN_RE = re.compile(r"^krepis>=([0-9]+\.[0-9]+\.[0-9]+)\s*$", re.MULTILINE)

# Lambdas required to carry a krepis floor >= KREPIS_FLEET_EVENTS_MIN_VERSION.
# Opt-in list (not "every lambda in the directory") — see module docstring's
# scope note for why the rest of the flow-doctor group isn't enforced here yet.
_KREPIS_FLOOR_REQUIRED = (
    "sf-telegram-notifier",
    "pipeline-watchdog",
    "saturday-integrity-sentinel",
    "freshness-monitor",
)


def _parsed_version(v: str) -> tuple[int, int, int]:
    major, minor, patch = v.split(".")
    return (int(major), int(minor), int(patch))


def test_alerting_lambdas_meet_krepis_fleet_events_floor():
    min_version = _parsed_version(KREPIS_FLEET_EVENTS_MIN_VERSION)
    failures = []

    for lambda_name in _KREPIS_FLOOR_REQUIRED:
        req_file = _LAMBDAS_DIR / lambda_name / "requirements.txt"
        assert req_file.exists(), f"{lambda_name}: requirements.txt not found at {req_file}"

        text = req_file.read_text()
        match = _KREPIS_PIN_RE.search(text)
        if match is None:
            failures.append(f"{lambda_name}: no `krepis>=X.Y.Z` line found in requirements.txt")
            continue

        pin = _parsed_version(match.group(1))
        if pin < min_version:
            failures.append(
                f"{lambda_name}: krepis floor {match.group(1)} is below the "
                f"fleet_events floor {KREPIS_FLEET_EVENTS_MIN_VERSION} — bump it, "
                f"or move this Lambda out of _KREPIS_FLOOR_REQUIRED with a stated reason "
                f"if it's deliberately exempt (e.g. tracked under a separate issue)."
            )

    assert not failures, "krepis fleet_events floor violations:\n" + "\n".join(failures)
