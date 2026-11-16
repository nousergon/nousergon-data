#!/usr/bin/env python3
"""Publish the arctic-migration lane's own console row (alpha-engine-config-I7029).

`overseer-arctic-migration` is one of the 9 registered `ec2-spot` components
that rendered `UNREPORTED` on the live console. Its DISPATCHER is separately
registered; the lane — the thing that actually ran the migrations — reported
nothing any console adapter could see.

WHY THIS FILE EXISTS SEPARATELY FROM THE OTHER SIX LANES
--------------------------------------------------------
Five lanes share `alpha-engine-config/infrastructure/overseer_spot_bootstrap.sh`
and get their publish from its `finish()` trap, once, for all of them. This one
does not: `infrastructure/lambdas/arctic-migration-dispatcher/index.py` builds
its own inline SSM command and clones THIS repo, not `alpha-engine-config`, so
that shared script is not on the box. The consolidation is explicitly out of
scope in `playbooks.yaml` ("this Lambda is invoked DIRECTLY... zero routing
decisions for the router to make"), so a second call site is the honest answer
rather than a forced merge of two genuinely different launch paths. It is
tracked as a follow-up.

What is NOT duplicated is the envelope. This uses
`nousergon_lib.fleet_check_result` — the canonical shared builder, already in
this repo's `requirements.txt` and therefore already installed on the box by
the bootstrap's `pip install -r requirements.txt`. One shape, two call sites;
never two shapes.

THE FAILURE PATH WRITES THE SAME TELEMETRY AS THE SUCCESS PATH
--------------------------------------------------------------
Except the completion claim (`observability-policy.md` §3.1). A non-zero
migration runner, an OOM or a spot reclaim publishes `error` with the cause;
nothing here advances any artifact a detector reads as proof of completion.

Usage (on the lane box, from the cloned repo root):
  python scripts/publish_lane_result.py --status error --summary "..." [--dry-run]
"""

from __future__ import annotations

import argparse
import logging
import sys
from typing import Protocol, cast

logger = logging.getLogger(__name__)

COMPONENT_ID = "overseer-arctic-migration"
LABEL = "Overseer lane: arctic-migration"


class _FleetCheckResult(Protocol):
    @staticmethod
    def build(*, check_id: str, label: str, status: str, summary: str,
              cadence_minutes: int | None,
              findings: list[dict[str, object]] | None = None,
              deep_link: str | None = None) -> dict[str, object]: ...

    @staticmethod
    def emit(envelope: dict[str, object], *, dry_run: bool = False) -> str | None: ...


class _Args(Protocol):
    status: str
    summary: str
    deep_link: str | None
    dry_run: bool


def build(status: str, summary: str,
          findings: list[dict[str, object]] | None = None,
          deep_link: str | None = None) -> dict[str, object]:
    from nousergon_lib import fleet_check_result as fcr
    typed_fcr = cast(_FleetCheckResult, fcr)  # Third-party annotations use bare dict.

    env = typed_fcr.build(
        check_id=COMPONENT_ID, label=LABEL, status=status, summary=summary,
        # This lane is event-driven ("merge-triggered; no scheduled wake" in
        # playbooks.yaml), so it has NO cadence to declare. The shared builder
        # rejects a non-positive cadence — correctly, for a scheduled check —
        # so the field is nulled immediately after. The console reads null as
        # "no freshness input, do not compute staleness"
        # (nousergon-console console/adapters/checks_envelope.py
        # ::_component_state), which is exactly right: an unauditable row is
        # honest, a fabricated cadence is not (alpha-engine-config-I7020).
        cadence_minutes=1,
        findings=findings, deep_link=deep_link,
    )
    env["cadence_minutes"] = None
    return env


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--status", required=True,
                    choices=["ok", "attention", "error"])
    ap.add_argument("--summary", required=True)
    ap.add_argument("--deep-link", default=None)
    ap.add_argument("--dry-run", action="store_true")
    args = cast(_Args, ap.parse_args(argv))  # argparse constructs attributes dynamically.
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    try:
        from nousergon_lib import fleet_check_result as fcr
        typed_fcr = cast(_FleetCheckResult, fcr)  # Third-party annotations use bare dict.
        env = build(args.status, args.summary, deep_link=args.deep_link)
        uri = typed_fcr.emit(env, dry_run=args.dry_run)
    except Exception:  # noqa: BLE001
        # A publish failure must not fail the lane: the lane's job is the
        # migration, and a successful run turned red by its own telemetry is a
        # worse outcome than a row the console renders as unreadable — which it
        # does, never as `ok`. This is the one sanctioned swallow here and it
        # is loud in the run log, which ships to S3 on every exit path.
        logger.warning("could not publish the %s console row", COMPONENT_ID,
                       exc_info=True)
        return 0
    logger.info("%s %s -> %s — %s", args.status, COMPONENT_ID,
                uri or "(not published)", args.summary)
    return 0


if __name__ == "__main__":
    sys.exit(main())
