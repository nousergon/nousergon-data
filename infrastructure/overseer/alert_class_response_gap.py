#!/usr/bin/env python3
"""
Overseer invariant-4 detector: a class in ``alert_classes:`` (playbooks.yaml)
that can terminate in a page and carries neither ``operator_reason`` nor
``migration_issue`` (alpha-engine-config-I8991, overseer-policy.md invariant
4 — "closed loop or tracked gap ... there is no third state").

This is a DIFFERENT axis from the existing schema-level requirement in
``playbooks.schema.json`` (``intake: none`` rows must carry one of the two
fields). That rule only covers drain-blind classes — ones the Overseer never
sees at all. Most classes ARE seen (``intake: bus``/``cw-alarm``) and are
routed to the alert-drain's T0-T3 tiers (``response: drain-queue``) or
declared operator-only (``response: operator``); either can still terminate
in an unactioned page, because ``severity`` is the push switch
(``api/services/alerting.py``: error/critical push the phone, everything
else is silent-in-channel) and neither ``response`` value guarantees a
disposition beyond "a human sees it."

A class is PAGEABLE here when:
  - its ``severities`` set intersects {error, critical, dynamic} (dynamic
    classes choose their severity at emit time and MAY reach error/critical,
    so they are treated as pageable rather than assumed safe), AND
  - its ``response`` is ``drain-queue`` or ``operator`` — NOT
    ``playbook:<name>``, which is already routed to an automated T2 agent
    and so is not in the third state this detector is for.

Every pageable class is checked unconditionally. The backlog found by the
alpha-engine-config-I8991 sweep was dispositioned under
alpha-engine-config-I8996.

Usage:
  python infrastructure/overseer/alert_class_response_gap.py \\
    --playbooks infrastructure/overseer/playbooks.yaml
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import cast

import yaml

# Empty compatibility marker for the completed I8996 backlog.
GRANDFATHERED_CLASSES: frozenset[str] = frozenset()

_PAGEABLE_SEVERITIES = frozenset({"error", "critical", "dynamic"})
_PAGEABLE_RESPONSES = frozenset({"drain-queue", "operator"})


def is_pageable(alert_class: dict) -> bool:
    """True when this row's severities/response combination can reach a
    human page with no automated disposition guaranteed."""
    severities = set(alert_class.get("severities", []))
    response = alert_class.get("response", "")
    return bool(severities & _PAGEABLE_SEVERITIES) and response in _PAGEABLE_RESPONSES


def has_disposition_field(alert_class: dict) -> bool:
    return "operator_reason" in alert_class or "migration_issue" in alert_class


def find_new_gaps(alert_classes: list[dict]) -> list[str]:
    """Classes that are pageable and lack both disposition fields."""
    return sorted(
        cast(str, c["class"])
        for c in alert_classes
        if is_pageable(c)
        and not has_disposition_field(c)
    )


def _load_alert_classes(playbooks_path: Path) -> list[dict]:
    data = yaml.safe_load(playbooks_path.read_text())
    return data.get("alert_classes", [])


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Detect alert_classes rows in the third state overseer-policy.md "
        "invariant 4 forbids: pageable, with neither operator_reason nor migration_issue."
    )
    parser.add_argument(
        "--playbooks",
        type=Path,
        default=Path(__file__).resolve().parent / "playbooks.yaml",
        help="Path to playbooks.yaml (default: sibling file).",
    )
    args = parser.parse_args(argv)

    alert_classes = _load_alert_classes(args.playbooks)

    new_gaps = find_new_gaps(alert_classes)
    ok = True
    if new_gaps:
        ok = False
        print(
            "overseer-policy.md invariant 4 violated — pageable alert_classes row(s) "
            "with neither operator_reason nor migration_issue (alpha-engine-config-I8991):",
            file=sys.stderr,
        )
        for name in new_gaps:
            print(f"  - {name}", file=sys.stderr)
    if ok:
        print(f"alert_class_response_gap: OK ({len(alert_classes)} classes checked)")
        return 0
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
