#!/usr/bin/env python3
"""trigger_surface_drift.py — does the AWS trigger surface say what playbooks.yaml says?

**alpha-engine-config-I9045.** ``playbooks.yaml`` declares, per playbook, every
leg that can wake it. Live AWS carried none of that. Measured 2026-08-28: all
four ``alpha-engine-alert-drain-*utc`` Scheduler schedules were ``DISABLED``, the
drain ran daily anyway off ``alpha-engine-freshness-monitor-cron``'s event-time
leg, every one of the four schedules had ``Description: null``, and the rule
doing the work said only *"Daily 12:00 UTC probe of the artifact freshness
registry"*. Anyone reading the AWS surface alone — an operator, an incident
responder, a cost audit, a conformance sweep — got the wrong answer, and the
correction existed only as a YAML comment in a private repo.

``infrastructure/lambdas/*/deploy.sh --reconcile-triggers`` now stamps a marker
derived from ``playbooks.yaml`` onto each resource it owns. **This is the check
that the stamp is still there and still true.** Without it the tags are a
one-time hand-wave that drifts the first time a leg is added, renamed or
retimed — the same class of defect as the one being fixed.

Findings (any of them exits 1):

  ``marker-missing``      A resource a ``deploy.sh`` declares it reconciles has
                          no ``[wakes: ...]`` marker live. Either the deploy has
                          not run since the marker landed, or something
                          overwrote the description.
  ``marker-drift``        A marker is present and DISAGREES with what
                          ``playbooks.yaml`` now implies — a wake leg was added
                          or removed and the surface still claims the old set.
  ``trigger-absent``      A reconciled resource does not exist live at all.
  ``undeclared-trigger``  A live EventBridge Scheduler schedule targets the
                          Overseer router with a ``playbook`` in its Input, and
                          ``playbooks.yaml`` does not list it under that
                          playbook's ``triggers:``. This one needs no coverage
                          gate and it is the reverse-direction catch: it is what
                          finds a wake path that exists on AWS and nowhere else.
                          It fired on the real thing — ``deploy.sh`` codified
                          only the ``1000``/``2200`` slots while ``0400`` and
                          ``1600`` existed live, so config#2902's zero-retry fix
                          reached two of four and nothing noticed for a year.

Reported but NOT red:

  ``marker-pending``      A resource ``playbooks.yaml`` implies that no
                          ``deploy.sh`` has opted into reconciling yet. Counted
                          and named on every run so the remaining coverage is a
                          number on a surface rather than an unstated backlog
                          (``alpha-engine-config-I9072``). It becomes graded the
                          moment its owning deploy script adds it to
                          ``RECONCILE_DESCRIPTION_TRIGGERS`` — coverage is
                          discovered by scanning the deploy scripts, so there is
                          no allowlist here that could be widened to hide one.

**Exit codes** follow the fleet convention (``check_port_registry_drift.py``):

  ``0``  every graded resource agrees with the declaration
  ``1``  drift — at least one finding above
  ``2``  **could not measure** — no AWS credentials, no ``aws`` CLI, or an
         AccessDenied. Distinct from ``0`` on purpose: a check that cannot see
         its subject has NOT passed, and reporting "no drift" because it lost
         its own access is how a detector grades itself green. ``--allow-unmeasured``
         downgrades this to 0 for a PR-context run where credentials are not
         expected; it never downgrades a real finding.

Usage:
  ./infrastructure/overseer/trigger_surface_drift.py            # everything
  ./infrastructure/overseer/trigger_surface_drift.py --json
  ./infrastructure/overseer/trigger_surface_drift.py --allow-unmeasured
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from pathlib import Path
from typing import Callable, NotRequired, TypedDict, cast

HERE = Path(__file__).parent.resolve()
REPO_ROOT = HERE.parent.parent
LAMBDAS_DIR = REPO_ROOT / "infrastructure" / "lambdas"

sys.path.insert(0, str(HERE))
import trigger_descriptions as td  # noqa: E402  (path must be set first)

#: The Lambda every router-targeting Scheduler schedule points at. A schedule
#: whose target is this function and whose Input names a playbook IS a wake leg,
#: whatever the repo does or does not say about it.
ROUTER_FUNCTION = "alpha-engine-overseer-dispatcher"

#: The bash array a ``deploy.sh`` declares to opt its resources into grading.
#: Discovery by scan rather than by registry, mirroring
#: ``infrastructure/scheduler/check-schedule-drift.py`` and
#: ``infrastructure/eventbridge/check-drift.py``: a deploy script that starts
#: reconciling a resource is covered here the moment it lands, with nothing to
#: remember to update.
_COVERAGE_ARRAY_RE = re.compile(
    r"^\s*RECONCILE_DESCRIPTION_TRIGGERS=\(\n(.*?)\n\s*\)", re.DOTALL | re.MULTILINE
)
_ENTRY_RE = re.compile(r'^\s*"([^"]+)"\s*$', re.MULTILINE)

#: The marker's leading token. Present => the resource has been stamped.
_MARKER_RE = re.compile(r"\[wakes:[^\]]*\](?:\s*\[sibling-legs:[^\]]*\])?")


class CouldNotMeasure(RuntimeError):
    """The live comparison could not be performed at all."""


class RouterSchedule(TypedDict):
    """The fields used from a router-targeting Scheduler schedule."""

    name: str
    playbook: str


class ResourceEntry(TypedDict):
    """The portion of ``trigger_descriptions.resource_index`` used here."""

    wakes: set[str]
    kinds: set[str]


class Finding(TypedDict):
    """A drift or pending row emitted by :func:`check`."""

    resource: str
    kind: str
    detail: str
    source_file: NotRequired[str]
    expected: NotRequired[str]
    actual: NotRequired[str]


Registry = dict[str, object]
Describe = Callable[[str, str], str | None]
ListRouterSchedules = Callable[[], list[RouterSchedule]]

# trigger_descriptions predates the strict typing rollout. Keep its untyped
# internals behind this module's precisely typed boundary rather than allowing
# Unknown to spread through the checker.
_load_registry = cast(Callable[[], Registry], td.load_registry)
_resource_index = cast(Callable[[Registry], dict[str, ResourceEntry]], td.resource_index)
_marker_for = cast(Callable[[Registry, str, str], str], td.marker_for)


# ── discovery ────────────────────────────────────────────────────────────────

def discover_reconciled_triggers(lambdas_dir: Path = LAMBDAS_DIR) -> dict[str, str]:
    """``{"scheduler:name": "infrastructure/lambdas/x/deploy.sh"}``."""
    covered: dict[str, str] = {}
    for deploy in sorted(lambdas_dir.glob("*/deploy.sh")):
        match = _COVERAGE_ARRAY_RE.search(deploy.read_text(encoding="utf-8"))
        if not match:
            continue
        rel = str(deploy.relative_to(REPO_ROOT))
        for entry in _ENTRY_RE.findall(match.group(1)):
            covered[entry] = rel
    return covered


# ── live reads ───────────────────────────────────────────────────────────────

def _aws(args: list[str]) -> tuple[int, str, str]:
    try:
        proc = subprocess.run(
            ["aws"] + args, capture_output=True, text=True, check=False
        )
    except FileNotFoundError as exc:  # no aws CLI on this machine
        raise CouldNotMeasure("the `aws` CLI is not installed") from exc
    return proc.returncode, proc.stdout.strip(), proc.stderr.strip()


def _raise_if_unmeasurable(err: str, what: str) -> None:
    """Separate 'cannot see' from 'saw, and it is wrong'.

    A credential or permission failure is NOT absence and must never be graded
    as one. The same posture ``check-schedule-drift.py`` takes, and the reason
    this module has a third exit code at all.
    """
    for token in (
        "AccessDenied", "UnauthorizedOperation", "ExpiredToken",
        "InvalidClientTokenId", "Unable to locate credentials",
        "You must specify a region", "SignatureDoesNotMatch",
        "NoCredentialProviders", "could not be found",
    ):
        if token in err:
            raise CouldNotMeasure(f"{what}: {err}")


def live_description(surface: str, name: str) -> str | None:
    """The live ``Description``, ``""`` when unset, or ``None`` when absent."""
    if surface == "scheduler":
        rc, out, err = _aws([
            "scheduler", "get-schedule", "--name", name,
            "--query", "Description", "--output", "text",
        ])
        not_found = "ResourceNotFoundException"
    else:
        rc, out, err = _aws([
            "events", "describe-rule", "--name", name,
            "--query", "Description", "--output", "text",
        ])
        not_found = "ResourceNotFoundException"
    if rc != 0:
        _raise_if_unmeasurable(err, f"reading {surface}:{name}")
        if not_found in err:
            return None
        raise CouldNotMeasure(f"reading {surface}:{name}: {err}")
    # `--output text` renders a null Description as the literal "None".
    return "" if out in ("", "None") else out


def live_router_schedules() -> list[RouterSchedule]:
    """Every live Scheduler schedule that targets the Overseer router.

    ``list-schedules`` does not return Target details, so each candidate needs a
    ``get-schedule``. Scoped by the ``alpha-engine-`` name prefix, which is the
    fleet's naming rule and what ``check-schedule-drift.py`` also scopes on.
    """
    rc, out, err = _aws([
        "scheduler", "list-schedules", "--name-prefix", "alpha-engine-",
        "--query", "Schedules[].Name", "--output", "text",
    ])
    if rc != 0:
        _raise_if_unmeasurable(err, "listing schedules")
        raise CouldNotMeasure(f"listing schedules: {err}")
    found: list[RouterSchedule] = []
    for name in (out.split() if out else []):
        rc, blob, err = _aws([
            "scheduler", "get-schedule", "--name", name,
            "--query", "{Arn:Target.Arn,Input:Target.Input}", "--output", "json",
        ])
        if rc != 0:
            _raise_if_unmeasurable(err, f"reading scheduler:{name}")
            continue
        try:
            decoded = cast(object, json.loads(blob))
        except json.JSONDecodeError:
            continue
        if not isinstance(decoded, dict):
            continue
        target = cast(dict[str, object], decoded)
        arn = target.get("Arn")
        if not isinstance(arn, str) or ROUTER_FUNCTION not in arn:
            continue
        try:
            raw_input = target.get("Input")
            payload = cast(
                object, json.loads(raw_input if isinstance(raw_input, str) else "{}")
            )
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            playbook = cast(dict[str, object], payload).get("playbook")
        else:
            playbook = None
        if isinstance(playbook, str) and playbook:
            found.append({"name": name, "playbook": playbook})
    return found


# ── the check ────────────────────────────────────────────────────────────────

def extract_marker(description: str | None) -> str | None:
    if not description:
        return None
    match = _MARKER_RE.search(description)
    return match.group(0) if match else None


def check(
    registry: Registry | None = None,
    covered: dict[str, str] | None = None,
    describe: Describe = live_description,
    list_router_schedules: ListRouterSchedules = live_router_schedules,
    source_file: str | None = None,
) -> tuple[list[Finding], list[Finding]]:
    """``(findings, pending)``. Every collaborator is injected so the whole
    verdict surface is testable without AWS — including, deliberately, the
    failure paths, because a check nobody has watched fail is not a check."""
    reg = registry if registry is not None else _load_registry()
    cov = covered if covered is not None else discover_reconciled_triggers()
    index = _resource_index(reg)

    # `--source-file` scopes the run to one deploy script's own resources, and
    # exists to remove a RACE rather than to narrow coverage. Each deploy
    # workflow asserts immediately after its own reconcile; unscoped, the
    # alert-drain job would also grade the freshness rules, which a CONCURRENT
    # job is reconciling in the same merge — a red caused by scheduling, not by
    # drift. The daily out-of-band run is unscoped and covers everything,
    # including the reverse sweep this mode skips (a live wake path nobody
    # declared belongs to no single deploy script).
    if source_file:
        cov = {k: v for k, v in cov.items() if v == source_file}
        if not cov:
            raise CouldNotMeasure(
                f"no deploy script at {source_file!r} declares "
                "RECONCILE_DESCRIPTION_TRIGGERS — nothing to grade, which is not a pass"
            )
        index = {k: v for k, v in index.items() if k in cov}

    findings: list[Finding] = []
    pending: list[Finding] = []

    for key in sorted(index):
        if key not in cov:
            pending.append({
                "resource": key,
                "kind": "marker-pending",
                "detail": (
                    f"{key} is implied by playbooks.yaml but no deploy.sh lists it in "
                    "RECONCILE_DESCRIPTION_TRIGGERS, so its AWS description is not "
                    "reconciled and cannot be graded (alpha-engine-config-I9072)."
                ),
            })
            continue

        surface, name = key.split(":", 1)
        expected = _marker_for(reg, surface, name)
        description = describe(surface, name)

        if description is None:
            findings.append({
                "resource": key, "kind": "trigger-absent", "source_file": cov[key],
                "detail": (
                    f"{key} is reconciled by {cov[key]} and declared in playbooks.yaml, "
                    "but does not exist live. Nothing can wake it."
                ),
            })
            continue

        actual = extract_marker(description)
        if actual is None:
            findings.append({
                "resource": key, "kind": "marker-missing", "source_file": cov[key],
                "expected": expected,
                "detail": (
                    f"{key} carries no [wakes: ...] marker live. Its wake linkage is "
                    "invisible to anyone reading AWS alone — the exact I9045 condition. "
                    f"Run `bash {cov[key]} --reconcile-triggers`, or let the merge that "
                    "changed it deploy."
                ),
            })
        elif actual != expected:
            findings.append({
                "resource": key, "kind": "marker-drift", "source_file": cov[key],
                "expected": expected, "actual": actual,
                "detail": (
                    f"{key}'s live marker no longer matches playbooks.yaml. The wake "
                    "declaration changed and the AWS surface still asserts the old one."
                ),
            })

    # Reverse direction — a live wake path the registry has never heard of.
    # Skipped under --source-file: an undeclared trigger belongs to no deploy
    # script, so a scoped run cannot own the finding and would report it on
    # whichever job happened to be scoped.
    if source_file:
        return findings, pending

    declared_scheduler = {
        key.split(":", 1)[1]: entry for key, entry in index.items()
        if key.startswith("scheduler:")
    }
    for live in list_router_schedules():
        entry = declared_scheduler.get(live["name"])
        if entry is None:
            findings.append({
                "resource": f"scheduler:{live['name']}",
                "kind": "undeclared-trigger",
                "detail": (
                    f"scheduler:{live['name']} dispatches playbook "
                    f"'{live['playbook']}' through {ROUTER_FUNCTION}, and no playbook in "
                    "playbooks.yaml declares it. It is a wake path that exists on AWS "
                    "and nowhere else."
                ),
            })
        elif live["playbook"] not in entry["wakes"]:
            findings.append({
                "resource": f"scheduler:{live['name']}",
                "kind": "undeclared-trigger",
                "detail": (
                    f"scheduler:{live['name']} dispatches playbook '{live['playbook']}' "
                    f"live, but playbooks.yaml declares it only for "
                    f"{sorted(entry['wakes'])}."
                ),
            })

    return findings, pending


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    p.add_argument("--json", action="store_true")
    p.add_argument(
        "--source-file",
        help="grade only the resources this deploy.sh reconciles, e.g. "
             "infrastructure/lambdas/alert-drain-dispatcher/deploy.sh",
    )
    p.add_argument(
        "--allow-unmeasured", action="store_true",
        help="exit 0 instead of 2 when the live comparison cannot be performed",
    )
    args = p.parse_args(argv)

    try:
        findings, pending = check(source_file=args.source_file)
    except CouldNotMeasure as exc:
        payload = {"status": "could_not_measure", "detail": str(exc)}
        print(json.dumps(payload, indent=2) if args.json
              else f"COULD NOT MEASURE: {exc}", file=sys.stderr)
        return 0 if args.allow_unmeasured else 2

    if args.json:
        print(json.dumps({
            "status": "drift" if findings else "ok",
            "findings": findings,
            "pending": pending,
        }, indent=2))
    else:
        for row in pending:
            print(f"  · [{row['kind']}] {row['resource']}")
        if findings:
            print(f"\n{len(findings)} finding(s):")
            for row in findings:
                print(f"  ✗ [{row['kind']}] {row['resource']}")
                print(f"      {row['detail']}")
                if "expected" in row:
                    print(f"      expected: {row['expected']}")
                if "actual" in row:
                    print(f"      actual:   {row['actual']}")
        else:
            print(
                f"\n  ✓ every reconciled trigger's AWS description matches "
                f"playbooks.yaml ({len(pending)} not yet reconciled)"
            )
    return 1 if findings else 0


if __name__ == "__main__":
    sys.exit(main())
