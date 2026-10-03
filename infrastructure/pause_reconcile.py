#!/usr/bin/env python3
"""pause_reconcile.py — reconcile three registers that each claim a trigger is off.

**The gap this closes (alpha-engine-config-I7118).**

Three artifacts make claims about whether a scheduled trigger is running, and
until this module nothing compared them:

  1. ``infrastructure/automation_pause.json`` — Brian's rulings. Names what is
     deliberately off (``paused``/``pending``) and what is deliberately on
     (``not_paused``).
  2. The fleet observability registry, ``nous-ergon-ops/governance/
     observability.d/*.yaml``, read here from the copy
     ``observability-registry-publish.yml`` syncs to
     ``s3://alpha-engine-research/ops/registry/observability.d/`` on every merge
     that changes it. Each row declares a ``lifecycle`` — ``in-service`` or one
     of the three non-service states (``disabled``/``deprecated``/``retired``),
     which ``observability-policy.md`` §8.3 requires be DECLARED, never inferred.
  3. Live AWS — the actual ``State`` of every EventBridge rule and EventBridge
     Scheduler schedule in the account.

``automation_pause.py --check`` already reconciles (1) against (3), but only for
the names (1) happens to list: its coverage is **hand-listed**, which is the
``observability-policy.md`` §2.2 defect one level up from the one it fixes. And
nothing at all reconciled (2) against (1): a registry row saying ``lifecycle:
disabled`` because a manifest entry said so is a hand-copied assertion about a
file in a different repository, and when the manifest entry is deleted the row
keeps saying "deliberately off" while the component runs again. A component
quietly running while its row says it is off is WORSE than the ``UNREPORTED``
state the declaration replaced, because ``UNREPORTED`` is loud and ``DISABLED``
is not.

**The join, stated once.** A trigger's off-ness is DECLARED if either register
declares it — the pause manifest naming it, or its observability row carrying a
non-service ``lifecycle``. The union is the declaration surface, and it is
derived from the account rather than from either list, so a trigger nobody named
in either place is a finding by construction instead of an omission nobody sees.

**Where this lives, and why (I7118 deliverable 3).** The two files are in
different repos, so one of them has to be read across a boundary. The options
were an unauthenticated raw-GitHub fetch of a private repo (rejected: it cannot
work without a credential, and adding one to reach a file we already publish is
a second copy of an existing path), a cross-repo source checkout under a scoped
deploy key or a GitHub App token (rejected: a new credential and a new IAM
identity to read something already in S3), or reading the registry from the
place the console already reads it. The last one costs nothing: the registry is
pushed to S3 by the same merge that changes it, so this copy is never staler
than the last registry change, and this module needs no second identity and no
second schedule. The manifest stays in the repo that owns it and this module
sits beside it.

**This module never acts.** It reads three registers and prints a verdict. It
cannot enable, disable, edit a manifest entry or edit a lifecycle declaration.
Deleting a ``lifecycle: disabled`` declaration because a manifest entry vanished
would INFER a decision from a file diff, which is the thing §8.3 forbids and the
reason I7118 specifies detect-only.

**Three exit codes, and the distinction is the point (alpha-engine-config-I7547).**
``0`` clean, ``1`` findings, ``2`` the detector could not run. A detect-only job
that exits 1 on every real finding renders in the GitHub Actions list exactly
like a job whose credentials expired, and four consecutive ``failure`` runs read
as a broken job — which is why this correct detector went unread for four days
running while it reported a champion/challenger arm missing. So the verdict is
published where it can be read WITHOUT opening the log: a rendered run summary,
a headline naming the finding count that the workflow uses as its failing job's
title, and a console row that now distinguishes ``error`` (this check broke)
from ``attention`` (this check works and found something). The exit code is
untouched — neutering it would remove the signal instead of making it legible.

Usage:
  ./infrastructure/pause_reconcile.py --check          # exit 1 on any finding
  ./infrastructure/pause_reconcile.py --check --json
  ./infrastructure/pause_reconcile.py --check --publish # + the console row
  ./infrastructure/pause_reconcile.py --check --markdown "$GITHUB_STEP_SUMMARY" \
      --github-output "$GITHUB_OUTPUT"                 # + the readable verdict
"""

from __future__ import annotations

import argparse
import json
import logging
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

import boto3
import yaml

HERE = Path(__file__).parent.resolve()
sys.path.insert(0, str(HERE))
import automation_pause as ap  # noqa: E402

REGION = ap.REGION
REGISTRY_BUCKET = "alpha-engine-research"
REGISTRY_PREFIX = "ops/registry/observability.d/"

#: `observability_registry.py::LIFECYCLE_NEEDS_REASON` — the three states that
#: declare a component is not expected to run. Duplicated as a literal rather
#: than imported because that module lives in another repository; the guard is
#: `tests/test_pause_reconcile.py::test_non_service_lifecycles_match_the_registry`,
#: which reads the published rows and fails if a fourth state appears.
NON_SERVICE_LIFECYCLES = frozenset({"disabled", "deprecated", "retired"})

#: Component ids that are never joinable to a trigger name. AWS-managed rules
#: carry no fleet row and never will.
_AWS_MANAGED_PREFIX = "DO-NOT-DELETE-"

#: EventBridge Scheduler puts every schedule in a group, and `get-schedule`
#: resolves within ONE group — omitting `--group-name` searches only this one.
#: `list-schedules` spans all of them, so an enumerate-then-describe pass that
#: drops the group can list schedules it will then fail to describe
#: (alpha-engine-config-I10009).
DEFAULT_SCHEDULE_GROUP = "default"

CHECK_ID = "automation-pause-reconcile"
CHECK_LABEL = "automation pause: manifest vs registry vs live AWS"
CADENCE_MINUTES = 1440  # the daily sf-arn-drift-check sweep

#: alpha-engine-config-I8189 — a machine-readable declared-pause set, so a
#: cross-repo consumer (crucible-evaluator's groom tile) can render "declared
#: off" instead of inferring "did not run or its writer broke" from absent
#: artifacts. One producer of pause truth (this module, already running daily
#: at 09:50 UTC), on the evaluator's existing S3 read path.
PAUSED_LANES_KEY = "ops/checks/automation-pause-reconcile/paused_lanes.json"
PAUSED_LANES_SCHEMA_VERSION = 1

logger = logging.getLogger(__name__)


# ── register 3: live AWS ─────────────────────────────────────────────────────

def _aws_json(args: list[str]) -> dict:
    proc = subprocess.run(
        ["aws"] + args + ["--region", REGION, "--output", "json"],
        capture_output=True, text=True, check=False,
    )
    if proc.returncode != 0:
        # RAISE, never return empty. An AccessDenied read as "no triggers
        # exist" would let this check grade itself green by losing its own
        # access — the posture automation_pause._live_state already takes.
        raise RuntimeError(f"aws {' '.join(args)}: {proc.stderr.strip()}")
    return json.loads(proc.stdout)


def live_triggers() -> list[dict]:
    """Every trigger in the account, both surfaces, with its live state.

    `aws events list-rule*` and `aws scheduler list-schedules` are DISJOINT
    APIs over disjoint resources, not aliases (alpha-engine-config-I6842: a
    Scheduler-triggered Lambda reads as untriggered when only the first is
    queried). Both are enumerated here or the sweep has a blind surface.

    **A Scheduler schedule is identified by (group, name), not by name**
    (alpha-engine-config-I10009). `list-schedules` spans every schedule group,
    but `get-schedule` defaults to the `default` group and raises
    `ResourceNotFoundException` for a schedule that exists in any other one. So
    the group is carried on every scheduler row here and handed to every
    per-schedule call below; dropping it makes this reconciler enumerate
    schedules it can then never describe. Measured live 2026-09-04: the four
    `crucible-v2`-group schedules landed on 2026-09-03 (I9994) and every run of
    `pause-reconcile.yml` since has exited 2 on
    `get-schedule --name alerts-sweep`, i.e. the pause reconciler — the daily
    check over the manifest that governs which fleet automation may run — was
    itself down for two days.
    """
    out: list[dict] = []
    for page in _paginate(["events", "list-rules"], "Rules"):
        out.append({"surface": "events", "name": page["Name"],
                    "state": page.get("State")})
    for page in _paginate(["scheduler", "list-schedules"], "Schedules"):
        out.append({"surface": "scheduler", "name": page["Name"],
                    "state": page.get("State"),
                    "group": page.get("GroupName") or DEFAULT_SCHEDULE_GROUP})
    return sorted(out, key=lambda t: (t["surface"], t["name"]))


def _paginate(cmd: list[str], key: str):
    token: str | None = None
    while True:
        args = list(cmd)
        if token:
            args += ["--next-token", token]
        page = _aws_json(args)
        yield from page.get(key, [])
        token = page.get("NextToken")
        if not token:
            return


#: How often the post-merge settle window re-reads live trigger state. One
#: enumeration is two cheap list calls (~2s measured 2026-10-03), so polling it is
#: affordable where re-running the whole reconcile (~150s) is not.
SETTLE_POLL_SECONDS = 30


def _join_key(t: dict) -> tuple[str, str]:
    """(surface, name-as-a-finding-names-it) — the qualified ``group/name`` for a
    Scheduler schedule outside the default group, as direction A reports it."""
    group = t.get("group") if t["surface"] == "scheduler" else None
    name = f"{group}/{t['name']}" if group and group != DEFAULT_SCHEDULE_GROUP else t["name"]
    return t["surface"], name


def settle(first: list[dict], findings: list[dict], budget_s: float, *,
           enumerate_=None, sleep=time.sleep, clock=time.monotonic,
           poll_s: float = SETTLE_POLL_SECONDS) -> list[dict] | None:
    """A fresh live snapshot if a trigger named by a finding MOVED within
    ``budget_s``, else None (alpha-engine-config-I7118 follow-up, 2026-10-03).

    WHY. On a push, this workflow starts in the same second as the deploy that
    APPLIES the change it grades, so it can read a trigger's state before that
    deploy flips it. Measured twice on 2026-10-03: nousergon-data#2022 moved
    ``data-collection-daily-heal`` to ``not_paused`` and the reconciler read it
    DISABLED at ~00:46Z, before ``Deploy data-collection stack`` enabled it at
    00:47:12Z; #2023 did the same with ``alpha-engine-crypto-balances-15min``
    (read DISABLED, enabled by ``Deploy crypto-balances`` at 02:24:56Z). Both
    runs reported ``undeclared-dark`` against declarations that were correct,
    and both cleared with nothing changed.

    This is a bounded wait for an applier, never a filter: a finding is only
    re-graded when its trigger's live state actually changed, the re-grade is
    the full reconcile on the fresh snapshot, and a finding whose trigger never
    moves is reported exactly as before. Read-only — it calls the same two
    list APIs as ``live_triggers()``.
    """
    enumerate_ = enumerate_ or live_triggers
    watched = {(f["surface"], f["trigger"]) for f in findings
               if f.get("surface") in ("events", "scheduler")}
    if not watched or budget_s <= 0:
        return None
    before = {k: v for k, v in ((_join_key(t), t["state"]) for t in first) if k in watched}
    deadline = clock() + budget_s
    fresh: list[dict] | None = None
    while clock() < deadline:
        sleep(poll_s)
        snap = enumerate_()
        now = {_join_key(t): t["state"] for t in snap}
        moved = {k for k in watched if now.get(k) != before.get(k)}
        if moved:
            fresh = snap
            if moved == watched:
                break
    return fresh


def sf_invoked_functions() -> set[str]:
    """Every Lambda any live Step Functions definition invokes.

    A pipeline stage is an invocation path that no EventBridge or Scheduler
    enumeration can see: `alpha-engine-predictor-inference` and
    `alpha-engine-research-runner` have only DEPRECATED rules of their own and
    are nonetheless invoked on every weekly and preopen run. Without this
    register, direction C below reports both as dark, and acting on that report
    would declare two live Crucible stages deliberately off. That is not a
    hypothetical: measured 2026-08-12, this register is the difference between
    seven findings and four, and the three it removes are all correct.

    `alpha-engine-config-I7117`'s gotcha states the mechanism — a definition
    names its Lambdas by `FunctionName` in the state body, not by an ARN a regex
    over `arn:aws:lambda` alone would find, so both forms are matched here.
    """
    import re
    names: set[str] = set()
    for sm in _aws_json(["stepfunctions", "list-state-machines"]).get("stateMachines", []):
        body = _aws_json([
            "stepfunctions", "describe-state-machine",
            "--state-machine-arn", sm["stateMachineArn"],
        ])["definition"]
        names |= set(re.findall(r'"FunctionName"\s*:\s*"([^"]+)"', body))
        names |= set(re.findall(
            r"arn:aws:lambda:[^\":]+:\d+:function:([A-Za-z0-9_-]+)", body))
    # `:live` and numeric versions are aliases OF a function; a registry row is
    # about the function, so the qualifier is stripped to match `_rows_for_target`.
    return {n.split(":")[0] for n in names}


INVOCATION_WINDOW_DAYS = 14


def window_start(manifest: dict, days: int = INVOCATION_WINDOW_DAYS):
    """The earliest instant an invocation may count as evidence.

    **A trailing window alone is wrong, and measurably so.** The declarations
    being graded were made under the 2026-08-07 ruling; a flat 14-day window on
    2026-08-12 reaches back to 2026-07-29 and counts nine PRE-ruling days as
    evidence that a component is running in defiance of it. Measured: that
    produced eleven `running-while-declared-off` findings, of which ten were
    invocations from before the pause — `alpha-engine-crypto-balances` scored
    853 and has not run once since 2026-08-07. Exactly one survives a
    post-ruling window, and it is a real defect.

    So the window opens at the LATER of the trailing bound and the day after
    the ruling this manifest records. It moves on its own when Brian issues a
    new ruling, because it reads the ruling's date out of the manifest rather
    than carrying a literal — the failure mode a fixed date literal inside a
    relative lookback always ends in.
    """
    import datetime
    now = datetime.datetime.now(datetime.timezone.utc)
    trailing = now - datetime.timedelta(days=days)
    ruled = datetime.datetime.fromisoformat(
        manifest["ruling"]["date"]).replace(tzinfo=datetime.timezone.utc)
    return max(trailing, ruled + datetime.timedelta(days=1))


def lambda_invocations(function_name: str, since=None) -> float:
    """AWS/Lambda Invocations for a function over the trailing window.

    **This is why direction C is not a trigger-map inference.** Measured
    2026-08-12, three of the four components whose every EventBridge and
    Scheduler trigger is dark were nonetheless invoking every day —
    `alpha-engine-sf-watch-liveness-probe` 18-52 times daily,
    `alpha-engine-overseer-dispatcher` 1-15, `alpha-engine-canary-replay-
    dispatcher` 4-12 — through cross-Lambda and event-time paths no trigger
    enumeration can see (`arming.py` documents one of them: alert-drain's
    freshness-CRITICAL path runs off the freshness monitors, which Brian kept
    enabled). A detector that declared those three `disabled` from the trigger
    map alone would have written three false decisions into the registry, which
    is worse than the gap it was closing.

    So the predicate is trigger-darkness AND invocation-silence, and the
    evidence for `disabled` stays what `alpha-engine-config-I7117` requires: the
    paused entry plus the live DISABLED, never the absence of a complaint.
    """
    import datetime
    end = datetime.datetime.now(datetime.timezone.utc)
    start = since or (end - datetime.timedelta(days=INVOCATION_WINDOW_DAYS))
    got = _aws_json([
        "cloudwatch", "get-metric-statistics",
        "--namespace", "AWS/Lambda", "--metric-name", "Invocations",
        "--dimensions", f"Name=FunctionName,Value={function_name}",
        "--start-time", start.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "--end-time", end.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "--period", "86400", "--statistics", "Sum",
    ])
    return sum(float(d.get("Sum") or 0) for d in got.get("Datapoints", []))


def is_ephemeral_one_shot(name: str, group: str = DEFAULT_SCHEDULE_GROUP) -> bool:
    """Does this Scheduler schedule delete itself the moment it fires?

    **The class this exists for (alpha-engine-config-I7547).** Two dispatchers
    mint one-shot EventBridge Scheduler schedules at RUNTIME to defer their own
    re-invocation rather than drop the work — ``sf-watch-defer-*``
    (``sf-watch-spot-dispatcher/index.py``, config#2226) and
    ``arctic-migration-defer-*`` (``arctic-migration-dispatcher/index.py``),
    both created with ``ActionAfterCompletion="DELETE"`` and both scoped by
    their IAM policy to exactly that name prefix. Measured 2026-08-17:
    ``arctic-migration-defer-0002-g1`` was created at 11:09:22-07:00 by
    ``alpha-engine-arctic-migration-dispatcher``, was reported by this module as
    ``undeclared-enabled`` minutes later, and no longer existed by the time it
    was described.

    Such a schedule is an **in-flight continuation of a component that already
    has a row**, not standing scheduled work a register failed to account for,
    and it cannot be declared: its name is generated (``defer-{migration:04d}-
    g{generation}``, or a digest when that would overflow), so a manifest entry
    naming a literal would be stale at the next generation and a prefix denylist
    would be the hand-maintained list §2.2 exists to forbid. The predicate is
    therefore read off the resource's OWN declared behaviour, which is
    self-maintaining for any future defer family and never needs editing.

    A schedule that vanished between ``list-schedules`` and here is ``True`` by
    proof rather than by assumption — self-deletion after firing is the only way
    that race resolves, and raising on it would make this check fail whenever a
    dispatcher happened to be deferring while it ran.

    ``group`` is REQUIRED for correctness, not an optimisation
    (alpha-engine-config-I10009). Without it `get-schedule` looks only in the
    ``default`` group, so a schedule living in any other group raises
    ``ResourceNotFoundException`` — which this function reads as "it deleted
    itself" and returns ``True`` for. That is the quiet half of the same bug:
    every non-default-group schedule would be silently classified an ephemeral
    one-shot and excused from the register it is missing from.
    """
    try:
        got = _aws_json(["scheduler", "get-schedule", "--name", name,
                         "--group-name", group or DEFAULT_SCHEDULE_GROUP])
    except RuntimeError as exc:
        if "ResourceNotFoundException" in str(exc):
            return True
        raise
    return got.get("ActionAfterCompletion") == "DELETE"


def trigger_targets(trigger: dict) -> list[str]:
    """The ARNs a trigger invokes. Empty is a legitimate answer (a rule whose
    targets were removed), and is reported as such rather than skipped.

    Scheduler rows carry their group (see `live_triggers`); it is passed
    through because `get-schedule` resolves only within one group."""
    if trigger["surface"] == "events":
        got = _aws_json(["events", "list-targets-by-rule", "--rule", trigger["name"]])
        return [t["Arn"] for t in got.get("Targets", [])]
    got = _aws_json(["scheduler", "get-schedule", "--name", trigger["name"],
                     "--group-name", trigger.get("group") or DEFAULT_SCHEDULE_GROUP])
    arn = (got.get("Target") or {}).get("Arn")
    return [arn] if arn else []


# ── register 2: the published observability registry ─────────────────────────

def load_registry(local_dir: Path | None = None) -> dict[str, dict]:
    """component_id -> row, from S3 (or a local directory, for tests).

    Reads the SAME copy the console reads. A local checkout of
    `nous-ergon-ops/governance/observability.d/` is deliberately NOT the default:
    this runs in CI for `nousergon-data`, where that checkout does not exist, and
    an optional local path that silently wins when present is how two readers of
    "the registry" end up grading different content.
    """
    if local_dir is not None:
        blobs = {p.name: p.read_text(encoding="utf-8") for p in sorted(local_dir.glob("*.yaml"))}
    else:
        blobs = _fetch_registry_from_s3()
    rows: dict[str, dict] = {}
    for name, text in blobs.items():
        row = yaml.safe_load(text)
        if not isinstance(row, dict) or not row.get("component_id"):
            raise RuntimeError(f"registry object {name} has no component_id")
        rows[row["component_id"]] = row
    if not rows:
        raise RuntimeError(
            f"the published registry at s3://{REGISTRY_BUCKET}/{REGISTRY_PREFIX} is "
            "empty — grading a trigger's declaration against an empty registry "
            "would report every component as undeclared, so this raises instead"
        )
    return rows


def _fetch_registry_from_s3() -> dict[str, str]:
    import tempfile
    with tempfile.TemporaryDirectory() as tmp:
        proc = subprocess.run(
            ["aws", "s3", "sync", f"s3://{REGISTRY_BUCKET}/{REGISTRY_PREFIX}", tmp,
             "--exclude", "*", "--include", "*.yaml", "--region", REGION],
            capture_output=True, text=True, check=False,
        )
        if proc.returncode != 0:
            raise RuntimeError(f"aws s3 sync of the published registry: {proc.stderr.strip()}")
        return {p.name: p.read_text(encoding="utf-8")
                for p in sorted(Path(tmp).glob("*.yaml"))}


def _lifecycle(row: dict | None) -> str | None:
    return (row or {}).get("lifecycle")


def _declares_off(row: dict | None) -> bool:
    return _lifecycle(row) in NON_SERVICE_LIFECYCLES


def _cites_the_manifest(row: dict | None) -> bool:
    return "automation_pause" in str((row or {}).get("lifecycle_reason") or "")


def _rows_for_target(rows: dict[str, dict], arn: str) -> list[dict]:
    """Registry rows whose component IS the thing this ARN names.

    Joined on the resource name at the tail of the ARN — the registry's
    `component_id` for a Lambda is its function name — with the qualifier
    (`:live`, a version) stripped, because a row is about the function, not
    about an alias of it.
    """
    if not arn or ":" not in arn:
        return []
    tail = arn.split(":")
    if "function" in tail:
        idx = tail.index("function")
        name = tail[idx + 1] if len(tail) > idx + 1 else ""
    else:
        name = tail[-1]
    row = rows.get(name)
    return [row] if row else []


# ── the reconciliation ───────────────────────────────────────────────────────

def reconcile(
    manifest: dict | None = None,
    rows: dict[str, dict] | None = None,
    triggers: list[dict] | None = None,
    targets_of=None,
    sf_invoked: set[str] | None = None,
    invocations_of=None,
    alarm_actions_of=None,
    alarm_breaching_of=None,
    is_ephemeral=None,
    noted: list[dict] | None = None,
) -> list[dict]:
    """Every disagreement between the three registers.

    Arguments are injectable so tests exercise the whole comparison without AWS
    and without the network — the same convention `automation_pause.check()`
    uses for `_live_state`. `alarm_actions_of` mirrors that for the
    `paused_alarms` direction (alpha-engine-config-I7174): a callable
    `alarm_name -> bool | None` (live `ActionsEnabled`, or `None` if the alarm
    does not exist), defaulting to `automation_pause._alarm_actions_enabled`.
    `alarm_breaching_of` mirrors it for the alarm's LIVE ``TreatMissingData``
    (alpha-engine-config-I8712): a callable `alarm_name -> bool`, `True` unless
    live state says otherwise, so a caller that supplies neither keeps the
    pre-I8712 behaviour of assuming every declared entry is `breaching`. Only a
    `breaching` alarm can false-page from its watched trigger's silence, so
    `alarm-unexpectedly-enabled`/`alarm-stale-disabled` below are graded only
    for one — grading a `notBreaching` alarm's `ActionsEnabled` against the
    silencing requirement was exactly the false positive I8712 fixed
    (`alpha-engine-ssm-reachability-probe-unreachable`, notBreaching, live
    `ActionsEnabled=True`, flagged `alarm-unexpectedly-enabled` against a live
    state that was never wrong). Defaults to a single bulk
    `automation_pause._live_alarm_actions()` read, not one call per alarm.

    `is_ephemeral` is `is_ephemeral_one_shot` and is called LAZILY — only for a
    Scheduler schedule about to be reported `undeclared-enabled`, so a clean run
    makes zero extra API calls and a run with findings makes at most one per
    candidate. `noted` is an optional list the caller passes in to receive what
    was excluded and why; excluding a class silently is how the next reader
    rediscovers it from scratch, so the exclusion renders even though it never
    reaches the exit code.
    """
    m = manifest if manifest is not None else ap.load_manifest()
    reg = rows if rows is not None else load_registry()
    trigs = triggers if triggers is not None else live_triggers()
    targets_of = targets_of or trigger_targets
    sf_live = sf_invoked if sf_invoked is not None else sf_invoked_functions()
    since = window_start(m)
    invocations_of = invocations_of or (lambda cid: lambda_invocations(cid, since=since))
    alarm_actions_of = alarm_actions_of or ap._alarm_actions_enabled
    if alarm_breaching_of is None:
        # One bulk read, cached for every entry this call grades — not one
        # `describe-alarms` per alarm. `.get(name)` is None for an alarm that
        # no longer exists live; that case is already reported as
        # `alarm-missing-in-aws` below via `alarm_actions_of`, so defaulting
        # its breaching-ness to True here changes nothing observable.
        _live_cache: dict[str, dict[str, bool]] | None = None

        def _default_breaching_of(name: str) -> bool:
            nonlocal _live_cache
            if _live_cache is None:
                _live_cache = ap._live_alarm_actions()
            row = _live_cache.get(name)
            return row["breaching"] if row is not None else True

        alarm_breaching_of = _default_breaching_of
    is_ephemeral = is_ephemeral or is_ephemeral_one_shot
    noted = noted if noted is not None else []

    paused = ap.paused_names(m)          # paused + pending
    kept = ap.kept_names(m)
    findings: list[dict] = []

    # ── direction A: a live trigger nobody's register accounts for ──────────
    for t in trigs:
        name, state = t["name"], t["state"]
        if name.startswith(_AWS_MANAGED_PREFIX):
            continue
        # `automation_pause.py::_live_triggers()` reports (and `not_paused`/
        # `paused`/`pending` therefore declare) a non-default-group Scheduler
        # schedule as ``"<group>/<name>"`` — see `_split_scheduler_name`'s
        # docstring and the `crucible-v2/heartbeat` et al. entries in
        # automation_pause.json (alpha-engine-config-I9994). This module's own
        # `live_triggers()` above carries the group as a separate field rather
        # than folding it into `name`, so matching bare `name` against
        # `paused`/`kept`/the registry silently fails for every schedule
        # outside the default group — reported `undeclared-enabled` on every
        # run regardless of what the manifest says (alpha-engine-config-I9681:
        # measured 2026-09-08, all four `crucible-v2`-group schedules flagged
        # despite being declared `not_paused` since 2026-09-04/I9994). The join
        # key below reconstructs the same qualified form so this direction
        # agrees with automation_pause.py about what a declaration names; the
        # bare `name` (plus `t["group"]`) is still what is handed to every
        # AWS-calling helper (`targets_of`, `is_ephemeral`), which need the
        # unqualified name and the group as separate arguments.
        group = t.get("group") if t["surface"] == "scheduler" else None
        declared_name = (
            f"{group}/{name}" if group and group != DEFAULT_SCHEDULE_GROUP else name
        )
        row = reg.get(declared_name) or reg.get(name)
        declared_off = declared_name in paused or _declares_off(row)
        if state != "ENABLED":
            if not declared_off:
                findings.append({
                    "kind": "undeclared-dark", "trigger": declared_name, "surface": t["surface"],
                    "detail": (
                        f"live State={state}, but no register declares it off: it is not "
                        f"named in automation_pause.json and its observability row says "
                        f"lifecycle={_lifecycle(row) or 'NO ROW'}. A component off with "
                        "nobody's decision behind it is indistinguishable from a broken "
                        "one, which is the collapse observability-policy.md §8.3 forbids. "
                        "Fix by DECLARING it — a manifest entry if it is a paused "
                        "decision, or lifecycle: deprecated/retired on its registry row "
                        "if it is superseded. Never by re-enabling it to clear this."
                    ),
                })
        else:
            # ENABLED. `unexpectedly-enabled` (manifest says paused) is
            # automation_pause.check()'s finding and is not repeated here. This
            # is the OTHER half, and the one I7118 Finding 2 is about: a row
            # asserting the component is deliberately off while it runs.
            if _declares_off(row):
                findings.append({
                    "kind": "running-while-declared-off", "trigger": declared_name,
                    "surface": t["surface"],
                    "detail": (
                        f"live State=ENABLED while its observability row declares "
                        f"lifecycle={_lifecycle(row)}. A row saying DISABLED renders "
                        "green-adjacent, is excluded from staleness_honesty's violation "
                        "set and is exempt from ABSENT candidacy — so a component running "
                        "again behind that declaration is silent in every surface. Either "
                        "the re-enable was intended and the row must be re-declared "
                        "in-service, or it was not and the trigger goes back off."
                    ),
                })
            elif declared_name not in kept and row is None:
                # A self-deleting one-shot is an in-flight continuation of an
                # already-registered dispatcher, not standing scheduled work —
                # see `is_ephemeral_one_shot`. Probed only here, so a clean run
                # pays nothing for it.
                if t["surface"] == "scheduler" and is_ephemeral(name, t.get("group")):
                    noted.append({
                        "id": name, "kind": "ephemeral-one-shot",
                        "detail": (
                            "live State=ENABLED and named in no register, but it declares "
                            "ActionAfterCompletion=DELETE — a one-shot minted at runtime by a "
                            "dispatcher to defer its own re-invocation, which deletes itself "
                            "when it fires. Its name is generated per run, so no manifest "
                            "entry could name it and none should: the component to hold "
                            "accountable is the dispatcher that minted it, which has its own "
                            "registry row. Excluded from the verdict, listed here so the "
                            "exclusion is visible rather than rediscovered."
                        ),
                    })
                    continue
                findings.append({
                    "kind": "undeclared-enabled", "trigger": declared_name,
                    "surface": t["surface"],
                    "detail": (
                        "live State=ENABLED, named in neither automation_pause.json nor "
                        "the observability registry. Scheduled work is running that no "
                        "register accounts for; under the 2026-08-07 ruling that is "
                        "either an un-pause nobody recorded or a trigger created mid-pause."
                    ),
                })

    # ── direction B: a declaration whose manifest entry is gone ─────────────
    live_names = {t["name"] for t in trigs}
    for cid, row in sorted(reg.items()):
        if not (_declares_off(row) and _cites_the_manifest(row)):
            continue
        # The row's own trigger, when the row IS a trigger.
        if row.get("substrate") in ("eventbridge",) and cid in live_names:
            if cid not in paused:
                findings.append({
                    "kind": "orphaned-lifecycle-declaration", "trigger": cid,
                    "surface": "registry",
                    "detail": (
                        f"observability.d/{cid}.yaml declares lifecycle="
                        f"{_lifecycle(row)} and cites automation_pause.json, but the "
                        "manifest no longer names it. The declaration is now a claim "
                        "about a file that does not say what it quotes. Re-derive it: "
                        "either the entry was deleted on an un-pause (the row goes "
                        "in-service) or the entry was renamed (the reason must be "
                        "restated against the current entry)."
                    ),
                })

    # ── direction C: a paused trigger whose sole target is still in-service ─
    # This is `alpha-engine-config-I7117`'s rule, generalised: it is derived from
    # the live target map rather than from a hand-built table of five, so it
    # holds for every component that acquires the property later.
    target_rows: dict[str, set[str]] = {}
    for t in trigs:
        if t["name"].startswith(_AWS_MANAGED_PREFIX):
            continue
        for arn in targets_of(t):
            for row in _rows_for_target(reg, arn):
                target_rows.setdefault(row["component_id"], set()).add(t["name"])
    live_states = {t["name"]: t["state"] for t in trigs}
    for cid, trigger_names in sorted(target_rows.items()):
        row = reg[cid]
        if cid in sf_live:
            continue  # a live pipeline stage — see sf_invoked_functions()
        all_dark = all(live_states.get(n) != "ENABLED" for n in trigger_names)
        if not all_dark:
            continue
        # The measured half. A row's lifecycle claim is graded against whether
        # the component RAN, never against the trigger map alone.
        ran = invocations_of(cid)
        if _declares_off(row):
            # NOT a finding, however many times it ran. Brian's ruling is
            # explicit that a paused component keeps its manual path — "everything
            # else has its schedule removed - MANUAL INVOCATION ONLY" — so
            # invocations behind a declared-off row are the intended state, not a
            # breach of it. Measured 2026-08-12: the one component this branch
            # flagged on a post-ruling window, `alpha-engine-predictor-health-check`,
            # shows 10 invocations at irregular single-call hours, which is the
            # signature of hand invocation, and its own EventBridge rule is
            # correctly DISABLED. Firing here would page on the ruling working.
            #
            # The unambiguous form of "running while declared off" is a LIVE
            # TRIGGER behind a declared-off row, and that is graded in direction A
            # above, where the evidence is the trigger's State rather than a
            # count that cannot tell a schedule from a human.
            continue
        if ran > 0:
            # in-service, and it IS in service. The row is telling the truth,
            # so this reconciler has nothing to say. That the trigger map does
            # not explain WHAT wakes it is a real gap and a different clause —
            # the registry's own wake/trigger declaration — filed separately as
            # alpha-engine-config-I7126, not smuggled in as a lifecycle finding.
            continue
        findings.append({
            "kind": "dark-and-undeclared-component", "trigger": cid,
            "surface": "registry",
            "detail": (
                f"every trigger that invokes it is off ({', '.join(sorted(trigger_names))}), "
                f"it was invoked 0 times since {since:%Y-%m-%d}, and its "
                f"row still declares lifecycle={_lifecycle(row)}. It renders as whatever its "
                "last invocation produced and becomes a fresh UNREPORTED the moment it falls "
                "out of the console's window. Declare the lifecycle from the evidence (the "
                "paused entry plus the live DISABLED plus this silence), or record the live "
                "invocation path that keeps it in-service. Step Functions lambda:invoke is "
                "already covered here (sf_invoked_functions)."
            ),
        })

    # ── direction E: an ENABLED trigger whose declared downstream is silent ──
    # alpha-engine-config-I9469. Direction C answers "the trigger is dark — is
    # the component it feeds still running some other way." This is the
    # opposite edge: the trigger IS firing, so a live audit of it alone reads
    # healthy, and the miss is invisible unless something reads the invocation
    # count of what it actually fires. `#9469` was found by an AD HOC 7-day
    # CloudWatch sweep across all 72 Lambdas; this makes that sweep a standing
    # check instead of something that has to be re-run by hand.
    #
    # The candidate that made this look real — `alpha-engine-research-thinktank`
    # at 0 invocations while `alpha-research-thinktank-daily` fires daily — is
    # NOT what this direction would have flagged, and that is the point: the
    # rule's LIVE target (`trigger_targets`, a real `list-targets-by-rule` read)
    # is `alpha-engine-thinktank-spot-dispatcher`, which had 10 invocations in
    # the same window. The old Lambda was never a live target — its IAM grant
    # and EventBridge wiring were already removed under `#5777` — so a
    # name-similarity guess treated a retired resource as "the declared
    # downstream" where a target-map read would not have. This direction reads
    # the target map, never the name, which is why it does not reproduce that
    # false positive.
    for t in trigs:
        name, state = t["name"], t["state"]
        if state != "ENABLED" or name.startswith(_AWS_MANAGED_PREFIX):
            continue
        if name in paused:
            continue  # a manual-invocation-only trigger left enabled by mistake
            # is direction A's "running-while-declared-off", not this one.
        for arn in targets_of(t):
            for row in _rows_for_target(reg, arn):
                cid = row["component_id"]
                if cid in sf_live or _declares_off(row):
                    # SF-invoked or itself declared off — its own service state
                    # is graded elsewhere (direction A / direction C); grading
                    # it again here off a single trigger's silence would just
                    # be a second, weaker copy of those.
                    continue
                if invocations_of(cid) > 0:
                    continue
                findings.append({
                    "kind": "enabled-trigger-silent-target", "trigger": name,
                    "surface": t["surface"],
                    "detail": (
                        f"live State=ENABLED and its live target is "
                        f"{cid!r} (row lifecycle={_lifecycle(row) or 'in-service'}), "
                        f"but {cid!r} was invoked 0 times since {since:%Y-%m-%d}. "
                        "The trigger firing on schedule is not evidence the work "
                        "happened — read the live target map, not the trigger's own "
                        "State, before treating an enabled schedule as healthy."
                    ),
                })

    # ── direction D: paused_alarms vs live CloudWatch, both directions ──────
    # Read-only mirror of `automation_pause.alarm_findings()`: justification is
    # RE-DERIVED here from `m` and `paused_names(m)`, never trusted from a
    # cached field, so an un-pause (a trigger's entry removed from `paused`)
    # changes what this direction reports on its very next read.
    for entry in ap.alarm_entries(m):
        name = entry["name"]
        justified = ap.alarm_justified(entry, m)
        live = alarm_actions_of(name)
        if live is None:
            findings.append({
                "kind": "alarm-missing-in-aws", "trigger": name, "surface": "cloudwatch",
                "detail": (
                    "declared in paused_alarms but no such CloudWatch alarm exists live."
                ),
            })
        elif not alarm_breaching_of(name):
            # notBreaching (alpha-engine-config-I8712): cannot latch ALARM from
            # the watched trigger's silence, so this direction's silencing
            # requirement does not apply — ActionsEnabled is not graded here
            # regardless of its live value. See the module-level note on
            # `alarm_breaching_of` above and `automation_pause.py`'s matching
            # fix in `alarm_findings()`.
            continue
        elif justified and live:
            findings.append({
                "kind": "alarm-unexpectedly-enabled", "trigger": name, "surface": "cloudwatch",
                "detail": (
                    f"watches {', '.join(entry['watches'])}, all still paused, but "
                    f"ActionsEnabled=true — the pause-caused page this entry exists to "
                    f"silence is live. Fix: automation_pause.py --enforce --alarms-only"
                ),
            })
        elif not justified and not live:
            findings.append({
                "kind": "alarm-stale-disabled", "trigger": name, "surface": "cloudwatch",
                "detail": (
                    f"watches {', '.join(entry['watches']) or 'nothing'}, no longer all "
                    f"paused, but ActionsEnabled=false — a pause lifted and this alarm "
                    f"was not re-armed. Fix: automation_pause.py --enforce --alarms-only"
                ),
            })

    # The declaration's own shape, offline (alpha-engine-config-I8047). Kept in
    # `automation_pause` and merely re-exported into this verdict rather than
    # re-derived: a second copy of the grammar is a second place to loosen it.
    findings.extend(ap.declaration_findings(m))

    return sorted(findings, key=lambda f: (f["kind"], f["trigger"]))


def declared_alarm_gaps(manifest: dict | None = None) -> list[dict]:
    """Every currently-justified `paused_alarms` entry, as a declared gap.

    Property 2 of alpha-engine-config-I7174: a paused component's silenced
    alarm must render AS a declared, bounded gap on the coverage surface, never
    be omitted. These rows are NOT drift — `reconcile()` does not return them
    and they never affect the check's exit code — they are always-present
    documentation of what is intentionally not paging right now, so the console
    row stays informative on every green run instead of going silent about the
    one thing worth knowing while the 2026-08-07/08-12 pause holds.
    """
    m = manifest if manifest is not None else ap.load_manifest()
    return [
        {
            "id": entry["name"], "kind": "declared-silenced-alarm",
            "detail": (
                f"silenced because {', '.join(entry['watches'])} "
                f"{'is' if len(entry['watches']) == 1 else 'are'} paused; owned by "
                f"{entry['issue'] or '<UNOWNED>'}, re-exam "
                f"{entry['re_exam'] or '<UNDATED>'}. {entry['reason']}"
            ),
        }
        for entry in ap.alarm_entries(m)
        if ap.alarm_justified(entry, m)
    ]


# ── the verdict, made readable without opening the log (I7547 deliverable 3) ─

def headline(findings: list[dict], checked: int, error: str | None = None) -> str:
    """One line, ≤ a job title, that says WHICH kind of red this is.

    `pause-reconcile.yml` uses this as the name of the job that carries the
    non-zero exit, so the Actions run page states the verdict where a reader
    lands rather than 60 lines into a log. The three forms are deliberately
    unmistakable at a glance, because the failure this closes is a HUMAN one:
    four consecutive `failure` runs that were four correct detections got read
    as one broken job, and the arm they were reporting stayed missing for five
    days (alpha-engine-config-I7547).
    """
    if error is not None:
        return f"BROKE — the reconciler could not run: {error.splitlines()[0][:120]}"
    if not findings:
        return f"clear — {checked} live trigger(s), every one declared"
    kinds = sorted({f["kind"] for f in findings})
    return (f"DRIFT — {len(findings)} finding(s) across {checked} live trigger(s): "
            + ", ".join(kinds))


def annotation(findings: list[dict], checked: int, error: str | None = None) -> str:
    """The same verdict as a GitHub workflow-command annotation.

    Annotations render at the top of the run page and on the check itself, so
    this is the second surface that carries the count without a log open. A
    breakage is `::error::` with a different title from a finding, because the
    two need different responses and GitHub renders both job outcomes as
    `failure`.
    """
    if error is not None:
        return f"::error title=pause reconcile BROKE::{error.splitlines()[0]}"
    if not findings:
        return f"::notice title=pause reconcile clear::{checked} live trigger(s), every one declared"
    return (f"::error title=pause reconcile: {len(findings)} finding(s)::"
            + " | ".join(f"[{f['kind']}] {f['surface']}:{f['trigger']}" for f in findings))


def render_markdown(findings: list[dict], checked: int, registry_rows: int,
                    gaps: list[dict] | None = None, noted: list[dict] | None = None,
                    error: str | None = None) -> str:
    """The run summary, as Markdown rather than as the raw `--json` blob.

    The workflow previously appended `--json` to `$GITHUB_STEP_SUMMARY`, which
    (a) renders as an unformatted wall a reader has to parse by eye and (b) cost
    a SECOND full enumeration of the account, so the summary could disagree with
    the verdict above it whenever live state moved between the two runs — which
    is exactly how a self-deleting one-shot schedule appeared in one and not the
    other. One run, one verdict, rendered.
    """
    out: list[str] = ["## Pause reconcile", "", f"**{headline(findings, checked, error)}**", ""]
    if error is not None:
        out += ["The three registers were NOT compared. This is a broken detector, not a",
                "clean fleet — nothing below was graded.", "", "```", error, "```", ""]
        return "\n".join(out)
    out += [f"`{checked}` live trigger(s) vs `{registry_rows}` registry row(s).", ""]
    if findings:
        out += ["### Findings — these fail the run", "",
                "| kind | surface | subject |", "|---|---|---|"]
        out += [f"| `{f['kind']}` | {f['surface']} | `{f['trigger']}` |" for f in findings]
        out += ["", "<details><summary>What each one means and how to clear it</summary>", ""]
        out += [f"**`{f['trigger']}`** ({f['kind']}) — {f['detail']}\n" for f in findings]
        out += ["</details>", ""]
    else:
        out += ["No disagreement between the pause manifest, the observability registry "
                "and live AWS.", ""]
    for title, rows, why in (
        ("Declared gaps — alarms deliberately silenced by the pause", gaps or [],
         "Expected to be non-empty for the duration of the pause. These do NOT affect "
         "the verdict; they render so a declared gap is never invisible."),
        ("Excluded — runtime one-shots that delete themselves", noted or [],
         "Not declarable and not findings. Listed so the exclusion is auditable."),
    ):
        if not rows:
            continue
        out += [f"### {title}", "", f"_{why}_", ""]
        out += [f"- `{r['id']}` — {r['detail']}" for r in rows]
        out += [""]
    return "\n".join(out)


def write_github_output(path: Path, findings: list[dict], checked: int,
                        error: str | None = None) -> None:
    """`$GITHUB_OUTPUT` key/values the workflow turns into a job name.

    Multi-line-safe by construction: every value here is a single line, and
    `headline` truncates the only one that could carry a newline.
    """
    verdict = "broken" if error is not None else ("drift" if findings else "clear")
    with path.open("a", encoding="utf-8") as fh:
        fh.write(f"verdict={verdict}\n")
        fh.write(f"findings={len(findings)}\n")
        fh.write(f"checked={checked}\n")
        fh.write(f"headline={headline(findings, checked, error)}\n")


# ── the console row ──────────────────────────────────────────────────────────

def publish(findings: list[dict], checked: int, dry_run: bool = False,
            declared_gaps: list[dict] | None = None) -> str | None:
    """This detector's own row on the console (observability-policy.md §2.2).

    A detector that reports nowhere is unobserved, and a green run is exactly as
    load-bearing as a red one: the row is written on EVERY run, so silence from
    this check is visible as staleness on its own row rather than as absence.

    `declared_gaps` (alpha-engine-config-I7174 property 2) are appended to the
    rendered findings but taken OUT of the status/summary computation: they are
    the currently-justified `paused_alarms` entries, expected to be non-empty
    for the whole duration of the 2026-08-07/08-12 pause, and counting them as
    ATTENTION would make this row permanently red for a state that is correct
    — the exact page-on-compliance failure I7174 exists to stop. They still
    render, because a declared gap that is invisible unless something is ALSO
    broken is an omission, not a declaration.
    """
    from nousergon_lib import fleet_check_result as fcr

    gaps = declared_gaps or []
    status = fcr.STATUS_OK if not findings else fcr.STATUS_ATTENTION
    if findings:
        by_kind: dict[str, int] = {}
        for f in findings:
            by_kind[f["kind"]] = by_kind.get(f["kind"], 0) + 1
        summary = (
            f"{len(findings)} disagreement(s) across {checked} live trigger(s): "
            + ", ".join(f"{k}={v}" for k, v in sorted(by_kind.items()))
        )
    else:
        summary = (
            f"{checked} live trigger(s); every off trigger is declared off by the pause "
            "manifest or by its registry lifecycle, and no declaration is orphaned"
        )
    if gaps:
        summary += f"; {len(gaps)} alarm(s) declared silenced by the pause"
    env = fcr.build(
        check_id=CHECK_ID, label=CHECK_LABEL, status=status, summary=summary,
        cadence_minutes=CADENCE_MINUTES,
        findings=[{"id": f["trigger"], "kind": f["kind"], "detail": f["detail"]}
                  for f in findings] + gaps,
        deep_link=(
            "https://github.com/nousergon/nousergon-data/blob/main/"
            "infrastructure/pause_reconcile.py"
        ),
    )
    return fcr.emit(env, dry_run=dry_run)


def publish_error(error: str, dry_run: bool = False) -> str | None:
    """The console row for a run that could not compare the registers.

    **The gap this closes (alpha-engine-config-I7547).** The breakage path used
    to `return 2` before reaching `publish()`, so a check that lost its AWS
    access wrote NOTHING and the console kept rendering yesterday's row. That is
    the §8.3 collapse this module exists to detect, committed by the detector
    itself: "we could not look" was indistinguishable from "we looked and it was
    the same as yesterday", and it stayed that way until the row aged out.

    `STATUS_ERROR` is a different state from `STATUS_ATTENTION` for the same
    reason the exit codes differ — `attention` means this check works and the
    FLEET has drift; `error` means the check is blind and the fleet's state is
    currently unknown. A blind detector reporting `attention` would understate
    it; reporting `ok` would be a lie.
    """
    from nousergon_lib import fleet_check_result as fcr

    env = fcr.build(
        check_id=CHECK_ID, label=CHECK_LABEL, status=fcr.STATUS_ERROR,
        summary=f"could not compare the three registers: {error.splitlines()[0][:400]}",
        cadence_minutes=CADENCE_MINUTES,
        findings=[{"id": CHECK_ID, "kind": "reconciler-broken", "detail": error}],
        deep_link=(
            "https://github.com/nousergon/nousergon-data/blob/main/"
            "infrastructure/pause_reconcile.py"
        ),
    )
    return fcr.emit(env, dry_run=dry_run)


def publish_paused_lanes(manifest: dict | None = None, dry_run: bool = False) -> str | None:
    """Publish the machine-readable declared-pause set (alpha-engine-config-I8189).

    A DATA artifact, not this check's own status envelope — deliberately a
    separate S3 key from `publish()`'s `fcr.emit` (which writes this check's
    console row, not a fact other repos read). The evaluator's groom tile
    (`crucible-evaluator/grading/tiles/groom.py`) reads this key to render a
    declared-off state for the paused groom lanes instead of inferring
    "groomer did not run or its writer broke" from absent artifacts —
    `observability-policy.md` §8.3 forbids inferring DISABLED, and forbids the
    symmetric failure of a declared pause rendering as an undeclared gap.

    ``paused`` is exactly ``automation_pause.paused_names(manifest)`` — the
    existing helper that already computes "every name for which DISABLED is
    the intended live state" (``paused`` + ``pending`` blocks) — not
    reimplemented here.

    Never raises: a failed publish of this artifact must not fail the check
    itself, the same posture `publish()`/`publish_error()` already take via
    `fcr.emit`. On any exception this logs a warning and returns None.
    """
    generated_at = datetime.now(UTC).isoformat()
    try:
        paused = sorted(ap.paused_names(manifest))
        body = {
            "schema_version": PAUSED_LANES_SCHEMA_VERSION,
            "generated_at": generated_at,
            "paused": paused,
        }
        uri = f"s3://{REGISTRY_BUCKET}/{PAUSED_LANES_KEY}"
        if dry_run:
            logger.info("[dry-run] would publish %s (%d paused lane(s))", uri, len(paused))
            return None
        boto3.client("s3").put_object(
            Bucket=REGISTRY_BUCKET, Key=PAUSED_LANES_KEY,
            Body=json.dumps(body, indent=2).encode(),
            ContentType="application/json",
        )
        return uri
    except Exception:  # noqa: BLE001 — a failed publish must not fail the check
        logger.warning(
            "could not publish declared-pause lane set to s3://%s/%s — "
            "cross-repo consumers (e.g. crucible-evaluator's groom tile) will "
            "not see this run's paused-lane set until the next scheduled run",
            REGISTRY_BUCKET, PAUSED_LANES_KEY, exc_info=True,
        )
        return None


def main() -> int:
    ap_ = argparse.ArgumentParser(
        description="reconcile the pause manifest, the observability registry and live AWS")
    ap_.add_argument("--check", action="store_true", required=True,
                     help="report every disagreement; exit 1 if there is one")
    ap_.add_argument("--json", action="store_true", help="machine-readable output")
    ap_.add_argument("--publish", action="store_true",
                     help="also write this check's own console row")
    ap_.add_argument("--dry-run", action="store_true",
                     help="with --publish, print the envelope instead of writing it")
    ap_.add_argument("--registry-dir", type=Path, default=None,
                     help="read the registry from a local directory instead of S3")
    ap_.add_argument("--markdown", type=Path, default=None,
                     help="append the rendered verdict to this file (use $GITHUB_STEP_SUMMARY)")
    ap_.add_argument("--github-output", type=Path, default=None,
                     help="append verdict/findings/headline to this file (use $GITHUB_OUTPUT)")
    ap_.add_argument("--settle", type=float, default=0, metavar="SECONDS",
                     help="on findings, wait up to SECONDS for their triggers' live state "
                          "to move (an on-merge deploy still applying), then re-grade; "
                          "the workflow passes it on push only")
    args = ap_.parse_args()

    noted: list[dict] = []
    try:
        triggers = live_triggers()
        rows = load_registry(args.registry_dir)
        findings = reconcile(rows=rows, triggers=triggers, noted=noted)
        if findings and args.settle > 0:
            fresh = settle(triggers, findings, args.settle)
            if fresh is not None:
                first, triggers, noted = findings, fresh, []
                findings = reconcile(rows=rows, triggers=triggers, noted=noted)
                still = {(f["kind"], f["trigger"]) for f in findings}
                for f in first:
                    if (f["kind"], f["trigger"]) not in still:
                        # Rendered, never silent: a reader of the summary sees
                        # the transient and that it converged.
                        noted.append({
                            "kind": "converged-during-settle",
                            "id": f"{f['surface']}:{f['trigger']}",
                            "detail": (f"[{f['kind']}] on the first read; the trigger's live "
                                       "state changed within the settle window and the "
                                       "full re-grade no longer reports it (an on-merge "
                                       "deploy applying the change being graded)."),
                        })
        gaps = declared_alarm_gaps()
    except RuntimeError as exc:
        # The verdict surfaces are written on the BREAKAGE path too, and say
        # something different from the drift path. A detector whose failure
        # renders identically to its findings is the whole of I7547 deliverable
        # 3 — silence here would reproduce it one level down.
        error = str(exc)
        print(f"ERROR: {error}", file=sys.stderr)
        print(annotation([], 0, error))
        if args.publish:
            publish_error(error, dry_run=args.dry_run)
        if args.markdown:
            with args.markdown.open("a", encoding="utf-8") as fh:
                fh.write(render_markdown([], 0, 0, error=error) + "\n")
        if args.github_output:
            write_github_output(args.github_output, [], 0, error=error)
        return 2

    if args.publish:
        # Publishing is best-effort by construction (`fcr.emit` never raises):
        # a check must not go red because its telemetry did. The verdict below
        # is unaffected either way.
        publish(findings, checked=len(triggers), dry_run=args.dry_run, declared_gaps=gaps)
        # alpha-engine-config-I8189: the declared-pause lane set, a separate
        # data artifact for cross-repo consumers. Same dry_run handling, same
        # never-raises posture as `publish()` above.
        publish_paused_lanes(dry_run=args.dry_run)

    print(annotation(findings, len(triggers)))
    if args.markdown:
        with args.markdown.open("a", encoding="utf-8") as fh:
            fh.write(render_markdown(findings, len(triggers), len(rows),
                                     gaps=gaps, noted=noted) + "\n")
    if args.github_output:
        write_github_output(args.github_output, findings, len(triggers))

    if args.json:
        print(json.dumps({"checked": len(triggers), "registry_rows": len(rows),
                          "findings": findings, "declared_alarm_gaps": gaps,
                          "excluded": noted}, indent=2))
    else:
        print(f"pause reconcile — {len(triggers)} live trigger(s) vs "
              f"{len(rows)} registry row(s), {len(gaps)} alarm(s) declared silenced")
        if not findings:
            print("  ✓ every off trigger is declared off by one of the two registers")
            print("  ✓ no lifecycle declaration cites a manifest entry that is gone")
            print("  ✓ no component runs while its row declares it off")
            print("  ✓ every paused_alarms entry's ActionsEnabled matches its justification")
        for f in findings:
            print(f"  ✗ [{f['kind']}] {f['surface']}:{f['trigger']}")
            print(f"      {f['detail']}")
        for g in gaps:
            print(f"  · [declared gap] {g['id']}")
            print(f"      {g['detail']}")
        for n in noted:
            print(f"  · [excluded: {n['kind']}] {n['id']}")
            print(f"      {n['detail']}")
    return 1 if findings else 0


if __name__ == "__main__":
    sys.exit(main())
