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

**Alarm-action ownership (alpha-engine-config-I7174).** A paused component's
absence-alarm (``treat_missing_data: breaching``) cannot tell "gated off by
declaration" from "upstream died" — it has no input saying which case it is
in, because the pause manifest and the alarm configuration were not connected.
The ``paused_alarms`` block in ``automation_pause.json`` closes that: each
entry names a watch-plane/liveness CloudWatch alarm and the trigger name(s) it
``watches``. The alarm is JUSTIFIED — i.e. its actions should be silenced — for
exactly as long as every name in ``watches`` is itself in ``paused_names()``.
That is computed live on every read, never cached in a second field, so lifting
a pause (deleting/moving the watched trigger's entry out of ``paused``) makes
the alarm's justification lapse on the very next ``--check``/``--enforce`` —
the SAME manifest edit that restores the trigger's schedule, no separate AWS
CLI command. ``enforce()`` disables actions on a justified-but-armed alarm and
RE-ENABLES actions on an alarm whose justification has lapsed; alarms are
silenced with ``disable-alarm-actions``, never deleted, so their history and
configuration survive for later reconstruction. Re-enabling an alarm is NOT
the trigger-reenable asymmetry below — it only resumes paging, it starts no
scheduled work, so it is safe for ``enforce()`` to do unattended.

``paused_alarms`` is graded for CORRECTNESS by the above: every entry in it must
still be justified and must match live state. It is graded for COMPLETENESS by
``armed_alarms`` (alpha-engine-config-I7023). Every live alarm with
``TreatMissingData=breaching`` can latch ALARM from silence alone, so each one
must be classified into exactly one of the two blocks — silenced because a
trigger is paused, or armed because it watches something genuinely running. An
alarm in NEITHER block is an ``alarm-undeclared`` finding, so a new one cannot be
born unclassified; an ``armed_alarms`` entry whose live actions are disabled is
``armed-but-silenced``, so a detector cannot be muted by hand with no
declaration saying why. This exists because the original ``paused_alarms`` list
was built from the alarms OBSERVED in ALARM on one morning: four members of the
class read OK at that moment — one of them because another component was
re-invoking their paused probe — and were therefore never listed, and kept
paging. A register built from symptoms is an incident log; ``armed_alarms`` is
what makes this one an audit.

**``paused_alarms`` may hold BOTH missing-data treatments; only ``breaching``
entries are graded for silence (alpha-engine-config-I8712).** The block's
membership is "an alarm watching a paused trigger", not "an alarm that must be
silenced" — its siblings are commonly declared together (e.g. a component's
``-dead``/breaching and ``-unreachable``/notBreaching probes watch the same
schedule). But only a ``breaching`` alarm can latch ALARM purely from a paused
trigger's silence, so only a ``breaching`` alarm has anything to be silenced
FOR. A ``notBreaching`` alarm cannot false-page while its watched trigger is
paused regardless of ``ActionsEnabled`` — CloudWatch treats the missing
datapoints as within threshold either way — so ``alarm_findings()`` grades
``alarm-unexpectedly-enabled``/``alarm-stale-disabled`` (and ``enforce()``
flips actions) **only for entries whose LIVE ``TreatMissingData`` is
``breaching``**; a ``notBreaching`` entry's ``ActionsEnabled`` state is never
graded here and is left exactly as it is found; whether it is separately
declared MUTED still falls to ``alarm_coverage_findings()``'s
``alarm-undeclared-silence`` scan below, which covers every alarm regardless of
missing-data treatment. Treating a ``notBreaching`` entry as if it needed
silencing is the defect this fixes: ``alpha-engine-ssm-reachability-probe-
unreachable`` (notBreaching, live ``ActionsEnabled=True``) was flagged
``alarm-unexpectedly-enabled`` against a live state that was never wrong. Do
**not** "fix" this by moving such an alarm into ``armed_alarms`` instead —
that block's own scope (above) is explicitly the ``breaching`` population;
widening it to notBreaching would just relocate the same category error
(alpha-engine-config-I8180 family: a reader took one classification word to
mean something narrower than its own scope statement, and made a true finding
permanently unclearable).

Usage:
  ./infrastructure/automation_pause.py --check     # verify; exit 1 on any finding
  ./infrastructure/automation_pause.py --enforce   # re-disable anything that came back,
                                                    # and reconcile alarm-action state
  ./infrastructure/automation_pause.py --enforce --alarms-only  # touch ONLY alarm actions;
                                                    # never disables/enables a trigger
  ./infrastructure/automation_pause.py --check --json
  ./infrastructure/automation_pause.py --check --alert-on-fail  # page via krepis.alerts
                                                    # (severity=error) on any finding —
                                                    # independent of any groom/sweep
                                                    # consumer (alpha-engine-config-I8110);
                                                    # run by pause-check-alert.yml on its own
                                                    # 4-hourly schedule, needs krepis installed

``--check`` needs ``events:DescribeRule`` + ``events:ListRules`` +
``scheduler:GetSchedule`` + ``scheduler:ListSchedules`` +
``cloudwatch:DescribeAlarms``; ``--enforce`` additionally needs
``events:DisableRule`` + ``scheduler:UpdateSchedule`` +
``cloudwatch:EnableAlarmActions`` + ``cloudwatch:DisableAlarmActions``. The
``ListRules``/``ListSchedules`` pair is alpha-engine-config-I9959's
``trigger-undeclared``/``trigger-out-of-scope`` completeness scan — the
account-wide enumeration ``_live_triggers()`` needs, on top of the per-name
``DescribeRule``/``GetSchedule`` the rest of ``--check`` already used; both
are already granted to ``github-actions-iam-drift-check``
(``pause_reconcile.py`` has called them from the same role since
alpha-engine-config-I7118). CI runs ``--check`` (read-only) and ``--enforce
--alarms-only`` (alarm actions only) under that role, daily in
``sf-arn-drift-check.yml``; ``pause-check-alert.yml`` runs ONLY
``--check --alert-on-fail`` (same role, same read-only permission set) on its
own schedule so a finding pages even while the groom that would otherwise
triage a red CI job is paused.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from datetime import UTC, date, datetime
from pathlib import Path

MANIFEST = Path(__file__).parent.resolve() / "automation_pause.json"
REGION = "us-east-1"

#: SCOPE, made explicit (alpha-engine-config-I9959). The 2026-08-07 ruling's own
#: text — "shut down all automatic AWS processes ... everything else has its
#: schedule removed" — covers every EventBridge rule (default bus) and every
#: EventBridge Scheduler schedule in the account, region us-east-1. The ONE
#: carve-out is a rule AWS itself creates and manages (e.g. Inspector's ECR
#: scan rule, ``DO-NOT-DELETE-AmazonInspectorEcrManagedRule``): the fleet owns
#: none of its lifecycle, so it is not "scheduled automation" this manifest can
#: classify. This MUST match ``pause_reconcile.py``'s ``_AWS_MANAGED_PREFIX`` —
#: two literals for one exclusion is a silent-drift risk the test file pins.
AWS_MANAGED_TRIGGER_PREFIX = "DO-NOT-DELETE-"

#: Every ``paused_alarms`` entry must name the OPEN tracker issue whose closure
#: ends the pause, in the fleet's own reference grammar. A free-text owner is
#: one nothing can resolve, which is the defect this field exists to remove
#: (alpha-engine-config-I8047 / -I8090).
DECLARATION_ISSUE_RE = re.compile(r"^alpha-engine-config-I\d+$")

#: And the date past which the suppression is no longer justified by anything.
DECLARATION_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")

#: Default EventBridge Scheduler group. ``get-schedule``/``update-schedule``
#: silently assume this when ``--group-name`` is omitted — they do NOT search
#: every group, they look in exactly this one and return
#: ``ResourceNotFoundException`` if the schedule lives elsewhere (measured
#: 2026-09-04: ``get-schedule --name heartbeat`` 404s while the schedule is
#: live and ENABLED in group ``crucible-v2``). Every schedule this manifest
#: named before 2026-09-04 happened to live here, which is what let that gap
#: go unnoticed until Crucible v2 created the first non-default group
#: (alpha-engine-config-I9994).
DEFAULT_SCHEDULE_GROUP = "default"


def _split_scheduler_name(name: str) -> tuple[str, str]:
    """Split a manifest/live scheduler name into ``(group, bare_name)``.

    A schedule outside ``DEFAULT_SCHEDULE_GROUP`` is named in the manifest and
    reported by ``_live_triggers()`` as ``"<group>/<name>"`` — never guessed or
    searched for, because two different groups may legally hold a schedule of
    the same bare name and silently picking one would be exactly the kind of
    inference ``observability-policy.md`` §8.3 forbids for a DISABLED/ENABLED
    classification. A name with no ``/`` is in ``DEFAULT_SCHEDULE_GROUP``,
    which covers every schedule this manifest declared before crucible-v2's
    scheduler group existed and needs no manifest-wide rewrite.
    """
    if "/" in name:
        group, _, bare = name.partition("/")
        return group, bare
    return DEFAULT_SCHEDULE_GROUP, name


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


def pending_names(manifest: dict | None = None) -> set[str]:
    """Triggers that must be BORN disabled but do not exist live yet.

    Separate from ``paused`` because ``check()`` requires every paused entry to
    exist live — a not-yet-created name there would be a permanent
    ``missing-in-aws`` finding. Keys prefixed ``_`` are prose, not triggers.
    """
    m = manifest if manifest is not None else load_manifest()
    return {k for k in m.get("pending", {}) if not k.startswith("_")}


def kept_names(manifest: dict | None = None) -> set[str]:
    """Triggers for which ENABLED is the INTENDED live state.

    The ``not_paused`` block holds two kinds of key: real trigger names, and
    prose labels grouping things the pause does not reach (``_non-eventbridge``
    covers a DLM policy and two SSM associations; ``_reactive-notifier-rules``
    names an EventPattern family with no schedule to remove). Prose keys carry
    the leading-underscore marker ``pending`` already uses, so the two are
    distinguishable without a second allowlist that could drift from this one.
    """
    m = manifest if manifest is not None else load_manifest()
    return {k for k in m.get("not_paused", {}) if not k.startswith("_")}


def paused_names(manifest: dict | None = None) -> set[str]:
    """Every name for which DISABLED is the INTENDED live state.

    Includes ``pending``. This is the question the drift checkers ask — "is a
    DISABLED trigger drift, or deliberate?" — and the answer is the same for
    both blocks. It is NOT the question ``check()`` asks, which is "does this
    exist live and is it off"; that one iterates ``paused_entries()`` and so
    still ignores ``pending``.

    Getting this wrong cost a red deploy on 2026-08-07: the pending block was
    wired into the bash helper but not here, so expense-collector correctly
    created its schedule DISABLED and its own post-deploy assertion then failed
    the deploy for the state it had just been told to write.
    """
    return {name for _, name, _ in paused_entries(manifest)} | pending_names(manifest)


def alarm_entries(manifest: dict | None = None) -> list[dict]:
    """Every declared ``paused_alarms`` entry: ``{name, reason, watches}``.

    ``_``-prefixed keys (``_why``) are prose, exactly the convention
    ``pending``/``not_paused`` already use, and are skipped here for the same
    reason: a prose value is invisible to every checker that iterates keys, so
    nothing that must be checked may live inside one.
    """
    m = manifest if manifest is not None else load_manifest()
    out: list[dict] = []
    for name, entry in sorted(m.get("paused_alarms", {}).items()):
        if name.startswith("_"):
            continue
        out.append({
            "name": name,
            "reason": entry.get("reason", ""),
            "watches": list(entry.get("watches", [])),
            # alpha-engine-config-I8047. Surfaced with the same `.get` default
            # as `reason` rather than a KeyError, because a malformed entry must
            # reach `declaration_findings()` and be REPORTED, not crash the
            # whole check — a detector that dies on the defect it looks for is
            # indistinguishable from one that found nothing.
            "issue": entry.get("issue", ""),
            "re_exam": entry.get("re_exam", ""),
        })
    return out


def armed_alarm_names(manifest: dict | None = None) -> set[str]:
    """Alarms deliberately left ARMED — the completeness counterpart to ``paused_alarms``.

    No ``watches`` field and nothing to reconcile: armed is the default state,
    and ``enforce()`` never silences an alarm that ``paused_alarms`` does not
    justify. The block exists so every breaching alarm is classified rather than
    merely every silenced one. ``_``-prefixed keys are prose, same convention as
    every other block here.
    """
    m = manifest if manifest is not None else load_manifest()
    return {k for k in m.get("armed_alarms", {}) if not k.startswith("_")}


def alarm_justified(entry: dict, manifest: dict | None = None) -> bool:
    """Is silencing ``entry`` still justified by the manifest, right now?

    True iff EVERY trigger it ``watches`` is currently in ``paused_names()``.
    An entry with an empty ``watches`` list is never justified — a declaration
    that names nothing to watch cannot be graded, and grading it as justified
    would silence an alarm on a claim nobody can verify.
    """
    watches = entry.get("watches") or []
    if not watches:
        return False
    names = paused_names(manifest)
    return all(w in names for w in watches)


def _aws(args: list[str]) -> tuple[int, str, str]:
    proc = subprocess.run(
        ["aws"] + args + ["--region", REGION], capture_output=True, text=True, check=False
    )
    return proc.returncode, proc.stdout.strip(), proc.stderr.strip()


def _live_triggers() -> list[dict]:
    """Every EventBridge rule (default bus) and Scheduler schedule live in the
    account — the enumeration ``trigger_coverage_findings()`` grades for
    completeness, as opposed to ``_live_state()``'s per-name lookup for the
    entries the manifest already names.

    Paginated EXPLICITLY, never via ``aws --no-paginate``: ``list-rules`` and
    ``list-schedules`` each cap at 100 per page, and a truncated first page
    read as "every trigger" would let an undeclared trigger on page two hide
    behind a clean run — the exact shape ``aws --no-paginate`` produces
    (measured elsewhere in the fleet: it returns the first page and no marker,
    silently). This loops on ``NextToken`` until AWS itself says there is none
    left, and RAISES on any non-zero AWS exit rather than returning a partial
    list — a permissions error or a truncated read must never be indistinguishable
    from "the account holds exactly this many triggers".
    """
    out: list[dict] = []
    for surface, cmd, key in (
        ("events", ["events", "list-rules"], "Rules"),
        ("scheduler", ["scheduler", "list-schedules"], "Schedules"),
    ):
        token: str | None = None
        while True:
            args = list(cmd) + ["--output", "json"]
            if token:
                args += ["--next-token", token]
            rc, out_text, err = _aws(args)
            if rc != 0:
                raise RuntimeError(f"aws {' '.join(cmd)}: {err}")
            page = json.loads(out_text or "{}")
            for row in page.get(key, []):
                name = row["Name"]
                if surface == "scheduler":
                    # `list-schedules` (no --group-name) enumerates EVERY
                    # group in the account, unlike `get-schedule`/
                    # `update-schedule` which only ever look in one — so the
                    # name reported here must carry the group whenever it is
                    # not the default, or a same-named lookup elsewhere would
                    # silently resolve to the wrong schedule (or none).
                    group = row.get("GroupName") or DEFAULT_SCHEDULE_GROUP
                    if group != DEFAULT_SCHEDULE_GROUP:
                        name = f"{group}/{name}"
                out.append({"surface": surface, "name": name, "state": row.get("State")})
            token = page.get("NextToken")
            if not token:
                break
    return out


def trigger_coverage_findings(manifest: dict | None = None,
                               triggers: list[dict] | None = None) -> list[dict]:
    """Is every live ENABLED trigger CLASSIFIED — not merely every declared one?

    The trigger-side mirror of ``alarm_coverage_findings()``. ``check()``'s
    other findings all iterate ``paused_entries()``/``kept_names()`` — the
    manifest's OWN hand-listed names — so a live, ENABLED trigger named in
    NEITHER ``paused`` nor ``not_paused`` was invisible to every one of them:
    the check ran daily, reported, and could not see the one case it most
    needed to. Found by hand on 2026-09-03 for exactly two triggers
    (``alpha-engine-preflight-sweep-daily``,
    ``alpha-engine-preopen-deploy-readiness-probe``; alpha-engine-config-I9937,
    classified by nousergon-data-PR1635) — nothing would have found a third.
    This closes that: the population is derived from live AWS
    (``_live_triggers()``), never from either block, so a name nobody listed
    is a finding by construction instead of an omission nobody sees.

    SCOPE (alpha-engine-config-I9959, made explicit — see
    ``AWS_MANAGED_TRIGGER_PREFIX``): every live EventBridge rule and Scheduler
    schedule in the account, except an AWS-managed rule, which is reported as
    ``trigger-out-of-scope`` rather than silently filtered — an excluded
    population that renders nowhere is indistinguishable from one nobody
    thought to exclude.
    """
    m = manifest if manifest is not None else load_manifest()
    trigs = triggers if triggers is not None else _live_triggers()
    declared = paused_names(m) | kept_names(m)
    out: list[dict] = []
    for t in trigs:
        name, state, surface = t["name"], t["state"], t["surface"]
        if name.startswith(AWS_MANAGED_TRIGGER_PREFIX):
            out.append({
                "trigger": name, "surface": surface, "kind": "trigger-out-of-scope",
                "detail": (
                    "AWS-managed rule — the 2026-08-07 ruling covers the fleet's "
                    "own scheduled automation, not a rule AWS itself creates and "
                    "manages (e.g. an Inspector ECR-scan rule); the fleet owns "
                    "none of its lifecycle. Declared here so the exclusion is "
                    "visible rather than a silent filter. Needs no paused/"
                    "not_paused entry."
                ),
            })
            continue
        if state != "ENABLED":
            continue
        if name in declared:
            continue
        out.append({
            "trigger": name, "surface": surface, "kind": "trigger-undeclared",
            "detail": (
                "live State=ENABLED, named in neither `paused` nor `not_paused` "
                "in automation_pause.json — this check has no register saying "
                "whether it should be running. Classify it: add it to "
                "`not_paused` with a one-line reason if it is deliberately "
                "kept, or move it under `paused` if it should be off. An "
                "unclassified enabled trigger is exactly what "
                "alpha-engine-preflight-sweep-daily and "
                "alpha-engine-preopen-deploy-readiness-probe were until "
                "alpha-engine-config-I9959."
            ),
        })
    return out


def _live_state(surface: str, name: str) -> str | None:
    """Return the live State, or None if the trigger does not exist.

    Any failure that is NOT a genuine not-found is raised. A permissions error
    read as absence would let this check grade itself green by losing its own
    access — the same failure mode check-schedule-drift.py guards against.
    """
    if surface == "events":
        if "/" in name:
            # A group-qualified scheduler name (alpha-engine-config-I9994) is
            # structurally not a legal EventBridge rule name — the API's own
            # naming constraint forbids "/" — so `kept_names()`'s "try both
            # surfaces" loop must not even ask; describe-rule 400s
            # (ValidationException) rather than 404ing, which `not_found`
            # cannot distinguish from a real not-found without inspecting the
            # error text more than this function already does elsewhere.
            return None
        rc, out, err = _aws(
            ["events", "describe-rule", "--name", name, "--query", "State", "--output", "text"]
        )
        not_found = "ResourceNotFoundException" in err
    else:
        group, bare = _split_scheduler_name(name)
        args = ["scheduler", "get-schedule", "--name", bare, "--query", "State", "--output", "text"]
        if group != DEFAULT_SCHEDULE_GROUP:
            args += ["--group-name", group]
        rc, out, err = _aws(args)
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
    group, bare = _split_scheduler_name(name)
    get_args = ["scheduler", "get-schedule", "--name", bare, "--output", "json"]
    if group != DEFAULT_SCHEDULE_GROUP:
        get_args += ["--group-name", group]
    rc, out, err = _aws(get_args)
    if rc != 0:
        raise RuntimeError(f"aws scheduler get-schedule --name {name}: {err}")
    spec = json.loads(out)
    for derived in ("Arn", "CreationDate", "LastModificationDate", "ResponseMetadata"):
        spec.pop(derived, None)
    spec["State"] = "DISABLED"
    rc, _, err = _aws(["scheduler", "update-schedule", "--cli-input-json", json.dumps(spec)])
    if rc != 0:
        raise RuntimeError(f"aws scheduler update-schedule --name {name}: {err}")


def _alarm_actions_enabled(name: str) -> bool | None:
    """Live ``ActionsEnabled`` for a CloudWatch alarm, or None if it does not exist.

    Same not-found-vs-raise posture as ``_live_state``: an access error read as
    "alarm does not exist" would let this check grade itself green by losing
    its own permission, rather than reporting the AccessDenied.
    """
    rc, out, err = _aws([
        "cloudwatch", "describe-alarms", "--alarm-names", name,
        "--query", "MetricAlarms[0].ActionsEnabled", "--output", "text",
    ])
    if rc != 0:
        raise RuntimeError(f"aws cloudwatch describe-alarms --alarm-names {name}: {err}")
    if out in ("None", ""):
        return None
    return out == "True"


def _live_alarm_actions() -> dict[str, dict[str, bool]]:
    """``{alarm name: {"enabled": ActionsEnabled, "breaching": bool}}`` for EVERY live alarm.

    One enumeration, two populations, because the manifest is graded against
    both and two independent scans of the same account can disagree with each
    other about it.

    ``TreatMissingData=breaching`` is the population that can go ALARM with
    nothing wrong, so it is what ``armed_alarms`` must classify. ``enabled`` is
    a WIDER question and is asked of every alarm (alpha-engine-config-I8047):
    an alarm muted by hand cannot page whatever its missing-data treatment is,
    and restricting the silence scan to the breaching subset made an undeclared
    mute of a ``notBreaching`` alarm invisible. That is not hypothetical — nine
    of the fourteen alarms Brian disabled by hand on 2026-08-13/14 are
    ``notBreaching``, so had they never been declared, the completeness check
    would have reported nothing at all.

    Paginated explicitly: DescribeAlarms caps at 100 per page and a truncated
    first page would silently shrink the very set being graded for completeness.
    """
    alarms: dict[str, dict[str, bool]] = {}
    token: str | None = None
    while True:
        args = [
            "cloudwatch", "describe-alarms", "--max-items", "100",
            "--query",
            "{a: MetricAlarms[].{n: AlarmName, e: ActionsEnabled, "
            "b: TreatMissingData}, t: NextToken}",
            "--output", "json",
        ]
        if token:
            args += ["--starting-token", token]
        rc, out, err = _aws(args)
        if rc != 0:
            raise RuntimeError(f"aws cloudwatch describe-alarms: {err}")
        page = json.loads(out or "{}")
        for row in page.get("a") or []:
            alarms[row["n"]] = {
                "enabled": bool(row["e"]),
                "breaching": row.get("b") == "breaching",
            }
        token = page.get("t")
        if not token:
            return alarms


def _live_breaching_alarms() -> dict[str, bool]:
    """``{alarm name: ActionsEnabled}`` for every live alarm that latches on silence."""
    return {n: v["enabled"] for n, v in _live_alarm_actions().items() if v["breaching"]}


def _live_silenced_alarms() -> set[str]:
    """Every live alarm whose actions are OFF, regardless of missing-data treatment.

    The population `alarm_coverage_findings()` grades for DECLARATION. A muted
    alarm is indistinguishable from a healthy one on every surface, so the
    question "is this mute declared" is asked of all of them.
    """
    return {n for n, v in _live_alarm_actions().items() if not v["enabled"]}


def _set_alarm_actions(name: str, enabled: bool) -> None:
    verb = "enable-alarm-actions" if enabled else "disable-alarm-actions"
    rc, _, err = _aws(["cloudwatch", verb, "--alarm-names", name])
    if rc != 0:
        raise RuntimeError(f"aws cloudwatch {verb} --alarm-names {name}: {err}")


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

    # ── the other direction: a KEPT trigger that stopped running ─────────────
    #
    # Until 2026-08-11 `not_paused` asserted nothing. It was a list of reasons,
    # and by this file's own standard — "an entry that can never fail is not a
    # record, it is a comment" — it was a comment. The paused half was already
    # two-directional; the kept half was not directional at all.
    #
    # What that costs is specific and was the reason this was written. Every
    # entry here is kept because something breaks without it: the two SF
    # triggers, the cost-safety backstops that stop a t3.large running all
    # night, the freshness monitors, and now the expense collector — the sole
    # guard against the provider-credit exhaustion that left every autonomous
    # lane dark for three days (config#6613). A `deploy.sh --reconcile-schedules`
    # rewrite, a console edit, or a CFN template edit can flip any of them to
    # DISABLED, and nothing here would have said so. A guard that silently
    # stopped running looks exactly like a guard with nothing to report.
    #
    # NOT a mirror of `enforce()`: this reports only. Re-ENABLING a trigger
    # unattended would let this script start scheduled work, which is the one
    # thing the ruling it implements exists to prevent.
    for name in sorted(kept_names()):
        surfaces = {s: _live_state(s, name) for s in ("events", "scheduler")}
        live = {s: v for s, v in surfaces.items() if v is not None}
        if not live:
            findings.append({
                "trigger": name, "surface": "unknown", "kind": "kept-but-missing",
                "detail": (
                    "listed as deliberately KEPT but exists on neither the events nor "
                    "the scheduler surface — it was deleted or renamed, and whatever it "
                    "protected is now unprotected. If it is prose rather than a trigger "
                    "name, prefix the key with '_' as the pending block does."
                ),
            })
            continue
        for surface, state in live.items():
            if state != "ENABLED":
                findings.append({
                    "trigger": name, "surface": surface, "kind": "kept-but-disabled",
                    "detail": (
                        f"deliberately kept ENABLED, but live state is {state}. A deploy, "
                        f"a --reconcile-schedules run or a console edit turned off a "
                        f"trigger the pause explicitly spared. Re-enable it deliberately "
                        f"— this script will not do it for you, because re-enabling "
                        f"scheduled work unattended is what the pause forbids."
                    ),
                })

    # ── alarm-action state, both directions (alpha-engine-config-I7174) ─────
    #
    # Unlike the trigger halves above, BOTH directions here are actionable by
    # `enforce()` — re-enabling an alarm's actions only resumes paging, it
    # starts no scheduled work, so it carries none of the risk that keeps
    # `enforce()` from ever re-enabling a kept-but-disabled TRIGGER.
    findings.extend(alarm_findings())

    # ── completeness: every live trigger CLASSIFIED, not merely every
    # declared one (alpha-engine-config-I9959) ─────────────────────────────
    findings.extend(trigger_coverage_findings())
    return findings


def alarm_findings() -> list[dict]:
    """Every disagreement between ``paused_alarms`` and live CloudWatch state.

    ``alarm-unexpectedly-enabled``/``alarm-stale-disabled`` grade whether
    ``ActionsEnabled`` matches ``alarm_justified()`` — but that grading only
    applies to a ``breaching`` alarm (alpha-engine-config-I8712, see the module
    docstring): only a ``breaching`` alarm can false-page from a paused
    trigger's silence, so only a ``breaching`` alarm has a silencing
    requirement to be graded against. A live-fetched ``breaching`` flag decides
    this per entry rather than trusting anything declared in the manifest,
    because the manifest has no ``treat_missing_data`` field to trust — the
    live value is the only source of truth for it.
    """
    out: list[dict] = []
    live_all = _live_alarm_actions()
    for entry in alarm_entries():
        name = entry["name"]
        justified = alarm_justified(entry)
        live_entry = live_all.get(name)
        if live_entry is None:
            out.append({
                "trigger": name, "surface": "cloudwatch", "kind": "alarm-missing-in-aws",
                "detail": (
                    "declared in paused_alarms but no such CloudWatch alarm exists live — "
                    "it was deleted or renamed. Remove or correct the entry."
                ),
            })
            continue
        live = live_entry["enabled"]
        if not live_entry["breaching"]:
            # notBreaching: cannot latch ALARM from the watched trigger's
            # silence, so its ActionsEnabled state is never graded here —
            # continues to whatever `alarm_coverage_findings()` says about a
            # DECLARED mute, but not to a "should be silenced" verdict this
            # alarm class structurally cannot need.
            continue
        if justified and live:
            out.append({
                "trigger": name, "surface": "cloudwatch", "kind": "alarm-unexpectedly-enabled",
                "detail": (
                    f"every trigger it watches ({', '.join(entry['watches'])}) is still "
                    f"paused, so this alarm should be silenced, but ActionsEnabled=true — "
                    f"the pause-caused page this entry exists to stop is live. "
                    f"Fix: ./infrastructure/automation_pause.py --enforce --alarms-only"
                ),
            })
        elif not justified and not live:
            out.append({
                "trigger": name, "surface": "cloudwatch", "kind": "alarm-stale-disabled",
                "detail": (
                    f"the trigger(s) it watches ({', '.join(entry['watches']) or 'none'}) "
                    f"are no longer all paused, so silencing is no longer justified, but "
                    f"ActionsEnabled=false — a pause was lifted and this alarm was not "
                    f"re-armed. Fix: ./infrastructure/automation_pause.py --enforce "
                    f"--alarms-only (re-enables it), then remove the stale paused_alarms "
                    f"entry."
                ),
            })

    out.extend(declaration_findings())
    out.extend(alarm_coverage_findings())
    return out


def declaration_findings(manifest: dict | None = None) -> list[dict]:
    """Is every ``paused_alarms`` entry's REASON still gradeable — not just its resource?

    ``alarm_justified()`` asks whether the trigger a suppression ``watches`` is
    still paused. That is the WEAKER of the two conditions
    (alpha-engine-config-I8090): what rots is the justification. A declaration
    saying "silenced under the 2026-08-12 ruling" keeps looking true forever,
    including after the ruling has been executed, reversed, or closed without
    the work.

    Two fields make the reason gradeable, and this function asserts only that
    they are PRESENT and WELL-FORMED. It deliberately does not resolve the
    issue's state or compare the date to today:

      * resolving the issue needs a credential for `nousergon/alpha-engine-config`,
        which this repo has none of and should not acquire to read a tracker;
      * so both temporal predicates are graded in one place, by
        `alpha-engine-config`'s `suppression-declaration-sweep`, which runs in
        the repo that owns the tracker and reads this manifest from the public
        checkout. Splitting "expired" from "owning issue closed" across two
        reports would give two half-answers to one question.

    What this DOES do is make that sweep impossible to satisfy vacuously: an
    entry with no `issue` cannot be graded against the tracker at all, and an
    entry with no `re_exam` has no date to be past. An ungradeable declaration
    reads exactly like a healthy one, which is the whole class.
    """
    out: list[dict] = []
    for entry in alarm_entries(manifest):
        name, issue, re_exam = entry["name"], entry["issue"], entry["re_exam"]
        if not DECLARATION_ISSUE_RE.match(issue):
            out.append({
                "trigger": name, "surface": "cloudwatch",
                "kind": "alarm-declaration-unowned",
                "detail": (
                    f"paused_alarms entry carries issue={issue!r}, which is not a "
                    f"resolvable tracker reference (expected e.g. "
                    f"'alpha-engine-config-I6984'). A suppression whose owner cannot "
                    f"be resolved can never be found stale — the clock and the "
                    f"tracker are the only two things that can retire it, and this "
                    f"field is what makes the second one reachable."
                ),
            })
        if not DECLARATION_DATE_RE.match(re_exam):
            out.append({
                "trigger": name, "surface": "cloudwatch",
                "kind": "alarm-declaration-undated",
                "detail": (
                    f"paused_alarms entry carries re_exam={re_exam!r}, which is not an "
                    f"ISO date (YYYY-MM-DD). Without one the suppression has no expiry "
                    f"and outlives its justification silently, which is "
                    f"alpha-engine-config-I8047 one level up."
                ),
            })
            continue
        try:
            date.fromisoformat(re_exam)
        except ValueError:
            out.append({
                "trigger": name, "surface": "cloudwatch",
                "kind": "alarm-declaration-undated",
                "detail": f"re_exam={re_exam!r} is not a real calendar date.",
            })
    return out


def alarm_coverage_findings() -> list[dict]:
    """Is every silence-latching alarm CLASSIFIED — not merely every listed one?

    ``alarm_findings()`` above grades the entries that exist. This grades the
    ones that do not: any live alarm with ``TreatMissingData=breaching`` in
    neither ``paused_alarms`` nor ``armed_alarms``, and any ``armed_alarms``
    entry that has been silenced or has vanished. Without this, the manifest can
    only ever be as complete as whatever was firing on the day someone wrote it.
    """
    out: list[dict] = []
    declared_paused = {e["name"] for e in alarm_entries()}
    declared_armed = armed_alarm_names()
    live = _live_breaching_alarms()

    # ── an UNDECLARED mute, of any alarm (alpha-engine-config-I8047) ────────
    #
    # The breaching scan below asks "is every alarm that can latch on silence
    # classified". This asks the narrower, louder question the ruling turns on:
    # is every alarm that is CURRENTLY SILENCED declared as such. The two
    # populations are not the same, and the difference is where the defect
    # lives — nine of the fourteen alarms Brian muted by hand on 2026-08-13/14
    # are `notBreaching`, so the breaching scan could not have seen them had
    # they never been declared. Codifying those fourteen as declared,
    # self-expiring suppressions is only half the ruling; this is the half that
    # must not weaken, because a mute nobody declared is still the defect.
    for name in sorted(_live_silenced_alarms() - declared_paused - declared_armed):
        out.append({
            "trigger": name, "surface": "cloudwatch", "kind": "alarm-undeclared-silence",
            "detail": (
                "ActionsEnabled=false live, and it appears in neither paused_alarms "
                "nor armed_alarms. A detector was muted with nothing recording who "
                "did it, why, or what would end it — and a muted alarm is "
                "indistinguishable from a healthy one on every surface the fleet "
                "reads. Either re-arm it, or add a paused_alarms entry naming the "
                "trigger(s) it watches, the owning issue and a re_exam date. "
                "observability-policy 8.3: DISABLED is DECLARED, never inferred."
            ),
        })

    for name in sorted(set(live) - declared_paused - declared_armed):
        out.append({
            "trigger": name, "surface": "cloudwatch", "kind": "alarm-undeclared",
            "detail": (
                "treat_missing_data=breaching, so it can latch ALARM on silence "
                "alone, but it appears in neither paused_alarms nor "
                "armed_alarms. Classify it: add it to paused_alarms with the "
                "trigger(s) it watches if it is quiet because they are paused, "
                "or to armed_alarms with a one-line reason if it watches "
                "something genuinely running. An unclassified alarm of this "
                "class is exactly what paged on 2026-08-14."
            ),
        })

    for name in sorted(declared_armed):
        if name not in live:
            out.append({
                "trigger": name, "surface": "cloudwatch", "kind": "armed-missing-in-aws",
                "detail": (
                    "declared in armed_alarms but no live alarm of that name has "
                    "treat_missing_data=breaching — it was deleted, renamed, or its "
                    "missing-data treatment was changed, and whatever it detected is "
                    "now undetected. Remove or correct the entry."
                ),
            })
        elif not live[name]:
            out.append({
                "trigger": name, "surface": "cloudwatch", "kind": "armed-but-silenced",
                "detail": (
                    "declared as deliberately ARMED, but ActionsEnabled=false — a "
                    "detector was muted by hand with no declaration saying why, and a "
                    "muted alarm is indistinguishable from a healthy one on every "
                    "surface. Either re-arm it, or move it to paused_alarms naming the "
                    "trigger(s) whose pause justifies the silence."
                ),
            })
    return out


#: The alert-class registry key this module pages under
#: (``infrastructure/overseer/playbooks.yaml`` → ``automation_pause_check``).
#: A literal, never ``__file__``: the default used to be ``__file__``, which in
#: CI is the runner's absolute checkout path
#: (``/home/runner/work/nousergon-data/nousergon-data/infrastructure/...``) — a
#: string that moves with the runner layout and that no registry row can key
#: on, so every page landed as registry drift (alpha-engine-config-I9409).
ALERT_SOURCE = "nousergon-data/infrastructure/automation_pause.py"


def alert_on_findings(findings: list[dict], source: str = ALERT_SOURCE) -> None:
    """Page independently of any groom or sweep consumer (alpha-engine-config-I8110).

    ``--check`` already runs inside ``sf-arn-drift-check.yml``'s daily cron —
    but that workflow's only reader, when a finding lands, is a GitHub Actions
    red X plus whatever downstream groom triages it, and the groom has been
    paused since 2026-08-12 (alpha-engine-config-I6984). The 2026-08-21
    drift (four schedules re-enabled, four alarms left muted) sat live for
    1h20m and was found by hand — ``--check`` would have caught it same-cycle
    had anything besides the paused groom been reading its output. This is
    that reader: it fans the finding out through ``krepis.alerts``, which pages
    Brian directly and needs no groom, no sweep, and no console reader in the
    loop.

    Severity is pinned to ``"error"`` deliberately, never ``"warning"``.
    alpha-engine-config-I7857 established that krepis.alerts delivers every
    severity to SNS/Telegram — nothing here is EVER dropped by severity choice
    — but only ``error``/``critical`` also trigger the Telegram phone push
    (``krepis.alerts.SEVERITY_PHONE_PUSH``). With the groom paused there is no
    other consumer watching the channel text, so the phone push is the only
    delivery this caller can rely on actually reaching Brian.

    ``krepis`` is imported lazily, inside this function, so that plain
    ``--check`` (the mode ``sf-arn-drift-check.yml`` already runs, with no
    krepis installed in that job) keeps working with nothing beyond the
    stdlib. Only the caller that opts into ``--alert-on-fail`` needs the
    dependency, and the workflow that passes that flag installs it.

    ``krepis.alerts.publish`` is best-effort and never raises on a channel
    failure (SNS/Telegram errors are caught and logged internally per its own
    docstring) — this function does not swallow anything further; a genuine
    defect here (e.g. krepis not installed despite the flag being passed)
    surfaces as an uncaught exception and a red job, which is the correct
    failure mode for a paging path that silently no-oped once already
    (config#1646).
    """
    from krepis.alerts import publish

    kinds = sorted(f"{f['kind']}:{f['surface']}:{f['trigger']}" for f in findings)
    kind_summary = ", ".join(sorted({f["kind"] for f in findings}))
    headline = f"automation_pause.py --check: {len(findings)} finding(s) — {kind_summary}"
    body = "\n".join(
        f"  [{f['kind']}] {f['surface']}:{f['trigger']} — {f['detail']}" for f in findings
    )
    publish(
        f"{headline}\n{body}",
        severity="error",
        source=source,
        # Keyed on the exact finding set, not a fixed string: a rerun within
        # the window that finds the SAME drift collapses to one page, but any
        # change to what is wrong (new trigger, cleared trigger, new kind)
        # mints a fresh key and pages again immediately rather than waiting
        # out the window. 180min is just under this check's own 4-hour
        # schedule, so an unresolved drift still pages on every cycle.
        dedup_key="automation-pause-check:" + ",".join(kinds),
        dedup_window_min=180,
    )


def enforce(alarms_only: bool = False) -> list[str]:
    """Drive live AWS to match the manifest.

    ``alarms_only`` restricts this to CloudWatch alarm-action state — never
    disables or enables a trigger. That is the mode safe to run unattended and
    on a schedule (see the CI wiring in sf-arn-drift-check.yml): touching only
    alarm actions cannot start scheduled work, the one thing full ``enforce()``
    deliberately never does automatically for a KEPT trigger.
    """
    acted: list[str] = []
    if not alarms_only:
        for surface, name, _ in paused_entries():
            state = _live_state(surface, name)
            if state is not None and state != "DISABLED":
                _disable(surface, name)
                acted.append(f"{surface}:{name}")

    live_all = _live_alarm_actions()
    for entry in alarm_entries():
        name = entry["name"]
        justified = alarm_justified(entry)
        live_entry = live_all.get(name)
        if live_entry is None:
            continue  # reported by alarm_findings(); nothing to act on
        if not live_entry["breaching"]:
            # notBreaching: see alarm_findings() / module docstring
            # (alpha-engine-config-I8712) — nothing to enforce, it cannot
            # false-page from the watched trigger's silence either way.
            continue
        live = live_entry["enabled"]
        if justified and live:
            _set_alarm_actions(name, enabled=False)
            acted.append(f"cloudwatch:{name}:disabled")
        elif not justified and not live:
            _set_alarm_actions(name, enabled=True)
            acted.append(f"cloudwatch:{name}:enabled")

    return acted


def main() -> int:
    ap = argparse.ArgumentParser(description="scheduled-automation pause check / enforce")
    mode = ap.add_mutually_exclusive_group(required=True)
    mode.add_argument("--check", action="store_true", help="verify the pause holds")
    mode.add_argument("--enforce", action="store_true", help="re-disable anything enabled")
    ap.add_argument("--json", action="store_true", help="machine-readable output")
    ap.add_argument("--alarms-only", action="store_true",
                     help="with --enforce, touch ONLY CloudWatch alarm-action state; "
                          "never disable or enable a trigger")
    ap.add_argument("--alert-on-fail", action="store_true",
                     help="with --check, page via krepis.alerts (severity=error) when "
                          "findings exist — independent of any groom/sweep consumer "
                          "(alpha-engine-config-I8110); requires krepis installed")
    args = ap.parse_args()
    if args.alarms_only and not args.enforce:
        ap.error("--alarms-only only makes sense with --enforce")
    if args.alert_on_fail and not args.check:
        ap.error("--alert-on-fail only makes sense with --check")

    entries = paused_entries()

    try:
        if args.enforce:
            acted = enforce(alarms_only=args.alarms_only)
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
        if findings and args.alert_on_fail:
            alert_on_findings(findings)
    except RuntimeError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2

    if args.json:
        print(json.dumps({"checked": len(entries),
                          "kept_checked": len(kept_names()),
                          "alarms_checked": len(alarm_entries()),
                          "armed_alarms_checked": len(armed_alarm_names()),
                          "findings": findings}, indent=2))
    else:
        kept = kept_names()
        alarms = alarm_entries()
        armed = armed_alarm_names()
        print(f"automation pause — {len(entries)} paused / {len(kept)} kept trigger(s), "
              f"{len(alarms)} silenced / {len(armed)} armed alarm(s) checked")
        if not findings:
            print("  ✓ every paused trigger exists live and is DISABLED")
            print("  ✓ every kept trigger exists live and is ENABLED")
            print("  ✓ every declared alarm's ActionsEnabled matches its current justification")
            print("  ✓ every breaching alarm live in CloudWatch is declared in one block "
                  "or the other")
            print("  ✓ every live ENABLED trigger in the account is declared paused or "
                  "kept, or is out of scope")
        for f in findings:
            print(f"  ✗ [{f['kind']}] {f['surface']}:{f['trigger']}")
            print(f"      {f['detail']}")

    return 1 if findings else 0


if __name__ == "__main__":
    sys.exit(main())
