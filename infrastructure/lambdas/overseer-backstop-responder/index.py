"""alpha-engine-overseer-backstop-responder — the dumb backstop's bounded
recovery attempt (alpha-engine-config-I4480, epic I2821).

overseer-policy.md §1 ("no failure class terminates in a notification"),
§4 inv. 1/3/16, §5 layer D, O3.

WHY. Degradation of the Overseer plane is the one class the Overseer cannot
cover for itself: its own response path is the thing that is down. Until now
the terminal state of that class was an SNS email. The 2026-07-27 audit found
`alpha-engine-watch-plane-overseer-intake-age` had been in ALARM for three
days with the alert-drain dead behind it and no autonomous response of any
kind; by 2026-07-28 that was five days. Detection worked perfectly throughout.
Nothing acted.

This Lambda sits between the backstop topic and the human, and does three
bounded things:

  1. **Reports state, always** — kill-switch values, intake depth AND age,
     last ledger per routed playbook, last probe invocation. The page becomes
     decision-shaped instead of an alarm name. Even when it takes no action,
     this alone converts a bare page into something answerable from a phone.
  2. **Attempts ONE recovery**, once per alarm per cooldown window, from a
     hardcoded reviewed allowlist: re-invoke the liveness probe, or re-dispatch
     one playbook through the router. Nothing else exists in the map, and an
     unmapped alarm reports without acting. It also declines to act when the
     target component's triggers are ALL paused in `automation_pause.json` —
     restarting automation an operator deliberately turned off is not recovery
     (alpha-engine-config-I7330).
  3. **Escalates on the second firing** inside the window — the attempt did not
     work, a human is needed now, and retrying would be a loop.

WHAT IT DELIBERATELY IS NOT
---------------------------
**Not an agent.** No LLM, no charter, no reasoning. The action set is a dict
literal reviewed in this file. §4 inv. 3 makes the backstop's dumbness
permanent, and an agent here would reintroduce exactly the silent-failure mode
the backstop exists to survive.

**Not dependent on the plane it rescues.** No bus, no intake queue, no S3
fallback prefix, no krepis, no flow-doctor, no nousergon-lib. Its only imports
are boto3 (present in the runtime) and the standard library — a third-party
dependency is one more thing that can fail non-obviously in the one component
that must not (§4 inv. 1, inv. 3). It MAY invoke the router as a recovery
action, but it never requires the router to be healthy in order to run or to
page: a failed invoke is a reported verdict, not an exception.

**Not a queue consumer.** It reads SQS *attributes* (depth, age of oldest) and
never receives or deletes a message. Nothing it does can lose an incident.

**Its paging path is independent of everything above.** Direct HTTPS to the
Telegram API. The SNS email subscription on the same topic still fires
independently, so the human has two paths that share no component — which is
the whole content of §5 layer D.

FAILURE POSTURE. Every gathering step and every action is individually
try/except'd, and its failure becomes a *line in the report* rather than an
exception. That is deliberate and is the documented deviation the
no-silent-fails rule requires: (a) swallowed = any AWS call failing during
evidence-gathering, (b) the primary deliverable — the page — must survive
partial blindness, because a backstop that crashes while reporting that the
plane is down is worse than one that reports "could not read X", (c) recording
surface = the page itself, which names every failed probe explicitly, plus
CloudWatch logs.
"""

from __future__ import annotations

import json
import os
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

import boto3

REGION = os.environ.get("AWS_REGION", "us-east-1")
ACCOUNT = os.environ.get("AWS_ACCOUNT_ID", "711398986525")

# Kill switch. Set to "false" to make this responder report-and-page only,
# without ever attempting a recovery action.
RECOVERY_ENABLED = os.environ.get("BACKSTOP_RECOVERY_ENABLED", "true").lower() == "true"

COOLDOWN_HOURS = int(os.environ.get("BACKSTOP_COOLDOWN_HOURS", "6"))
STATE_BUCKET = os.environ.get("BACKSTOP_STATE_BUCKET", "alpha-engine-research")
STATE_PREFIX = os.environ.get("BACKSTOP_STATE_PREFIX", "overseer/_backstop/attempts")

ROUTER_FUNCTION = os.environ.get(
    "OVERSEER_ROUTER_FUNCTION", "alpha-engine-overseer-dispatcher"
)
PROBE_FUNCTION = os.environ.get(
    "OVERSEER_PROBE_FUNCTION", "alpha-engine-overseer-liveness-probe"
)
# The probe's ONLY two triggers. Kept beside PROBE_FUNCTION rather than read
# out of ALARM_ACTIONS because `_probe_health()` runs on every firing, for every
# alarm, and must not depend on which alarm happened to fire.
PROBE_TRIGGERS = (
    "alpha-engine-overseer-liveness-0650-daily",
    "alpha-engine-overseer-liveness-1450-daily",
)

INTAKE_QUEUE = os.environ.get("OVERSEER_INTAKE_QUEUE", "nousergon-overseer-intake")
INTAKE_DLQ = os.environ.get("OVERSEER_INTAKE_DLQ", "nousergon-overseer-intake-dlq")

TELEGRAM_TOKEN_PARAM = os.environ.get(
    "TELEGRAM_TOKEN_PARAM", "/alpha-engine/TELEGRAM_BOT_TOKEN"
)
TELEGRAM_CHAT_PARAM = os.environ.get(
    "TELEGRAM_CHAT_PARAM", "/alpha-engine/TELEGRAM_CHAT_ID"
)

# ── The action allowlist ─────────────────────────────────────────────────────
# THE ENTIRE AUTHORITY OF THIS LAMBDA. Adding a row is a reviewed change, never
# a runtime decision. An alarm absent from this map is reported, not acted on —
# report-only is the safe default, and a missing row must never mean "improvise".
#
# `redispatch` payloads mirror what the scheduler sends, minus the run id (the
# executor mints its own). `is_drill` is always false: a drill would prove the
# pipe works while leaving the real backlog untouched.
#
# `triggers` names every schedule/rule the action's TARGET component runs on. It
# is what makes the pause check below possible: recovering a component whose own
# triggers an operator deliberately disabled is not recovery, it is restarting
# paused automation (alpha-engine-config-I7330). Names are pinned against
# automation_pause.json by test_handler.py — a typo here would silently disable
# the gate rather than fail it.
ALARM_ACTIONS: dict[str, dict] = {
    "alpha-engine-watch-plane-overseer-intake-age": {
        "action": "redispatch",
        "playbook": "alert-drain",
        "payload": {"trigger": "backstop-recovery", "is_drill": "false"},
        "rationale": "queue ageing means the drain is not draining — one dispatch",
        "component": "alert-drain",
        "triggers": [
            "alpha-engine-alert-drain-0400utc",
            "alpha-engine-alert-drain-1000utc",
            "alpha-engine-alert-drain-1600utc",
            "alpha-engine-alert-drain-2200utc",
        ],
    },
    "alpha-engine-watch-plane-overseer-intake-depth": {
        "action": "redispatch",
        "playbook": "alert-drain",
        "payload": {"trigger": "backstop-recovery", "is_drill": "false"},
        "rationale": "queue depth means the drain is not draining — one dispatch",
        "component": "alert-drain",
        "triggers": [
            "alpha-engine-alert-drain-0400utc",
            "alpha-engine-alert-drain-1000utc",
            "alpha-engine-alert-drain-1600utc",
            "alpha-engine-alert-drain-2200utc",
        ],
    },
    "alpha-engine-watch-plane-overseer-liveness-probe-errors": {
        "action": "invoke_probe",
        "rationale": "a probe erroring every invocation blinds the whole plane",
        "component": "overseer-liveness-probe",
        "triggers": [
            "alpha-engine-overseer-liveness-0650-daily",
            "alpha-engine-overseer-liveness-1450-daily",
        ],
    },
}

# Per-component dispatch flags to report. Component -> (function, variable).
#
# NOTE THE POLARITY. Every variable here is named `*_DISPATCH_ENABLED`, so
# `true` means the component WILL dispatch — it is an enable flag, not a kill
# switch, and the two read as exact opposites. This block was previously
# rendered under the heading "kill switches", which made `router: true` read as
# "the router is stopped" when it means "the router is running". On 2026-08-14
# that line was taken as proof the overseer plane was off and a P1 was filed
# against the wrong component (alpha-engine-config-I7330). `_dispatch_state()`
# now renders the semantic state and carries the raw variable alongside it, so
# the value can never disagree with its label again.
DISPATCH_FLAGS = [
    ("router", ROUTER_FUNCTION, "OVERSEER_DISPATCH_ENABLED"),
    ("alert-drain", "alpha-engine-alert-drain-dispatcher", "ALERT_DRAIN_DISPATCH_ENABLED"),
    ("ci-watch", "alpha-engine-ci-watch-dispatcher", "CI_WATCH_DISPATCH_ENABLED"),
    ("sf-watch", "alpha-engine-sf-watch-spot-dispatcher", "SF_WATCH_DISPATCH_ENABLED"),
]

# The automation pause manifest, bundled into the deploy package by deploy.sh.
#
# Read from disk, never from the network: this Lambda must not acquire a runtime
# dependency on the plane it rescues (see WHAT IT DELIBERATELY IS NOT, above).
# It mirrors overseer-liveness-probe/deploy.sh, which bundles playbooks.yaml for
# the same reason — a registry edit ships through the normal code path.
#
# Staleness is already covered: infrastructure/automation_pause.py --check runs
# in the daily drift sweep and fails in BOTH directions when the manifest and
# live AWS disagree, so a bundled copy that has drifted is a red check rather
# than a silent wrong answer here.
PAUSE_MANIFEST = Path(__file__).parent / "automation_pause.json"

# Playbook -> S3 prefix whose newest object proves the playbook last completed.
LEDGER_PREFIXES = {
    "alert-drain": "overseer/drain_ledger/",
    "dispatch-router": "overseer/dispatch_ledger/",
}


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


# ── Evidence gathering — every step degrades to a string, never raises ───────


def _dispatch_state() -> dict[str, str]:
    """Per-component dispatch state, rendered so the value agrees with the label.

    Returns e.g. ``{"router": "ENABLED (OVERSEER_DISPATCH_ENABLED=true)"}``. The
    raw variable stays in the string because it names the thing an operator would
    actually flip; the leading word is what the reader acts on. See the polarity
    note on DISPATCH_FLAGS for why this is not a raw boolean.
    """
    out: dict[str, str] = {}
    lam = boto3.client("lambda", region_name=REGION)
    for label, function, var in DISPATCH_FLAGS:
        try:
            cfg = lam.get_function_configuration(FunctionName=function)
            raw = cfg.get("Environment", {}).get("Variables", {}).get(var)
            if raw is None:
                out[label] = f"ENABLED ({var} unset — defaults to enabled)"
            elif str(raw).strip().lower() == "true":
                out[label] = f"ENABLED ({var}={raw})"
            else:
                out[label] = f"STOPPED ({var}={raw})"
        except Exception as exc:  # noqa: BLE001 — see FAILURE POSTURE
            out[label] = f"UNREADABLE: {type(exc).__name__}"
    return out


def _paused_trigger_names() -> set[str] | None:
    """Every trigger name the pause manifest says is deliberately off.

    Returns None when the manifest cannot be read or parsed. None means UNKNOWN,
    not "nothing is paused" — the caller acts and says so in the page, because a
    backstop that silently stops acting because it could not read a file is the
    failure mode this Lambda exists to survive. Both blocks count: ``paused`` is
    what is off now, ``pending`` is what must be born off.
    """
    try:
        manifest = json.loads(PAUSE_MANIFEST.read_text(encoding="utf-8"))
    except Exception as exc:  # noqa: BLE001 — see FAILURE POSTURE
        print(f"WARN: pause manifest unreadable ({type(exc).__name__}) — the "
              f"pause check cannot run; actions proceed and the page says so")
        return None
    names: set[str] = set()
    for surface in ("events_rules", "scheduler_schedules"):
        names |= set(manifest.get("paused", {}).get(surface, {}))
    names |= {k for k in manifest.get("pending", {}) if not k.startswith("_")}
    return names


def _pause_verdict(spec: dict) -> str | None:
    """Return a skip reason when the action's target component is fully paused.

    ALL of the component's triggers must be paused. A component with even one
    live trigger is still running on its own cadence, so a recovery dispatch is
    still the right response to its alarm — it is only when an operator has
    turned every trigger off that acting means restarting paused automation.
    """
    triggers = spec.get("triggers") or []
    if not triggers:
        return None
    paused = _paused_trigger_names()
    if paused is None:
        return None
    if all(t in paused for t in triggers):
        component = spec.get("component") or spec["action"]
        return (f"{component} is paused in automation_pause.json "
                f"({len(triggers)} trigger(s), all disabled) — reporting only")
    return None


def _queue_age_seconds(queue_name: str) -> int | None:
    """Age of the oldest message, from CloudWatch.

    `ApproximateAgeOfOldestMessage` is an AWS/SQS **metric**, NOT a
    GetQueueAttributes attribute — asking SQS for it returns
    `InvalidAttributeName` and, because attribute names are validated as a set,
    it takes the depth read down with it. Found by live-invoking the deployed
    function (overseer-policy §4 inv. 14); the unit fakes had happily returned
    a value for it.

    Age is the load-bearing half: a queue being drained and refilled looks
    identical to one that is not being drained until you look at age (§5 C).
    """
    cw = boto3.client("cloudwatch", region_name=REGION)
    now = _utcnow()
    resp = cw.get_metric_statistics(
        Namespace="AWS/SQS",
        MetricName="ApproximateAgeOfOldestMessage",
        Dimensions=[{"Name": "QueueName", "Value": queue_name}],
        StartTime=now - timedelta(minutes=30),
        EndTime=now,
        Period=300,
        Statistics=["Maximum"],
    )
    pts = sorted(resp.get("Datapoints") or [], key=lambda p: p["Timestamp"])
    return int(pts[-1]["Maximum"]) if pts else None


def _queue_state() -> dict[str, str]:
    out: dict[str, str] = {}
    for label, name in (("intake", INTAKE_QUEUE), ("dlq", INTAKE_DLQ)):
        depth = "?"
        try:
            sqs = boto3.client("sqs", region_name=REGION)
            url = sqs.get_queue_url(QueueName=name)["QueueUrl"]
            attrs = sqs.get_queue_attributes(
                QueueUrl=url,
                AttributeNames=[
                    "ApproximateNumberOfMessages",
                    "ApproximateNumberOfMessagesNotVisible",
                ],
            )["Attributes"]
            depth = (
                f"{attrs.get('ApproximateNumberOfMessages', '?')} visible / "
                f"{attrs.get('ApproximateNumberOfMessagesNotVisible', '?')} in flight"
            )
        except Exception as exc:  # noqa: BLE001 — see FAILURE POSTURE
            out[label] = f"UNREADABLE: {type(exc).__name__}"
            continue
        # Age is gathered separately so a CloudWatch failure cannot cost us the
        # depth reading, and vice versa. Two facts, two failure domains.
        try:
            age = _queue_age_seconds(name)
            age_str = "n/a" if age is None else f"{age // 3600}h{(age % 3600) // 60:02d}m"
        except Exception as exc:  # noqa: BLE001 — see FAILURE POSTURE
            age_str = f"UNREADABLE ({type(exc).__name__})"
        out[label] = f"{depth} / oldest {age_str}"
    return out


def _last_ledger_ages() -> dict[str, str]:
    """Newest object under each ledger prefix, as an age.

    Deliberately an AGE, not a timestamp: 'last drain ledger 4h ago' is
    actionable on a phone; an ISO timestamp requires arithmetic at 3am.
    """
    out: dict[str, str] = {}
    s3 = boto3.client("s3", region_name=REGION)
    now = _utcnow()
    for playbook, prefix in LEDGER_PREFIXES.items():
        try:
            newest = None
            # Date-partitioned prefixes: walk at most three day-partitions, so
            # this stays bounded regardless of how much history accumulates —
            # a backstop must not get slower as the fleet gets older.
            for delta in (0, 1, 2):
                day = (now - timedelta(days=delta)).strftime("%Y-%m-%d")
                resp = s3.list_objects_v2(
                    Bucket=STATE_BUCKET, Prefix=f"{prefix}{day}/"
                )
                for obj in resp.get("Contents") or []:
                    if newest is None or obj["LastModified"] > newest:
                        newest = obj["LastModified"]
                if newest is not None:
                    break
            if newest is None:
                out[playbook] = "NONE in the last 3 days"
            else:
                mins = int((now - newest).total_seconds() // 60)
                out[playbook] = f"{mins // 60}h{mins % 60:02d}m ago"
        except Exception as exc:  # noqa: BLE001 — see FAILURE POSTURE
            out[playbook] = f"UNREADABLE: {type(exc).__name__}"
    return out


def _probe_health() -> str:
    try:
        cw = boto3.client("cloudwatch", region_name=REGION)
        now = _utcnow()
        stats = {}
        for metric in ("Invocations", "Errors"):
            resp = cw.get_metric_statistics(
                Namespace="AWS/Lambda",
                MetricName=metric,
                Dimensions=[{"Name": "FunctionName", "Value": PROBE_FUNCTION}],
                StartTime=now - timedelta(hours=24),
                EndTime=now,
                Period=86400,
                Statistics=["Sum"],
            )
            pts = resp.get("Datapoints") or []
            stats[metric] = int(sum(p["Sum"] for p in pts))
        inv, err = stats.get("Invocations", 0), stats.get("Errors", 0)
        if inv == 0:
            # A deliberately-disabled component is STATE, not an incident
            # (overseer-policy §7). This Lambda already carries the pause
            # manifest on disk for `_pause_verdict()`; not consulting it here
            # meant a probe Brian switched off on 2026-08-07 was reported as
            # "not running at all" in every page for two weeks, which reads as
            # a plane-wide blindness rather than as his own ruling.
            paused = _paused_trigger_names()
            if paused is None:
                return ("NOT INVOKED in 24h — and the pause manifest is "
                        "UNREADABLE, so whether that is deliberate is unknown")
            if all(t in paused for t in PROBE_TRIGGERS):
                return ("NOT INVOKED in 24h — PAUSED by declaration, both "
                        "triggers off in automation_pause.json. Not running "
                        "BY DESIGN, not a fault")
            return "NOT INVOKED in 24h — the probe is not running at all"
        if err >= inv:
            return f"{inv} invocations, {err} errors in 24h — failing EVERY run"
        return f"{inv} invocations, {err} errors in 24h"
    except Exception as exc:  # noqa: BLE001 — see FAILURE POSTURE
        return f"UNREADABLE: {type(exc).__name__}"


# ── Cooldown ────────────────────────────────────────────────────────────────


def _window_start(now: datetime) -> str:
    """Floor `now` to the cooldown window. Deterministic and stateless — two
    firings in the same window map to the same key without any clock state."""
    hours = (now.hour // COOLDOWN_HOURS) * COOLDOWN_HOURS
    return now.replace(hour=hours, minute=0, second=0, microsecond=0).strftime(
        "%Y-%m-%dT%H%MZ"
    )


def _attempt_key(alarm: str, now: datetime) -> str:
    safe = urllib.parse.quote(alarm, safe="")
    return f"{STATE_PREFIX}/{safe}/{_window_start(now)}.json"


def _is_missing_key(exc: Exception) -> bool:
    """True when `exc` means "that S3 key does not exist" — matched by name and
    error code rather than by exception class (see _claim_attempt)."""
    if type(exc).__name__ in ("NoSuchKey", "404"):
        return True
    code = getattr(exc, "response", {}).get("Error", {}).get("Code")
    return code in ("NoSuchKey", "404", "NotFound")


def _claim_attempt(alarm: str, now: datetime) -> tuple[bool, dict | None]:
    """Return (is_first_in_window, prior_record).

    Fails OPEN on a state-store error — if S3 is unreadable we would rather
    make one extra bounded recovery attempt than make none. Both branches are
    named in the page, so the human sees which happened.
    """
    key = _attempt_key(alarm, now)
    try:
        s3 = boto3.client("s3", region_name=REGION)
    except Exception as exc:  # noqa: BLE001 — see FAILURE POSTURE / fail-open
        print(f"WARN: no s3 client ({type(exc).__name__}) — treating as first "
              f"attempt in window")
        return True, None
    try:
        obj = s3.get_object(Bucket=STATE_BUCKET, Key=key)
        return False, json.loads(obj["Body"].read())
    except Exception as exc:  # noqa: BLE001 — see FAILURE POSTURE / fail-open
        # Detect "absent" by error IDENTITY, not by class: referencing
        # `s3.exceptions.NoSuchKey` requires the client to be healthy enough to
        # expose its exception factory, which is exactly what is not guaranteed
        # in the one component that must survive a degraded account. Caught by
        # test_page_still_sent_when_every_evidence_call_fails.
        if not _is_missing_key(exc):
            print(f"WARN: cooldown state unreadable ({type(exc).__name__}) — "
                  f"treating as first attempt in window")
        # Fall THROUGH to the claim write in both cases. Returning early on an
        # unreadable read would skip the claim, so every subsequent firing would
        # also read-fail and also act — an unbounded retry loop in the component
        # whose entire contract is "one bounded attempt". Caught by
        # test_second_firing_in_window_escalates_instead_of_retrying.
    try:
        s3.put_object(
            Bucket=STATE_BUCKET, Key=key,
            Body=json.dumps({"alarm": alarm, "claimed_at": now.isoformat()}).encode(),
            ContentType="application/json",
        )
    except Exception as exc:  # noqa: BLE001 — see FAILURE POSTURE
        print(f"WARN: cooldown claim write failed ({type(exc).__name__}) — a "
              f"second firing this window may retry")
    return True, None


# ── The two permitted actions ───────────────────────────────────────────────


def _do_invoke_probe() -> dict:
    lam = boto3.client("lambda", region_name=REGION)
    resp = lam.invoke(
        FunctionName=PROBE_FUNCTION, InvocationType="RequestResponse",
        Payload=b"{}",
    )
    body = resp["Payload"].read()[:500].decode("utf-8", "replace")
    return {
        "ok": not resp.get("FunctionError"),
        "function_error": resp.get("FunctionError"),
        "response": body,
    }


def _do_redispatch(playbook: str, payload: dict) -> dict:
    lam = boto3.client("lambda", region_name=REGION)
    resp = lam.invoke(
        FunctionName=ROUTER_FUNCTION, InvocationType="RequestResponse",
        Payload=json.dumps({"playbook": playbook, "payload": payload}).encode(),
    )
    body = resp["Payload"].read()[:800].decode("utf-8", "replace")
    try:
        verdict = json.loads(body)
    except json.JSONDecodeError:
        verdict = {"unparseable": body}
    return {
        "ok": not resp.get("FunctionError") and verdict.get("routed") is True,
        "function_error": resp.get("FunctionError"),
        "verdict": verdict,
    }


def _attempt(spec: dict) -> dict:
    """Dispatch one allowlisted action. Never raises — a failed recovery is a
    reported verdict, because the page must go out either way."""
    try:
        if spec["action"] == "invoke_probe":
            return {"attempted": "invoke_probe", **_do_invoke_probe()}
        if spec["action"] == "redispatch":
            return {
                "attempted": f"redispatch:{spec['playbook']}",
                **_do_redispatch(spec["playbook"], spec["payload"]),
            }
        return {"attempted": None, "ok": False,
                "error": f"unknown action {spec['action']!r} in the allowlist"}
    except Exception as exc:  # noqa: BLE001 — see FAILURE POSTURE
        return {"attempted": spec.get("action"), "ok": False,
                "error": f"{type(exc).__name__}: {exc}"}


# ── Paging — independent of every component above ───────────────────────────


def _telegram(text: str) -> bool:
    try:
        ssm = boto3.client("ssm", region_name=REGION)
        token = ssm.get_parameter(Name=TELEGRAM_TOKEN_PARAM, WithDecryption=True)[
            "Parameter"]["Value"]
        chat = ssm.get_parameter(Name=TELEGRAM_CHAT_PARAM, WithDecryption=True)[
            "Parameter"]["Value"]
        data = urllib.parse.urlencode({
            "chat_id": chat, "text": text[:4000], "disable_notification": "false",
        }).encode()
        req = urllib.request.Request(
            f"https://api.telegram.org/bot{token}/sendMessage", data=data,
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status == 200
    except Exception as exc:  # noqa: BLE001 — SNS email on the same topic is the other path
        print(f"ERROR: BACKSTOP_PAGE_FAILED {type(exc).__name__}: {exc} — the SNS "
              f"email subscription on the same topic is the surviving path")
        return False


# How each CloudWatch state renders. ALARM is the only one that is an incident.
#
# The responder used to read `AlarmName` and `NewStateReason` and nothing else,
# so every transition — including a recovery — rendered as "🚨 OVERSEER
# BACKSTOP" with the full plane-state dump beneath it. 45 alarms in this
# account carry `OKActions` pointing at the backstop topic, so this was a
# 45-alarm class, not one alarm's quirk. Measured instance: on 2026-08-21 the
# newly-created `alpha-engine-pipeline-deadman-weekly-freshness` went
# INSUFFICIENT_DATA -> OK at 15:10 PDT and paged as an emergency. CloudWatch's
# OK-reason text reuses the phrase "Threshold Crossed", so the reason string
# alone cannot tell a reader which direction it moved — the state field is the
# only thing that can, and it was the one field not being read.
STATE_RENDER = {
    "ALARM": ("🚨 OVERSEER BACKSTOP", True),
    "OK": ("🟢 BACKSTOP RECOVERED", False),
    "INSUFFICIENT_DATA": ("⚪ BACKSTOP — no data", False),
}


def _render(alarm: str, reason: str, evidence: dict, outcome: dict,
            state: str = "ALARM") -> str:
    header, is_incident = STATE_RENDER.get(state, ("🔔 BACKSTOP", True))

    if not is_incident:
        # A recovery needs the fact and nothing else. The plane-state dump and
        # the RECOVERY block exist to help someone act; on a green transition
        # there is nothing to act on, and printing them anyway is what made a
        # recovery indistinguishable from an outage at a glance.
        return "\n".join([
            f"{header} — {alarm}",
            "",
            f"State is now {state}. No action taken and none needed.",
            "",
            f"Reason: {reason or '(none given)'}",
            "",
            "Auto-fix: none needed (recovered).",
        ])

    lines = [
        f"{header} — {alarm}",
        "",
        f"Alarm reason: {reason or '(none given)'}",
        "",
        "PLANE STATE",
        f"  intake: {evidence['queues'].get('intake')}",
        f"  dlq:    {evidence['queues'].get('dlq')}",
        f"  probe:  {evidence['probe']}",
    ]
    lines.append("  last ledger:")
    for k, v in evidence["ledgers"].items():
        lines.append(f"    {k}: {v}")
    lines.append("  dispatch flags:")
    for k, v in evidence["dispatch"].items():
        lines.append(f"    {k}: {v}")
    lines += ["", "RECOVERY"]
    if outcome.get("escalated"):
        lines += [
            "  ⚠️ SECOND firing inside the cooldown window — the earlier",
            "     recovery attempt did NOT fix this. Not retrying.",
            "     A human is needed now.",
            f"  earlier attempt: {json.dumps(outcome.get('prior') or {})[:300]}",
        ]
    elif outcome.get("skipped"):
        lines.append(f"  no action: {outcome['skipped']}")
    else:
        res = outcome.get("result") or {}
        lines += [
            f"  attempted: {res.get('attempted')}",
            f"  result:    {'OK' if res.get('ok') else 'FAILED'}",
            f"  detail:    {json.dumps({k: v for k, v in res.items() if k not in ('attempted', 'ok')})[:600]}",
        ]
    # Every Telegram message says what is being fixed automatically
    # (nous-ergon-ops observability-policy § Notification — auto-fix line).
    lines += ["", _auto_fix_line(outcome)]
    return "\n".join(lines)


def _auto_fix_line(outcome: dict) -> str:
    """The ``Auto-fix:`` line, from what this responder actually did."""
    if outcome.get("escalated"):
        return ("Auto-fix: none left. The automatic recovery already ran once "
                "and did not fix it, so it is not retried; tonight's nightly "
                "sweep (7pm PT) takes it.")
    if outcome.get("skipped"):
        return (f"Auto-fix: none ({outcome['skipped']}); tonight's nightly "
                "sweep (7pm PT) picks it up.")
    res = outcome.get("result") or {}
    verdict = "succeeded" if res.get("ok") else "failed"
    return (f"Auto-fix: this responder ran one bounded recovery "
            f"({res.get('attempted')}) and it {verdict}. If the alarm fires "
            "again inside the cooldown it escalates instead of retrying.")


def handler(event: dict, context) -> dict:  # noqa: ARG001 — Lambda contract
    now = _utcnow()
    # Default ALARM: an event we could not parse is treated as an incident, not
    # silently downgraded to a recovery. Failing toward the loud side is the
    # posture the rest of this Lambda already takes.
    alarm, reason, state = "(unknown)", "", "ALARM"
    try:
        record = (event.get("Records") or [{}])[0]
        msg = json.loads(record.get("Sns", {}).get("Message") or "{}")
        alarm = msg.get("AlarmName") or "(unknown)"
        reason = msg.get("NewStateReason") or ""
        state = msg.get("NewStateValue") or "ALARM"
    except Exception as exc:  # noqa: BLE001 — page anyway; a malformed event is itself news
        reason = f"(could not parse SNS message: {type(exc).__name__})"

    # A non-ALARM transition gathers no evidence and attempts no recovery.
    #
    # This returns BEFORE `_claim_attempt()` deliberately. The cooldown window
    # is what makes a second ALARM inside it escalate as "the earlier recovery
    # did NOT fix this"; letting an OK transition consume that window would
    # make the next real firing look like a first attempt, or make a first
    # attempt look like a second. A recovery must not spend the incident
    # budget of the incident it is ending.
    if state != "ALARM":
        text = _render(alarm, reason, {}, {}, state=state)
        print(text)
        return {"alarm": alarm, "state": state, "paged": _telegram(text),
                "evidence": {}, "outcome": {"skipped": f"state={state} — "
                                            f"not an incident"}}

    evidence = {
        "dispatch": _dispatch_state(),
        "queues": _queue_state(),
        "ledgers": _last_ledger_ages(),
        "probe": _probe_health(),
    }

    spec = ALARM_ACTIONS.get(alarm)
    pause_skip = _pause_verdict(spec) if spec else None
    if spec is None:
        outcome = {"skipped": "alarm has no entry in the reviewed action "
                              "allowlist — reporting only"}
    elif not RECOVERY_ENABLED:
        outcome = {"skipped": "BACKSTOP_RECOVERY_ENABLED=false (operator "
                              "kill switch) — reporting only"}
    elif pause_skip is not None:
        # Checked BEFORE _claim_attempt: a paused component must not consume its
        # own cooldown window, or the first firing after the pause lifts would
        # escalate as a "second firing" without a first attempt ever happening.
        outcome = {"skipped": pause_skip}
    else:
        first, prior = _claim_attempt(alarm, now)
        if not first:
            outcome = {"escalated": True, "prior": prior}
        else:
            outcome = {"result": _attempt(spec), "rationale": spec["rationale"]}

    text = _render(alarm, reason, evidence, outcome, state=state)
    print(text)
    paged = _telegram(text)
    return {
        "alarm": alarm, "state": state, "paged": paged,
        "evidence": evidence, "outcome": outcome,
        "window": _window_start(now),
    }
