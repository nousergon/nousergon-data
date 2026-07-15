"""alpha-engine-canary-replay-liveness-probe — the Thursday-path watchdog
FOR the Saturday-replay canary itself (alpha-engine-config#2246).

WHY: the Thursday canary is the ONLY thing that would otherwise notice a
regression before Saturday — but nothing watches the canary's OWN dispatch.
If ``canary-replay-dispatcher`` silently fails to launch, or the spot box
dies before writing its completion marker, the canary's absence looks
identical to "nothing scheduled today" from the outside. This probe is the
external observer of that producer (mirrors the groom/sf-watch liveness
probes' philosophy — a schedule-driven check external to the thing it's
watching, since a dead process cannot reliably report its own death).

MUCH SIMPLER than sf-watch-liveness-probe/ci-watch-dispatcher's liveness
counterpart: this canary has exactly ONE scheduled trigger (the Thursday
EventBridge rule) and ONE deterministic completion-marker key per ISO week
(``_scheduled_run_token`` — duplicated here in lockstep from
canary-replay-dispatcher/index.py rather than imported, since Lambdas
deploy independently; a lockstep test in this file pins the two copies
equal). No dispatch-pipe wiring to verify, no reclaim/relaunch machinery,
no per-pipeline sweep — just: did this week's canary run, and did it pass?

SELF-GATING SCHEDULE: runs on a tight recurring cron (every 15 min) rather
than a single precisely-timed one-shot, and no-ops outside the check
window — [CHECK_START_MINUTES_AFTER_DISPATCH, CHECK_WINDOW_HOURS] after the
most recent Thursday 09:00 UTC dispatch. This absorbs spot-boot + probe
runtime variance without needing exact timing, and naturally re-checks a
transient S3 read hiccup on the next tick.

PAGE ON: (a) the marker never appears within the window (canary silently
failed to dispatch, or the box died before writing it), or (b) the marker
appears with ``overall_status: FAIL`` (a real probe regression — the
canary did its job). Uses ``nousergon_lib.alerts.publish``'s built-in
``dedup_key`` (keyed on run_token, which is already unique per ISO week) —
no hand-rolled fingerprint/clear state needed; next week's token naturally
resets dedup.

DISABLED-DISPATCHER CARVE-OUT: before treating a missing marker as an
incident, this probe reads the Thursday EventBridge rule's live State. A
DISABLED dispatcher rule means no dispatch was ever supposed to fire — a
deliberate operator disable is state, not an incident (mirrors
sf-watch-liveness-probe's kill-switch posture). This is also the deploy
SEQUENCING mechanism: this Lambda + its own (harmless, read-only) schedule
can go live FIRST while the dispatcher's rule is still DISABLED — it will
no-op cleanly every week instead of paging — and the dispatcher rule is
enabled only once this probe is confirmed live (deploy.sh header).

Fail-loud (CLAUDE.md no-silent-fails): an S3 read error OTHER than a clean
"object doesn't exist" RAISES, surfacing via the Lambda Errors metric,
rather than being silently read as "no marker yet."
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta, timezone

import boto3
from nousergon_lib import alerts

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

REGION = os.environ.get("AWS_REGION", "us-east-1")
MARKER_BUCKET = os.environ.get("CANARY_REPLAY_MARKER_BUCKET", "alpha-engine-research")
DISPATCH_RULE_NAME = os.environ.get(
    "CANARY_REPLAY_DISPATCH_RULE_NAME", "alpha-engine-canary-replay-thursday"
)

DISPATCH_HOUR_UTC = int(os.environ.get("CANARY_REPLAY_DISPATCH_HOUR_UTC", "9"))
# Nominal probe runtime is ~5-10 min + ~1-2 min spot boot; 30 min floor gives
# ample margin before the FIRST check, so a normal run is never flagged
# mid-flight.
CHECK_START_MINUTES_AFTER_DISPATCH = int(
    os.environ.get("CANARY_REPLAY_CHECK_START_MINUTES", "30")
)
# 20h ceiling keeps checking (idempotently) until Friday ~05:00 UTC —
# comfortably before Saturday's weekly run, so a late-discovered failure
# still leaves a full business day to investigate.
CHECK_WINDOW_HOURS = float(os.environ.get("CANARY_REPLAY_CHECK_WINDOW_HOURS", "20"))


def _scheduled_run_token(dispatch_time: datetime) -> str:
    """MUST stay in lockstep with canary-replay-dispatcher/index.py's
    identically-named function — a regression test in this file pins both
    copies' source text equal."""
    iso_year, iso_week, _ = dispatch_time.isocalendar()
    return f"sched-{iso_year}w{iso_week:02d}"


def _most_recent_thursday_dispatch(now: datetime) -> datetime:
    """The most recent Thursday DISPATCH_HOUR_UTC:00 at-or-before `now`."""
    days_since_thursday = (now.weekday() - 3) % 7  # Monday=0 .. Thursday=3 .. Sunday=6
    candidate = (now - timedelta(days=days_since_thursday)).replace(
        hour=DISPATCH_HOUR_UTC, minute=0, second=0, microsecond=0
    )
    if candidate > now:
        candidate -= timedelta(days=7)
    return candidate


def _error_code(exc: Exception) -> str:
    return str(getattr(exc, "response", {}).get("Error", {}).get("Code", ""))


def _dispatch_rule_enabled() -> bool:
    """True iff the Thursday dispatch rule is ENABLED. A missing rule reads
    as disabled (nothing to check yet — the rule is created before this
    Lambda's own schedule per deploy.sh's bootstrap order, but a defensive
    read here costs nothing). Any OTHER describe_rule error RAISES
    (fail-loud — misreading this as 'disabled' would silently suppress a
    real incident)."""
    try:
        rule = boto3.client("events", region_name=REGION).describe_rule(Name=DISPATCH_RULE_NAME)
    except Exception as exc:  # noqa: BLE001 — inspect code below; re-raise if unexpected
        if _error_code(exc) == "ResourceNotFoundException":
            return False
        raise
    return rule.get("State") == "ENABLED"


def _read_marker(run_token: str) -> dict | None:
    """The completion marker for `run_token`, or None if it genuinely
    doesn't exist yet. Any OTHER S3 error RAISES (fail-loud — see module
    docstring)."""
    key = f"tmp/canary/{run_token}.json"
    try:
        obj = boto3.client("s3", region_name=REGION).get_object(Bucket=MARKER_BUCKET, Key=key)
    except Exception as exc:  # noqa: BLE001 — inspect code below; re-raise if unexpected
        if _error_code(exc) in {"NoSuchKey", "404"}:
            return None
        raise
    return json.loads(obj["Body"].read())


ALERTS_SNS_TOPIC_ARN = os.environ.get(
    "CANARY_REPLAY_ALERTS_SNS_TOPIC_ARN",
    f"arn:aws:sns:{REGION}:711398986525:alpha-engine-alerts",
)


def _page(message: str, run_token: str) -> None:
    # Explicit sns_topic_arn avoids alerts.publish's default dynamic
    # sts:GetCallerIdentity resolution — tighter, more auditable IAM policy
    # for this Lambda (see iam-policy.json).
    alerts.publish(
        message,
        severity="critical",
        source="canary-replay-liveness-probe",
        dedup_key=f"canary-replay:{run_token}",
        sns_topic_arn=ALERTS_SNS_TOPIC_ARN,
    )


def handler(event: dict, context) -> dict:  # noqa: ARG001 — Lambda contract
    """Scheduled entrypoint. Read-only + self-gating — see module docstring.
    Raises on an unexpected AWS API failure so the check can never silently
    no-op on a real error."""
    now = datetime.now(timezone.utc)
    dispatch_time = _most_recent_thursday_dispatch(now)
    elapsed = now - dispatch_time
    window_start = timedelta(minutes=CHECK_START_MINUTES_AFTER_DISPATCH)
    window_end = timedelta(hours=CHECK_WINDOW_HOURS)

    if not (window_start <= elapsed <= window_end):
        logger.info(
            "canary-replay liveness: outside check window (dispatch=%s, elapsed=%s) — no-op",
            dispatch_time.isoformat(), elapsed,
        )
        return {"checked": False, "reason": "outside_check_window",
                "elapsed_seconds": elapsed.total_seconds()}

    if not _dispatch_rule_enabled():
        # Deliberate operator disable (or not-yet-enabled during rollout) is
        # STATE, not an incident — see module docstring's disabled-dispatcher
        # carve-out. No dispatch was ever supposed to fire this week.
        logger.info(
            "canary-replay liveness: dispatch rule '%s' is disabled — nothing "
            "to check this week", DISPATCH_RULE_NAME,
        )
        return {"checked": False, "reason": "dispatch_rule_disabled"}

    run_token = _scheduled_run_token(dispatch_time)
    marker = _read_marker(run_token)

    if marker is None:
        logger.warning(
            "canary-replay liveness: NO completion marker for %s after %s — paging",
            run_token, elapsed,
        )
        _page(
            f"\U0001f6a8 Saturday-replay canary — NO completion marker for `{run_token}` "
            f"{elapsed} after its Thursday {DISPATCH_HOUR_UTC:02d}:00 UTC dispatch. The "
            "canary either failed to launch or its spot box died before finishing. "
            "Check alpha-engine-canary-replay-dispatcher's CloudWatch logs.",
            run_token,
        )
        return {"checked": True, "run_token": run_token, "marker_found": False, "paged": True}

    overall_status = marker.get("overall_status")
    if overall_status != "PASS":
        failed_probes = [p.get("name") for p in marker.get("probes", []) if p.get("status") != "PASS"]
        logger.warning(
            "canary-replay liveness: %s completed with overall_status=%s (failed probes: %s) — paging",
            run_token, overall_status, failed_probes,
        )
        _page(
            f"\U0001f6a8 Saturday-replay canary `{run_token}` completed with "
            f"overall_status={overall_status!r} — failed probe(s): {failed_probes}. "
            "This is exactly the bug class the canary exists to catch BEFORE "
            "Saturday's weekly run — investigate before then.",
            run_token,
        )
        return {"checked": True, "run_token": run_token, "marker_found": True,
                "overall_status": overall_status, "paged": True}

    logger.info("canary-replay liveness: %s PASS — all clean", run_token)
    return {"checked": True, "run_token": run_token, "marker_found": True,
            "overall_status": "PASS", "paged": False}
