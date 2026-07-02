"""alpha-engine-weekly-schedule-adjuster — hold the weekly research SF to the
day AFTER the last NYSE trading day of the week.

Normal weeks -> the Saturday cron (``alpha-engine-saturday``) fires as always.
Trailing-holiday weeks (Good Friday; a Friday-observed July-4 / Christmas) ->
the week's last trading day is Thursday, so the run shifts ONE DAY EARLIER to
Friday: this reconciler disables the Saturday cron for that week and stands up
a one-shot rule on the run day (byte-identical weekly input). Rationale: the
research inputs are trading-day-gated (frozen at the last close), so running
later in a market-closed weekend gains no freshness — run as early as the last
trading day's data has settled (T+1), which is Saturday normally and Friday on
a trailing-holiday week.

WEEKLY RECONCILER, not a daily gate. Runs mid-week (Wed) and reconciles the
LIVE schedule to the calendar for the CURRENT week's weekend:

  * ``run_day == Saturday``  -> ensure Saturday cron ENABLED + drop stale one-shots
  * ``run_day  < Saturday``  -> ensure Saturday cron DISABLED + one-shot on run_day

FAIL-SAFE (the whole point of a reconciler over a gate): the Saturday cron is
the UNTOUCHED baseline. If this Lambda never runs, or errors, the Saturday cron
stays in whatever state it was — a normal week leaves it ENABLED, so the weekly
run STILL happens Saturday. A broken adjuster degrades to the normal Saturday
run, NEVER to a missed run. (A daily gate has the opposite failure mode: a
broken gate = missed run.) On the holiday branch the one-shot is created BEFORE
the Saturday cron is disabled, so a mid-run failure also leaves Saturday firing.

Managed OUTSIDE CloudFormation (operator-deployed via deploy.sh) — same
rationale as the sibling event Lambdas (eod-success-friday-shell-trigger,
sf-telegram-notifier, spot-orphan-reaper): keeps the github-actions-lambda-deploy
OIDC role's blast radius narrow.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import date, datetime, timedelta, timezone

import boto3

# Vendored NYSE trading calendar. The Lambda needs ONLY "is this date an NYSE
# session?", and the canonical ``nousergon_lib`` is now mypyc/pydantic-compiled
# (platform-specific ``.so``), so it cannot be bundled into a Lambda zip from a
# dev Mac (linux/py3.12 mismatch — the built wheels are darwin/py3.14). We vendor
# the static NYSE holiday set (through 2030, verbatim from
# ``nousergon_lib.trading_calendar.NYSE_HOLIDAYS``) + a pure-Python session check.
# test_handler.py::test_vendored_holidays_match_lib asserts this stays in
# lockstep with the lib (drift guard), so the copy can't silently diverge.
_NYSE_HOLIDAYS = frozenset({
    date(2026, 1, 1), date(2026, 1, 19), date(2026, 2, 16), date(2026, 4, 3),
    date(2026, 5, 25), date(2026, 6, 19), date(2026, 7, 3), date(2026, 9, 7),
    date(2026, 11, 26), date(2026, 12, 25),
    date(2027, 1, 1), date(2027, 1, 18), date(2027, 2, 15), date(2027, 3, 26),
    date(2027, 5, 31), date(2027, 6, 18), date(2027, 7, 5), date(2027, 9, 6),
    date(2027, 11, 25), date(2027, 12, 24),
    date(2028, 1, 17), date(2028, 2, 21), date(2028, 4, 14), date(2028, 5, 29),
    date(2028, 6, 19), date(2028, 7, 4), date(2028, 9, 4), date(2028, 11, 23),
    date(2028, 12, 25),
    date(2029, 1, 1), date(2029, 1, 15), date(2029, 2, 19), date(2029, 3, 30),
    date(2029, 5, 28), date(2029, 6, 19), date(2029, 7, 4), date(2029, 9, 3),
    date(2029, 11, 22), date(2029, 12, 25),
    date(2030, 1, 1), date(2030, 1, 21), date(2030, 2, 18), date(2030, 4, 19),
    date(2030, 5, 27), date(2030, 6, 19), date(2030, 7, 4), date(2030, 9, 2),
    date(2030, 11, 28), date(2030, 12, 25),
})


def is_trading_day(d: date) -> bool:
    """NYSE session? A weekday that is not a holiday. (Early-close half-days
    count as sessions — correct for 'last trading day of the week'.) The holiday
    set covers through 2030; extend it (and the drift-guard test) before then.
    """
    return d.weekday() < 5 and d not in _NYSE_HOLIDAYS


logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

REGION = os.environ.get("AWS_REGION", "us-east-1")
ACCOUNT_ID = os.environ.get("ACCOUNT_ID", "711398986525")

SATURDAY_RULE = "alpha-engine-saturday"          # the normal weekly cron (CFN-owned)
ONESHOT_PREFIX = "alpha-engine-weekly-oneshot-"  # <prefix>YYYYMMDD, created per holiday week
SATURDAY_WEEKDAY = 5                              # date.weekday(): Mon=0 .. Sat=5

WEEKLY_SF_ARN = (
    f"arn:aws:states:{REGION}:{ACCOUNT_ID}:stateMachine:ne-weekly-freshness-pipeline"
)
SFN_TARGET_ROLE_ARN = os.environ.get(
    "SFN_TARGET_ROLE_ARN",
    f"arn:aws:iam::{ACCOUNT_ID}:role/alpha-engine-eventbridge-sfn-role",
)
TRADING_EC2_INSTANCE_ID = os.environ.get("TRADING_EC2_INSTANCE_ID", "i-09b539c844515d549")
SNS_TOPIC_ARN = os.environ.get(
    "SNS_TOPIC_ARN", f"arn:aws:sns:{REGION}:{ACCOUNT_ID}:alpha-engine-alerts"
)


# ---------------------------------------------------------------------------
# Calendar
# ---------------------------------------------------------------------------
def weekly_run_day(d: date) -> date:
    """The day AFTER the last NYSE trading day of d's Mon–Sun week.

    Walks back from Sunday to the week's last trading session, then +1 calendar
    day. Normal weeks -> Saturday; a Friday-holiday week -> Friday; a
    (hypothetical) Thu+Fri closure -> Thursday.
    """
    sunday = d + timedelta(days=6 - d.weekday())
    probe = sunday
    while not is_trading_day(probe):
        probe -= timedelta(days=1)
    return probe + timedelta(days=1)


def _oneshot_name(run_day: date) -> str:
    return f"{ONESHOT_PREFIX}{run_day.strftime('%Y%m%d')}"


def _weekly_input() -> str:
    # byte-shape identical to the Saturday cron's target Input
    return (
        "{\n"
        f'  "ec2_instance_id": ["{TRADING_EC2_INSTANCE_ID}"],\n'
        f'  "sns_topic_arn": "{SNS_TOPIC_ARN}",\n'
        '  "pipeline_role": "weekly"\n'
        "}\n"
    )


# ---------------------------------------------------------------------------
# EventBridge reconciliation helpers (idempotent)
# ---------------------------------------------------------------------------
def _rule_state(events, name: str) -> str | None:
    try:
        return events.describe_rule(Name=name)["State"]
    except events.exceptions.ResourceNotFoundException:
        return None


def _ensure_saturday(events, *, enabled: bool) -> str:
    state = _rule_state(events, SATURDAY_RULE)
    want = "ENABLED" if enabled else "DISABLED"
    if state is None:
        # The CFN-owned rule is missing — do NOT create it here (CFN is SoT).
        logger.warning("%s not found — cannot reconcile state to %s", SATURDAY_RULE, want)
        return "missing"
    if state == want:
        return "already_" + want.lower()
    if enabled:
        events.enable_rule(Name=SATURDAY_RULE)
    else:
        events.disable_rule(Name=SATURDAY_RULE)
    logger.info("%s: %s -> %s", SATURDAY_RULE, state, want)
    return want.lower()


def _ensure_oneshot(events, run_day: date) -> str:
    """Create/refresh a one-shot rule that fires the weekly SF once on run_day."""
    name = _oneshot_name(run_day)
    # specific-date cron: cron(min hour day month ? year) fires exactly once
    expr = f"cron(0 9 {run_day.day} {run_day.month} ? {run_day.year})"
    events.put_rule(
        Name=name,
        ScheduleExpression=expr,
        State="ENABLED",
        Description=(
            f"One-shot weekly SF on {run_day.isoformat()} 09:00 UTC "
            f"(day after last trading day; {SATURDAY_RULE} suppressed this week). "
            "Auto-managed by alpha-engine-weekly-schedule-adjuster; reaped after firing."
        ),
    )
    events.put_targets(
        Rule=name,
        Targets=[
            {
                "Id": "weekly-oneshot",
                "Arn": WEEKLY_SF_ARN,
                "RoleArn": SFN_TARGET_ROLE_ARN,
                "Input": _weekly_input(),
            }
        ],
    )
    logger.info("one-shot ready: %s (%s)", name, expr)
    return name


def _drop_stale_oneshots(events, today: date, keep: str | None) -> list[str]:
    """Delete adjuster-created one-shot rules whose run date is strictly past."""
    dropped: list[str] = []
    paginator = events.get_paginator("list_rules")
    for page in paginator.paginate(NamePrefix=ONESHOT_PREFIX):
        for rule in page.get("Rules", []):
            name = rule["Name"]
            if name == keep:
                continue
            try:
                run_day = datetime.strptime(name[len(ONESHOT_PREFIX):], "%Y%m%d").date()
            except ValueError:
                continue  # unrecognized suffix — leave it alone
            if run_day >= today:
                continue  # future/today one-shot (another holiday week) — keep
            # remove targets then delete the rule
            tids = [t["Id"] for t in events.list_targets_by_rule(Rule=name).get("Targets", [])]
            if tids:
                events.remove_targets(Rule=name, Ids=tids)
            events.delete_rule(Name=name)
            dropped.append(name)
            logger.info("reaped stale one-shot: %s (ran %s)", name, run_day.isoformat())
    return dropped


# ---------------------------------------------------------------------------
# Handler
# ---------------------------------------------------------------------------
def _today(event: dict) -> date:
    # EventBridge scheduled events carry ISO "time"; fall back to wall clock.
    t = (event or {}).get("time")
    if t:
        return datetime.fromisoformat(t.replace("Z", "+00:00")).astimezone(timezone.utc).date()
    return datetime.now(timezone.utc).date()


def handler(event: dict, context) -> dict:  # noqa: ARG001 — Lambda contract
    events = boto3.client("events", region_name=REGION)
    today = _today(event)
    run_day = weekly_run_day(today)

    if run_day < today:
        # Reconciler ran after this week's run day already passed — leave the
        # baseline alone, just reap anything stale. (Shouldn't happen on a Wed tick.)
        dropped = _drop_stale_oneshots(events, today, keep=None)
        return {"acted": "past", "run_day": run_day.isoformat(), "reaped": dropped}

    if run_day.weekday() == SATURDAY_WEEKDAY:
        # Normal week — the CFN Saturday cron owns it. Ensure it's enabled
        # (heals a prior holiday week's disable) and clean up spent one-shots.
        sat = _ensure_saturday(events, enabled=True)
        dropped = _drop_stale_oneshots(events, today, keep=None)
        return {
            "acted": "normal", "run_day": run_day.isoformat(),
            "saturday": sat, "reaped": dropped,
        }

    # Trailing-holiday week — shift earlier. One-shot FIRST (fail-safe), then
    # suppress the Saturday cron.
    name = _ensure_oneshot(events, run_day)
    sat = _ensure_saturday(events, enabled=False)
    dropped = _drop_stale_oneshots(events, today, keep=name)
    logger.info(
        "HOLIDAY-SHIFT week: weekly SF -> %s (%s); %s disabled",
        run_day.isoformat(), name, SATURDAY_RULE,
    )
    return {
        "acted": "holiday_shift", "run_day": run_day.isoformat(),
        "oneshot": name, "saturday": sat, "reaped": dropped,
    }
