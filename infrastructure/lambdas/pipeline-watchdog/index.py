"""alpha-engine-pipeline-watchdog — daily NYSE-trading-day-aware Step-Function watchdog.

Phase 4 of the pipeline-reporting-revamp arc (ROADMAP L3050, plan doc
``~/Development/alpha-engine-docs/private/pipeline-reporting-revamp-260524.md``
§3.5 / Phase 0 Q2 lock).

**What this Lambda does:** triggered daily by EventBridge cron at
14:00 UTC (≈ 07:00 PT, well after every SF's expected start time). For each
of the 3 Step Functions, checks whether at least one execution started in
the expected window. If a check fails, publishes an alert via
``alpha_engine_lib.alerts.publish`` to a DISTINCT SNS topic
(``alpha-engine-watchdog-alerts``, NOT the existing ``alpha-engine-alerts``
topic) and routes Telegram through flow-doctor forum topics
(``PIPELINE_OBSERVER_TELEGRAM_TOPICS`` — config#1742 T2) — channel
independence preserved per plan doc §3.5.

**Per-SF watchdog semantics:**

  - **Weekday SF** (``ne-preopen-trading-pipeline``)
      Watch-day: TODAY is a trading day. If trading_calendar reports that
      ``last_closed_trading_day(now_utc).date() == now_utc.date() - 1``
      (i.e., the prior calendar day was a trading session), the Weekday SF
      should have fired today by 13:00 UTC. Alert if 0 executions started
      in the last 24h.

  - **EOD SF** (``ne-postclose-trading-pipeline``)
      Watch-day: same condition as Weekday — EOD fires after the trading
      day's daemon shutdown, which only happens on trading days. Window
      is TRADING-DAY-AWARE, NOT a fixed 24h calendar window: today's EOD
      fires ~20:00 UTC (post market close at 13:00 PT + daemon shutdown),
      which is AFTER the watchdog's 14:00 UTC cron firing — so the most
      recent EXPECTED EOD execution is the PREVIOUS trading day's. The
      window starts at ``previous_trading_day(today) @ 20:00 UTC`` +
      slack. After a holiday weekend (Fri close → Mon holiday → Tue
      watchdog) the gap is ~66h, not 24h. Alert if 0 executions started
      in that window. See ``_eod_window_seconds`` for the derivation and
      the 2026-05-26 morning false-positive Telegram alert that drove
      this fix.

  - **Saturday SF** (``ne-weekly-freshness-pipeline``)
      Watch-day: TODAY is Sunday (weekday 6) — Saturday SF fires at 09:00
      UTC Saturday; by Sunday 14:00 UTC any missed firing is 24+h overdue.
      Alert if 0 executions started in the last 7 days. (One CW alarm with
      a 7-day window would suffice for Saturday too, but bundling all 3
      checks into one Lambda eliminates a moving part and unifies the
      operator-facing message format.)

**Fail-loud semantics** (per ``feedback_no_silent_fails`` + the
``feedback_wire_orphaned_producer_must_fail_loud`` discipline):

  - ``states:ListExecutions`` failure → raises. EventBridge retry policy
    + CW alarm on Lambda errors page the operator. We MUST NOT silently
    skip a check.
  - ``alerts.publish`` failure → already non-raising by lib design, but
    publish failures are logged at WARNING + surfaced in the Lambda
    response dict so the CW alarm path catches them too.
  - Non-trading-day skip is the intended skip path — returns
    ``{"checked": [...], "skipped": [...]}`` with explicit reasons per
    SF. NOT a swallow.

**Why a Lambda not a pure CW alarm**: per Phase 0 Q2 SOTA-lock, a dumb
``AWS/States ExecutionsStarted`` alarm with a 24h window would
false-positive every weekend for Weekday + EOD (alert hygiene defect:
operator desensitization → silenced watchdog → defeats purpose). The
``alpha_engine_lib.trading_calendar.last_closed_trading_day`` chokepoint
encodes NYSE holiday + weekend awareness, so the Lambda fires cleanly
only when there's genuinely a missing execution on an expected
trading day.

**Why publish to a DISTINCT SNS topic**: channel independence (plan
doc §3.5). If the operator's regular ``alpha-engine-alerts`` → email
path silently breaks, this watchdog's separate publish path still
reaches the operator. The Telegram fan-out via the lib is the
non-overlapping second channel.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional

import boto3

from alpha_engine_lib import alerts
from alpha_engine_lib.trading_calendar import (
    last_closed_trading_day,
    previous_trading_day,
)
from flow_doctor_telegram import notify_via_flow_doctor
from nousergon_lib.flow_doctor_fleet import PIPELINE_OBSERVER_TELEGRAM_TOPICS


logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

_FLOW_NAME = "pipeline-watchdog"
_DB_BASENAME = "flow_doctor_pipeline_watchdog"

REGION = os.environ.get("AWS_REGION", "us-east-1")
ACCOUNT_ID = os.environ.get("ACCOUNT_ID", "711398986525")

SATURDAY_SF_ARN = (
    f"arn:aws:states:{REGION}:{ACCOUNT_ID}:stateMachine:ne-weekly-freshness-pipeline"
)
WEEKDAY_SF_ARN = (
    f"arn:aws:states:{REGION}:{ACCOUNT_ID}:stateMachine:ne-preopen-trading-pipeline"
)
EOD_SF_ARN = (
    f"arn:aws:states:{REGION}:{ACCOUNT_ID}:stateMachine:ne-postclose-trading-pipeline"
)

# Watchdog-specific SNS topic — distinct from `alpha-engine-alerts` per
# channel-independence requirement (§3.5). Audit subscribers (email,
# pagerduty, anything operator wants) attach to THIS topic without
# polluting the trade-decision alert channel.
WATCHDOG_SNS_TOPIC_ARN = os.environ.get(
    "WATCHDOG_SNS_TOPIC_ARN",
    f"arn:aws:sns:{REGION}:{ACCOUNT_ID}:alpha-engine-watchdog-alerts",
)

# Per-SF expected-window seconds. Weekday cron fires at 12:45 UTC, which
# is BEFORE the watchdog's 14:00 UTC cron firing, so a 24h calendar window
# correctly captures today's expected weekday execution. Saturday cron
# fires at 09:00 UTC Sat, watchdog runs Sundays at 14:00 UTC → 7d calendar
# window correctly captures that Saturday firing.
#
# EOD SF is the exception — it fires AFTER market close (~20:00 UTC)
# which is AFTER the watchdog's 14:00 UTC firing today, so the most
# recent EXPECTED EOD execution at watchdog time is the PREVIOUS
# trading day's EOD. After a holiday weekend (Fri close → Mon holiday →
# Tue 14:00 UTC watchdog), that gap is ~66h, not 24h. EOD uses
# ``_eod_window_seconds`` instead of a constant.
WINDOW_SECONDS_DAILY = 24 * 3600  # 86_400 — Weekday SF
WINDOW_SECONDS_WEEKLY = 7 * 24 * 3600  # 604_800 — Saturday SF

# EOD window slack — added to the gap-to-previous-trading-day-EOD so a
# late EOD firing (daemon shut down slightly later than the nominal time)
# or clock skew between watchdog + SF control plane doesn't false-positive
# on the boundary.
EOD_WINDOW_SLACK_SECONDS = 3600  # 1 hour

# Nominal expected EOD firing time in UTC. Daemon shuts down ~13:15 PT
# after the 13:00 PT (US market close), which is ~20:15 UTC during PDT.
# A wider window via SLACK above absorbs the ~30 min spread.
EOD_EXPECTED_UTC_HOUR = 20


def _eod_window_seconds(now_utc: datetime) -> int:
    """EOD SF runs after the trading day's market close (~20:00 UTC). At
    the watchdog's 14:00 UTC firing time the most recent EXPECTED EOD
    execution is the PREVIOUS trading day's EOD (today's hasn't fired
    yet — it will fire ~6h after the watchdog runs).

    Window start = previous_trading_day(today) @ ``EOD_EXPECTED_UTC_HOUR``.
    Window seconds = ``now - window_start + slack``.

    Examples:
      - Wed 14:00 UTC, post-normal-Tue:
          prev_td = Tue, prev_eod_expected = Tue 20:00 UTC,
          gap = 18h, window = 18h + 1h slack = 19h.
      - Tue 14:00 UTC, post-Memorial-Mon-holiday:
          prev_td = Fri (because Mon was holiday), prev_eod_expected =
          Fri 20:00 UTC, gap = 4 days × 24h − 6h = 90h, window = 90h +
          1h slack = 91h.
      - Mon 14:00 UTC after normal weekend:
          prev_td = Fri, prev_eod_expected = Fri 20:00 UTC, gap = 3
          days × 24h − 6h = 66h, window = 67h. (No false alert on
          Monday morning post-weekend.)

    Why this is the right cutover rather than "always use a 24h window
    that we extend on holidays": the watchdog's purpose is to detect a
    missing EXPECTED firing. The expectation is set by NYSE's session
    calendar, not by clock arithmetic. A 24h window encodes "we expect a
    daily firing" but the firing isn't daily on weekends and holidays.
    Pulling the window start from ``previous_trading_day`` makes the
    encoded expectation match the actual schedule — which is exactly
    what ``feedback_dual_source_audit_must_assess_every_downstream_consumer``
    + ``feedback_no_silent_fails`` argue for at substrate level.
    """
    prev_td = previous_trading_day(now_utc.date())
    # Construct the previous-trading-day EOD-expected timestamp via
    # ``now_utc.replace`` + ``timedelta`` (avoids ``datetime.combine``,
    # which the existing test pattern's ``patch("index.datetime")`` mock
    # doesn't proxy through to the real classmethod).
    days_back = (now_utc.date() - prev_td).days
    prev_eod_expected = now_utc.replace(
        hour=EOD_EXPECTED_UTC_HOUR, minute=0, second=0, microsecond=0
    ) - timedelta(days=days_back)
    gap_seconds = int((now_utc - prev_eod_expected).total_seconds())
    # Defensive: if the gap somehow goes negative (e.g., a future test
    # passing a now_utc earlier than prev_eod_expected), clamp to the slack
    # so the window is at least non-zero and we don't ListExecutions with
    # a negative timedelta.
    return max(gap_seconds, 0) + EOD_WINDOW_SLACK_SECONDS

# Status filter for "real" executions — anything that actually started.
# FAILED / TIMED_OUT / ABORTED executions still START the SF — what
# matters for the watchdog is "did the EventBridge fire reach the SF
# control plane", not "did the workload succeed". The plan doc / SF JSON
# Phase 3 will continue to alert on FAILED via the SF HandleFailure
# email — that's a different concern.
_STARTED_STATUSES = ("RUNNING", "SUCCEEDED", "FAILED", "TIMED_OUT", "ABORTED")


@dataclass(frozen=True)
class CheckResult:
    """Per-SF outcome of one watchdog check."""

    sf_label: str
    sf_arn: str
    checked: bool  # False = today is not a watch-day for this SF; alert NOT emitted
    skip_reason: Optional[str] = None
    executions_seen: Optional[int] = None
    alert_emitted: bool = False
    alert_detail: Optional[str] = None


def _is_trading_day_now(now_utc: datetime) -> bool:
    """True iff today's calendar date in NYSE local terms is a trading day.

    ``last_closed_trading_day`` returns a date object; if it equals today's
    NYSE date, the market closed today (we're checking post-close) → today
    IS a trading day. If it equals yesterday or earlier, today is a weekend
    or holiday.

    The lib helper already handles UTC ↔ ET ↔ PT rollover; we hand it the
    tz-aware UTC datetime and trust its NYSE-local-time interpretation.
    """
    trading_day = last_closed_trading_day(now_utc)
    # The helper returns ``trading_day`` ≤ today's NYSE date. Equality means
    # the most recent close == today's date in NYSE local terms.
    #
    # At our cron firing time (14:00 UTC = 07:00 PT = 10:00 ET), the NYSE
    # session hasn't yet opened (09:30 ET). So on a trading day, the most
    # recent CLOSED session is YESTERDAY's session, not today's. We expect
    # the helper to return yesterday's date on trading days at this hour.
    #
    # Concretely: 2026-05-27 Wed 14:00 UTC → trading_day=2026-05-26 (Tue)
    # 2026-05-30 Sat 14:00 UTC → trading_day=2026-05-29 (Fri)
    # 2026-05-25 Mon 14:00 UTC (Memorial Day) → trading_day=2026-05-22 (Fri)
    #
    # So "today is a trading day" semantically means "we EXPECT today's
    # Weekday + EOD SF firings" — which is true iff today's NYSE-local
    # calendar date is itself a session. We can ask the helper a SECOND
    # time at a synthetic post-close instant (today 22:00 UTC = 17:00 ET)
    # to get today's date if it's a session.
    synthetic_post_close = now_utc.replace(hour=22, minute=0, second=0, microsecond=0)
    post_close_trading_day = last_closed_trading_day(synthetic_post_close)
    return post_close_trading_day == synthetic_post_close.date()


def _count_executions_in_window(
    sf_arn: str,
    window_seconds: int,
    *,
    client: Optional[object] = None,
) -> int:
    """Return the number of executions that STARTED for ``sf_arn`` in the
    last ``window_seconds``. Counts ALL terminal statuses + RUNNING; what
    matters is whether the SF fired, not whether the workload succeeded.

    Uses ``states:ListExecutions`` with paginated startDate filtering —
    AWS does not support a startDate filter on ListExecutions directly,
    so we page through statusFilter results and apply the time cutoff in
    Python. maxResults=100 per page; we stop at the first page whose
    oldest entry is older than the window (lex-sortable by startDate desc).
    """
    if client is None:  # pragma: no cover — production path
        client = boto3.client("stepfunctions", region_name=REGION)

    cutoff_utc = datetime.now(timezone.utc) - timedelta(seconds=window_seconds)
    seen = 0

    for status_filter in _STARTED_STATUSES:
        next_token: Optional[str] = None
        while True:
            kwargs = {
                "stateMachineArn": sf_arn,
                "statusFilter": status_filter,
                "maxResults": 100,
            }
            if next_token:
                kwargs["nextToken"] = next_token
            resp = client.list_executions(**kwargs)
            execs = resp.get("executions") or []
            for exec_row in execs:
                start = exec_row.get("startDate")
                if start is None:
                    continue
                # Duck-type: ListExecutions returns boto3 datetime objects
                # with tzinfo+astimezone. Skip anything that's not datetime-
                # shaped (defensive against missing-field edge cases). Use
                # ``hasattr`` rather than ``isinstance(start, datetime)`` so
                # tests can patch ``index.datetime`` without false-tripping
                # the typecheck (MagicMock isn't a type).
                if not hasattr(start, "astimezone"):
                    continue
                start_utc = (
                    start.astimezone(timezone.utc)
                    if start.tzinfo
                    else start.replace(tzinfo=timezone.utc)
                )
                if start_utc >= cutoff_utc:
                    seen += 1
                else:
                    # Executions are returned newest-first; once we see one
                    # older than the cutoff we can stop paging this status.
                    next_token = None
                    break
            else:
                next_token = resp.get("nextToken")
                if not next_token:
                    break
                continue
            break  # broke out of inner for-else → stop paging this status

    return seen


def _check_sf(
    *,
    sf_label: str,
    sf_arn: str,
    is_watch_day: bool,
    skip_reason_if_not_watching: str,
    window_seconds: int,
    client: Optional[object] = None,
) -> CheckResult:
    if not is_watch_day:
        logger.info(
            "watchdog skip: sf=%s reason=%s", sf_label, skip_reason_if_not_watching
        )
        return CheckResult(
            sf_label=sf_label,
            sf_arn=sf_arn,
            checked=False,
            skip_reason=skip_reason_if_not_watching,
        )

    seen = _count_executions_in_window(sf_arn, window_seconds, client=client)
    if seen > 0:
        logger.info(
            "watchdog clear: sf=%s executions_in_window=%d", sf_label, seen
        )
        return CheckResult(
            sf_label=sf_label,
            sf_arn=sf_arn,
            checked=True,
            executions_seen=seen,
        )

    # 0 executions in window on a watch-day → alert.
    window_hours = window_seconds // 3600
    message = (
        f"{sf_label} has not executed in the last {window_hours}h on a trading-day window. "
        f"Expected at least 1 execution since "
        f"{(datetime.now(timezone.utc) - timedelta(seconds=window_seconds)).isoformat()}. "
        f"Either the EventBridge schedule did not fire, the SF control plane is wedged, "
        f"or upstream IAM/permissions are broken. Investigate: "
        f"`aws stepfunctions list-executions --state-machine-arn {sf_arn} --max-results 10`."
    )
    # Dedup-key collapses repeated daily fires on a persistent outage into
    # one alert per (SF, date) within the lib's default 60-min window —
    # extended here to 12h so we don't re-page the operator on the same
    # already-acknowledged outage.
    dedup_key = (
        f"pipeline-watchdog-{sf_label}-{datetime.now(timezone.utc).date().isoformat()}"
    )
    result = alerts.publish(
        message=message,
        severity="error",
        source="alpha-engine-pipeline-watchdog",
        sns=True,
        telegram=False,
        sns_topic_arn=WATCHDOG_SNS_TOPIC_ARN,
        dedup_key=dedup_key,
        dedup_window_min=12 * 60,
    )
    telegram_ok = notify_via_flow_doctor(
        message,
        silent=False,
        severity="error",
        dedup_key=dedup_key,
        flow_name=_FLOW_NAME,
        topics=PIPELINE_OBSERVER_TELEGRAM_TOPICS,
        db_basename=_DB_BASENAME,
        context={"sf_label": sf_label, "sf_arn": sf_arn},
    )
    logger.warning(
        "watchdog ALERT: sf=%s sns_ok=%s telegram_ok=%s dedup_skipped=%s",
        sf_label,
        result.sns.ok,
        telegram_ok,
        getattr(result, "dedup_skipped", False),
    )
    return CheckResult(
        sf_label=sf_label,
        sf_arn=sf_arn,
        checked=True,
        executions_seen=0,
        alert_emitted=True,
        alert_detail=(
            f"sns_ok={result.sns.ok} telegram_ok={telegram_ok} "
            f"dedup_skipped={getattr(result, 'dedup_skipped', False)}"
        ),
    )


def handler(event: dict, context) -> dict:  # noqa: ARG001 — Lambda contract
    """EventBridge cron handler. Runs the 3 per-SF checks + returns a
    structured summary the Lambda console / CW logs can read at a glance."""
    now_utc = datetime.now(timezone.utc)
    is_trading_today = _is_trading_day_now(now_utc)
    is_sunday = now_utc.weekday() == 6  # Mon=0..Sun=6

    weekday = _check_sf(
        sf_label="Weekday SF",
        sf_arn=WEEKDAY_SF_ARN,
        is_watch_day=is_trading_today,
        skip_reason_if_not_watching=(
            "today is not a NYSE trading day (weekend / holiday) per "
            "alpha_engine_lib.trading_calendar"
        ),
        window_seconds=WINDOW_SECONDS_DAILY,
    )
    eod = _check_sf(
        sf_label="EOD SF",
        sf_arn=EOD_SF_ARN,
        is_watch_day=is_trading_today,
        skip_reason_if_not_watching=(
            "today is not a NYSE trading day (weekend / holiday) per "
            "alpha_engine_lib.trading_calendar"
        ),
        # Trading-day-aware: previous_trading_day-based, NOT a 24h calendar
        # window. Today's EOD fires ~20:00 UTC (after market close + daemon
        # shutdown); watchdog runs 14:00 UTC, so the most recent EXPECTED
        # EOD is the PREVIOUS trading day's. After a holiday weekend (Fri
        # close → Mon holiday → Tue 14:00 UTC watchdog) the gap is ~66h,
        # not 24h — see ``_eod_window_seconds`` for derivation. Closes the
        # 2026-05-26 morning false-positive Telegram alert ("EOD SF has
        # not executed in the last 24 hours") on the first trading day
        # after Memorial Day.
        window_seconds=_eod_window_seconds(now_utc),
    )
    saturday = _check_sf(
        sf_label="Saturday SF",
        sf_arn=SATURDAY_SF_ARN,
        is_watch_day=is_sunday,
        skip_reason_if_not_watching=(
            f"today (weekday={now_utc.weekday()}) is not Sunday; Saturday SF "
            "watch-day is Sunday so missed firings are 24+h overdue"
        ),
        window_seconds=WINDOW_SECONDS_WEEKLY,
    )

    summary = {
        "fired_at_utc": now_utc.isoformat(),
        "is_trading_today": is_trading_today,
        "is_sunday": is_sunday,
        "checks": [
            {
                "sf_label": c.sf_label,
                "checked": c.checked,
                "skip_reason": c.skip_reason,
                "executions_seen": c.executions_seen,
                "alert_emitted": c.alert_emitted,
                "alert_detail": c.alert_detail,
            }
            for c in (weekday, eod, saturday)
        ],
    }
    logger.info("watchdog summary: %s", summary)
    return summary
