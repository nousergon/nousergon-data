"""alpha-engine-pipeline-watchdog — daily NYSE-trading-day-aware Step-Function watchdog.

Phase 4 of the pipeline-reporting-revamp arc (ROADMAP L3050, plan doc
``~/Development/alpha-engine-docs/private/pipeline-reporting-revamp-260524.md``
§3.5 / Phase 0 Q2 lock).

**What this Lambda does:** triggered daily by EventBridge cron at
14:00 UTC (≈ 07:00 PT, well after every SF's expected start time). For each
of the 3 Step Functions, checks whether at least one execution started in
the expected window. If a check fails, publishes an alert via
``nousergon_lib.alerts.publish`` to a DISTINCT SNS topic
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

  - **EOD reconcile SF** (``ne-postclose-reconcile-pipeline``, split out of
      the EOD SF 2026-09-30)
      Same watch-day and trading-day-aware window as the EOD SF: it is
      started later the same evening, when ne-data-collection-eod reaches a
      terminal state. It is also in the prior-day failed-run check.

  - **Saturday SF** (``ne-weekly-freshness-pipeline``)
      Watch-day: TODAY is Sunday (weekday 6) — Saturday SF fires at 09:00
      UTC Saturday; by Sunday 14:00 UTC any missed firing is 24+h overdue.
      Alert if 0 CADENCE-ROLE executions started in the last 7 days. (One
      CW alarm with a 7-day window would suffice for Saturday too, but
      bundling all 3 checks into one Lambda eliminates a moving part and
      unifies the operator-facing message format.)

      The role filter is load-bearing, not a refinement. Since
      2026-07-29 this same state machine also runs a post-close-chained
      daily EXERCISE run (alpha-engine-config#5489, ``pipeline_role=
      "exercise"``), so ~5 executions a week land here that say nothing
      about the Saturday cron. An unfiltered count is satisfied by them
      unconditionally: the check would report healthy forever with the
      cron completely dead (alpha-engine-config#5597 / #5590). See
      ``WEEKLY_CADENCE_ROLES``.

  - **Weekly-SF silence deadman** (``ne-weekly-freshness-pipeline``,
      alpha-engine-config#6738 / sf-pipeline-policy §2.6 rule 1)
      Watch-day: EVERY day. Reads the DECLARED exercise cadence from SSM
      ``/alpha-engine/weekly-sf/exercise-cadence`` — the same parameter the
      post-close reconcile SF's ``ReadExerciseCadence`` task reads to decide whether to
      chain the exercise launch — derives every run-slot the declaration
      expects over a trailing 5-day window, and pages on any slot with NO
      matching execution. Where the Saturday-SF check above asks "did the
      weekly cron fire at all this week", this asks the per-day question:
      the 2026-08-05/06 postclose never fired, so the chained
      ``pipeline_role=exercise`` launch was silent for two days with zero
      signal — an absence, invisible to every failure-triggered path.
      A slot the declaration does NOT expect is classified ``GATED_OFF``
      and reported as such, never conflated with silence. The slot logic is
      imported from ``scripts/weekly_sf_silence_deadman.py`` (one
      implementation, two entry points), not reimplemented here.

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
``nousergon_lib.trading_calendar.last_closed_trading_day`` chokepoint
encodes NYSE holiday + weekend awareness, so the Lambda fires cleanly
only when there's genuinely a missing execution on an expected
trading day.

**Why publish to a DISTINCT SNS topic**: channel independence (plan
doc §3.5). If the operator's regular ``alpha-engine-alerts`` → email
path silently breaks, this watchdog's separate publish path still
reaches the operator. The Telegram fan-out via the lib is the
non-overlapping second channel.

**Preopen schedule-buffer canary** (alpha-engine-config#2412): a 4th check,
added alongside the 3 liveness checks above. The Weekday SF's trigger
(``WeekdayPipelineSchedule`` in alpha-engine-orchestration.yaml) has been
moved earlier TWICE after finishing after the 06:30 PT open —
06:00→05:45 PT (2026-05-19, 13-min buffer) then 05:45→05:15 PT
(2026-07-13, ~30-min buffer) — both times the erosion was noticed
anecdotally, days after it started. This check reads the finish
(``stopDate``) of the most recent CLOSED trading day's SUCCEEDED Weekday-SF
execution and alerts before the buffer is fully consumed again, instead of
after.

Because the watchdog's own cron (14:00 UTC, NOT DST-aware — see deploy.sh)
fires well before today's session closes (~20:00 UTC) on every date
regardless of PDT/PST, ``last_closed_trading_day(now_utc)`` at check time
always resolves to the PRIOR trading day, never today's still-in-flight
run — see ``_is_trading_day_now`` above for the same reasoning. This is
deliberate: the canary is a next-morning retrospective on a fully-
completed session, never a same-day live check, so there is no race with
whether today's execution has finished yet.

Thresholds (America/Los_Angeles, DST-aware via ``zoneinfo`` — comparisons
resolve the correct UTC offset per calendar date, no manual DST math):
market open is a fixed 06:30 PT year-round; ``HARD_ALERT_TIME_PT`` (06:15,
a 15-min/~25%-of-45-min-runtime buffer floor) fires a severity=error alert;
``WARN_TREND_TIME_PT`` (06:10) fires a severity=warning early-warning
alert below the hard floor. A finish at/after 06:30 itself (an actual
missed-open) gets a distinct "MISSED THE OPEN" message so the alert
doesn't read the same as a mere late-but-before-open finish. The issue's
own recommendation (06:20, gated on ``RunDaemon`` state reached) targets a
different, live/in-flight design point than this retrospective
finish-time check, so it does not transfer directly — see PR body.

Does NOT filter by ``input.pipeline_role``. Instead takes the
EARLIEST-started SUCCEEDED execution per PT calendar day as the proxy for
"the scheduled run" (a same-day manual rerun, if any, would start later,
in response to a problem with the scheduled one) — mirrors the existing
unfiltered-count convention this file already uses for the Weekday/EOD
checks above.

CORRECTED 2026-08-09 (alpha-engine-config#6738): this paragraph used to
justify the absence of the role filter with "this Lambda's IAM role does
not grant ``states:DescribeExecution``". That claim is stale —
``iam-policy.json`` carries a ``DescribeSFExecutions`` statement and the
LIVE role was measured to carry it. Skipping the filter here is now a cost
choice (one DescribeExecution per row of a 14-day walk), not a permission
constraint; revisit under alpha-engine-config-I6748.

Also computes a rolling-median trend over the last
``ROLLING_WINDOW_TRADING_DAYS`` (5) trading days with SUCCEEDED data: a
persistent creep that never individually crosses the hard floor on any
single day (e.g. 06:08, 06:09, 06:11, 06:09, 06:12) still surfaces as an
early-warning once the median crosses ``WARN_TREND_TIME_PT``. Requires at
least 3 of the 5 days present or the trend check is skipped (insufficient
data), never fabricated from fewer points.

No new SNS topic / Telegram channel — reuses ``WATCHDOG_SNS_TOPIC_ARN`` +
``notify_via_flow_doctor`` exactly like the 3 existing checks, via a
shared ``_publish_watchdog_alert`` helper factored out of ``_check_sf``'s
alert path (config#2412 PR — DRY, no behavior change to the 3 existing
checks).
"""

from __future__ import annotations

import json
import logging
import os
import sys
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Optional
from zoneinfo import ZoneInfo

import boto3

from nousergon_lib import alerts
from nousergon_lib.pipeline_status.completion_marker import read_marker
from nousergon_lib.trading_calendar import (
    is_trading_day,
    last_closed_trading_day,
    next_trading_day,
    previous_trading_day,
)
from flow_doctor_telegram import notify_via_flow_doctor
from nousergon_lib.flow_doctor_fleet import PIPELINE_OBSERVER_TELEGRAM_TOPICS

# The weekly-SF silence deadman's slot derivation + classification
# (alpha-engine-config#6738). ONE implementation, two entry points: the
# operator CLI at ``scripts/weekly_sf_silence_deadman.py`` and this Lambda.
# deploy.sh copies that file flat into the zip alongside index.py — the same
# device already used for ``flow_doctor_telegram.py`` — so the scheduled check
# and the manual rerun can never diverge in what they consider a silent slot.
# Its module-level imports are stdlib only (boto3 is imported inside its
# ``main``), so this costs the Lambda no cold-start weight beyond the file.
try:  # deployed layout: flat next to index.py in /var/task
    from weekly_sf_silence_deadman import (  # noqa: E402
        _is_gate_noop,
        _is_weekly_gate_out,
        compute_expected_slots,
        evaluate,
        fetch_execution_records,
        last_due_day,
        load_cadence_from_ssm,
    )
except ModuleNotFoundError:  # in-repo layout (pytest, ci.yml glob runner)
    sys.path.insert(
        0, str(Path(__file__).resolve().parents[3] / "scripts")
    )
    from weekly_sf_silence_deadman import (  # noqa: E402
        _is_gate_noop,
        _is_weekly_gate_out,
        compute_expected_slots,
        evaluate,
        fetch_execution_records,
        last_due_day,
        load_cadence_from_ssm,
    )


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
# The collector-dependent half of the post-close pipeline, split into its own
# machine 2026-09-30 (alpha-engine-config-I11269 follow-up). Started by
# alpha-engine-eod-backstop on ne-data-collection-eod's terminal event (~19:30-
# 21:00 ET) or its 02:15 UTC reconcile backstop — both the same PT calendar day
# as the trading day, so the PT-date bucketing below is exact for it too.
RECONCILE_SF_ARN = (
    f"arn:aws:states:{REGION}:{ACCOUNT_ID}:stateMachine:ne-postclose-reconcile-pipeline"
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

# Roles that count as "the Saturday cadence fired" on the weekly SF. The
# cadence role itself plus the recovery overlays that legitimately stand in
# for it (a watch-rerun IS the cadence run, resubmitted). Deliberately
# EXCLUDES "exercise" (the daily debugging cadence, alpha-engine-config#5489)
# and every ad-hoc role — none of them is evidence the Saturday cron fired.
#
# This set is the local mirror of nousergon_lib.pipeline_status.roles
# .cadence_filter("weekly"); it is declared here rather than imported so
# this fix ships without waiting on a lib release. alpha-engine-config#5592
# replaces it with the import (nousergon-lib-PR270).
WEEKLY_CADENCE_ROLES = frozenset({"weekly", "watch-rerun", "recovery"})

# Ceiling on DescribeExecution calls per role-filtered walk. The weekly SF
# sees ~5 exercise runs + reruns per 7-day window, so this is ~5x headroom;
# exceeding it raises rather than returning a truncated count.
_MAX_ROLE_DESCRIBES = 200

# ── Preopen schedule-buffer canary (alpha-engine-config#2412) ──────────────

PT_ZONE = ZoneInfo("America/Los_Angeles")

# Market open is a fixed local-clock time year-round; zoneinfo resolves the
# correct UTC offset (PDT/PST) per calendar date when combined with a date,
# so no manual DST arithmetic is needed anywhere below.
MARKET_OPEN_TIME_PT = time(6, 30)

# 15-min buffer floor (~25% of the ~46-49 min observed total runtime) —
# breaching this is a severity=error alert. Issue #2412 recommended 06:20
# but for a DIFFERENT check shape (gated on the SF reaching RunDaemon state
# live, same-day); this check is a next-morning retrospective on the
# finish timestamp, so the two thresholds are not directly comparable —
# 06:15 is used per the binding design note in the PR, named explicitly
# here rather than silently inherited.
HARD_ALERT_TIME_PT = time(6, 15)

# Early-warning floor below the hard buffer-floor breach — severity=warning.
WARN_TREND_TIME_PT = time(6, 10)

# How many trading days back the median-trend signal looks.
ROLLING_WINDOW_TRADING_DAYS = 5

# Minimum trading days with SUCCEEDED data required before the trend median
# is trusted — never compute a "5-day median" off 1-2 points.
_MIN_TREND_DAYS = 3

# Calendar-day lookback for the ListExecutions walk that builds the
# per-trading-day SUCCEEDED-finish map. Generous relative to
# ROLLING_WINDOW_TRADING_DAYS(5) so a holiday week doesn't starve the
# median of data.
BUFFER_LOOKBACK_CALENDAR_DAYS = 14

# ── Weekly-SF silence deadman (alpha-engine-config#6738) ───────────────────

# SSM parameter carrying the DECLARED exercise cadence. This is the same
# parameter ``step_function_eod.json``'s ``ReadExerciseCadence`` task reads at
# execution time (config#6689), which is what makes the deadman's expectation
# provably identical to the launcher's actual behaviour. Deliberately NOT the
# in-repo ``infrastructure/weekly_cadence.json`` manifest: a manifest copy
# baked into the zip is a snapshot of whatever the last CODE deploy carried,
# and a cadence flip that ships via ``deploy-infrastructure.sh`` alone would
# leave the detector expecting the old cadence with nothing to say so.
CADENCE_SSM_PARAM = "/alpha-engine/weekly-sf/exercise-cadence"

# Trailing days of run-slots the deadman re-checks each firing. Five covers a
# full trading week's exercise slots plus one weekly-cron slot.
SILENCE_WINDOW_DAYS = 5

# Dedup window for a silence alert. Keyed on the SILENT SLOT (role + day), not
# on the firing date, so a genuinely new silent day always pages while the same
# slot — still visible in the 5-day window on each of the next four firings —
# does not re-page. 7 days > the window, so exactly one page per silent slot.
SILENCE_DEDUP_WINDOW_MIN = 7 * 24 * 60


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
    # "NEVER_FIRED" | "FIRED_AND_FAILED" | "CLEAR" | None (skipped). Status-
    # blind counting used to make the first two indistinguishable
    # (alpha-engine-config-I6991) — this field is what a reader (console,
    # log grep, test) checks instead of re-deriving it from the message
    # string.
    outcome: Optional[str] = None


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


def _role_clause(role_filter: Optional[frozenset]) -> str:
    """Names the role filter in the alert body, so an operator reading
    "has not executed" knows a same-SF exercise run does not contradict it."""
    if not role_filter:
        return ""
    return (
        f" counting only executions with pipeline_role in "
        f"{sorted(role_filter)} (other roles on this state machine — e.g. the "
        f"daily exercise run — are NOT evidence the cadence fired)"
    )


@dataclass(frozen=True)
class _ExecutionFacts:
    """Everything a status walk needs to classify ONE execution, read from a
    SINGLE ``DescribeExecution``: its declared ``pipeline_role`` (input) and
    whether the run-day gate itself declared the run a no-op (output).

    One call, both facts — the same device
    ``weekly_sf_silence_deadman.fetch_execution_records`` uses. Splitting
    them would double this Lambda's DescribeExecution fan-out for no gain.
    """

    role: Optional[str]
    is_gate_out: bool


def _execution_facts(client: object, execution_arn: str) -> _ExecutionFacts:
    """``input.pipeline_role`` and the run-day gate's own verdict for one
    execution, off one ``DescribeExecution``.

    ``ListExecutions`` returns neither the input nor the output, so both
    facts cost one describe per candidate — the same one the role filter
    already paid before alpha-engine-config-I8057/I8224/I9233.

    **Role.** A malformed input JSON yields ``role=None`` (the execution then
    does not count toward a role-filtered check) — never a swallow: the
    caller's alert path is what surfaces it, and a cadence run that lost its
    role IS the outage this watchdog exists to catch.

    **Gate verdict.** ``is_gate_out`` is ``_is_weekly_gate_out`` applied to
    the execution's own OUTPUT — imported from ``weekly_sf_silence_deadman``,
    never restated here, so the two gate classifications in this one Lambda
    cannot disagree about what a no-op is (the I6991/2026-08-16 failure
    mode). It is a POSITIVE test only: it fires on
    ``weekly_run_day_gate.Payload.is_weekly_run_day is False`` and never on
    the key's absence, because a real run does not carry that key at all.

    An output that is PRESENT but not parseable as JSON raises: that is the
    state machine's own result contract broken, and a detector that shrugs
    at it would silently reclassify every execution as a real run.
    """
    resp = client.describe_execution(executionArn=execution_arn)

    role: Optional[str] = None
    raw_input = resp.get("input")
    if raw_input:
        try:
            parsed = json.loads(raw_input)
        except (TypeError, ValueError):
            logger.warning(
                "watchdog: unparseable execution input, role=None: %s", execution_arn
            )
            parsed = None
        if isinstance(parsed, dict):
            candidate = parsed.get("pipeline_role")
            role = candidate if isinstance(candidate, str) and candidate else None

    is_gate_out = False
    raw_output = resp.get("output")
    if raw_output:
        try:
            output_json = json.loads(raw_output)
        except (TypeError, ValueError) as exc:
            raise RuntimeError(
                f"pipeline-watchdog: {execution_arn} returned an output that is "
                f"not JSON ({exc}) — the weekly SF's result contract is broken "
                f"and the run-day-gate verdict cannot be read; this walk's "
                f"counts are not trustworthy"
            ) from exc
        if isinstance(output_json, dict):
            is_gate_out = _is_weekly_gate_out(output_json)

    return _ExecutionFacts(role=role, is_gate_out=is_gate_out)


# Pseudo-status for a SUCCEEDED execution that deliberately did no work.
# It is NOT "SUCCEEDED" and it is NOT a failure — a third bucket, so no
# caller can accidentally sum it into either (alpha-engine-config-I8045).
#
# ``ne-weekly-freshness-pipeline``'s cron fires THU-SAT and the gate
# self-selects the single correct day; on the other two days the execution
# terminates SUCCEEDED at ``WeeklyRunDaySkip`` having entered five states
# and dispatched nothing. Which day that was is read from the gate's own
# output verdict (``_execution_facts``), NOT from how long the execution
# took: alpha-engine-config-I8057 measured the longest genuine gate-out at
# 5.7s and the shortest execution that really ran at 12.6s, both inside the
# 90s ceiling the previous ``_row_is_gate_noop`` duration heuristic used, so
# that test could misread either direction and had no confirmation step.
GATE_SKIP = "GATE_SKIP"


def _status_counts_in_window(
    sf_arn: str,
    window_seconds: int,
    *,
    client: Optional[object] = None,
    role_filter: Optional[frozenset] = None,
    gate_noop_aware: bool = False,
) -> "dict[str, int]":
    """Return counts of executions that STARTED for ``sf_arn`` in the last
    ``window_seconds``, keyed by terminal status (``RUNNING``, ``SUCCEEDED``,
    ``FAILED``, ``TIMED_OUT``, ``ABORTED``).

    This is the ONE walk implementation — ``_count_executions_in_window``
    below is a thin sum-of-all wrapper over it, so a status-blind caller and
    a status-aware caller can never see a different execution set
    (alpha-engine-config-I6991: a window containing only FAILED executions
    used to be indistinguishable from a healthy window because the walk
    only ever produced a single unclassified total).

    Uses ``states:ListExecutions`` with paginated startDate filtering —
    AWS does not support a startDate filter on ListExecutions directly,
    so we page through statusFilter results (which ARE status-exact per
    AWS's API contract — each page for ``statusFilter=X`` contains only
    status-X executions) and apply the time cutoff in Python. maxResults=100
    per page; we stop at the first page whose oldest entry is older than the
    window (lex-sortable by startDate desc).

    ``role_filter`` — count ONLY executions whose ``input.pipeline_role``
    is in the set. Required wherever a state machine carries more than one
    cadence: from 2026-07-29 the weekly SF also runs a post-close-chained
    daily EXERCISE run (alpha-engine-config#5489), so "did the Saturday
    cron fire" became unanswerable by an unfiltered count — 5 exercise runs
    a week satisfy a 7-day window on their own and the check could never
    alert again, no matter how dead the cron was (alpha-engine-config#5597 / #5590).

    An execution with NO role does not count toward a filtered check. The
    Saturday cron's EventBridge input sets ``pipeline_role="weekly"``
    explicitly, so an untagged execution is a manual/ad-hoc run — and a
    cadence run that somehow lost its role is itself an outage this
    watchdog should report, not paper over.
    """
    if client is None:  # pragma: no cover — production path
        client = boto3.client("stepfunctions", region_name=REGION)

    cutoff_utc = datetime.now(timezone.utc) - timedelta(seconds=window_seconds)
    counts: "dict[str, int]" = {}
    described = 0

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
                    bucket = status_filter
                    # Both the role filter and the gate-out verdict live
                    # inside the execution, not on the ListExecutions row —
                    # so either one costs a describe, and ONE describe pays
                    # for both (``_execution_facts``). The gate verdict is
                    # asked only of SUCCEEDED executions: WeeklyRunDaySkip is
                    # a SUCCEEDED terminal, and a FAILED/ABORTED run is never
                    # a designed skip whatever its output says.
                    wants_gate_verdict = gate_noop_aware and status_filter == "SUCCEEDED"
                    if role_filter is not None or wants_gate_verdict:
                        described += 1
                        if described > _MAX_ROLE_DESCRIBES:
                            # Never silently truncate a coverage check: a
                            # truncated walk that found nothing is
                            # indistinguishable from a dead cron.
                            raise RuntimeError(
                                f"pipeline-watchdog: describe-backed walk for {sf_arn} "
                                f"exceeded {_MAX_ROLE_DESCRIBES} DescribeExecution "
                                f"calls in a {window_seconds}s window — the count "
                                f"cannot be trusted; widen the cap or narrow the "
                                f"window before this check is believed"
                            )
                        facts = _execution_facts(
                            client, exec_row.get("executionArn")
                        )
                        if wants_gate_verdict and facts.is_gate_out:
                            bucket = GATE_SKIP
                        if role_filter is not None and facts.role not in role_filter:
                            continue
                    counts[bucket] = counts.get(bucket, 0) + 1
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

    return counts


def _count_executions_in_window(
    sf_arn: str,
    window_seconds: int,
    *,
    client: Optional[object] = None,
    role_filter: Optional[frozenset] = None,
) -> int:
    """Total executions that STARTED for ``sf_arn`` in the window, across
    ALL statuses — a thin sum over ``_status_counts_in_window``. Retained
    for callers that only need "did it fire at all" (none in this file as
    of I6991; kept as the tested, documented primitive other watchdog code
    may still want)."""
    return sum(
        _status_counts_in_window(
            sf_arn, window_seconds, client=client, role_filter=role_filter
        ).values()
    )


def _publish_watchdog_alert(
    message: str,
    *,
    severity: str,
    dedup_key: str,
    context: dict,
    dedup_window_min: int = 12 * 60,
) -> str:
    """Publish one watchdog alert via the shared SNS + Telegram fan-out.

    Factored out of ``_check_sf``'s alert path (alpha-engine-config#2412) so
    the preopen-buffer canary below reuses the exact same channel-
    independence + dedup wiring rather than duplicating it — no new SNS
    topic or Telegram channel. Returns a compact ``alert_detail`` string for
    the caller's ``CheckResult``.
    """
    result = alerts.publish(
        message=message,
        severity=severity,
        source="alpha-engine-pipeline-watchdog",
        sns=True,
        telegram=False,
        sns_topic_arn=WATCHDOG_SNS_TOPIC_ARN,
        dedup_key=dedup_key,
        dedup_window_min=dedup_window_min,
    )
    telegram_ok = notify_via_flow_doctor(
        message,
        silent=False,
        severity=severity,
        dedup_key=dedup_key,
        flow_name=_FLOW_NAME,
        topics=PIPELINE_OBSERVER_TELEGRAM_TOPICS,
        db_basename=_DB_BASENAME,
        context=context,
        # Must match the SNS/bus alerts.publish() source= above exactly —
        # both paths alert on the same event, and the registered
        # `pipeline_watchdog_stuck_sf` class in playbooks.yaml keys on this
        # string (config-I3513).
        source="alpha-engine-pipeline-watchdog",
    )
    logger.warning(
        "watchdog ALERT: severity=%s dedup_key=%s sns_ok=%s telegram_ok=%s dedup_skipped=%s",
        severity,
        dedup_key,
        result.sns.ok,
        telegram_ok,
        getattr(result, "dedup_skipped", False),
    )
    return (
        f"sns_ok={result.sns.ok} telegram_ok={telegram_ok} "
        f"dedup_skipped={getattr(result, 'dedup_skipped', False)}"
    )


def _check_sf(
    *,
    sf_label: str,
    sf_arn: str,
    is_watch_day: bool,
    skip_reason_if_not_watching: str,
    window_seconds: int,
    client: Optional[object] = None,
    role_filter: Optional[frozenset] = None,
    gate_noop_aware: bool = False,
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

    counts = _status_counts_in_window(
        sf_arn,
        window_seconds,
        client=client,
        role_filter=role_filter,
        gate_noop_aware=gate_noop_aware,
    )
    seen = sum(counts.values())
    gate_skips = counts.get(GATE_SKIP, 0)
    # "Fired" and "healthy" are separate questions (alpha-engine-config-I6991).
    # RUNNING counts as healthy-so-far — a still-in-flight execution is not a
    # failure and must not be reported as one. Only SUCCEEDED/RUNNING clear
    # the check; a window whose every execution terminated FAILED/TIMED_OUT/
    # ABORTED is an alert, not a "watchdog clear".
    # A run-day gate no-op is bucketed as GATE_SKIP and is deliberately in
    # NEITHER term (alpha-engine-config-I8045). It is not a success — it
    # dispatched nothing — and it is not a failure — it was correct to do
    # nothing. Before I8045 it landed in `SUCCEEDED` and cleared this check
    # on its own, so a window holding one gate-out and one genuinely failed
    # weekly run reported "watchdog clear" and the failure never paged.
    healthy_seen = counts.get("SUCCEEDED", 0) + counts.get("RUNNING", 0)
    window_hours = window_seconds // 3600

    if healthy_seen == 0 and gate_skips > 0 and gate_skips == seen:
        # Everything in the window was a designed skip. The schedule fired and
        # the control plane is fine, so this is neither NEVER_FIRED nor
        # FIRED_AND_FAILED — and it does NOT page from here: the weekly-SF
        # silence deadman owns the "the real run is missing" alert, and
        # `observability-policy.md` §7.2a is one notification per failure, not
        # one per detector that noticed it. Recorded loudly and carried on the
        # CheckResult so the absence is visible rather than inferred.
        logger.warning(
            "watchdog: sf=%s saw %d execution(s) in the last %dh and EVERY one "
            "declared itself a run-day-gate no-op (is_weekly_run_day=false, "
            "dispatched nothing). Not a clear: no real run succeeded. The "
            "weekly-SF silence deadman owns the page for this condition; this "
            "check reports ONLY_GATE_SKIPS.",
            sf_label, seen, window_hours,
        )
        return CheckResult(
            sf_label=sf_label,
            sf_arn=sf_arn,
            checked=True,
            executions_seen=seen,
            outcome="ONLY_GATE_SKIPS",
        )

    if healthy_seen > 0:
        logger.info(
            "watchdog clear: sf=%s executions_in_window=%d status_counts=%s",
            sf_label, seen, dict(counts),
        )
        return CheckResult(
            sf_label=sf_label,
            sf_arn=sf_arn,
            checked=True,
            executions_seen=seen,
            outcome="CLEAR",
        )

    if seen == 0:
        # 0 executions in window on a watch-day → the SF never fired.
        message = (
            f"{sf_label} has not executed in the last {window_hours}h on a trading-day window"
            f"{_role_clause(role_filter)}. "
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
        alert_detail = _publish_watchdog_alert(
            message,
            severity="error",
            dedup_key=dedup_key,
            context={"sf_label": sf_label, "sf_arn": sf_arn},
        )
        return CheckResult(
            sf_label=sf_label,
            sf_arn=sf_arn,
            checked=True,
            executions_seen=0,
            alert_emitted=True,
            alert_detail=alert_detail,
            outcome="NEVER_FIRED",
        )

    # seen > 0 but healthy_seen == 0: every execution in the window fired
    # and terminated FAILED/TIMED_OUT/ABORTED — status-blind counting used
    # to report this identically to a clean run (alpha-engine-config-I6991,
    # measured 2026-08-06: "watchdog clear: sf=Weekday SF
    # executions_in_window=1" on a run that failed 3.5s after starting).
    # This is a DIFFERENT operator action from "never fired" — the schedule
    # and control plane are fine, the workload failed — so the message says
    # so explicitly and uses a distinct dedup key.
    status_breakdown = ", ".join(
        f"{status}={count}" for status, count in sorted(counts.items()) if count
    )
    message = (
        f"{sf_label} fired and FAILED in the last {window_hours}h window"
        f"{_role_clause(role_filter)}: {seen} execution(s) started "
        f"({status_breakdown}), none SUCCEEDED and none still RUNNING. "
        f"This is NOT a missed-schedule condition — the EventBridge trigger and "
        f"SF control plane are working; the workload itself failed. Investigate: "
        f"`aws stepfunctions list-executions --state-machine-arn {sf_arn} "
        f"--status-filter FAILED --max-results 10`."
    )
    dedup_key = (
        f"pipeline-watchdog-fired-failed-{sf_label}-"
        f"{datetime.now(timezone.utc).date().isoformat()}"
    )
    alert_detail = _publish_watchdog_alert(
        message,
        severity="error",
        dedup_key=dedup_key,
        context={"sf_label": sf_label, "sf_arn": sf_arn, "status_counts": dict(counts)},
    )
    return CheckResult(
        sf_label=sf_label,
        sf_arn=sf_arn,
        checked=True,
        executions_seen=seen,
        alert_emitted=True,
        alert_detail=alert_detail,
        outcome="FIRED_AND_FAILED",
    )


@dataclass(frozen=True)
class BufferCheckResult:
    """Outcome of one preopen schedule-buffer canary run."""

    checked: bool
    skip_reason: Optional[str] = None
    target_trading_day: Optional[str] = None  # ISO date of the day evaluated
    finish_pt: Optional[str] = None  # ISO datetime, America/Los_Angeles
    minutes_before_open: Optional[float] = None  # negative = after open
    alert_emitted: bool = False
    alert_severity: Optional[str] = None  # None | "warning" | "error"
    alert_detail: Optional[str] = None
    trend_median_minutes_before_open: Optional[float] = None
    trend_days_used: Optional[int] = None
    trend_alert_emitted: bool = False


def _iter_succeeded_weekday_executions(
    client: object, lookback_days: int
) -> "list[tuple[datetime, datetime]]":
    """Return ``(start_utc, stop_utc)`` for every SUCCEEDED Weekday-SF
    execution that started within the last ``lookback_days`` calendar days,
    newest first (as returned by ListExecutions).

    No ``pipeline_role`` filter. The grant for it DOES exist (see the
    module docstring's 2026-08-09 correction); filtering on
    status=SUCCEEDED at the API layer is enough to keep this walk cheap
    without paying one DescribeExecution per row — alpha-engine-config
    -I6748 revisits whether the precision is worth the cost.
    """
    cutoff_utc = datetime.now(timezone.utc) - timedelta(days=lookback_days)
    rows: list[tuple[datetime, datetime]] = []
    next_token: Optional[str] = None
    while True:
        kwargs = {
            "stateMachineArn": WEEKDAY_SF_ARN,
            "statusFilter": "SUCCEEDED",
            "maxResults": 100,
        }
        if next_token:
            kwargs["nextToken"] = next_token
        resp = client.list_executions(**kwargs)
        execs = resp.get("executions") or []
        stop_paging = False
        for row in execs:
            start = row.get("startDate")
            stop = row.get("stopDate")
            if start is None or stop is None or not hasattr(start, "astimezone"):
                continue
            start_utc = (
                start.astimezone(timezone.utc)
                if start.tzinfo
                else start.replace(tzinfo=timezone.utc)
            )
            if start_utc < cutoff_utc:
                # Newest-first ordering → everything after this is older
                # still. Stop paging.
                stop_paging = True
                break
            stop_utc = (
                stop.astimezone(timezone.utc)
                if stop.tzinfo
                else stop.replace(tzinfo=timezone.utc)
            )
            rows.append((start_utc, stop_utc))
        if stop_paging:
            break
        next_token = resp.get("nextToken")
        if not next_token:
            break
    return rows


def _weekday_finish_by_trading_day(
    client: object, lookback_days: int = BUFFER_LOOKBACK_CALENDAR_DAYS
) -> "dict[date, datetime]":
    """Map PT calendar trading-day → finish (``stopDate``, PT-zoned) of that
    day's EARLIEST-started SUCCEEDED Weekday-SF execution — the proxy for
    "the scheduled 05:15 AM PT run" (see module docstring for why not
    role-filtered). A same-day manual rerun, if any, starts later and is
    ignored in favor of the earlier (scheduled) one.
    """
    by_day: "dict[date, tuple[datetime, datetime]]" = {}
    for start_utc, stop_utc in _iter_succeeded_weekday_executions(client, lookback_days):
        pt_date = start_utc.astimezone(PT_ZONE).date()
        existing = by_day.get(pt_date)
        if existing is None or start_utc < existing[0]:
            by_day[pt_date] = (start_utc, stop_utc.astimezone(PT_ZONE))
    return {d: finish_pt for d, (_, finish_pt) in by_day.items()}


def _classify_buffer_severity(finish_pt: datetime, target_date: date) -> Optional[str]:
    """Return ``"error"`` (hard floor breach or missed-open), ``"warning"``
    (early-warning floor), or ``None`` (quiet) for one day's finish time.
    ``finish_pt`` must already be zoned to ``PT_ZONE``.
    """
    # MARKET_OPEN_TIME_PT (06:30) > HARD_ALERT_TIME_PT (06:15) always, so a
    # finish at/after open is already caught by the hard-floor comparison —
    # the "missed the open" distinction is made by the caller (message
    # wording only), not a separate severity tier.
    hard_pt = datetime.combine(target_date, HARD_ALERT_TIME_PT, tzinfo=PT_ZONE)
    warn_pt = datetime.combine(target_date, WARN_TREND_TIME_PT, tzinfo=PT_ZONE)
    if finish_pt >= hard_pt:
        return "error"
    if finish_pt >= warn_pt:
        return "warning"
    return None


def _minutes_before_open(finish_pt: datetime, target_date: date) -> float:
    open_pt = datetime.combine(target_date, MARKET_OPEN_TIME_PT, tzinfo=PT_ZONE)
    return (open_pt - finish_pt).total_seconds() / 60.0


def _rolling_trend_median(
    by_day: "dict[date, datetime]", target_date: date, window: int = ROLLING_WINDOW_TRADING_DAYS
) -> "Optional[tuple[float, int]]":
    """Median ``minutes_before_open`` (negative = after open) across the
    most recent ``window`` trading days with SUCCEEDED data, up to and
    including ``target_date``. Returns ``(median, days_used)`` or ``None``
    if fewer than ``_MIN_TREND_DAYS`` days have data — never fabricates a
    trend from too few points.
    """
    days = sorted((d for d in by_day if d <= target_date), reverse=True)[:window]
    if len(days) < _MIN_TREND_DAYS:
        return None
    deltas = sorted(_minutes_before_open(by_day[d], d) for d in days)
    n = len(deltas)
    median = (
        deltas[n // 2]
        if n % 2 == 1
        else (deltas[n // 2 - 1] + deltas[n // 2]) / 2.0
    )
    return median, n


def _check_preopen_buffer(
    *,
    now_utc: datetime,
    is_watch_day: bool,
    client: Optional[object] = None,
) -> BufferCheckResult:
    """Preopen schedule-buffer canary (alpha-engine-config#2412). See module
    docstring for full design rationale."""
    if not is_watch_day:
        logger.info("preopen-buffer skip: today is not a NYSE trading day")
        return BufferCheckResult(
            checked=False,
            skip_reason=(
                "today is not a NYSE trading day (weekend / holiday) per "
                "nousergon_lib.trading_calendar"
            ),
        )

    if client is None:  # pragma: no cover — production path
        client = boto3.client("stepfunctions", region_name=REGION)

    target_date = last_closed_trading_day(now_utc)
    by_day = _weekday_finish_by_trading_day(client)
    finish_pt = by_day.get(target_date)

    if finish_pt is None:
        # No SUCCEEDED execution for the target day — either it genuinely
        # didn't run (the existing Weekday-SF liveness check above already
        # covers that) or it ran and FAILED (the SF's own HandleFailure
        # path covers that). Either way, do NOT double-page here.
        logger.info(
            "preopen-buffer defer: no SUCCEEDED Weekday-SF execution found for %s "
            "— deferring to the Weekday-SF liveness check / SF failure alert",
            target_date,
        )
        return BufferCheckResult(
            checked=True,
            target_trading_day=target_date.isoformat(),
            skip_reason=(
                f"no SUCCEEDED execution for {target_date}; deferred to the "
                f"Weekday-SF liveness check (0-executions case) or the SF's own "
                f"failure alert — not double-paged here"
            ),
        )

    minutes_before_open = _minutes_before_open(finish_pt, target_date)
    severity = _classify_buffer_severity(finish_pt, target_date)
    trend = _rolling_trend_median(by_day, target_date)
    trend_median, trend_days = trend if trend else (None, None)
    trend_severity = (
        "warning"
        if trend_median is not None
        and trend_median <= (
            (MARKET_OPEN_TIME_PT.hour * 60 + MARKET_OPEN_TIME_PT.minute)
            - (WARN_TREND_TIME_PT.hour * 60 + WARN_TREND_TIME_PT.minute)
        )
        else None
    )

    if severity is None and trend_severity is None:
        logger.info(
            "preopen-buffer clear: %s finished %s (%.1fmin before 06:30 PT open)",
            target_date, finish_pt.isoformat(), minutes_before_open,
        )
        return BufferCheckResult(
            checked=True,
            target_trading_day=target_date.isoformat(),
            finish_pt=finish_pt.isoformat(),
            minutes_before_open=minutes_before_open,
            trend_median_minutes_before_open=trend_median,
            trend_days_used=trend_days,
        )

    context = {"target_trading_day": target_date.isoformat(), "sf_arn": WEEKDAY_SF_ARN}
    alert_detail = None
    alert_emitted = False
    trend_alert_emitted = False

    if severity is not None:
        missed_open = finish_pt >= datetime.combine(
            target_date, MARKET_OPEN_TIME_PT, tzinfo=PT_ZONE
        )
        if missed_open:
            headline = (
                f"ne-preopen-trading-pipeline MISSED THE 06:30 PT OPEN on "
                f"{target_date}: RunDaemon-ready at {finish_pt.strftime('%H:%M:%S %Z')}, "
                f"{-minutes_before_open:.1f}min AFTER open."
            )
        else:
            headline = (
                f"ne-preopen-trading-pipeline schedule-buffer breach on {target_date}: "
                f"finished {finish_pt.strftime('%H:%M:%S %Z')}, only "
                f"{minutes_before_open:.1f}min before the 06:30 PT open "
                f"(hard floor {HARD_ALERT_TIME_PT.strftime('%H:%M')} PT)."
            )
        message = (
            f"{headline} This is the schedule-buffer-erosion canary "
            f"(alpha-engine-config#2412) — the trigger (WeekdayPipelineSchedule) "
            f"has already been moved earlier twice for this exact failure mode: "
            f"06:00→05:45 PT (2026-05-19), 05:45→05:15 PT (2026-07-13). "
            f"Investigate stage-duration creep (CodeFreshnessGate / predictor "
            f"health / chronic-gap self-heal) or move the trigger earlier again."
        )
        dedup_key = f"pipeline-watchdog-preopen-buffer-{target_date.isoformat()}"
        alert_detail = _publish_watchdog_alert(
            message, severity=severity, dedup_key=dedup_key, context=context
        )
        alert_emitted = True
    elif trend_severity is not None:
        # Single-day reading is quiet but the 5-day median has crossed the
        # warn floor — a creep no single day's threshold caught.
        message = (
            f"ne-preopen-trading-pipeline schedule-buffer TREND warning: median "
            f"finish over the last {trend_days} trading days is "
            f"{-trend_median if trend_median < 0 else trend_median:.1f}min "
            f"{'after' if trend_median < 0 else 'before'} the 06:30 PT open — "
            f"below the {WARN_TREND_TIME_PT.strftime('%H:%M')} PT early-warning "
            f"floor even though {target_date} itself finished with buffer to "
            f"spare. This is the schedule-buffer-erosion canary "
            f"(alpha-engine-config#2412); the trigger has been moved earlier "
            f"twice before for this pattern (2026-05-19, 2026-07-13)."
        )
        dedup_key = f"pipeline-watchdog-preopen-buffer-trend-{target_date.isoformat()}"
        alert_detail = _publish_watchdog_alert(
            message, severity="warning", dedup_key=dedup_key, context=context
        )
        trend_alert_emitted = True

    return BufferCheckResult(
        checked=True,
        target_trading_day=target_date.isoformat(),
        finish_pt=finish_pt.isoformat(),
        minutes_before_open=minutes_before_open,
        alert_emitted=alert_emitted,
        alert_severity=severity,
        alert_detail=alert_detail,
        trend_median_minutes_before_open=trend_median,
        trend_days_used=trend_days,
        trend_alert_emitted=trend_alert_emitted,
    )


# ── Prior-day failed-run check (alpha-engine-config#6732) ─────────────────
#
# sf-pipeline-policy §4.1 requires a liveness detector INDEPENDENT of the
# pipeline's own notifier that is sensitive to started-but-never-succeeded.
# The three liveness checks above answer only "did it fire" — a day where
# every execution FAILED satisfies them by design (``_STARTED_STATUSES``),
# failure alerting was delegated to the SF's own HandleFailure path (which
# is exactly the dependent channel §4.1 says not to rely on), and the
# independent deadman (ExecutionsSucceeded, 7-day period) needs a week of
# silence to breach. This check closes that combination: on the watchdog
# firing after a trading day whose weekday/EOD executions all terminated
# without success AND without a DEGRADED completion marker (the Option-A
# visible-degrade terminal, which status-keyed watchers already engage on,
# config#6692), page. Zero-execution days are deliberately excluded — that
# is the never-fired case the ``_check_sf`` liveness checks above own.
#
# alpha-engine-config-I7036: the weekly SF (``ne-weekly-freshness-pipeline``)
# joins this check now that ``WriteCompletionMarkerDegraded`` exists for it
# (alpha-engine-config-I6891 / nousergon-data-PR1319, merged 2026-08-12). Its
# cadence is NOT weekday-trading-day like the two entries above — the
# expected cycle day is "day after the week's last trading session" (usually
# Saturday, Friday/Thursday on a holiday-shortened week per real precedent
# 2026-07-03), and the SF is ALSO chain-launched daily as a
# ``pipeline_role=exercise`` dry exercise run and self-gates a 2s
# ``WeeklyRunDayGateChoice`` SUCCEEDED no-op on every THU/FRI cron firing
# that isn't the real day. Neither an exercise run nor a gate-skip may be
# counted as the weekly cycle — see ``_weekly_real_statuses_for_day``, which
# reuses ``weekly_sf_silence_deadman``'s own role/gate-noop discrimination
# (``_is_gate_noop``) rather than re-deriving it, so this check and the
# silence deadman below can never disagree about what a real weekly cycle
# execution looks like.

MARKER_BUCKET = os.environ.get("COMPLETION_MARKER_BUCKET", "alpha-engine-research")

# (sf_label suffix, SF ARN, marker pipeline segment, cadence). Marker keys
# follow the definitions' WriteCompletionMarker states: the {date} segment
# is the UTC date-part of $$.Execution.StartTime — for the two
# ``trading_day``-cadence pipelines every legal start (preopen 12:15 UTC,
# postclose ~20:15–23:00 UTC incl. the backstop path; the split-out reconcile
# ~23:30–02:15 UTC, i.e. still the same PT evening) lands on the same UTC
# calendar date as the PT trading date, so a lookup by PT trading day is
# exact. The ``weekly``-cadence entry's target date is instead derived by
# ``_last_due_weekly_day`` (the real cycle day, not a trading day) and its
# execution count comes from ``_weekly_real_statuses_for_day`` instead of
# the generic ``_statuses_for_day`` — see ``_check_failed_day``'s
# ``cadence`` branch.
_FAILED_DAY_PIPELINES = (
    ("Weekday SF", WEEKDAY_SF_ARN, "ne-preopen-trading-pipeline", "trading_day"),
    ("EOD SF", EOD_SF_ARN, "ne-postclose-trading-pipeline", "trading_day"),
    ("EOD reconcile SF", RECONCILE_SF_ARN, "ne-postclose-reconcile-pipeline", "trading_day"),
    ("Weekly SF", SATURDAY_SF_ARN, "ne-weekly-freshness-pipeline", "weekly"),
)


@dataclass(frozen=True)
class FailedDayCheckResult:
    """Per-SF outcome of one prior-day failed-run check."""

    sf_label: str
    checked: bool
    target_trading_day: Optional[str] = None
    skip_reason: Optional[str] = None
    executions_on_day: Optional[int] = None
    succeeded_on_day: Optional[int] = None
    marker_status: Optional[str] = None  # None = not consulted
    alert_emitted: bool = False
    alert_detail: Optional[str] = None


def _statuses_for_day(
    client: object, sf_arn: str, target_date: date
) -> "dict[str, int]":
    """Count executions per status whose ``startDate`` falls on
    ``target_date`` in PT calendar terms. Pages newest-first per status and
    stops once rows predate the target day. Date comparison only — no
    datetime construction, so ``patch("index.datetime")`` in tests never
    bites (see the module's ``_eod_window_seconds`` note for the pattern).
    """
    counts: "dict[str, int]" = {}
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
            stop_paging = False
            for row in resp.get("executions") or []:
                start = row.get("startDate")
                if start is None or not hasattr(start, "astimezone"):
                    continue
                start_utc = (
                    start.astimezone(timezone.utc)
                    if start.tzinfo
                    else start.replace(tzinfo=timezone.utc)
                )
                start_pt_date = start_utc.astimezone(PT_ZONE).date()
                if start_pt_date == target_date:
                    counts[status_filter] = counts.get(status_filter, 0) + 1
                elif start_pt_date < target_date:
                    # Newest-first ordering → everything further is older.
                    stop_paging = True
                    break
            if stop_paging:
                break
            next_token = resp.get("nextToken")
            if not next_token:
                break
    return counts


def _completion_marker_status(
    s3_client: object, pipeline_name: str, target_date: date
) -> str:
    """Status field of the day's completion marker, ``"ABSENT"`` when the
    key does not exist, ``"UNREADABLE"`` on any other failure.

    UNREADABLE is deliberately distinct from ABSENT and both page (§2.3a:
    a missing verdict propagates as UNKNOWN, never as pass) — the swallowed
    failure modes here (S3 errors other than NoSuchKey, unparseable JSON,
    missing status field) all route to the caller's alert path plus a
    WARNING log, never to a silent pass.

    alpha-engine-config-I8217: every object under ``_sf_completion/`` is
    written by a Step Functions ``States.Format`` body, so the S3 object is
    a JSON string LITERAL containing the JSON object — one ``json.loads``
    yields a ``str``, not a ``dict``. This function used to do exactly that
    single decode and then guard with ``isinstance(marker, dict)``, which
    was therefore ALWAYS FALSE: every read fell through to "no usable status
    field" (UNREADABLE) regardless of what the marker actually said, for
    every pipeline, every date, since the guard was written. Delegates to
    ``nousergon_lib.pipeline_status.completion_marker.read_marker``, the
    fleet's canonical marker reader (alpha-engine-config-I8154/I8186),
    which unwraps the double-encoding and classifies NoSuchKey as absence
    rather than failure — closing this file's independent, broken
    reimplementation of the same fix ``crucible-predictor/monitoring/
    drift_detector.py::_load_json_maybe_wrapped`` already carried.
    """
    key = f"_sf_completion/{pipeline_name}/{target_date.isoformat()}.json"
    try:
        marker = read_marker(s3_client, bucket=MARKER_BUCKET, key=key)
    except Exception as exc:  # classified below — never a silent pass
        logger.warning(
            "failed-day check: marker s3://%s/%s unreadable (%s: %s)",
            MARKER_BUCKET, key, type(exc).__name__, exc,
        )
        return "UNREADABLE"
    if marker is None:
        return "ABSENT"
    status = marker.get("status") if isinstance(marker, dict) else None
    if isinstance(status, str) and status:
        return status
    logger.warning(
        "failed-day check: marker s3://%s/%s has no usable status field",
        MARKER_BUCKET, key,
    )
    return "UNREADABLE"


def _last_due_weekly_day(now_utc: datetime) -> Optional[date]:
    """Most recent weekly-role run-slot day that is fully due, per the
    silence deadman's own slot derivation (alpha-engine-config-I7036) — the
    day AFTER the week's last trading session, never assumed to be Saturday
    (a holiday-shortened week lands the real cycle on Friday or Thursday;
    real precedent 2026-07-03). ``cadence`` is passed as ``"off"`` here
    because ``compute_expected_slots`` only uses it to gate EXERCISE slots
    (never-gated weekly slots per that function's own docstring) — this
    call cares about weekly slots only, so no SSM read is needed here and
    this check does not race the separate weekly-silence check's own SSM
    fetch. Returns ``None`` only if the 14-day lookback contains no weekly
    slot at all (should not happen in steady state; defensive).
    """
    through = last_due_day(now_utc.date())
    slots = compute_expected_slots(
        through,
        cadence="off",
        window_days=14,
        is_trading_day=is_trading_day,
        next_trading_day=next_trading_day,
    )
    weekly_days = sorted(s.day for s in slots if s.role == "weekly" and s.day <= through)
    return weekly_days[-1] if weekly_days else None


def _weekly_real_statuses_for_day(client: object, target_date: date) -> "dict[str, int]":
    """Status counts of REAL, non-gate-noop executions of the weekly CYCLE
    ``target_date`` (alpha-engine-config-I7036, corrected by -I7440).

    Reuses ``weekly_sf_silence_deadman.fetch_execution_records`` (role and
    run_date read from each execution's own input, never inferred from name —
    no launch path passes an explicit SF ``Name``) and ``_is_gate_noop``
    (excludes a SUCCEEDED execution whose own OUTPUT carries the gate's
    ``is_weekly_run_day: false`` verdict — ``WeeklyRunDayGateChoice``'s
    designed skip on a THU/FRI cron firing that isn't the real cycle day.
    Never a duration test: alpha-engine-config-I8057 measured a 5.7s gate-out
    and a 12.6s real run). window_days=12
    covers ``target_date`` even on the longest holiday-shortened weeks, plus
    the multi-day recovery tail below, while capping the
    ``describe_execution`` fan-out this makes.

    **Two corrections, either of which alone false-paged cycle 2026-08-15**
    (alpha-engine-config-I7440 — the cycle recovered to a SUCCEEDED terminal
    and this check paged anyway):

    1. **Role set.** This counted only ``role == "weekly"`` — the Saturday
       cron — so every execution from the §2.5 mechanical recovery path
       (``watch-rerun``) and every sf-watch substrate relaunch (``recovery``)
       was discarded. A cycle whose cron run failed and whose rerun succeeded
       could therefore NEVER clear this check: the policy-mandated recovery
       mechanism was invisible to the detector watching the thing it
       recovers. It now uses ``WEEKLY_CADENCE_ROLES``, the same constant the
       liveness check above already passes as its ``role_filter`` — which is
       what the block comment above always claimed ("can never disagree") and
       what was measurably false on 2026-08-16, when the two checks in this
       one invocation saw 1 execution and 24 executions for the same SF on
       the same day and reached opposite verdicts.
    2. **Cycle key.** This matched on the execution's UTC START date. A
       recovery rerun of Saturday's cycle legitimately starts Sunday — the
       run that closed cycle 2026-08-15 started 2026-08-16T03:21Z — so
       matching on start excludes exactly the executions recovery produces.
       It now matches the execution's own ``run_date`` input, which is the
       key the run-slot mutex and the completion marker's ``cycle_key``
       already use. Records with no parseable run_date fall back to the UTC
       start date, preserving the old behaviour for anything that predates
       run_date being carried.
    """
    since_anchor = target_date + timedelta(days=2)
    records = fetch_execution_records(client, window_days=12, today=since_anchor)
    counts: "dict[str, int]" = {}
    for record in records:
        if record.role not in WEEKLY_CADENCE_ROLES:
            continue
        cycle = getattr(record, "run_date", None)
        if cycle is None:
            cycle = record.start.astimezone(timezone.utc).date()
        if cycle != target_date:
            continue
        if _is_gate_noop(record):
            continue
        counts[record.status] = counts.get(record.status, 0) + 1
    return counts


def _check_failed_day(
    *,
    sf_label: str,
    sf_arn: str,
    pipeline_name: str,
    target_date: date,
    cadence: str = "trading_day",
    client: Optional[object] = None,
    s3_client: Optional[object] = None,
) -> FailedDayCheckResult:
    """Page when ``target_date`` had executions for this SF but none
    SUCCEEDED (as a REAL cycle — see ``cadence``) and no DEGRADED marker was
    written. See the block comment above for why this exists.

    ``cadence="trading_day"`` (Weekday/EOD SFs): ``target_date`` is the most
    recent closed trading day, and any execution starting that day counts.
    ``cadence="weekly"`` (Weekly SF, alpha-engine-config-I7036):
    ``target_date`` is the real weekly cycle day (see
    ``_last_due_weekly_day``), and only ``pipeline_role="weekly"``,
    non-gate-noop executions count — see ``_weekly_real_statuses_for_day``.
    """
    if client is None:  # pragma: no cover — production path
        client = boto3.client("stepfunctions", region_name=REGION)

    if cadence == "weekly":
        counts = _weekly_real_statuses_for_day(client, target_date)
    else:
        counts = _statuses_for_day(client, sf_arn, target_date)
    total = sum(counts.values())
    succeeded = counts.get("SUCCEEDED", 0)

    if total == 0:
        return FailedDayCheckResult(
            sf_label=sf_label,
            checked=True,
            target_trading_day=target_date.isoformat(),
            executions_on_day=0,
            succeeded_on_day=0,
            skip_reason=(
                "no executions on the target day — never-fired is the "
                "liveness checks' case, not this one's"
                if cadence != "weekly"
                else
                "no real (non-gate-skip, pipeline_role=weekly) execution on "
                "the target cycle day — deferred to the weekly-SF silence "
                "deadman (never-fired), not double-paged here"
            ),
        )
    if succeeded > 0:
        return FailedDayCheckResult(
            sf_label=sf_label,
            checked=True,
            target_trading_day=target_date.isoformat(),
            executions_on_day=total,
            succeeded_on_day=succeeded,
        )

    day_label = "cycle day" if cadence == "weekly" else "trading day"

    running = counts.get("RUNNING", 0)
    if running > 0:
        # A prior-day execution still RUNNING at 14:00 UTC the next day is
        # inside the definitions' top-level TimeoutSeconds only marginally
        # (postclose ceiling 18h from a ~20:15 UTC start). Don't declare the
        # day failed yet — but say so at warning severity rather than
        # silently deferring: an 18h run is itself a hang in the making.
        message = (
            f"{sf_label} ({pipeline_name}) still has {running} RUNNING "
            f"execution(s) from {day_label} {target_date} at watchdog time — "
            f"no SUCCEEDED execution for that day yet. This is at or beyond "
            f"the definition's top-level timeout horizon; investigate: "
            f"`aws stepfunctions list-executions --state-machine-arn {sf_arn} "
            f"--max-results 10`."
        )
        dedup_key = f"pipeline-watchdog-failed-day-{pipeline_name}-{target_date.isoformat()}"
        alert_detail = _publish_watchdog_alert(
            message,
            severity="warning",
            dedup_key=dedup_key,
            context={"sf_label": sf_label, "sf_arn": sf_arn,
                     "target_trading_day": target_date.isoformat()},
        )
        return FailedDayCheckResult(
            sf_label=sf_label,
            checked=True,
            target_trading_day=target_date.isoformat(),
            executions_on_day=total,
            succeeded_on_day=0,
            alert_emitted=True,
            alert_detail=alert_detail,
        )

    if s3_client is None:  # pragma: no cover — production path
        s3_client = boto3.client("s3", region_name=REGION)
    marker_status = _completion_marker_status(s3_client, pipeline_name, target_date)

    if marker_status == "DEGRADED":
        # Option-A visible degrade (config#6692): the run terminated in a
        # Fail state on purpose, the marker says so, and status-keyed
        # watchers engaged. Not a silent failure — do not double-page.
        logger.info(
            "failed-day clear (degraded-by-design): sf=%s day=%s marker=DEGRADED",
            sf_label, target_date,
        )
        return FailedDayCheckResult(
            sf_label=sf_label,
            checked=True,
            target_trading_day=target_date.isoformat(),
            executions_on_day=total,
            succeeded_on_day=0,
            marker_status=marker_status,
        )

    if marker_status == "SUCCEEDED":
        # The day's own pipeline wrote a SUCCEEDED marker, which only its
        # WriteCompletionMarker state on a SUCCEEDED terminal does — that is
        # a real verdict, and §2.3a's rule is that a MISSING verdict is
        # UNKNOWN, not that a present one may be overruled by this check's
        # own execution walk. Clear, do not page.
        #
        # But the walk and the marker have now DISAGREED, and exactly one of
        # them is wrong. Say so at warning severity rather than clearing
        # quietly: a counting bug in the walk (which is what
        # alpha-engine-config-I7440 was) is otherwise completely invisible
        # from the moment the marker starts covering for it.
        logger.warning(
            "failed-day clear (marker SUCCEEDED) but the execution walk found "
            "0 SUCCEEDED of %d on sf=%s day=%s — the walk and the completion "
            "marker disagree; one of them is wrong. Not paging (the marker is "
            "the authoritative verdict), but the walk's role/cycle-key "
            "filtering needs checking (alpha-engine-config-I7440).",
            total, sf_label, target_date,
        )
        return FailedDayCheckResult(
            sf_label=sf_label,
            checked=True,
            target_trading_day=target_date.isoformat(),
            executions_on_day=total,
            succeeded_on_day=0,
            marker_status=marker_status,
        )

    marker_clause = {
        "ABSENT": "no completion marker was written",
        "UNREADABLE": "the completion marker could not be read/parsed (UNKNOWN ≠ pass)",
    }.get(marker_status, f"completion marker status={marker_status!r}")
    rerun_clause = (
        "Recover with `python scripts/weekly_sf_rerun.py "
        "--execution-arn <failed execution arn> --dry-run` (then `--start`)"
        if cadence == "weekly"
        else
        "Recover mechanically with the rerun helper: "
        "`python scripts/weekday_sf_rerun.py "
        "--execution-arn <failed execution arn> --dry-run` (then `--start`)"
    )
    message = (
        f"{sf_label} ({pipeline_name}) FAILED {day_label} {target_date}: "
        f"{total} execution(s) started, none SUCCEEDED, and {marker_clause}. "
        f"This is the independent started-but-never-succeeded detector "
        f"(sf-pipeline-policy §4.1, alpha-engine-config#6732) — do not assume "
        f"the SF's own failure notification fired. {rerun_clause}; "
        f"list candidates: `aws stepfunctions list-executions "
        f"--state-machine-arn {sf_arn} --max-results 10`."
    )
    dedup_key = f"pipeline-watchdog-failed-day-{pipeline_name}-{target_date.isoformat()}"
    alert_detail = _publish_watchdog_alert(
        message,
        severity="error",
        dedup_key=dedup_key,
        context={"sf_label": sf_label, "sf_arn": sf_arn,
                 "target_trading_day": target_date.isoformat(),
                 "marker_status": marker_status},
    )
    return FailedDayCheckResult(
        sf_label=sf_label,
        checked=True,
        target_trading_day=target_date.isoformat(),
        executions_on_day=total,
        succeeded_on_day=0,
        marker_status=marker_status,
        alert_emitted=True,
        alert_detail=alert_detail,
    )


@dataclass(frozen=True)
class SilenceCheckResult:
    """Outcome of one weekly-SF silence-deadman run."""

    checked: bool
    cadence: Optional[str] = None
    evaluation_date: Optional[str] = None  # last fully-due day evaluated
    slots_evaluated: int = 0
    ok: int = 0
    gated_off: int = 0
    critical: int = 0
    critical_slots: tuple = ()
    gated_off_slots: tuple = ()
    alerts_emitted: int = 0
    degraded_reason: Optional[str] = None


def _silence_evaluation_date(now_utc: datetime) -> date:
    """The last day whose expected run-slots are fully DUE at cron time.

    Thin adapter over the deadman module's ``last_due_day`` — the SHARED
    definition, so the scheduled check and the operator CLI cannot disagree
    about whether today's slot counts. Same retrospective discipline as
    ``_eod_window_seconds`` and ``_check_preopen_buffer``: only look at
    windows that have fully elapsed. Evaluating "today" at the 14:00 UTC
    firing would classify every trading day's not-yet-due exercise slot
    (chained off that day's ~20:00 UTC postclose) as CRITICAL and page daily.
    """
    return last_due_day(now_utc.date())


def _check_weekly_silence(
    *,
    now_utc: datetime,
    client: Optional[object] = None,
    ssm_client: Optional[object] = None,
) -> SilenceCheckResult:
    """Weekly-SF silence deadman: a day the DECLARED cadence says the pipeline
    should have run and no execution exists at all (alpha-engine-config#6738,
    sf-pipeline-policy §2.6 rule 1).

    Distinct from the Saturday-SF liveness check above, which asks only "did
    the weekly cron fire in the last 7 days". This asks the per-day question
    the 2026-08-05/06 outage needed and nothing answered: for EVERY run-slot
    the declaration expects, does a matching execution exist? Postclose never
    fired on those two days, so the chained ``pipeline_role=exercise`` launch
    never happened — an absence, not a failure, and therefore invisible to
    every failure-triggered alert path.

    Deliberately NOT rebuilt on ``AWS/States`` ``ExecutionsStarted``: that
    metric carries no ``pipeline_role`` dimension, so it cannot separate a
    gated-off day from a dead one, which is why the prior CloudWatch deadman
    could never fire (alpha-engine-config#5599, alarm deleted 2026-08-09).

    No watch-day gate. Unlike the three liveness checks, the calendar
    awareness lives INSIDE the slot derivation (``compute_expected_slots``
    consults ``nousergon_lib.trading_calendar`` per candidate day), so a
    weekend or holiday firing simply derives zero expected slots for those
    days rather than being skipped wholesale.
    """
    if client is None:  # pragma: no cover — production path
        client = boto3.client("stepfunctions", region_name=REGION)
    if ssm_client is None:  # pragma: no cover — production path
        ssm_client = boto3.client("ssm", region_name=REGION)

    try:
        cadence = load_cadence_from_ssm(ssm_client)
    except Exception as exc:  # noqa: BLE001
        # NOT a swallow — the failure is converted into a severity=error page
        # on the independent watchdog topic AND recorded in the handler's
        # returned summary as degraded_reason. Rationale for not re-raising
        # (per the fail-loud carve-out): (a) the failure mode is "the cadence
        # declaration is unreadable", overwhelmingly an IAM grant that has not
        # been applied yet — this Lambda's exec-role policy is operator-
        # applied by design (infrastructure/iam/README.md single-writer rule),
        # so the code can legitimately reach production one deploy ahead of
        # its grant; (b) raising here would abort the handler AFTER the four
        # other checks have already published, discarding their summary and
        # burning EventBridge retries on a condition no retry can fix; (c) the
        # recording surfaces are this alert (same topic, same dedup wiring as
        # every other watchdog page) and the summary dict in CloudWatch Logs.
        # The check reports UNKNOWN, never OK — an unreadable declaration is
        # never rendered as "no silent slots".
        reason = f"{type(exc).__name__}: {exc}"
        logger.error(
            "weekly-silence deadman DEGRADED: cannot read %s — %s",
            CADENCE_SSM_PARAM, reason,
        )
        _publish_watchdog_alert(
            (
                f"weekly-SF silence deadman could NOT read its declared cadence from "
                f"SSM {CADENCE_SSM_PARAM} ({reason}). The deadman is the only detector "
                f"for a day the weekly pipeline should have run and produced NO "
                f"execution at all (the 2026-08-05/06 postclose silence) — it is "
                f"reporting UNKNOWN, not healthy, until this is fixed. If this is "
                f"AccessDenied the exec-role grant has not been applied yet; apply it "
                f"with: `bash infrastructure/lambdas/pipeline-watchdog/deploy.sh "
                f"--apply-iam` (alpha-engine-config#6738)."
            ),
            severity="error",
            dedup_key=(
                f"pipeline-watchdog-weekly-silence-degraded-{now_utc.date().isoformat()}"
            ),
            context={"ssm_param": CADENCE_SSM_PARAM, "sf_arn": SATURDAY_SF_ARN},
        )
        return SilenceCheckResult(
            checked=False,
            evaluation_date=_silence_evaluation_date(now_utc).isoformat(),
            degraded_reason=f"cannot read {CADENCE_SSM_PARAM}: {reason}",
            alerts_emitted=1,
        )

    evaluation_date = _silence_evaluation_date(now_utc)
    # Anchor the fetch to THIS invocation's now, not the wall clock, so the
    # fetch window and the slot window can never be measured from different
    # instants (the deadman CLI keeps its own _today() default).
    executions = fetch_execution_records(
        client, SILENCE_WINDOW_DAYS, today=now_utc.date()
    )
    slots = compute_expected_slots(
        evaluation_date,
        cadence,
        SILENCE_WINDOW_DAYS,
        is_trading_day,
        next_trading_day,
    )
    results = evaluate(slots, executions)

    critical = [r for r in results if r.classification == "CRITICAL"]
    gated = [r for r in results if r.classification == "GATED_OFF"]
    ok = [r for r in results if r.classification == "OK"]

    alerts_emitted = 0
    for r in critical:
        day = r.slot.day.isoformat()
        message = (
            f"ne-weekly-freshness-pipeline SILENCE on {day} ({r.slot.role} slot): "
            f"{r.detail}. The declared cadence "
            f"(SSM {CADENCE_SSM_PARAM} = '{cadence}') EXPECTS this run — this is not "
            f"a gated-off day and not a failed run, it is a slot that produced no "
            f"execution at all, which no failure-triggered alert can see. Measured "
            f"precedent: postclose never fired on 2026-08-05/06 and the chained "
            f"exercise launch was silent for two days with zero signal "
            f"(alpha-engine-config#6738 / #6689). Investigate the LAUNCHER, not the "
            f"pipeline: for an exercise slot check that day's "
            f"ne-postclose-reconcile-pipeline execution reached LaunchWeeklyExerciseRun; "
            f"for a weekly slot check the alpha-engine-saturday EventBridge rule. "
            f"Reproduce locally: `./scripts/weekly_sf_silence_deadman.py --live "
            f"--window-days {SILENCE_WINDOW_DAYS} --json --no-notify`."
        )
        _publish_watchdog_alert(
            message,
            severity="error",
            dedup_key=f"pipeline-watchdog-weekly-silence-{r.slot.role}-{day}",
            context={
                "sf_arn": SATURDAY_SF_ARN,
                "slot_day": day,
                "slot_role": r.slot.role,
                "declared_cadence": cadence,
            },
            dedup_window_min=SILENCE_DEDUP_WINDOW_MIN,
        )
        alerts_emitted += 1

    logger.info(
        "weekly-silence deadman: cadence=%s through=%s ok=%d gated_off=%d critical=%d",
        cadence, evaluation_date.isoformat(), len(ok), len(gated), len(critical),
    )
    return SilenceCheckResult(
        checked=True,
        cadence=cadence,
        evaluation_date=evaluation_date.isoformat(),
        slots_evaluated=len(results),
        ok=len(ok),
        gated_off=len(gated),
        critical=len(critical),
        # Both lists are reported, not just the failing one: "gated off by
        # declaration" and "expected and absent" are the two states §2.6
        # requires an operator to be able to tell apart, and a summary that
        # only ever names failures cannot show that the quiet days were quiet
        # ON PURPOSE.
        critical_slots=tuple(
            f"{r.slot.day.isoformat()}:{r.slot.role}" for r in critical
        ),
        gated_off_slots=tuple(
            f"{r.slot.day.isoformat()}:{r.slot.role}" for r in gated
        ),
        alerts_emitted=alerts_emitted,
    )


def handler(event: dict, context) -> dict:  # noqa: ARG001 — Lambda contract
    """EventBridge cron handler. Runs the 3 per-SF liveness checks, the
    preopen schedule-buffer canary, and the 2 prior-day failed-run checks,
    and returns a structured summary the Lambda console / CW logs can read
    at a glance."""
    now_utc = datetime.now(timezone.utc)
    is_trading_today = _is_trading_day_now(now_utc)
    is_sunday = now_utc.weekday() == 6  # Mon=0..Sun=6

    weekday = _check_sf(
        sf_label="Weekday SF",
        sf_arn=WEEKDAY_SF_ARN,
        is_watch_day=is_trading_today,
        skip_reason_if_not_watching=(
            "today is not a NYSE trading day (weekend / holiday) per "
            "nousergon_lib.trading_calendar"
        ),
        window_seconds=WINDOW_SECONDS_DAILY,
    )
    eod = _check_sf(
        sf_label="EOD SF",
        sf_arn=EOD_SF_ARN,
        is_watch_day=is_trading_today,
        skip_reason_if_not_watching=(
            "today is not a NYSE trading day (weekend / holiday) per "
            "nousergon_lib.trading_calendar"
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
    # alpha-engine-config-I11269 follow-up (2026-09-30): the reconcile half has
    # its own trigger (the collection's terminal event), so it can go silent
    # on its own. Same trading-day-aware window as the post-close SF: it fires
    # later the same evening, so the previous trading day's run is the most
    # recent one expected at 14:00 UTC.
    eod_reconcile = _check_sf(
        sf_label="EOD reconcile SF",
        sf_arn=RECONCILE_SF_ARN,
        is_watch_day=is_trading_today,
        skip_reason_if_not_watching=(
            "today is not a NYSE trading day (weekend / holiday) per "
            "nousergon_lib.trading_calendar"
        ),
        window_seconds=_eod_window_seconds(now_utc),
    )
    saturday = _check_sf(
        # alpha-engine-config-I8045: the weekly SF is the one state machine
        # with a run-day gate, so it is the one that needs GATE_SKIP bucketing.
        gate_noop_aware=True,
        sf_label="Saturday SF",
        sf_arn=SATURDAY_SF_ARN,
        is_watch_day=is_sunday,
        skip_reason_if_not_watching=(
            f"today (weekday={now_utc.weekday()}) is not Sunday; Saturday SF "
            "watch-day is Sunday so missed firings are 24+h overdue"
        ),
        window_seconds=WINDOW_SECONDS_WEEKLY,
        role_filter=WEEKLY_CADENCE_ROLES,
    )
    preopen_buffer = _check_preopen_buffer(
        now_utc=now_utc,
        is_watch_day=is_trading_today,
    )

    # Prior-day failed-run checks (config#6732, extended to the weekly SF by
    # alpha-engine-config-I7036). The trading_day-cadence target is the most
    # recent CLOSED trading day, which exists on every calendar day — so
    # these run on weekends and holidays too (a Friday failure pages
    # Saturday 14:00 UTC, not Monday). The weekly-cadence target is instead
    # the most recent DUE weekly cycle day (see ``_last_due_weekly_day`` —
    # not a trading day, and not always Saturday).
    failed_day_trading_target = last_closed_trading_day(now_utc)
    failed_day_weekly_target = _last_due_weekly_day(now_utc)
    failed_day: list = []
    for label, arn, pipeline, cadence in _FAILED_DAY_PIPELINES:
        if cadence == "weekly":
            if failed_day_weekly_target is None:  # pragma: no cover — defensive
                failed_day.append(FailedDayCheckResult(
                    sf_label=label,
                    checked=False,
                    skip_reason=(
                        "no weekly run-slot found in the 14-day lookback — "
                        "cannot derive a target cycle day"
                    ),
                ))
                continue
            target_date = failed_day_weekly_target
        else:
            target_date = failed_day_trading_target
        failed_day.append(_check_failed_day(
            sf_label=label,
            sf_arn=arn,
            pipeline_name=pipeline,
            target_date=target_date,
            cadence=cadence,
        ))

    # Weekly-SF silence deadman (config#6738). Runs on EVERY calendar day —
    # its own slot derivation is the trading-calendar gate, so there is no
    # watch-day to skip on. Ordered last so that if it ever raises despite the
    # degraded path below, the five checks above have already published.
    weekly_silence = _check_weekly_silence(now_utc=now_utc)

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
                "outcome": c.outcome,
            }
            for c in (weekday, eod, eod_reconcile, saturday)
        ],
        "preopen_buffer_check": {
            "checked": preopen_buffer.checked,
            "skip_reason": preopen_buffer.skip_reason,
            "target_trading_day": preopen_buffer.target_trading_day,
            "finish_pt": preopen_buffer.finish_pt,
            "minutes_before_open": preopen_buffer.minutes_before_open,
            "alert_emitted": preopen_buffer.alert_emitted,
            "alert_severity": preopen_buffer.alert_severity,
            "alert_detail": preopen_buffer.alert_detail,
            "trend_median_minutes_before_open": preopen_buffer.trend_median_minutes_before_open,
            "trend_days_used": preopen_buffer.trend_days_used,
            "trend_alert_emitted": preopen_buffer.trend_alert_emitted,
        },
        "failed_day_checks": [
            {
                "sf_label": c.sf_label,
                "checked": c.checked,
                "target_trading_day": c.target_trading_day,
                "skip_reason": c.skip_reason,
                "executions_on_day": c.executions_on_day,
                "succeeded_on_day": c.succeeded_on_day,
                "marker_status": c.marker_status,
                "alert_emitted": c.alert_emitted,
                "alert_detail": c.alert_detail,
            }
            for c in failed_day
        ],
        "weekly_silence_check": {
            "checked": weekly_silence.checked,
            "cadence": weekly_silence.cadence,
            "evaluation_date": weekly_silence.evaluation_date,
            "slots_evaluated": weekly_silence.slots_evaluated,
            "ok": weekly_silence.ok,
            "gated_off": weekly_silence.gated_off,
            "critical": weekly_silence.critical,
            "critical_slots": list(weekly_silence.critical_slots),
            "gated_off_slots": list(weekly_silence.gated_off_slots),
            "alerts_emitted": weekly_silence.alerts_emitted,
            "degraded_reason": weekly_silence.degraded_reason,
        },
    }
    logger.info("watchdog summary: %s", summary)
    return summary
